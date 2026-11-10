#!/usr/bin/env python3
"""Publisher for the public Binance futures PAPER dashboard (GitHub Pages).

Reads the paper bot SQLite database in read-only mode and, every PUSH_EVERY_S
seconds, commits data.json when the payload has materially changed.
Never writes to the bot database, never publishes .env or secrets, and never
starts, stops, or otherwise changes the trading process.

Usage: python3 push.py [--once] [--no-push]
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

BOT = Path("/workspace/binance-paper-bot")
DB = BOT / "data" / "paper_engine.sqlite3"
HERE = Path(os.path.dirname(os.path.abspath(__file__)))
STATE = HERE / "state"
HIST_FILE = STATE / "history.json"
HASH_FILE = STATE / "last_payload_hash.txt"
LOG_HINT = BOT / "logs" / "publisher.log"
WIB = timezone(timedelta(hours=7))
PUSH_EVERY_S = 120
DATA_MSG = "data snapshot"
PUSHER_NAME = "binance-futures-pusher"
PUSHER_EMAIL = "pusher@users.noreply.github.com"

# Defaults match main.py / .env.example when those env vars are unset.
DEFAULTS = {
    "STARTING_BALANCE_USDT": "1000",
    "MAX_POSITIONS": "5",
    "LEVERAGE": "10",
    "TAKER_FEE_RATE": "0.0005",
    "TAKE_PROFIT_USDT": "10",
    "STOP_LOSS_USDT": "5",
    "EXCLUDE_TOP_VOLUME": "50",
    "MAX_UNIVERSE_SYMBOLS": "200",
    "STRATEGY_INTERVAL": "15m",
}


def log(*a) -> None:
    print(datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB"), *a, flush=True)


def wib_from_ms(ms) -> str | None:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, WIB).isoformat(timespec="seconds")
    except (OSError, OverflowError, ValueError):
        return None


def connect() -> sqlite3.Connection:
    uri = f"file:{DB}?mode=ro"
    con = sqlite3.connect(uri, uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def find_runtime() -> tuple[int | None, bool]:
    """Return (pid, alive) for the bot's main.py process. Read-only; no signals except existence."""
    try:
        for pid_s in os.listdir("/proc"):
            if not pid_s.isdigit():
                continue
            pid = int(pid_s)
            try:
                cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="ignore")
            except OSError:
                continue
            if "main.py" in cmd and "binance-paper-bot" in (cmd + read_cwd(pid)):
                return pid, True
            # started from the bot directory as `.venv/bin/python -u main.py`
            if "main.py" in cmd and "binance" not in cmd.lower():
                if read_cwd(pid).rstrip("/").endswith("/binance-paper-bot"):
                    return pid, True
        return None, False
    except OSError:
        return None, False


def read_cwd(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/cwd")
    except OSError:
        return ""


def proc_env(pid: int | None) -> dict[str, str]:
    out = dict(DEFAULTS)
    if not pid:
        return out
    try:
        raw = open(f"/proc/{pid}/environ", "rb").read().split(b"\0")
    except OSError:
        return out
    for item in raw:
        if not item or b"=" not in item:
            continue
        k, _, v = item.partition(b"=")
        try:
            key = k.decode()
        except UnicodeDecodeError:
            continue
        if key in DEFAULTS:
            out[key] = v.decode(errors="replace")
    return out


def load_hist() -> dict[float, list]:
    try:
        return {float(k): v for k, v in json.load(open(HIST_FILE)).items()}
    except Exception:
        return {}


def save_hist(h: dict) -> dict:
    now = time.time()
    out = {}
    for t in sorted(h):
        if t < now - 30 * 86400:
            continue
        key = t if t >= now - 6 * 3600 else (t // 300) * 300
        out[key] = h[t]
    STATE.mkdir(parents=True, exist_ok=True)
    tmp = str(HIST_FILE) + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({str(k): v for k, v in out.items()}, fh)
    os.replace(tmp, HIST_FILE)
    return out


def downsample(h: dict, since: float) -> list:
    now = time.time()
    bk = {}
    for t in sorted(h):
        if t < since:
            continue
        step = 60 if t >= now - 3 * 3600 else 900
        bk[(t // step) * step] = (t, h[t])
    rows = []
    for t, v in (bk[k] for k in sorted(bk)):
        eq, bal = v[0], v[1] if len(v) > 1 else None
        rows.append([int(t), round(eq, 4), None if bal is None else round(bal, 4)])
    return rows


def rget(con, sql, args=()) -> list[sqlite3.Row]:
    return list(con.execute(sql, args))


def one(con, sql, args=()) -> sqlite3.Row | None:
    return con.execute(sql, args).fetchone()


def build(h: dict) -> dict:
    pid, alive = find_runtime()
    env = proc_env(pid)
    starting = float(env["STARTING_BALANCE_USDT"])
    leverage = int(env["LEVERAGE"])
    max_pos = int(env["MAX_POSITIONS"])
    fee_rate = float(env["TAKER_FEE_RATE"])
    interval = env["STRATEGY_INTERVAL"]
    exclude_top = int(env["EXCLUDE_TOP_VOLUME"])
    universe_size = int(env["MAX_UNIVERSE_SYMBOLS"])

    con = connect()
    try:
        acct = one(con, "SELECT * FROM paper_account WHERE id=1")
        ctrl = one(con, "SELECT * FROM bot_control WHERE id=1")
        risk = one(con, "SELECT take_profit_usdt, stop_loss_usdt FROM risk_settings WHERE id=1")
        positions = rget(con, "SELECT * FROM paper_positions ORDER BY opened_at DESC")
        marks = {row["symbol"]: (row["mark_price"], row["updated_at"])
                 for row in rget(con, "SELECT symbol, mark_price, updated_at FROM market_prices")}
        trades = rget(con, "SELECT * FROM paper_trades ORDER BY id DESC LIMIT 150")
        signals = rget(con, "SELECT * FROM strategy_signals ORDER BY id DESC LIMIT 40")
        events = rget(con, "SELECT * FROM runtime_events ORDER BY id DESC LIMIT 40")
        closed_all = rget(con, "SELECT * FROM paper_trades WHERE action='CLOSE' ORDER BY id ASC")
        feed = one(con, "SELECT COUNT(*) AS n, MAX(updated_at) AS latest FROM market_prices")
    finally:
        con.close()

    now = time.time()
    now_ms = int(now * 1000)
    wallet = float(acct["wallet"]) if acct else starting
    realized = float(acct["realized_pnl"]) if acct else 0.0
    fees_paid = float(acct["fees_paid"]) if acct else 0.0
    funding_net = float(acct["funding_net"]) if acct else 0.0
    tp = float(risk["take_profit_usdt"]) if risk else float(env["TAKE_PROFIT_USDT"])
    sl = float(risk["stop_loss_usdt"]) if risk else float(env["STOP_LOSS_USDT"])

    # Pair each CLOSE with the most recent earlier OPEN of the same symbol.
    opens_by_sym: dict[str, list] = {}
    chronological = list(reversed(trades))
    entry_for: dict[int, float] = {}
    pending: dict[str, float] = {}
    for t in chronological:
        sym = t["symbol"]
        if t["action"] == "OPEN":
            pending[sym] = float(t["price"])
        elif t["action"] == "CLOSE":
            entry_for[t["id"]] = pending.pop(sym, None)

    opn = []
    locked = 0.0
    unrealized = 0.0
    unrealized_net = 0.0
    for p in positions:
        symbol = p["symbol"]
        side = p["side"]
        qty = float(p["quantity"])
        entry = float(p["entry_price"])
        margin = float(p["margin"])
        entry_fee = float(p["entry_fee"])
        pos_fund = float(p["funding_net"])
        mark_t = marks.get(symbol)
        mark = float(mark_t[0]) if mark_t and mark_t[0] else entry
        direction = 1.0 if side == "LONG" else -1.0
        gross = (mark - entry) * qty * direction
        exit_fee = qty * mark * fee_rate
        net = gross - entry_fee - exit_fee + pos_fund
        locked += margin
        unrealized += gross
        unrealized_net += net
        held_min = (now_ms - int(p["opened_at"])) / 60000 if p["opened_at"] else None
        pnl_pct = (net / margin * 100) if margin else None
        opn.append({
            "symbol": symbol,
            "side": (side or "").lower(),
            "quantity": qty,
            "entry": entry,
            "mark": mark,
            "margin_usdt": round(margin, 4),
            "entry_fee": round(entry_fee, 6),
            "funding_usdt": round(pos_fund, 6),
            "pnl_usdt": round(net, 4),
            "pnl_gross_usdt": round(gross, 4),
            "pnl_pct": None if pnl_pct is None else round(pnl_pct, 2),
            "leverage": leverage,
            "notional_usdt": round(qty * mark, 4),
            "held_min": None if held_min is None else round(held_min, 2),
            "opened_wib": wib_from_ms(p["opened_at"]),
        })

    equity = wallet + locked + unrealized
    today = datetime.now(WIB).date()
    today_start = datetime.combine(today, datetime.min.time(), tzinfo=WIB)
    today_ms = int(today_start.timestamp() * 1000)
    today_realized = round(sum(float(t["net_pnl"] or 0) for t in closed_all if int(t["created_at"] or 0) >= today_ms), 4)
    today_pnl = round(today_realized + unrealized_net, 4)

    closed_rows = []
    trade_rows = []
    for t in trades:
        row = {
            "id": t["id"],
            "symbol": t["symbol"],
            "side": (t["side"] or "").lower(),
            "action": t["action"],
            "quantity": t["quantity"],
            "price": t["price"],
            "fee": t["fee"],
            "pnl_usdt": t["net_pnl"],
            "reason": t["reason"],
            "created_wib": wib_from_ms(t["created_at"]),
        }
        if t["action"] == "CLOSE":
            row["exit"] = t["price"]
            row["entry"] = entry_for.get(t["id"])
            row["closed_wib"] = row["created_wib"]
            closed_rows.append(row)
        trade_rows.append(row)

    wins = sum(1 for t in closed_all if (t["net_pnl"] or 0) > 0)
    n_closed = len(closed_all)

    hb_ms = feed["latest"] if feed else None
    universe_live = int(feed["n"] or 0) if feed else 0
    heartbeat_wib = wib_from_ms(hb_ms)
    heartbeat_age_s = None if hb_ms is None else round(now - (int(hb_ms) / 1000), 1)
    state = ctrl["state"] if ctrl else "IDLE"
    state_wib = wib_from_ms(ctrl["updated_at"]) if ctrl else None

    h[now] = [equity, wallet]
    # Start of history: first sample or account start (we do not have ledger created time).
    hist_since = min(h) if h else now

    return {
        "generated_at": datetime.now(WIB).isoformat(timespec="seconds"),
        "generated_ts": int(now),
        "paper_only": True,
        "exchange": "binance",
        "mode": "PAPER",
        "push_every_s": PUSH_EVERY_S,
        "daemon": {
            "alive": alive,
            "pid": pid,
            "state": state,
            "state_updated_wib": state_wib,
            "heartbeat_wib": heartbeat_wib,
            "heartbeat_age_s": heartbeat_age_s,
            "universe_live": universe_live,
        },
        "snapshot": {
            "equity_usdt": round(equity, 4),
            "wallet_usdt": round(wallet, 4),
            "realized_usdt": round(realized, 4),
            "unrealized_usdt": round(unrealized, 4),
            "unrealized_net_usdt": round(unrealized_net, 4),
            "fees_usdt": round(fees_paid, 4),
            "funding_net_usdt": round(funding_net, 6),
            "today_wib": today.isoformat(),
            "today_pnl_usdt": today_pnl,
            "today_realized_usdt": today_realized,
            "open_count": len(opn),
            "max_open": max_pos,
            "starting_balance_usdt": starting,
            "margin_used_usdt": round(locked, 4),
            "strategy": f"Heikin-Ashi colour flip {interval}",
            "leverage": leverage,
            "taker_fee_rate": fee_rate,
        },
        "settings": {
            "leverage": leverage,
            "take_profit_usdt": tp,
            "stop_loss_usdt": sl,
            "max_positions": max_pos,
            "universe_size": universe_size,
            "universe_live": universe_live,
            "exclude_top_volume": exclude_top,
            "interval": interval,
            "starting_balance_usdt": starting,
            "taker_fee_rate": fee_rate,
        },
        "stats": {
            "closed_trades": n_closed,
            "wins": wins,
            "win_rate": round(100 * wins / n_closed, 1) if n_closed else None,
            "closed_pnl_usdt": round(sum(float(t["net_pnl"] or 0) for t in closed_all), 4),
            "fees_usdt": round(fees_paid, 4),
            "funding_net_usdt": round(funding_net, 6),
        },
        "open": opn,
        "closed": closed_rows,
        "trades": trade_rows,
        "signals": [{
            "symbol": s["symbol"],
            "action": s["action"],
            "side": (s["side"] or "").lower() if s["side"] else None,
            "reason": s["reason"],
            "candle_wib": wib_from_ms(s["candle_time"]),
            "created_wib": wib_from_ms(s["created_at"]),
        } for s in signals],
        "activity": [{
            "level": e["level"],
            "message": e["message"],
            "created_wib": wib_from_ms(e["created_at"]),
        } for e in events],
        "history": downsample(h, hist_since),
    }


def payload_hash(d: dict) -> str:
    """Stable hash of published facts, ignoring generation timestamps and chart history."""
    skip = {"generated_at", "generated_ts", "history"}
    body = {k: v for k, v in d.items() if k not in skip}
    # Heartbeat age changes every cycle even if the feed timestamp does not; use the timestamp only.
    daemon = dict(body.get("daemon") or {})
    daemon.pop("heartbeat_age_s", None)
    body["daemon"] = daemon
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


def git(*a, check=True) -> str:
    r = subprocess.run(["git", "-C", str(HERE), *a], capture_output=True, text=True, timeout=120)
    if check and r.returncode:
        raise RuntimeError(f"git {' '.join(a)}: {r.stderr.strip()[:400]}")
    return r.stdout.strip()


def is_data_commit(rev: str) -> bool:
    return git("log", "-1", "--format=%s%x1f%an", rev) == f"{DATA_MSG}\x1f{PUSHER_NAME}"


def commit(msg: str, path: str) -> None:
    git("add", path)
    git("-c", f"user.name={PUSHER_NAME}", "-c", f"user.email={PUSHER_EMAIL}",
        "commit", "-q", "--allow-empty", "-m", msg, "--", path)


def push_data(R: str) -> None:
    """Replace our previous data snapshot when it is still the tip; never drop a user commit."""
    base = git("rev-parse", f"{R}~1") if is_data_commit(R) else R
    git("reset", "-q", "--mixed", base)
    commit(DATA_MSG, "data.json")
    git("push", "-q", f"--force-with-lease=refs/heads/main:{R}", "origin", "HEAD:main")


def write_data(h: dict) -> dict:
    d = build(h)
    tmp = HERE / "data.json.tmp"
    with open(tmp, "w") as fh:
        json.dump(d, fh, separators=(",", ":"))
    os.replace(tmp, HERE / "data.json")
    return d


def last_hash() -> str | None:
    try:
        return HASH_FILE.read_text().strip() or None
    except OSError:
        return None


def save_hash(value: str) -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    tmp = str(HASH_FILE) + ".tmp"
    with open(tmp, "w") as fh:
        fh.write(value + "\n")
    os.replace(tmp, HASH_FILE)


def main() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    once = "--once" in sys.argv
    nopush = "--no-push" in sys.argv
    h = load_hist()
    last_push = 0.0
    fails = 0
    while True:
        try:
            if time.time() - last_push >= PUSH_EVERY_S or once or last_push == 0:
                if not nopush:
                    git("fetch", "-q", "origin", "main")
                    R = git("rev-parse", "refs/remotes/origin/main")
                else:
                    R = None
                h = save_hist(h)
                d = write_data(h)
                h = save_hist(h)
                digest = payload_hash(d)
                changed = digest != last_hash()
                if not changed:
                    last_push = time.time()
                    fails = 0
                    log(f"unchanged eq={d['snapshot']['equity_usdt']} open={d['snapshot']['open_count']} "
                        f"state={d['daemon']['state']} alive={d['daemon']['alive']}")
                else:
                    if not nopush:
                        push_data(R)
                    save_hash(digest)
                    last_push = time.time()
                    fails = 0
                    log(f"pushed eq={d['snapshot']['equity_usdt']} open={d['snapshot']['open_count']} "
                        f"state={d['daemon']['state']} alive={d['daemon']['alive']} hist={len(d['history'])}")
        except Exception as e:
            fails += 1
            last_push = time.time() - PUSH_EVERY_S + 30 * min(fails, 4)
            log("ERROR", repr(e))
            traceback.print_exc()
        if once:
            break
        time.sleep(15)


if __name__ == "__main__":
    main()
