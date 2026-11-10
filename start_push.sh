#!/usr/bin/env bash
# Idempotent: starts push.py as a detached background loop unless it is already running.
cd "$(dirname "$0")" || exit 1
PIDF=push.pid
LOG=/workspace/binance-paper-bot/logs/publisher.log
mkdir -p "$(dirname "$LOG")"
if [ -f "$PIDF" ]; then
  P=$(cat "$PIDF" 2>/dev/null)
  if [ -n "$P" ] && kill -0 "$P" 2>/dev/null && tr '\0' ' ' < /proc/$P/cmdline | grep -q "push.py"; then
    echo "push.py already running (pid $P)"; exit 0
  fi
fi
nohup setsid python3 -u push.py >> "$LOG" 2>&1 < /dev/null &
echo $! > "$PIDF"
sleep 1
echo "started push.py (pid $(cat $PIDF)), log: $LOG"
