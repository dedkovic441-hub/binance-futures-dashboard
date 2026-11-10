#!/usr/bin/env bash
cd "$(dirname "$0")" || exit 1
P=$(cat push.pid 2>/dev/null); [ -n "$P" ] && kill "$P" 2>/dev/null && echo "stopped $P" || echo "not running"
