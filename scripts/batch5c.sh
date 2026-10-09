#!/usr/bin/env bash
# Start or resume batch 5c in the background, under caffeinate.
# Usage: scripts/batch5c.sh prepare|rounds   Progress: tail -f results/batch5c/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch5c
PID_FILE=results/batch5c/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 5c is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch5c.py "$1" >> results/batch5c/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 5c ($1) started (pid $!). Progress: tail -f results/batch5c/batch.log"
