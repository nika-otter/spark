#!/usr/bin/env bash
# Start or resume batch 5b in the background, under caffeinate.
# Usage: scripts/batch5b.sh prepare|rounds   Progress: tail -f results/batch5b/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch5b
PID_FILE=results/batch5b/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 5b is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch5b.py "$1" >> results/batch5b/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 5b ($1) started (pid $!). Progress: tail -f results/batch5b/batch.log"
