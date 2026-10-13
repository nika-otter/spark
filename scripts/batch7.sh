#!/usr/bin/env bash
# Start or resume batch 7 in the background, under caffeinate.
# Usage: scripts/batch7.sh prepare|rounds   Progress: tail -f results/batch7/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch7
PID_FILE=results/batch7/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 7 is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch7.py "$1" >> results/batch7/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 7 ($1) started (pid $!). Progress: tail -f results/batch7/batch.log"
