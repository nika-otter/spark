#!/usr/bin/env bash
# Start or resume batch 6 in the background, under caffeinate.
# Usage: scripts/batch6.sh prepare|rounds   Progress: tail -f results/batch6/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch6
PID_FILE=results/batch6/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 6 is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch6.py "$1" >> results/batch6/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 6 ($1) started (pid $!). Progress: tail -f results/batch6/batch.log"
