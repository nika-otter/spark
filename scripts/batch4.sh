#!/usr/bin/env bash
# Start or resume batch 4 in the background, under caffeinate.
# Progress: tail -f results/batch4/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch4
PID_FILE=results/batch4/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 4 is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch4.py >> results/batch4/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 4 started (pid $!). Progress: tail -f results/batch4/batch.log"
