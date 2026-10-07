#!/usr/bin/env bash
# Start or resume batch 5a in the background, under caffeinate.
# Progress: tail -f results/batch5a/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch5a
PID_FILE=results/batch5a/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 5a is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

nohup caffeinate -ims .venv/bin/python scripts/batch5a.py >> results/batch5a/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 5a started (pid $!). Progress: tail -f results/batch5a/batch.log"
