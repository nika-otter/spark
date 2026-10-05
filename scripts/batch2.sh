#!/usr/bin/env bash
# Start or resume batch 2 in the background, under caffeinate.
# Progress: tail -f results/batch2/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch2
PID_FILE=results/batch2/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 2 is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

docker compose up -d --wait > /dev/null 2>&1
nohup caffeinate -ims .venv/bin/python scripts/batch2.py >> results/batch2/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 2 started (pid $!). Progress: tail -f results/batch2/batch.log"
