#!/usr/bin/env bash
# Start or resume batch 3 in the background, under caffeinate.
# Progress: tail -f results/batch3/batch.log
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/.docker/bin:/usr/local/bin"

mkdir -p results/batch3
PID_FILE=results/batch3/batch.pid
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "batch 3 is already running (pid $(cat "$PID_FILE"))"
  exit 0
fi

docker compose up -d --wait > /dev/null 2>&1
nohup caffeinate -ims .venv/bin/python scripts/batch3.py >> results/batch3/batch.log 2>&1 &
echo $! > "$PID_FILE"
echo "batch 3 started (pid $!). Progress: tail -f results/batch3/batch.log"
