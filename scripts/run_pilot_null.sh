#!/usr/bin/env bash
# Pilot experiment: NULL share in the join key (LEFT JOIN grid + INNER control).
# Usage: scripts/run_pilot_null.sh [generate|run|all]   (default: all)
set -euo pipefail
cd "$(dirname "$0")/.."

# Keep the Mac awake for the whole grid: host sleep pauses the Docker VM and
# invalidates timings (seen in the first attempt, results/pilot_null_attempt1_host_sleep).
if [ -z "${CAFFEINATED:-}" ] && command -v caffeinate > /dev/null; then
  CAFFEINATED=1 exec caffeinate -ims "$0" "$@"
fi

ROWS=55000000
PAYLOAD=100
LEFT_GRID=(0 0.01 0.02 0.05 0.08 0.12 0.2 0.3 0.5)
INNER_GRID=(0 0.12 0.5)
MODES=(aqe_off aqe_on)

submit() {
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    /opt/project/jobs/run_experiment.py --experiment pilot_null \
    --param rows=$ROWS --param payload_bytes=$PAYLOAD "$@"
}

generate() {
  for ns in "${LEFT_GRID[@]}"; do
    submit --generate-only --param null_share="$ns" 2>&1 | grep -E "Error|Exception" || true
    echo "$(date +%T) generated null_share=$ns"
  done
}

run() {
  for join in left inner; do
    if [ "$join" = left ]; then grid=("${LEFT_GRID[@]}"); else grid=("${INNER_GRID[@]}"); fi
    for ns in "${grid[@]}"; do
      for mode in "${MODES[@]}"; do
        echo "$(date +%T) start join=$join null_share=$ns mode=$mode"
        submit --mode "$mode" --param join="$join" --param null_share="$ns" 2>&1 \
          | grep -E '\|run-[0-9]+ |Error|Exception' || true
      done
    done
  done
}

case "${1:-all}" in
  generate) generate ;;
  run) run ;;
  all) generate; run ;;
  *) echo "usage: $0 [generate|run|all]" >&2; exit 1 ;;
esac
echo "$(date +%T) done"
