#!/usr/bin/env bash
# Download the three real datasets of batch 5 step 1c into data/raw/ (host).
# Sources (official): NYC TLC trip records (yellow, 2018), ClickBench hits
# (single Parquet file), Wikimedia hourly pageviews (2024-01-01, 24 hours).
# Each finished file is listed with its URL and UTC download time in
# data/raw/downloads.tsv. Re-running skips files that are complete.
set -euo pipefail
cd "$(dirname "$0")/../data/raw"
get() {  # url target
  if [ -f "$2" ] && [ "$(stat -f%z "$2")" = "$(curl -sIL "$1" | awk 'tolower($1)=="content-length:"{v=$2} END{print v+0}')" ]; then
    return
  fi
  curl -sfL --http1.1 --retry 5 --retry-delay 10 -C - -o "$2" "$1"
  printf "%s\t%s\t%s\t%s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1" "$2" "$(stat -f%z "$2")" >> downloads.tsv
}
for m in 01 02 03 04 05 06 07 08 09 10 11 12; do
  get "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-$m.parquet" "taxi/yellow_tripdata_2018-$m.parquet"
done
for h in $(seq -w 0 23); do
  get "https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240101-${h}0000.gz" "pageviews/pageviews-20240101-${h}0000.gz"
done
get "https://datasets.clickhouse.com/hits_compatible/hits.parquet" "hits/hits.parquet"
echo done
