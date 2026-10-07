"""Load the three real datasets of batch 5 step 1c into Parquet (Spark).

Raw files are copied from the host (data/raw, see scripts/download_real.sh)
to /opt/spark-data/raw/. Output per dataset, in the layout of jobs/generate.py:
  /opt/spark-data/datasets/real_<name>/data/          Parquet
  /opt/spark-data/datasets/real_<name>/manifest.json  row count, schema, key
                                                      distributions, sources
Datasets:
  taxi       NYC TLC yellow taxi trips 2018 (12 monthly Parquet files); the
             monthly files differ in some column types, every column is cast
             to TAXI_SCHEMA; all rows are kept
  hits       ClickBench hits (hits_compatible/hits.parquet), all rows, the 15
             columns of HITS_COLUMNS (fixed before any measurement, see
             results/README.md for the reason of each column)
  pageviews  Wikimedia hourly pageviews 2024-01-01 00:00-23:00 (24 files);
             line format "project page_title views response_bytes"; the hour
             is taken from the file name; lines without 4 fields are dropped
             and counted
Key statistics: for every key used by the queries of experiments.real_query,
the number of distinct values, NULL count and the share of the 5 most frequent
values.

Usage: spark-submit prepare_real.py taxi|hits|pageviews
"""

import json
import os
import sys
import time

from pyspark.sql import functions as F

from spark_conf import DATA_DIR, build_session

RAW = DATA_DIR + "/raw"
DATASETS = DATA_DIR + "/datasets"

TAXI_SCHEMA = [
    ("VendorID", "bigint"), ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"), ("passenger_count", "bigint"),
    ("trip_distance", "double"), ("RatecodeID", "bigint"), ("store_and_fwd_flag", "string"),
    ("PULocationID", "bigint"), ("DOLocationID", "bigint"), ("payment_type", "bigint"),
    ("fare_amount", "double"), ("extra", "double"), ("mta_tax", "double"),
    ("tip_amount", "double"), ("tolls_amount", "double"), ("improvement_surcharge", "double"),
    ("total_amount", "double"), ("congestion_surcharge", "double"), ("airport_fee", "double"),
]

# Fixed before any measurement (batch 5, step 1c); not chosen by compressibility.
HITS_COLUMNS = [
    "WatchID", "UserID", "CounterID", "RegionID",          # row id and join keys
    "EventTime", "EventDate",                              # time
    "ClientIP", "OS", "ResolutionWidth", "IsRefresh",      # numeric
    "TraficSourceID", "SearchEngineID", "AdvEngineID",     # numeric (small codes)
    "URL", "Title",                                        # long text
]

KEYS = {
    "taxi": [["PULocationID"], ["PULocationID", "pickup_hour"]],
    "hits": [["UserID"], ["CounterID"], ["RegionID"]],
    "pageviews": [["project"], ["project", "page_title"]],
}


def load_taxi(spark):
    files = sorted(f for f in os.listdir(RAW + "/taxi") if f.endswith(".parquet"))
    parts = []
    for f in files:
        df = spark.read.parquet("{}/taxi/{}".format(RAW, f))
        parts.append(df.select(*[F.col(c).cast(t).alias(c) for c, t in TAXI_SCHEMA]))
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out, ["{}/taxi/{}".format(RAW, f) for f in files], {}


def load_hits(spark):
    path = RAW + "/hits/hits.parquet"
    return spark.read.parquet(path).select(*HITS_COLUMNS), [path], {}


def load_pageviews(spark):
    files = sorted(f for f in os.listdir(RAW + "/pageviews") if f.endswith(".gz"))
    raw = spark.read.text(["{}/pageviews/{}".format(RAW, f) for f in files]) \
        .withColumn("file", F.input_file_name())
    parts = F.split(F.col("value"), " ")
    hour = F.to_timestamp(F.regexp_extract("file", r"pageviews-(\d{8}-\d{2})0000", 1), "yyyyMMdd-HH")
    parsed = raw.select(
        F.size(parts).alias("n"),
        parts.getItem(0).alias("project"), parts.getItem(1).alias("page_title"),
        parts.getItem(2).cast("bigint").alias("views"),
        parts.getItem(3).cast("bigint").alias("response_bytes"),
        hour.alias("hour"))
    total = raw.count()
    df = parsed.where(F.col("n") == 4).drop("n")
    return df, ["{}/pageviews/{}".format(RAW, f) for f in files], {"raw_lines": total}


LOADERS = {"taxi": load_taxi, "hits": load_hits, "pageviews": load_pageviews}


def key_stats(df, key, total):
    counts = df.groupBy(*key).count()
    top = counts.orderBy(F.desc("count")).limit(5).collect()
    nulls = df.where(" OR ".join("{} IS NULL".format(k) for k in key)).count()
    return {
        "distinct": counts.count(),
        "null_rows": nulls,
        "top5": [{"key": [str(r[k]) for k in key], "rows": r["count"], "share": r["count"] / total}
                 for r in top],
        "top1_share": top[0]["count"] / total,
        "top5_share": sum(r["count"] for r in top) / total,
    }


def main():
    name = sys.argv[1]
    spark, _ = build_session("prepare-real-" + name)
    target = "{}/real_{}".format(DATASETS, name)
    start = time.time()
    df, sources, extra = LOADERS[name](spark)
    df.write.mode("overwrite").parquet(target + "/data")
    df = spark.read.parquet(target + "/data")
    total = df.count()
    if name == "taxi":
        df = df.withColumn("pickup_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
    manifest = {
        "dataset_id": "real_" + name, "table": "facts", "path": target + "/data",
        "row_count": total, "sources": sources, "schema": df.schema.simpleString(),
        "spark_version": spark.version, "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "keys": {"+".join(k): key_stats(df, k, total) for k in KEYS[name]},
    }
    manifest.update(extra)
    if name == "pageviews":
        manifest["dropped_lines"] = extra["raw_lines"] - total
    manifest["write_and_stats_seconds"] = round(time.time() - start, 1)
    with open(target + "/manifest.json.tmp", "w") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    os.replace(target + "/manifest.json.tmp", target + "/manifest.json")
    print(json.dumps(manifest, indent=2, sort_keys=True))
    spark.stop()


if __name__ == "__main__":
    main()
