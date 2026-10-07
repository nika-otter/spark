"""Run one TPC-H / TPC-DS query as one Spark application (batch 5, step 1).

Tables are read from /opt/spark-data/<bench>_sf<SF>/<table>/ (Parquet written
by scripts/gen_tpc.py) and registered as temporary views; the query text is
/opt/project/bench/<bench>/<query>.sql (copied from the Spark source tree,
sql/core/src/test/resources/<bench>/, tag v4.1.3). Each run writes the result
to the noop format. Broadcast thresholds and all AQE values are at their
defaults (BASELINE_CONF) unless a mode changes them.

--profile: one run, AQE on with defaults; records every shuffle exchange
  (data size, bytes written, ratio) via jobs/metrics.py. The codec is set
  with --codec (a core conf, so one application per codec).
--modes M1,M2,...: timing. Modes are SQL confs changed at runtime between
  runs of the same application: aqe_off, aqe_on, aqe_on_rule2 (advisory
  64MB/r, skew threshold 256MB/r, r from --rule2-ratio). One warm-up round
  (every mode once), then --runs measured rounds; within each round the
  modes run in a randomized order (seeded by query and round), so the modes
  are interleaved.

Usage (inside the master container):
  spark-submit run_bench.py --bench tpch --query q1 --profile --codec zstd --csv ...
  spark-submit run_bench.py --bench tpch --query q1 --modes aqe_off,aqe_on,aqe_on_rule2 \
      --rule2-ratio 3.1 --warmup 1 --runs 3 --csv ...
"""

import argparse
import os
import random
import time

import metrics
from run_experiment import release_shuffle_files
from spark_conf import BASELINE_CONF, DATA_DIR, RESULTS_DIR, build_session, effective_conf, \
    environment_info

MIB = 1024 * 1024
PROJECT = "/opt/project"
ADVISORY = "spark.sql.adaptive.advisoryPartitionSizeInBytes"
SKEW = "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"


def mode_conf(mode, r):
    conf = {
        "spark.sql.adaptive.enabled": "false" if mode == "aqe_off" else "true",
        ADVISORY: BASELINE_CONF[ADVISORY],
        SKEW: BASELINE_CONF[SKEW],
    }
    if mode == "aqe_on_rule2":
        if not r or r <= 0:
            raise ValueError("aqe_on_rule2 needs --rule2-ratio > 0")
        conf[ADVISORY] = str(int(64 * MIB / r))
        conf[SKEW] = str(int(256 * MIB / r))
    elif mode not in ("aqe_off", "aqe_on"):
        raise ValueError("unknown mode " + mode)
    return conf


# DuckDB's dsdgen names this column as in TPC-DS v2+; the Spark query texts
# (q30) use the v1.4 name.
RENAMES = {"customer": {"c_last_review_date_sk": "c_last_review_date"}}


def register_tables(spark, data_dir):
    tables = sorted(d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d)))
    for t in tables:
        df = spark.read.parquet(os.path.join(data_dir, t))
        for old, new in RENAMES.get(t, {}).items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
        df.createOrReplaceTempView(t)
    return tables


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bench", required=True, choices=("tpch", "tpcds"))
    parser.add_argument("--sf", type=int, default=10)
    parser.add_argument("--query", required=True)
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--codec", default="lz4")
    parser.add_argument("--modes", default="")
    parser.add_argument("--rule2-ratio", type=float, default=0.0)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--csv", required=True)
    parser.add_argument("--job-id", default=None)
    args = parser.parse_args()

    modes = ["aqe_on"] if args.profile else [m for m in args.modes.split(",") if m]
    experiment = "{}_{}".format(args.bench, "profile" if args.profile else "timing")
    app_name = "{}__{}__{}".format(experiment, args.query, args.codec if args.profile else "+".join(modes))
    spark, _ = build_session(app_name, {"spark.io.compression.codec": args.codec})
    sc = spark.sparkContext
    data_dir = "{}/{}_sf{}".format(DATA_DIR, args.bench, args.sf)
    register_tables(spark, data_dir)
    with open("{}/bench/{}/{}.sql".format(PROJECT, args.bench, args.query)) as f:
        text = f.read().strip().rstrip(";")
    base_conf = effective_conf(spark)
    env = environment_info(spark)
    out_dir = "{}/runs/{}/{}".format(RESULTS_DIR, experiment, args.query)

    if args.profile:
        schedule = [(0, False, "aqe_on")]
    else:
        schedule = []
        for rnd in range(args.warmup + args.runs):
            order = list(modes)
            random.Random("{}-{}-{}".format(args.bench, args.query, rnd)).shuffle(order)
            schedule += [(rnd, rnd < args.warmup, m) for m in order]

    for i, (rnd, is_warmup, mode) in enumerate(schedule):
        conf = mode_conf(mode, args.rule2_ratio)
        for key, value in conf.items():
            spark.conf.set(key, value)
        label = "{}|{}|{}|run-{}".format(args.bench, args.query, mode, i)
        sc.setJobDescription(label)
        start = time.perf_counter()
        spark.sql(text).write.format("noop").mode("overwrite").save()
        elapsed = time.perf_counter() - start
        sc.setJobDescription(None)
        record = {
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "job_id": args.job_id,
            "experiment": experiment,
            "scenario_id": args.query,
            "mode": mode,
            "run_index": i,
            "round": rnd,
            "is_warmup": is_warmup,
            "run_label": label,
            "params": {"bench": args.bench, "sf": args.sf, "query": args.query,
                       "codec": args.codec, "rule2_ratio": args.rule2_ratio if mode == "aqe_on_rule2" else 0.0,
                       "round": rnd},
            "datasets": [{"dataset_id": "{}_sf{}".format(args.bench, args.sf)}],
            "elapsed_seconds": round(elapsed, 3),
            "environment": env,
            "conf": dict(base_conf, **conf),
            "metrics": metrics.collect(spark, label),
        }
        json_path = "{}/{}_{}_run-{}.json".format(out_dir, env["application_id"],
                                                  "profile-" + args.codec if args.profile else "timing", i)
        metrics.save(record, json_path, args.csv)
        release_shuffle_files(sc)
        m = record["metrics"]
        print("{} {}: {:.2f} s, shuffles={}, r_overall={}".format(
            label, "warm-up" if is_warmup else "measured", elapsed,
            len(m["exchanges_detail"]), m["compression_ratio_overall"]), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
