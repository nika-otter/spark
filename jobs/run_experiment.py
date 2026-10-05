"""Run one (scenario, mode) as a single Spark application.

1 warm-up run followed by N measured runs; every run writes to the noop
format, nothing is cached. Each run is recorded via metrics.save().

Between runs (outside the timed section) the driver JVM is asked to GC so
that Spark's ContextCleaner deletes the previous run's shuffle files.
Without this, shuffle files of all earlier runs stay in the executors' local
dirs until the application ends and run times drift upward with run index.

Usage (inside the master container):
  spark-submit run_experiment.py --experiment pilot_null --mode aqe_on \
      --param join=left --param null_share=0.12 [--runs 5] [--warmup 1] \
      [--csv /opt/project/results/runs.csv]

  spark-submit run_experiment.py --generate-only ...   # create datasets only
"""

import argparse
import gc
import os
import time

import metrics
from experiments import MODES, build_scenario
from generate import data_path, ensure_dataset, load_manifest, manifest_path
from spark_conf import RESULTS_DIR, build_session, effective_conf, environment_info


def release_shuffle_files(sc, settle_seconds=5):
    gc.collect()
    sc._jvm.System.gc()
    time.sleep(settle_seconds)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment", required=True)
    parser.add_argument("--mode", choices=sorted(MODES))
    parser.add_argument("--param", action="append", default=[])
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--csv", default=RESULTS_DIR + "/runs.csv")
    parser.add_argument("--generate-only", action="store_true")
    parser.add_argument("--job-id", default=None, help="batch job id recorded with each run")
    args = parser.parse_args()

    scenario = build_scenario(args.experiment, args.param)

    if args.generate_only:
        spark, _ = build_session("generate-" + scenario.scenario_id)
        for spec in scenario.tables.values():
            ensure_dataset(spark, spec)
        spark.stop()
        return

    if not args.mode:
        parser.error("--mode is required unless --generate-only")
    for spec in scenario.tables.values():
        if not os.path.exists(manifest_path(spec)):
            raise SystemExit("dataset missing, run --generate-only first: " + spec.dataset_id())

    overrides = dict(scenario.conf)
    overrides.update(MODES[args.mode])
    app_name = "{}__{}__{}".format(scenario.experiment, scenario.scenario_id, args.mode)
    spark, _ = build_session(app_name, overrides)
    sc = spark.sparkContext

    conf = effective_conf(spark)
    env = environment_info(spark)
    manifests = [load_manifest(spec) for spec in scenario.tables.values()]
    tables = {name: spark.read.parquet(data_path(spec)) for name, spec in scenario.tables.items()}
    out_dir = "{}/runs/{}/{}/{}".format(RESULTS_DIR, scenario.experiment, scenario.scenario_id, args.mode)

    for i in range(args.warmup + args.runs):
        is_warmup = i < args.warmup
        label = "{}|{}|run-{}".format(scenario.scenario_id, args.mode, i)
        df = scenario.query(tables)

        sc.setJobDescription(label)
        start = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        elapsed = time.perf_counter() - start
        sc.setJobDescription(None)
        del df

        record = {
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "job_id": args.job_id,
            "experiment": scenario.experiment,
            "scenario_id": scenario.scenario_id,
            "mode": args.mode,
            "run_index": i,
            "is_warmup": is_warmup,
            "run_label": label,
            "params": scenario.params,
            "datasets": manifests,
            "elapsed_seconds": round(elapsed, 3),
            "environment": env,
            "conf": conf,
            "metrics": metrics.collect(spark, label),
        }
        json_path = "{}/{}_run-{}.json".format(out_dir, env["application_id"], i)
        metrics.save(record, json_path, args.csv)
        release_shuffle_files(sc)

        m = record["metrics"]
        print("{} {}: {:.2f} s, joins={}, skew={} ({} splits), partitions {} -> {}".format(
            label, "warm-up" if is_warmup else "measured", elapsed,
            {k: v for k, v in m["join_strategies"].items() if v},
            m["aqe"]["skew_join_applied"], m["aqe"]["skew_splits"],
            m["aqe"]["partitions_before"], m["aqe"]["partitions_after"]), flush=True)

    spark.stop()


if __name__ == "__main__":
    main()
