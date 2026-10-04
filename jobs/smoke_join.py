"""Smoke test, part 2: join fact with a selectively filtered dim, write to noop.

Usage: spark-submit smoke_join.py --aqe true|false
Writes results/smoke_aqe_<on|off>_<timestamp>.json and ..._plan.txt.
"""

import argparse
import time

from pyspark.sql import functions as F

from spark_conf import (
    DATA_DIR,
    build_session,
    effective_conf,
    environment_info,
    executor_task_summary,
    last_sql_execution,
    write_run_record,
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aqe", choices=["true", "false"], required=True)
    args = parser.parse_args()

    run_name = "smoke_aqe_{}".format("on" if args.aqe == "true" else "off")
    spark, _ = build_session(run_name, {"spark.sql.adaptive.enabled": args.aqe})

    fact = spark.read.parquet(DATA_DIR + "/smoke/fact")
    dim = spark.read.parquet(DATA_DIR + "/smoke/dim")

    # category == 7 keeps 1,000 of 1,000,000 dim rows (0.1%).
    result = fact.join(dim.where(F.col("category") == 7), "dim_id")

    start = time.perf_counter()
    result.write.format("noop").mode("overwrite").save()
    elapsed = time.perf_counter() - start

    execution = last_sql_execution(spark)
    record = {
        "run": run_name,
        "elapsed_seconds": round(elapsed, 3),
        "sql_execution_duration_ms": execution["duration"],
        "final_physical_plan": execution["planDescription"],
        "tasks_per_executor": executor_task_summary(spark),
        "environment": environment_info(spark),
        "conf": effective_conf(spark),
    }
    base = write_run_record(run_name, record)

    print("{}: {:.3f} s -> {}.json".format(run_name, elapsed, base))
    print("tasks per executor:", record["tasks_per_executor"])
    print(record["final_physical_plan"])

    spark.stop()


if __name__ == "__main__":
    main()
