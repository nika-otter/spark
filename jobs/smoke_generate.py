"""Smoke test, part 1: write two tables with spark.range to the shared volume.

fact: 10M rows, dim_id spread uniformly over the 1M dim keys.
dim:  1M rows with a 64-char payload so that its Parquet size is well above
      the 10 MB broadcast threshold (the static planner cannot broadcast it).
"""

from pyspark.sql import functions as F

from spark_conf import DATA_DIR, build_session

FACT_ROWS = 10_000_000
DIM_ROWS = 1_000_000


def main():
    spark, _ = build_session("smoke-generate")

    fact = spark.range(FACT_ROWS).select(
        F.col("id"),
        ((F.col("id") * 7919) % DIM_ROWS).alias("dim_id"),
        (F.rand(seed=42) * 100).alias("amount"),
    )
    dim = spark.range(DIM_ROWS).select(
        F.col("id").alias("dim_id"),
        (F.col("id") % 1000).alias("category"),
        F.sha2(F.col("id").cast("string"), 256).alias("payload"),
    )

    fact.write.mode("overwrite").parquet(DATA_DIR + "/smoke/fact")
    dim.write.mode("overwrite").parquet(DATA_DIR + "/smoke/dim")

    for name in ("fact", "dim"):
        df = spark.read.parquet("{}/smoke/{}".format(DATA_DIR, name))
        print("{}: {} rows".format(name, df.count()))

    spark.stop()


if __name__ == "__main__":
    main()
