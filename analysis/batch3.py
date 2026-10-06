"""Summaries for batch 3 (experiment C2, rule check, experiment D).

Reads finished jobs of results/batch3 (measured runs only) and writes
  results/exp_c2_summary.csv
  results/exp_rules_summary.csv
  results/exp_d_summary.csv
  results/exp_d_plans/<scenario>__<mode>.txt   (final plan of one measured run)

Aggregation as in analysis/batch2.py (timings: median, IQR, minimum; other
metrics: median over runs). Additional columns:
  ratio_facts / ratio_overall   measured shuffle compression ratio (median)
  rule2_r, advisory_bytes, skew_threshold_bytes   rule-2 values in the run config
  null_partition_mib_aqe_off    largest join-task read with AQE off (compressed)
  join_strategies               distinct strategy counts seen in the runs

Run with the project-local venv:  .venv/bin/python analysis/batch3.py
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import MIB, attach_null_partition, load_runs, summarize

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch3"
PARAM_KEYS = ["join", "null_share", "rows", "payload_kind", "payload_random_fraction",
              "shuffle_partitions", "codec", "accurate_skew_factor", "rule2_ratio", "n_dims",
              "num_files"]
LEVEL_NAMES = {1.0: "1x", 0.53: "2x", 0.19: "4x", 0.04: "8x", 0.0: "16x"}


def run_conf(json_path):
    # json_path is relative to the directory of the per-job CSV
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def extra_columns(df, keys):
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key))
        row["ratio_facts"] = g["compression_ratio_facts"].median()
        row["ratio_overall"] = g["compression_ratio_overall"].median()
        row["ratio_overall_min"] = g["compression_ratio_overall"].min()
        row["ratio_overall_max"] = g["compression_ratio_overall"].max()
        row["shuffle_exchanges"] = "|".join(map(str, sorted(set(g["shuffle_exchanges"]))))
        row["num_stages_median"] = g["num_stages"].median()
        row["join_strategies"] = " || ".join(sorted(set(g["join_strategies"])))
        conf = run_conf(g["json_path"].iloc[0])["conf"]
        row["rule2_r"] = g["rule2_ratio"].iloc[0] if g["rule2_ratio"].iloc[0] > 0 else None
        row["advisory_bytes"] = conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"]
        row["skew_threshold_bytes"] = conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]
        row["accurate_skew_factor_conf"] = conf["spark.shuffle.accurateBlockSkewedFactor"]
        rows.append(row)
    return pd.DataFrame(rows)


def build(df, experiment, keys):
    d = df[df["experiment"] == experiment]
    s = summarize(d, keys).merge(extra_columns(d, keys), on=keys)
    if "aqe_off" in set(s["mode"]):
        s = attach_null_partition(s, keys)
    return s


def save_plans(df):
    out = RESULTS / "exp_d_plans"
    out.mkdir(exist_ok=True)
    d = df[df["experiment"] == "exp_d_pipeline"]
    for (scenario, mode), g in d.groupby(["scenario_id", "mode"]):
        plan = run_conf(g.sort_values("job_id")["json_path"].iloc[0])["metrics"]["final_physical_plan"]
        (out / "{}__{}.txt".format(scenario, mode)).write_text(plan)


def main():
    df = load_runs(BATCH, PARAM_KEYS)
    df["level"] = df["payload_random_fraction"].map(LEVEL_NAMES)

    out = {
        "exp_c2": build(df, "exp_c2_dose", ["payload_random_fraction", "level", "codec", "mode"]),
        "exp_rules": build(df, "exp_rules", ["mode"]),
        "exp_d": build(df, "exp_d_pipeline",
                       ["n_dims", "join", "null_share", "shuffle_partitions", "mode"]),
    }
    for name, summary in out.items():
        summary.to_csv(RESULTS / "{}_summary.csv".format(name), index=False)
    save_plans(df)

    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, summary in out.items():
            print("==", name)
            print(summary.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
