"""Summaries for batch 5b (step 1c, real datasets).

Reads finished round jobs of results/batch5b (measured runs only) and writes
  results/exp_real_summary.csv   per dataset, query, shuffle partitions, mode:
                                 the usual timing columns (median, IQR, min),
                                 skew runs, splits, partitions after
                                 coalescing, longest join task, spill, the
                                 compression ratio of the join shuffle (the
                                 exchange with the most records = fact side)
                                 and the join strategies of the final plan
  results/exp_real_keys.csv      key distributions from the dataset manifests
                                 (distinct values, NULL rows, top-1 and top-5
                                 shares); read from results/batch5b/manifests/
The static planner's choice is the join strategy of the AQE-off runs.

Run with the project-local venv:  .venv/bin/python analysis/batch5b.py
(copy the manifests first: see main()).
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import MIB, summarize

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch5b"


def run_json(json_path):
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def load():
    done = {p.name for p in (BATCH / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((BATCH / "jobs").glob("r*.csv")) if p.stem in done]
    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_warmup"]].copy()
    params = df["params"].apply(json.loads)
    for k in ("dataset", "query", "shuffle_partitions", "rule2_ratio", "accurate_skew_factor", "join_key"):
        df[k] = params.apply(lambda p, k=k: p.get(k))
    df["round"] = df["job_id"].str.extract(r"^r(\d+)_").astype(int)
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    return df


def join_shuffle(g):
    ratios, written = [], []
    for p in g["json_path"]:
        ex = run_json(p)["metrics"]["exchanges_detail"]
        if ex:
            fact = max(ex, key=lambda e: e["records_written"] or 0)
            ratios.append(fact["ratio"])
            written.append(fact["shuffle_bytes_written"])
    conf = run_json(g["json_path"].iloc[0])["conf"]
    return {
        "join_key": g["join_key"].iloc[0],
        "join_shuffle_ratio_median": pd.Series(ratios).median() if ratios else None,
        "join_shuffle_written_mib_median": pd.Series(written).median() / MIB if written else None,
        "ratio_overall": g["compression_ratio_overall"].median(),
        "join_strategies": " || ".join(sorted(set(g["join_strategies"]))),
        "join_stage_tasks_median": g["join_stage_tasks"].median(),
        "rule2_ratio": g["rule2_ratio"].iloc[0],
        "advisory_bytes": conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"],
        "skew_threshold_bytes": conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"],
        "accurate_skew_factor_conf": conf.get("spark.shuffle.accurateBlockSkewedFactor"),
        "broadcast_threshold": conf.get("spark.sql.autoBroadcastJoinThreshold"),
    }


def keys_table():
    rows = []
    for path in sorted((BATCH / "manifests").glob("real_*.json")):
        m = json.loads(path.read_text())
        for key, k in m["keys"].items():
            rows.append({"dataset": m["dataset_id"], "rows": m["row_count"], "key": key,
                         "distinct": k["distinct"], "null_rows": k["null_rows"],
                         "top1_share": k["top1_share"], "top5_share": k["top5_share"],
                         "top5": "; ".join("{}={:.4f}".format("/".join(t["key"]), t["share"])
                                           for t in k["top5"])})
    return pd.DataFrame(rows)


def main():
    df = load()
    keys = ["dataset", "query", "shuffle_partitions", "mode"]
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key))
        row.update(join_shuffle(g))
        rows.append(row)
    s = summarize(df, keys).merge(pd.DataFrame(rows), on=keys)
    s.to_csv(RESULTS / "exp_real_summary.csv", index=False)
    k = keys_table()
    k.to_csv(RESULTS / "exp_real_keys.csv", index=False)
    with pd.option_context("display.width", 300, "display.max_columns", None):
        print(k.round(4).to_string(index=False))
        print(s.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
