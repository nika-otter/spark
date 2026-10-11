"""Summaries for batch 6.

validation(): step 0 table from the AQE-on trials (results/batch6_validation.csv):
plan shape, rows written to the shuffle (for the aggregation: equal to the
input rows means partial aggregation did not reduce them), shuffle ratio,
largest / median shuffle-read task, AQE reader actions, MapStatus class.

Run with the project-local venv:  .venv/bin/python analysis/batch6.py
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import summarize
from batch5c import join_children_ok, plan_tree

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch6"
MIB = 1024 * 1024


def run_json(json_path):
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def shape(plan):
    names = [n for _, n, _ in plan_tree(plan)]
    if "Window" in names:
        i = names.index("Window")
        return " > ".join(names[i:i + 4])
    if "SortMergeJoin" in names:
        ok = join_children_ok(plan)
        return "SMJ children: " + " | ".join(c[1] for _, cs in ok for c in cs)
    aggs = [n for n in names if n.endswith("Aggregate")]
    return " > ".join(aggs[:1] + [n for n in names if n in ("AQEShuffleRead", "ShuffleQueryStage", "Exchange")][:2]
                      + aggs[1:2])


def validation():
    trials = json.loads((BATCH / "trials.json").read_text())
    rows = []
    for label, t in sorted(trials.items()):
        rec = run_json(t["json_path"])
        m = rec["metrics"]
        ex = max(m["exchanges_detail"], key=lambda e: e["records_written"] or 0)
        js = m["join_stage"] or {}
        rd = js.get("shuffle_read_bytes") or {}
        readers = m["aqe"]["shuffle_readers"]
        rows.append({
            "trial": label, "seconds": t["seconds"], "plan_shape": shape(m["final_physical_plan"]),
            "rows_in": [d for d in rec["datasets"]][0].get("stats", {}).get("row_count")
            if rec["datasets"][0].get("stats") else None,
            "shuffle_records": ex["records_written"], "shuffle_data_mib": (ex["data_size_bytes"] or 0) / MIB,
            "shuffle_ratio": ex["ratio"], "r_overall": t["r_overall"],
            "stage_tasks": js.get("tasks"),
            "task_read_max_mib": (rd.get("max") or 0) / MIB, "task_read_median_mib": (rd.get("median") or 0) / MIB,
            "skew_applied": m["aqe"]["skew_join_applied"],
            "skewed_partitions": m["aqe"]["skewed_partitions"],
            "partitions_after": "|".join(map(str, m["aqe"]["partitions_after"])),
            "reader_kinds": "|".join(sorted({r["node_name"] for r in readers})),
            "map_status_classes": "|".join(sorted({s["status_class"] or "" for s in rec.get("map_status", [])})),
        })
    return pd.DataFrame(rows)


PARAMS = ("operator", "payload_random_fraction", "shuffle_partitions", "hot_share", "dataset", "key", "codec",
          "executor_memory", "null_share", "min_highly_compress", "accurate_skew_factor", "rule2_ratio")
LEVELS = {1.0: "1x", 0.19: "4x", 0.04: "8x"}


def load(prefix):
    done = {p.name for p in (BATCH / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((BATCH / "jobs").glob(prefix + "*.csv")) if p.stem in done]
    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_warmup"]].copy()
    params = df["params"].apply(json.loads)
    for k in PARAMS:
        df[k] = params.apply(lambda p, k=k: p.get(k))
    df["level"] = df["payload_random_fraction"].map(LEVELS)
    df["round"] = df["job_id"].str.extract(r"_r(\d+)_").astype(int)
    df["label"] = df["job_id"].str.extract(r"_r\d+_\d+_(.*)$")
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    return df


def extra(g):
    rec = run_json(g["json_path"].iloc[0])
    conf = rec["conf"]
    row = {"stage_tasks_median": g["join_stage_tasks"].median(),
           "ratio_overall_median": g["compression_ratio_overall"].median(),
           "ratio_facts_median": g["compression_ratio_facts"].median(),
           "skew_threshold_mib": int(conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]) / MIB,
           "advisory_mib": int(conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) / MIB,
           "accurate_skew_factor_conf": conf.get("spark.shuffle.accurateBlockSkewedFactor"),
           "executor_memory_conf": conf.get("spark.executor.memory"),
           "codec_conf": conf.get("spark.io.compression.codec"),
           "min_highly_compress_conf": conf.get("spark.shuffle.minNumPartitionsToHighlyCompress"),
           "shuffle_records_median": None, "reader_kinds": None}
    recs, ms = [], []
    for jp in g["json_path"]:
        m = run_json(jp)
        ex = m["metrics"]["exchanges_detail"]
        if ex:
            recs.append(max(e["records_written"] or 0 for e in ex))
        for st in m.get("map_status", []):
            ms.append(st)
    row["shuffle_records_median"] = pd.Series(recs).median() if recs else None
    if ms:
        d = pd.DataFrame(ms)
        fact = d[d["map_statuses"] == d["map_statuses"].max()]
        row["map_status_class"] = "|".join(sorted(set(fact["status_class"])))
        row["map_status_size_estimate_kib_median"] = fact["size_estimate_bytes"].median() / 1024
        row["map_status_java_serialized_kib_median"] = fact["java_serialized_bytes"].median() / 1024
        row["map_status_count"] = int(fact["map_statuses"].median())
    return row


def build(df, keys):
    rows = []
    for key, g in df.groupby(keys, sort=True, dropna=False):
        row = dict(zip(keys, key))
        row.update(extra(g))
        rows.append(row)
    return summarize(df, keys).merge(pd.DataFrame(rows), on=keys)


def main():
    out = {"batch6_validation": validation()}
    out["exp_op_summary"] = build(load("s1_"), ["label", "mode"])
    out["exp_zstd_kt_summary"] = build(load("s2_"), ["dataset", "key", "shuffle_partitions", "mode"])
    out["exp_mem_summary"] = build(load("s3_"), ["label", "executor_memory", "mode"])
    df = load("s4c_")
    df["variant"] = df["label"]
    out["exp_minhc_summary"] = build(df, ["variant"])
    for name, frame in out.items():
        frame.to_csv(RESULTS / (name + ".csv"), index=False)
    with pd.option_context("display.width", 320, "display.max_columns", None, "display.max_colwidth", 70):
        for name, frame in out.items():
            print("==", name)
            print(frame.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
