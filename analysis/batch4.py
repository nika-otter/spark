"""Summaries for batch 4 (steps 1-3).

Reads finished jobs of results/batch4 (measured runs only) and writes
  results/exp_d2_summary.csv          step 1a (INNER pipelines) and 1b (stress)
  results/exp_d2_plans/<scenario>__<mode>.txt
  results/exp_e_summary.csv           step 2a, 2b, 2c (data volume)
  results/exp_f_summary.csv           step 2d (hot key) + 55M NULL rows of 2a/2b
  results/spark35_summary.csv         step 3: Spark 3.5.9 next to 4.1.3

Aggregation as in analysis/batch2.py and batch3.py (timings: median, IQR,
minimum; other metrics: median over runs; skew as "runs with skew / runs").
Per-join columns come from the `joins` / `exchanges_detail` records of the
run JSONs (jobs/metrics.py, batch 4):
  output_rows_median                 rows produced by the topmost join
  broadcast_build_sides              for every BroadcastHashJoin in the final
                                     plan: key:build side (left = the facts
                                     chain is broadcast, right = the dim)
  k2_*                               stress variant: the join on k2 (skew,
                                     splits, longest task, stage, spill), the
                                     compression ratio and bytes written of the
                                     facts-side exchange feeding it, and the
                                     task count of the stage writing it (the
                                     k1 join stage)
In step 3, the 4.1.3 columns are copied from exp_b/exp_c2/exp_rules summaries
of batches 2 and 3 (suffix _413); 3.5.9 columns have suffix _359.

Run with the project-local venv:  .venv/bin/python analysis/batch4.py
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import MIB, attach_null_partition, load_runs, summarize

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch4"
PARAM_KEYS = ["join", "null_share", "hot_share", "hot_keys", "rows", "num_files",
              "payload_bytes", "payload_kind", "payload_random_fraction", "shuffle_partitions",
              "codec", "accurate_skew_factor", "rule2_ratio", "n_dims", "match_rate"]
LEVEL_NAMES = {1.0: "1x", 0.53: "2x", 0.19: "4x", 0.04: "8x", 0.0: "16x"}


def run_json(json_path):
    # json_path is relative to the directory of the per-job CSV
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def load():
    df = load_runs(BATCH, PARAM_KEYS)
    df["step"] = df["job_id"].str.extract(r"^s(\d)_").astype(int)
    df["level"] = df["payload_random_fraction"].map(LEVEL_NAMES)
    df["spark_version"] = df["json_path"].map(lambda p: run_json(p)["environment"]["spark_version"])
    return df


def conf_columns(g):
    conf = run_json(g["json_path"].iloc[0])["conf"]
    return {
        "rule2_r": g["rule2_ratio"].iloc[0] if g["rule2_ratio"].iloc[0] > 0 else None,
        "advisory_bytes": conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"],
        "skew_threshold_bytes": conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"],
        "accurate_skew_factor_conf": conf.get("spark.shuffle.accurateBlockSkewedFactor"),
    }


def common_columns(g):
    row = {
        "ratio_facts": g["compression_ratio_facts"].median(),
        "ratio_overall": g["compression_ratio_overall"].median(),
        "shuffle_exchanges": "|".join(map(str, sorted(set(g["shuffle_exchanges"])))),
        "num_stages_median": g["num_stages"].median(),
        "join_strategies": " || ".join(sorted(set(g["join_strategies"]))),
        "output_rows_median": g["output_rows"].median(),
        "output_rows_min": g["output_rows"].min(),
        "output_rows_max": g["output_rows"].max(),
        "spark_version": "|".join(sorted(set(g["spark_version"]))),
    }
    row.update(conf_columns(g))
    return row


def build(df, keys, extra=None):
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key))
        row.update(common_columns(g))
        if extra:
            row.update(extra(g))
        rows.append(row)
    s = summarize(df, keys).merge(pd.DataFrame(rows), on=keys)
    if "aqe_off" in set(s["mode"]):
        s = attach_null_partition(s, keys)
    return s


# --------------------------------------------------------------------------- step 1

def broadcast_sides(g):
    sides = set()
    for p in g["json_path"]:
        joins = run_json(p)["metrics"]["joins"]
        sides.add(",".join("{}:{}".format(j["left_keys"], j["build_side"])
                           for j in sorted(joins, key=lambda j: j["left_keys"])
                           if j["strategy"] == "BroadcastHashJoin"))
    return {"broadcast_build_sides": " || ".join(sorted(sides))}


def k2_join(g):
    rows = []
    for p in g["json_path"]:
        m = run_json(p)["metrics"]
        j = [j for j in m["joins"] if j["left_keys"] == "k2"]
        # the facts-side k2 exchange is written by the stage of the k1 join
        k1 = [j for j in m["joins"] if j["left_keys"] == "k1"]
        k1_stage = (k1[0]["stage"] or {}) if len(k1) == 1 else {}
        ex = [e for e in m["exchanges_detail"] if e["partition_key"] == "k2"]
        facts_ex = max(ex, key=lambda e: e["records_written"] or 0) if ex else None
        if len(j) != 1:
            continue
        j = j[0]
        st = j["stage"] or {}
        task = st.get("task_duration_ms") or {}
        reads = st.get("task_shuffle_read_bytes") or {}
        rows.append({
            "k2_strategy": j["strategy"],
            "k2_skew": j["skewed_partitions"] > 0,
            "k2_skewed_partitions": j["skewed_partitions"],
            "k2_splits": j["skew_splits"],
            "k2_stage_s": (st.get("duration_ms") or float("nan")) / 1000,
            "k2_stage_tasks": st.get("num_tasks"),
            "k2_longest_task_s": (task.get("max") or float("nan")) / 1000,
            "k2_task_read_max_mib": (reads.get("max") or float("nan")) / MIB,
            "k2_spill_disk_mib": (st.get("spill_disk_bytes") or 0) / MIB,
            "k2_spill_memory_mib": (st.get("spill_memory_bytes") or 0) / MIB,
            "k2_stage_count": len(j["stages"]),
            "k2_exchange_ratio": facts_ex["ratio"] if facts_ex else None,
            "k2_exchange_written_mib": (facts_ex["shuffle_bytes_written"] or 0) / MIB if facts_ex else None,
            "k2_exchange_map_tasks": k1_stage.get("num_tasks"),
        })
    if not rows:
        return {}
    r = pd.DataFrame(rows)
    out = {"k2_strategy": "|".join(sorted(set(r["k2_strategy"]))),
           "k2_skew_runs": "{}/{}".format(int(r["k2_skew"].sum()), len(r)),
           "k2_stage_count": "|".join(map(str, sorted(set(r["k2_stage_count"]))))}
    for col in ("k2_skewed_partitions", "k2_splits", "k2_stage_s", "k2_stage_tasks",
                "k2_longest_task_s", "k2_task_read_max_mib", "k2_spill_disk_mib",
                "k2_spill_memory_mib", "k2_exchange_ratio", "k2_exchange_written_mib",
                "k2_exchange_map_tasks"):
        out[col + "_median"] = r[col].median()
    for col in ("k2_stage_s", "k2_longest_task_s"):
        out[col + "_q25"] = r[col].quantile(0.25)
        out[col + "_q75"] = r[col].quantile(0.75)
    return out


def step1(df):
    d = df[df["step"] == 1]
    keys = ["experiment", "join", "n_dims", "rows", "null_share", "shuffle_partitions", "mode"]
    s = build(d, keys, lambda g: dict(broadcast_sides(g), **k2_join(g)))
    out = RESULTS / "exp_d2_plans"
    out.mkdir(exist_ok=True)
    for (scenario, mode), g in d.groupby(["scenario_id", "mode"]):
        plan = run_json(g.sort_values("job_id")["json_path"].iloc[0])["metrics"]["final_physical_plan"]
        (out / "{}__{}.txt".format(scenario, mode)).write_text(plan)
    return s


# --------------------------------------------------------------------------- step 2

def step2(df):
    d = df[df["step"] == 2]
    e = build(d[d["experiment"].isin(["exp_e_volume", "exp_e_coalesce"])],
              ["experiment", "null_share", "level", "shuffle_partitions", "rows", "num_files", "mode"])
    keys = ["experiment", "join", "null_share", "hot_share", "hot_keys", "level",
            "shuffle_partitions", "rows", "mode"]
    f = build(d[(d["experiment"] == "exp_f_hotkey")
                | ((d["experiment"] == "exp_e_volume") & (d["rows"] == 55_000_000))], keys)
    f["skew_source"] = f.apply(
        lambda r: "null_key" if r["null_share"] > 0 else
        ("hot x{}".format(int(r["hot_keys"])) if r["hot_keys"] > 1 else "hot"), axis=1)
    return e, f


# --------------------------------------------------------------------------- step 3

STEP3 = {
    # experiment -> (summary of 4.1.3, keys)
    "exp_b_partitions": ("exp_b_summary.csv", ["shuffle_partitions", "accurate_skew_factor", "mode"]),
    "exp_c2_dose": ("exp_c2_summary.csv", ["payload_random_fraction", "codec", "mode"]),
    "exp_rules": ("exp_rules_summary.csv", ["mode"]),
}
SIDE_COLS = ["runs", "wall_s_median", "wall_s_q25", "wall_s_q75", "wall_s_min",
             "join_stage_s_median", "join_stage_s_q25", "join_stage_s_q75",
             "longest_join_task_s_median", "longest_join_task_s_q25", "longest_join_task_s_q75",
             "skew_runs", "skewed_partitions_median", "skew_splits_median", "partitions_after",
             "spill_disk_mib_median", "join_task_read_max_mib_median", "ratio_overall",
             "null_partition_mib_aqe_off"]


def step3(df):
    d = df[df["step"] == 3]
    parts = []
    for experiment, (old_csv, keys) in STEP3.items():
        new = build(d[d["experiment"] == experiment], keys)
        old = pd.read_csv(RESULTS / old_csv)
        if experiment == "exp_c2_dose":
            old = old[old["codec"] == "lz4"]
        old = old[[c for c in keys + SIDE_COLS if c in old.columns]]
        new = new[[c for c in keys + SIDE_COLS if c in new.columns]]
        m = new.merge(old, on=keys, how="left", suffixes=("_359", "_413"))
        m.insert(0, "experiment", experiment)
        parts.append(m)
    s = pd.concat(parts, ignore_index=True)
    s["level"] = s["payload_random_fraction"].map(LEVEL_NAMES)
    return s


def main():
    df = load()
    out = {}
    if (df["step"] == 1).any():
        out["exp_d2"] = step1(df)
    if (df["step"] == 2).any():
        out["exp_e"], out["exp_f"] = step2(df)
    if (df["step"] == 3).any():
        out["spark35"] = step3(df)
    for name, summary in out.items():
        summary.to_csv(RESULTS / "{}_summary.csv".format(name), index=False)
    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, summary in out.items():
            print("==", name)
            print(summary.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
