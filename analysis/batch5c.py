"""Summaries for batch 5c (steps 0-3).

  results/batch5c_validation.csv   step 0: per scenario, from the AQE-on trial:
                                   join strategy, whether both join children are
                                   Sort directly over a shuffle stage (AQEShuffleRead
                                   in between is allowed: it is added after
                                   OptimizeSkewedJoin), the largest fact-side
                                   partition (uncompressed: exact UnsafeRow bytes from
                                   prepare_keytables.py; compressed: uncompressed /
                                   fact-exchange ratio of the trial) and the skew
                                   threshold of each mode
                                   max(skewedPartitionThresholdInBytes, 5 x median)
                                   with the median likewise estimated
  results/exp_real_wall_iqr.csv    step 1: wall time median and IQR of every 5b point
  results/exp_real_kt_summary.csv  step 2
  results/exp_hot_kt_summary.csv   step 3 (+ final plans in results/exp_hot_kt_plans/)
Column notes for step 2/3: largest_task_read_mib_aqe_off = largest join-task
shuffle read with AQE off (one task = one partition of both sides, compressed);
threshold_mib = max(skew threshold of the mode, 5 x median join-task read with
AQE off), compressed.

Run with the project-local venv:  .venv/bin/python analysis/batch5c.py
(the key-table manifests must be copied to results/batch5c/manifests/ first).
"""

import json
import re
from pathlib import Path

import pandas as pd

from batch2 import MIB, summarize

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch5c"
THRESHOLDS = {  # mode -> skew threshold in bytes (None: rule 2, depends on r)
    "aqe_on": 256 * MIB, "aqe_on_rule1": 256 * MIB, "aqe_on_fixed16": 64 * MIB,
    "aqe_on_rule2": None, "aqe_on_rules12": None, "aqe_on_force": 256 * MIB,
}


def run_json(json_path):
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def plan_tree(plan):
    """[(depth, name, line)] of the final plan tree; depth = column of the
    "+- " / ":- " marker (-1 for the root), a direct child is 3 columns deeper."""
    tree = plan.split("== Final Plan ==", 1)[1].split("== Initial Plan ==", 1)[0] \
        if "== Final Plan ==" in plan else plan.split("\n\n", 1)[0]
    out = []
    for line in tree.splitlines():
        if not line.strip():
            continue
        marker = re.search(r"[+:]- ", line)
        depth = marker.start() if marker else -1
        rest = line[marker.end():] if marker else line
        name = re.match(r"\s*(?:\*\s*)?([A-Za-z]+)", rest)
        if name:
            out.append((depth, name.group(1), line.strip()))
    return out


def join_children_ok(plan):
    """For every SortMergeJoin: is each child Sort -> [AQEShuffleRead] -> ShuffleQueryStage?"""
    nodes = plan_tree(plan)
    results = []
    for i, (depth, name, line) in enumerate(nodes):
        if name != "SortMergeJoin":
            continue
        children = []
        for j in range(i + 1, len(nodes)):
            if nodes[j][0] <= depth:
                break
            if nodes[j][0] == depth + 3:
                children.append(j)
        ok = []
        for c in children:
            chain, k = [], c
            while k < len(nodes) and len(chain) < 3:
                chain.append(nodes[k][1])
                nxt = [m for m in range(k + 1, len(nodes)) if nodes[m][0] == nodes[k][0] + 3]
                if not nxt:
                    break
                k = nxt[0]
            good = chain[0] == "Sort" and (chain[1:2] == ["ShuffleQueryStage"]
                                           or chain[1:3] == ["AQEShuffleRead", "ShuffleQueryStage"])
            ok.append((good, " > ".join(chain)))
        results.append((line, ok))
    return results


def manifests():
    out = {}
    for p in (BATCH / "manifests").glob("*.json"):
        m = json.loads(p.read_text())
        out[(m["facts_dataset_id"], m["key"])] = m
    return out


def validation():
    trials = json.loads((BATCH / "trials.json").read_text())
    mans = manifests()
    rows = []
    for label, t in sorted(trials.items()):
        rec = run_json(t["json_path"])
        p = rec["params"]
        facts_id = [d for d in rec["datasets"] if d.get("table", "facts") == "facts"][0]["dataset_id"]
        key = p.get("join_key") or "k"
        sp = str(p["shuffle_partitions"])
        m = rec["metrics"]
        checks = join_children_ok(m["final_physical_plan"])
        fact_ex = max(m["exchanges_detail"], key=lambda e: e["records_written"] or 0)
        man = mans.get((facts_id, key))
        row = {"scenario": label, "join_strategies": json.dumps({k: v for k, v in m["join_strategies"].items() if v}),
               "smj_children": " | ".join("; ".join(c[1] for c in ok) for _, ok in checks),
               "both_children_sort_over_shuffle": all(all(c[0] for c in ok) for _, ok in checks) if checks else False,
               "skew_applied_in_trial": m["aqe"]["skew_join_applied"],
               "fact_exchange_ratio": fact_ex["ratio"], "r_overall": t["r_overall"]}
        if man:
            fp = man["fact_partitions"][sp]
            ratio = fact_ex["ratio"]
            row.update({
                "largest_partition_rows": fp["largest_rows"],
                "largest_partition_mib_uncompressed": fp["largest_bytes"] / MIB,
                "largest_partition_mib_compressed_est": fp["largest_bytes"] / ratio / MIB,
                "median_partition_mib_compressed_est": fp["median_bytes"] / ratio / MIB,
                "fact_total_mib_uncompressed": fp["total_bytes"] / MIB,
                "fact_exchange_data_size_mib": fact_ex["data_size_bytes"] / MIB,
            })
            med5 = 5 * fp["median_bytes"] / ratio
            for mode, thr in THRESHOLDS.items():
                thr = thr if thr is not None else 256 * MIB / t["r_overall"]
                row["threshold_mib_" + mode] = max(thr, med5) / MIB
        rows.append(row)
    return pd.DataFrame(rows)


def step1():
    s = pd.read_csv(RESULTS / "exp_real_summary.csv")
    return s[["dataset", "query", "shuffle_partitions", "mode", "runs", "wall_s_median", "wall_s_q25",
              "wall_s_q75", "wall_s_min"]]


def load(prefix):
    done = {p.name for p in (BATCH / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((BATCH / "jobs").glob(prefix + "*.csv")) if p.stem in done]
    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_warmup"]].copy()
    params = df["params"].apply(json.loads)
    for k in ("dataset", "key", "variant", "shuffle_partitions", "rule2_ratio", "accurate_skew_factor"):
        df[k] = params.apply(lambda p, k=k: p.get(k))
    df["round"] = df["job_id"].str.extract(r"_r(\d+)_").astype(int)
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    return df


def extra(g):
    conf = run_json(g["json_path"].iloc[0])["conf"]
    return {"skew_threshold_conf_mib": int(conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"]) / MIB,
            "advisory_conf_mib": int(conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"]) / MIB,
            "accurate_skew_factor_conf": conf.get("spark.shuffle.accurateBlockSkewedFactor"),
            "force_conf": conf.get("spark.sql.adaptive.forceOptimizeSkewedJoin"),
            "broadcast_threshold_conf": conf.get("spark.sql.autoBroadcastJoinThreshold"),
            "rule2_ratio": g["rule2_ratio"].iloc[0],
            "join_strategies": " || ".join(sorted(set(g["join_strategies"]))),
            "join_stage_tasks_median": g["join_stage_tasks"].median()}


def build(df, keys):
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key))
        row.update(extra(g))
        rows.append(row)
    s = summarize(df, keys).merge(pd.DataFrame(rows), on=keys)
    point = [k for k in keys if k != "mode"]
    off = s[s["mode"] == "aqe_off"][point + ["join_task_read_max_mib_median", "join_task_read_median_mib_median"]] \
        .rename(columns={"join_task_read_max_mib_median": "largest_task_read_mib_aqe_off",
                         "join_task_read_median_mib_median": "median_task_read_mib_aqe_off"})
    s = s.merge(off, on=point, how="left")
    s["threshold_mib"] = [max(c, 5 * m) if mode != "aqe_off" else None
                          for c, m, mode in zip(s["skew_threshold_conf_mib"], s["median_task_read_mib_aqe_off"], s["mode"])]
    return s


def main():
    out = {"batch5c_validation": validation(), "exp_real_wall_iqr": step1()}
    if any((BATCH / "jobs").glob("s2_*.csv")):
        out["exp_real_kt_summary"] = build(load("s2_"), ["dataset", "key", "shuffle_partitions", "mode"])
    if any((BATCH / "jobs").glob("s3_*.csv")):
        df = load("s3_")
        out["exp_hot_kt_summary"] = build(df, ["variant", "mode"])
        plans = RESULTS / "exp_hot_kt_plans"
        plans.mkdir(exist_ok=True)
        for (variant, mode), g in df.groupby(["variant", "mode"]):
            plan = run_json(g.sort_values("job_id")["json_path"].iloc[0])["metrics"]["final_physical_plan"]
            (plans / "variant{}__{}.txt".format(variant, mode)).write_text(plan)
    for name, frame in out.items():
        frame.to_csv(RESULTS / (name + ".csv"), index=False)
    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, frame in out.items():
            print("==", name)
            print(frame.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
