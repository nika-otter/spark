"""Summaries for batch 5a (steps 1a, 1b, 2, 3a-3c).

Reads finished jobs of results/batch5a (per-job CSVs and run JSONs) and writes
  results/tpc_shuffle_ratios.csv      every shuffle exchange of the TPC profiles
                                      (bench, query, codec, data size, bytes
                                      written, ratio)
  results/tpc_ratio_summary.csv       per bench and codec: min, quartiles, max,
                                      share > 2 and > 4 of the per-shuffle ratios
                                      (unweighted; also weighted by data size)
  results/tpc_timing_summary.csv      per bench, query, mode: median, IQR, min of
                                      the measured runs; rule 2 / AQE on time
                                      ratio per round (median, min, max) and
                                      ratio of medians
  results/exp_g_summary.csv           step 2 (S1-S5 x 6 modes)
  results/exp_h_stress_summary.csv    step 3a (incl. k2 join columns, r used)
  results/exp_h_iter.csv              step 3a iterated rule: r used / measured per
                                      iteration and round
  results/exp_h_sens_summary.csv      step 3b
  results/exp_h_sample.csv            step 3c: r on 1 %, 10 %, 100 % of facts
Aggregation of the experiment steps as in analysis/batch2.py (timings: median,
IQR, minimum; other metrics: median; skew as "runs with skew / runs").

Run with the project-local venv:  .venv/bin/python analysis/batch5.py
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import MIB, summarize
from batch4 import k2_join

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch5a"
LEVEL_NAMES = {1.0: "1x", 0.19: "4x", 0.04: "8x", 0.0: "16x"}


def run_json(json_path):
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def load_jobs(prefix):
    done = {p.name for p in (BATCH / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((BATCH / "jobs").glob(prefix + "*.csv"))
              if p.stem in done]
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    params = df["params"].apply(json.loads)
    keys = sorted({k for p in params for k in p})
    for k in keys:
        df[k] = params.apply(lambda p, k=k: p.get(k))
    return df


# --------------------------------------------------------------------------- step 1

def tpc_shuffles():
    df = load_jobs("1")
    prof = df[df["experiment"].str.endswith("_profile")]
    rows = []
    for _, r in prof.iterrows():
        rec = run_json(r["json_path"])
        for e in rec["metrics"]["exchanges_detail"]:
            rows.append({"bench": r["bench"], "query": r["query"], "codec": r["codec"],
                         "partition_key": e["partition_key"], "records": e["records_written"],
                         "data_size_bytes": e["data_size_bytes"],
                         "bytes_written": e["shuffle_bytes_written"], "ratio": e["ratio"]})
    return pd.DataFrame(rows), prof


def ratio_summary(sh):
    out = []
    for (bench, codec), g in sh.dropna(subset=["ratio"]).groupby(["bench", "codec"]):
        r = g["ratio"]
        w = g["data_size_bytes"]
        out.append({
            "bench": bench, "codec": codec, "queries": g["query"].nunique(), "shuffles": len(g),
            "min": r.min(), "q25": r.quantile(0.25), "median": r.median(), "q75": r.quantile(0.75),
            "max": r.max(), "share_gt2": (r > 2).mean(), "share_gt4": (r > 4).mean(),
            "share_gt2_weighted": w[r > 2].sum() / w.sum(), "share_gt4_weighted": w[r > 4].sum() / w.sum(),
            "overall_ratio": g["data_size_bytes"].sum() / g["bytes_written"].sum(),
            "shuffles_below_1mib_written": int((g["bytes_written"] < MIB).sum()),
        })
    return pd.DataFrame(out)


def tpc_timing():
    df = load_jobs("1")
    t = df[df["experiment"].str.endswith("_timing") & ~df["is_warmup"]]
    rows = []
    for (bench, query), g in t.groupby(["bench", "query"]):
        per_mode = {}
        for mode, m in g.groupby("mode"):
            s = m["elapsed_seconds"]
            per_mode[mode] = s.median()
            rows.append({"bench": bench, "query": query, "mode": mode, "runs": len(m),
                         "wall_s_median": s.median(), "wall_s_q25": s.quantile(0.25),
                         "wall_s_q75": s.quantile(0.75), "wall_s_min": s.min(),
                         "rule2_ratio": m["rule2_ratio"].max(),
                         "join_strategies": " || ".join(sorted(set(m["join_strategies"]))),
                         "skew_runs": "{}/{}".format(int(m["skew_join_applied"].sum()), len(m)),
                         "partitions_after": " || ".join(sorted(set(m["partitions_after"].astype(str)))),
                         "spill_disk_mib_median": m["spill_disk_bytes"].median() / MIB})
        on = g[g["mode"] == "aqe_on"].set_index("round")["elapsed_seconds"]
        r2 = g[g["mode"] == "aqe_on_rule2"].set_index("round")["elapsed_seconds"]
        per_round = (r2 / on).dropna()
        for row in rows[-len(per_mode):]:
            row["rule2_over_on_round_median"] = per_round.median() if len(per_round) else None
            row["rule2_over_on_round_min"] = per_round.min() if len(per_round) else None
            row["rule2_over_on_round_max"] = per_round.max() if len(per_round) else None
            row["rule2_over_on_of_medians"] = (per_mode.get("aqe_on_rule2", float("nan"))
                                               / per_mode.get("aqe_on", float("nan")))
            row["off_over_on_of_medians"] = (per_mode.get("aqe_off", float("nan"))
                                             / per_mode.get("aqe_on", float("nan")))
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- steps 2, 3

def prepare(df):
    df = df[~df["is_warmup"]].copy()
    df["round"] = df["job_id"].str.extract(r"_r(\d+)_").astype(float)
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    df["level"] = df["payload_random_fraction"].map(LEVEL_NAMES)
    return df


def extra(g):
    conf = run_json(g["json_path"].iloc[0])["conf"]
    return {
        "join_stage_tasks_median": g["join_stage_tasks"].median(),
        "ratio_overall": g["compression_ratio_overall"].median(),
        "advisory_bytes": conf["spark.sql.adaptive.advisoryPartitionSizeInBytes"],
        "skew_threshold_bytes": conf["spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes"],
        "accurate_skew_factor_conf": conf.get("spark.shuffle.accurateBlockSkewedFactor"),
        "coalesce_enabled": conf.get("spark.sql.adaptive.coalescePartitions.enabled"),
        "rule2_ratio": g["rule2_ratio"].iloc[0],
    }


def build(df, keys, more=None):
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key))
        row.update(extra(g))
        if more:
            row.update(more(g))
        rows.append(row)
    return summarize(df, keys).merge(pd.DataFrame(rows), on=keys)


def step2():
    df = load_jobs("2_")
    if df.empty:
        return None
    df = prepare(df)
    df["scenario"] = df["job_id"].str.extract(r"_(S\d)_")
    return build(df, ["scenario", "null_share", "level", "shuffle_partitions", "mode"])


def step3(state):
    out = {}
    df = load_jobs("3a_")
    if not df.empty:
        df = prepare(df)
        df["variant"] = df["job_id"].str.extract(r"_\d\d_stress_(.*)$")
        it = df[df["mode"] == "aqe_on_rules12_iter"]
        out["exp_h_stress"] = build(df[df["mode"] != "aqe_on_rules12_iter"], ["variant", "mode"], k2_join_batch5)
        out["exp_h_iter"] = it[["job_id", "round", "iteration", "rule2_ratio", "compression_ratio_overall",
                                "elapsed_seconds", "skew_join_applied", "skew_splits"]] \
            .rename(columns={"rule2_ratio": "r_used", "compression_ratio_overall": "r_measured"}) \
            .sort_values(["round", "iteration"])
    df = load_jobs("3b_")
    if not df.empty:
        out["exp_h_sens"] = build(prepare(df), ["rule2_factor", "mode"])
    rows = []
    for level in ("1x", "4x", "16x"):
        row = {"level": level}
        for sample, name in ((0.01, "r_sample_1pct"), (0.1, "r_sample_10pct"), (0.0, "r_full")):
            v = state.get("sample_{}_{:g}".format(level, sample))
            row[name] = v["r_overall"] if v else None
        rows.append(row)
    out["exp_h_sample"] = pd.DataFrame(rows)
    return out


def k2_join_batch5(g):
    # batch4.k2_join resolves json paths against results/batch4
    import batch4
    saved = batch4.BATCH
    batch4.BATCH = BATCH
    try:
        return k2_join(g)
    finally:
        batch4.BATCH = saved


def main():
    state = json.loads((BATCH / "ratios.json").read_text()) if (BATCH / "ratios.json").exists() else {}
    out = {}
    sh, _ = tpc_shuffles()
    if not sh.empty:
        out["tpc_shuffle_ratios"] = sh
        out["tpc_ratio_summary"] = ratio_summary(sh)
    t = tpc_timing()
    if not t.empty:
        out["tpc_timing_summary"] = t
    s2 = step2()
    if s2 is not None:
        out["exp_g_summary"] = s2
    for name, frame in step3(state).items():
        out[name if name.endswith(("iter", "sample")) else name + "_summary"] = frame
    for name, frame in out.items():
        frame.to_csv(RESULTS / "{}.csv".format(name), index=False)
    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, frame in out.items():
            if name != "tpc_shuffle_ratios":
                print("==", name)
                print(frame.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
