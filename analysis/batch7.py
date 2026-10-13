"""Summaries for batch 7 (gap filling).

  results/exp_b_gap_summary.csv         experiment B, AQE off at 200 / 1000 / 2000 /
                                        2001 partitions: wall time, join-stage
                                        duration, longest join task, spill (median,
                                        IQR, min), shuffle ratio; anchors (200, 2001)
                                        next to the batch-2 values of exp_b_summary.csv
  results/exp_d_gap_summary.csv         pipelines INNER n=5/20 (exp_d2_inner) and LEFT
                                        n=5/20 (exp_d_pipeline): aqe_off_hints and
                                        aqe_off; wall time, SMJ / BHJ counts; anchors
                                        next to exp_d2_summary.csv / exp_d_summary.csv
  results/tpc_shuffle_ratios_tpcds_zstd.csv   every shuffle exchange of the TPC-DS
                                        zstd profile (same columns as
                                        tpc_shuffle_ratios.csv)
  results/tpc_ratio_summary.csv         rebuilt from tpc_shuffle_ratios.csv + the
                                        zstd file (the three earlier rows unchanged)
Per-shuffle ratios come from the run records (Exchange "data size" / "shuffle
bytes written" SQL metrics), as for tpc_shuffle_ratios.csv in batch 5a.
An anchor "falls within the old IQR" if its new median lies in [q25, q75] of the
old series.

Run with the project-local venv:  .venv/bin/python analysis/batch7.py
"""

import json
from pathlib import Path

import pandas as pd

from batch2 import MIB, summarize
from batch5 import ratio_summary

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch7"


def run_json(json_path):
    with open((BATCH / "jobs" / json_path).resolve()) as f:
        return json.load(f)


def load(prefix):
    done = {p.name for p in (BATCH / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((BATCH / "jobs").glob(prefix + "*.csv")) if p.stem in done]
    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_warmup"]].copy()
    params = df["params"].apply(json.loads)
    for k in ("shuffle_partitions", "n_dims", "join"):
        df[k] = params.apply(lambda p, k=k: p.get(k))
    df["round"] = df["job_id"].str.extract(r"^r(\d+)_").astype(int)
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    return df


def within(value, lo, hi):
    return bool(lo <= value <= hi)


def b_gap():
    df = load("r")
    df = df[df["experiment"] == "exp_b_partitions"]
    s = summarize(df, ["shuffle_partitions", "mode"])
    s["ratio_overall_median"] = df.groupby(["shuffle_partitions", "mode"])["compression_ratio_overall"] \
        .median().values
    old = pd.read_csv(RESULTS / "exp_b_summary.csv")
    old = old[(old["mode"] == "aqe_off") & (old["accurate_skew_factor"] < 0)]
    rows = []
    for _, r in s.iterrows():
        o = old[old["shuffle_partitions"] == r["shuffle_partitions"]]
        row = {}
        if len(o):
            o = o.iloc[0]
            for m in ("wall_s", "longest_join_task_s", "join_stage_s"):
                row["old_{}_median".format(m)] = o[m + "_median"]
                row["old_{}_q25".format(m)] = o[m + "_q25"]
                row["old_{}_q75".format(m)] = o[m + "_q75"]
                row["new_{}_within_old_iqr".format(m)] = within(r[m + "_median"], o[m + "_q25"], o[m + "_q75"])
        rows.append(row)
    return pd.concat([s.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def d_gap():
    df = load("r")
    df = df[df["experiment"].isin(["exp_d2_inner", "exp_d_pipeline"])].copy()
    df["join"] = df["join"].fillna("inner")
    keys = ["experiment", "join", "n_dims", "mode"]
    s = summarize(df, keys)
    strat = df.groupby(keys)["join_strategies"].agg(lambda v: " || ".join(sorted(set(v)))).reset_index()
    s = s.merge(strat, on=keys)
    s["smj"] = s["join_strategies"].apply(lambda v: json.loads(v.split(" || ")[0]).get("SortMergeJoin", 0))
    s["bhj"] = s["join_strategies"].apply(lambda v: json.loads(v.split(" || ")[0]).get("BroadcastHashJoin", 0))
    old = pd.concat([pd.read_csv(RESULTS / "exp_d2_summary.csv").query("experiment == 'exp_d2_inner'"),
                     pd.read_csv(RESULTS / "exp_d_summary.csv").query("join == 'left' and null_share == 0")
                     .assign(experiment="exp_d_pipeline")], ignore_index=True)
    rows = []
    for _, r in s.iterrows():
        o = old[(old["experiment"] == r["experiment"]) & (old["n_dims"] == r["n_dims"])
                & (old["join"] == r["join"]) & (old["mode"] == r["mode"])]
        row = {}
        if len(o):
            o = o.iloc[0]
            row.update({"old_wall_s_median": o["wall_s_median"], "old_wall_s_q25": o["wall_s_q25"],
                        "old_wall_s_q75": o["wall_s_q75"], "old_join_strategies": o["join_strategies"],
                        "new_wall_within_old_iqr": within(r["wall_s_median"], o["wall_s_q25"], o["wall_s_q75"])})
        hints = old[(old["experiment"] == r["experiment"]) & (old["n_dims"] == r["n_dims"])
                    & (old["join"] == r["join"]) & (old["mode"] == "aqe_on_hints")]
        if len(hints):
            row["aqe_on_hints_join_strategies"] = hints.iloc[0]["join_strategies"]
            row["aqe_on_hints_wall_s_median"] = hints.iloc[0]["wall_s_median"]
        rows.append(row)
    return pd.concat([s.reset_index(drop=True), pd.DataFrame(rows)], axis=1)


def tpcds_zstd():
    done = {p.name for p in (BATCH / "done").iterdir()}
    rows = []
    for p in sorted((BATCH / "jobs").glob("3_profile_tpcds_*_zstd.csv")):
        if p.stem not in done:
            continue
        r = pd.read_csv(p).iloc[-1]
        params = json.loads(r["params"])
        for e in run_json(r["json_path"])["metrics"]["exchanges_detail"]:
            rows.append({"bench": "tpcds", "query": params["query"], "codec": "zstd",
                         "partition_key": e["partition_key"], "records": e["records_written"],
                         "data_size_bytes": e["data_size_bytes"],
                         "bytes_written": e["shuffle_bytes_written"], "ratio": e["ratio"]})
    return pd.DataFrame(rows)


def main():
    out = {"exp_b_gap_summary": b_gap(), "exp_d_gap_summary": d_gap()}
    z = tpcds_zstd()
    if not z.empty:
        out["tpc_shuffle_ratios_tpcds_zstd"] = z
        allr = pd.concat([pd.read_csv(RESULTS / "tpc_shuffle_ratios.csv"), z], ignore_index=True)
        out["tpc_ratio_summary"] = ratio_summary(allr)
    for name, frame in out.items():
        frame.to_csv(RESULTS / (name + ".csv"), index=False)
    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, frame in out.items():
            if name != "tpc_shuffle_ratios_tpcds_zstd":
                print("==", name)
                print(frame.round(3).to_string(index=False))


if __name__ == "__main__":
    main()
