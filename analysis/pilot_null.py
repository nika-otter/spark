"""Summary table and plot for the pilot_null experiment.

Reads results/runs.csv (measured runs only, warm-ups excluded) and writes
  results/pilot_null_summary.csv
  results/pilot_null_time.png

Aggregation over the measured runs of each (join, null_share, mode):
  median_time_s               median wall-clock time
  min_time_s / max_time_s     spread of wall-clock time
  join_max_task_ms_median     median over runs of the longest join-stage task
  task_read_max_over_median   median over runs of max/median join-stage task
                              shuffle read (see jobs/metrics.py for meaning)
  skew_runs                   runs in which skew join was applied, "k/n"
  skew_splits_median          median over runs of skewed partition splits
  spill_*_bytes_median        median over runs of total spill

Run with the project-local venv:  .venv/bin/python analysis/pilot_null.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

RESULTS = Path(__file__).resolve().parent.parent / "results"
COLORS = {"aqe_off": "#2a78d6", "aqe_on": "#eb6834"}  # validated categorical slots 1-2
LABELS = {"aqe_off": "AQE off", "aqe_on": "AQE on"}


def load_runs():
    df = pd.read_csv(RESULTS / "runs.csv")
    df = df[(df["experiment"] == "pilot_null") & (~df["is_warmup"])].copy()
    params = df["params"].apply(json.loads)
    df["join"] = params.apply(lambda p: p["join"])
    df["null_share"] = params.apply(lambda p: p["null_share"])
    df["rows"] = params.apply(lambda p: p["rows"])
    return df


def summarize(df):
    g = df.groupby(["join", "null_share", "mode"])
    out = pd.DataFrame({
        "runs": g.size(),
        "median_time_s": g["elapsed_seconds"].median(),
        "min_time_s": g["elapsed_seconds"].min(),
        "max_time_s": g["elapsed_seconds"].max(),
        "join_max_task_ms_median": g["join_task_duration_ms_max"].median(),
        "task_read_max_over_median": g["task_read_max_over_median"].median(),
        "skew_runs": g["skew_join_applied"].apply(lambda s: "{}/{}".format(int(s.sum()), len(s))),
        "skew_splits_median": g["skew_splits"].median(),
        "partitions_after": g["partitions_after"].agg(lambda s: "|".join(sorted(set(map(str, s))))),
        "spill_memory_bytes_median": g["spill_memory_bytes"].median(),
        "spill_disk_bytes_median": g["spill_disk_bytes"].median(),
    }).reset_index()
    return out.sort_values(["join", "null_share", "mode"])


def plot(summary, path):
    left = summary[summary["join"] == "left"]
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    for mode, dodge in (("aqe_off", -0.3), ("aqe_on", 0.3)):
        s = left[left["mode"] == mode]
        x = s["null_share"] * 100 + dodge
        ax.errorbar(x, s["median_time_s"],
                    yerr=[s["median_time_s"] - s["min_time_s"], s["max_time_s"] - s["median_time_s"]],
                    color=COLORS[mode], linewidth=2, marker="o", markersize=6,
                    capsize=3, elinewidth=1, label=LABELS[mode])
        ax.annotate(LABELS[mode], (x.iloc[-1], s["median_time_s"].iloc[-1]),
                    xytext=(6, 0), textcoords="offset points", va="center",
                    color="#0b0b0b", fontsize=9)
    ax.set_xlabel("NULL share in join key k, %")
    ax.set_ylabel("Wall-clock time, s (median; bars: min–max)")
    ax.set_title("facts LEFT JOIN dim, sort-merge, 200 shuffle partitions", fontsize=11, loc="left")
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e5e4e0", linewidth=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(path)


def main():
    summary = summarize(load_runs())
    summary.to_csv(RESULTS / "pilot_null_summary.csv", index=False)
    plot(summary, RESULTS / "pilot_null_time.png")
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
