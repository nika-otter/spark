"""Summaries and plots for batch 2 (INNER check, experiments A, B, C).

Reads the per-job CSVs of finished jobs (results/batch2/jobs/*.csv with a
marker in results/batch2/done/), measured runs only, and writes
  results/inner_check_summary.csv
  results/exp_a_summary.csv        results/exp_a_time.png
  results/exp_b_summary.csv        results/exp_b_time.png
  results/exp_c_summary.csv

Timing metrics are reported as median, interquartile range (q25, q75) and
minimum over the measured runs of a point. Other metrics are reported as the
median over runs; skew is reported as "runs with skew join / runs".
"NULL partition" size is the largest join-task shuffle read of the AQE-off
runs of the same dataset (with AQE off one task reads exactly one shuffle
partition of each side); it is a property of the data and is repeated for all
modes of a point.

Run with the project-local venv:  .venv/bin/python analysis/batch2.py
"""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

RESULTS = Path(__file__).resolve().parent.parent / "results"
BATCH = RESULTS / "batch2"
MIB = 1024 * 1024

TIMINGS = {
    "wall_s": "elapsed_seconds",
    "join_stage_s": "join_stage_duration_s",
    "longest_join_task_s": "join_task_duration_s_max",
    "executor_run_time_s": "executor_run_time_s",
}
PARAM_KEYS = ["join", "null_share", "rows", "payload_kind", "shuffle_partitions", "codec",
              "accurate_skew_factor"]
LABEL_OFFSET = {"aqe_off": 0, "aqe_on": 8, "aqe_on_skew64m": -8, "aqe_on_accskew": -8}
MODE_STYLE = {
    "aqe_off": ("#2a78d6", "o", "AQE off"),
    "aqe_on": ("#eb6834", "s", "AQE on"),
    "aqe_on_skew64m": ("#1baf7a", "^", "AQE on, skew threshold 64MB"),
    "aqe_on_accskew": ("#1baf7a", "^", "AQE on, accurateBlockSkewedFactor=5"),
}


def load_runs(batch=BATCH, param_keys=PARAM_KEYS):
    done = {p.name for p in (batch / "done").iterdir()}
    frames = [pd.read_csv(p) for p in sorted((batch / "jobs").glob("*.csv")) if p.stem in done]
    df = pd.concat(frames, ignore_index=True)
    df = df[~df["is_warmup"]].copy()
    params = df["params"].apply(json.loads)
    for key in param_keys:
        df[key] = params.apply(lambda p, k=key: p.get(k))
    df["round"] = df["job_id"].str.extract(r"^r(\d+)_").astype(int)
    df["join_stage_duration_s"] = df["join_stage_duration_ms"] / 1000
    df["join_task_duration_s_max"] = df["join_task_duration_ms_max"] / 1000
    df["executor_run_time_s"] = df["executor_run_time_ms"] / 1000
    return df


def summarize(df, keys):
    rows = []
    for key, g in df.groupby(keys, sort=True):
        row = dict(zip(keys, key if isinstance(key, tuple) else (key,)))
        row["runs"] = len(g)
        row["rounds"] = g["round"].nunique()
        for name, col in TIMINGS.items():
            s = g[col]
            row[name + "_median"] = s.median()
            row[name + "_q25"] = s.quantile(0.25)
            row[name + "_q75"] = s.quantile(0.75)
            row[name + "_min"] = s.min()
        row["skew_runs"] = "{}/{}".format(int(g["skew_join_applied"].sum()), len(g))
        row["skewed_partitions_median"] = g["skewed_partitions"].median()
        row["skew_splits_median"] = g["skew_splits"].median()
        row["partitions_before"] = "|".join(sorted(set(g["partitions_before"].astype(str))))
        row["partitions_after"] = "|".join(sorted(set(g["partitions_after"].astype(str)),
                                                  key=lambda v: int(v.split("|")[0])))
        row["facts_scan_map_tasks"] = "|".join(map(str, sorted(set(g["facts_scan_map_tasks"]))))
        row["shuffle_write_mib_median"] = g["shuffle_write_bytes"].median() / MIB
        row["join_task_read_max_mib_median"] = g["join_task_read_bytes_max"].median() / MIB
        row["join_task_read_median_mib_median"] = g["join_task_read_bytes_median"].median() / MIB
        row["join_task_peak_mem_max_mib_median"] = g["join_task_peak_mem_max"].median() / MIB
        row["spill_memory_mib_median"] = g["spill_memory_bytes"].median() / MIB
        row["spill_disk_mib_median"] = g["spill_disk_bytes"].median() / MIB
        row["gc_time_s_median"] = g["gc_time_ms"].median() / 1000
        rows.append(row)
    return pd.DataFrame(rows)


def attach_null_partition(summary, keys):
    """Copy the AQE-off largest task read (NULL partition) to every mode of a point."""
    point_keys = [k for k in keys if k != "mode"]
    if not point_keys:
        value = summary.loc[summary["mode"] == "aqe_off", "join_task_read_max_mib_median"]
        return summary.assign(null_partition_mib_aqe_off=value.iloc[0] if len(value) else None)
    off = summary[summary["mode"] == "aqe_off"][point_keys + ["join_task_read_max_mib_median"]]
    off = off.rename(columns={"join_task_read_max_mib_median": "null_partition_mib_aqe_off"})
    return summary.merge(off, on=point_keys, how="left")


def errorbar(ax, x, s, prefix, mode, label=None):
    if s.empty:
        return
    color, marker, default_label = MODE_STYLE[mode]
    y = s[prefix + "_median"]
    ax.errorbar(x, y, yerr=[y - s[prefix + "_q25"], s[prefix + "_q75"] - y],
                color=color, marker=marker, markersize=7, linewidth=2, capsize=3,
                elinewidth=1, label=label or default_label)
    ax.annotate(label or default_label, (x.iloc[-1], y.iloc[-1]), xytext=(8, LABEL_OFFSET[mode]),
                textcoords="offset points", va="center", fontsize=8, color="#0b0b0b")


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, fontsize=11, loc="left")
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", color="#e5e4e0", linewidth=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.legend(frameon=False, loc="upper left", fontsize=8)


def plot_exp_a(summary, path):
    small = summary[summary["rows"] == summary["rows"].min()]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.6), dpi=150)
    for ax, prefix, ylabel in ((axes[0], "wall_s", "Wall-clock time, s"),
                               (axes[1], "join_stage_s", "Join stage duration, s")):
        for mode, dodge in (("aqe_off", -0.3), ("aqe_on", 0.0), ("aqe_on_skew64m", 0.3)):
            s = small[small["mode"] == mode].sort_values("null_share")
            errorbar(ax, s["null_share"] * 100 + dodge, s, prefix, mode)
        style(ax, "Exp. A: {:,} facts rows, LEFT JOIN, 200 partitions".format(
            int(small["rows"].iloc[0])), "NULL share in join key k, %",
            ylabel + " (median; bars: IQR)")
    fig.tight_layout()
    fig.savefig(path)


def plot_exp_b(summary, path):
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    series = (
        ("aqe_off", summary[(summary["mode"] == "aqe_off")], None),
        ("aqe_on", summary[(summary["mode"] == "aqe_on") & (summary["accurate_skew_factor"] < 0)], None),
        ("aqe_on_accskew", summary[(summary["mode"] == "aqe_on")
                                   & (summary["accurate_skew_factor"] > 0)], None),
    )
    # Categorical x axis: 2000 and 2001 must stay apart.
    ticks = sorted(summary["shuffle_partitions"].unique())
    position = {t: i for i, t in enumerate(ticks)}
    for mode, s, label in series:
        s = s.sort_values("shuffle_partitions")
        errorbar(ax, s["shuffle_partitions"].map(position), s, "wall_s", mode, label)
    ax.set_xticks(range(len(ticks)))
    ax.set_xticklabels([str(t) for t in ticks])
    ax.set_xlim(-0.4, len(ticks) - 0.2)
    style(ax, "Exp. B: 55M rows, 20% NULL, LEFT JOIN", "spark.sql.shuffle.partitions (categorical)",
          "Wall-clock time, s (median; bars: IQR)")
    fig.tight_layout()
    fig.savefig(path)


def main():
    df = load_runs()
    out = {}

    keys = ["null_share", "mode"]
    out["inner_check"] = summarize(df[df["experiment"] == "inner_check"], keys)

    keys = ["rows", "null_share", "mode"]
    a = summarize(df[df["experiment"] == "exp_a_small"], keys)
    out["exp_a"] = attach_null_partition(a, keys)

    keys = ["shuffle_partitions", "accurate_skew_factor", "mode"]
    out["exp_b"] = summarize(df[df["experiment"] == "exp_b_partitions"], keys)

    keys = ["payload_kind", "codec", "mode"]
    c = summarize(df[df["experiment"] == "exp_c_compression"], keys)
    out["exp_c"] = attach_null_partition(c, keys)

    for name, summary in out.items():
        summary.to_csv(RESULTS / "{}_summary.csv".format(name), index=False)
    plot_exp_a(out["exp_a"], RESULTS / "exp_a_time.png")
    plot_exp_b(out["exp_b"], RESULTS / "exp_b_time.png")

    with pd.option_context("display.width", 300, "display.max_columns", None):
        for name, summary in out.items():
            print("==", name)
            print(summary.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
