"""Thesis figures (Ukrainian labels), saved as PNG (300 dpi) and PDF.

  results/figures/pilot_null.{png,pdf}
  results/figures/exp_a.{png,pdf}
  results/figures/exp_b.{png,pdf}
  results/figures/exp_c2.{png,pdf}
  results/figures/exp_d.{png,pdf}
  results/figures/exp_d2.{png,pdf}         batch 4, step 1a
  results/figures/exp_d2_stress.{png,pdf}  batch 4, step 1b
  results/figures/exp_e.{png,pdf}          batch 4, step 2a-2c
  results/figures/exp_f.{png,pdf}          batch 4, step 2d
  results/figures/spark35.{png,pdf}        batch 4, step 3
  results/figures/tpc_ratios, tpc_timing   batch 5, step 1a/1b
  results/figures/exp_real                 batch 5, step 1c
  results/figures/exp_g, exp_h             batch 5, steps 2 and 3

Conventions: a legend identifies the series (no direct labels); points are
medians over measured runs, bars span the interquartile range; lines join
only adjacent measured points of one series; series measured at isolated
points are drawn as markers only.

Run with the project-local venv:  .venv/bin/python analysis/figures.py [name ...]
"""

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, MaxNLocator  # noqa: E402

RESULTS = Path(__file__).resolve().parent.parent / "results"
FIGURES = RESULTS / "figures"

INK = "#0b0b0b"
GRID = "#e5e4e0"
# Validated categorical slots (analysis/README in the dataviz skill): blue, orange, aqua, yellow.
C = {"blue": "#2a78d6", "orange": "#eb6834", "aqua": "#1baf7a", "yellow": "#eda100",
     "violet": "#4a3aa7"}

MODE = {
    "aqe_off": (C["blue"], "o", "AQE вимкнено"),
    "aqe_on": (C["orange"], "s", "AQE увімкнено (типові параметри)"),
    "aqe_on_nocoalesce": (C["aqua"], "^", "AQE увімкнено, без об'єднання партицій"),
    "aqe_on_skew64m": (C["aqua"], "^", "AQE увімкнено, поріг skew 64 МБ"),
    "aqe_on_accskew": (C["aqua"], "^", "AQE увімкнено + accurateBlockSkewedFactor = 5"),
    "aqe_on_rule1": (C["aqua"], "^", "AQE увімкнено + правило 1"),
    "aqe_on_rule2": (C["violet"], "D", "AQE увімкнено + правило 2"),
    "aqe_on_rules12": (C["yellow"], "v", "AQE увімкнено + правила 1 і 2"),
    "aqe_on_hints": (C["aqua"], "^", "AQE увімкнено + broadcast-підказки"),
}

NULL_X = "Частка NULL у ключі з'єднання, %"
IQR_NOTE = "медіана; планки — міжквартильний розмах"


def setup():
    plt.rcParams.update({
        "font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9, "legend.fontsize": 8,
        "axes.edgecolor": "#52514e", "axes.labelcolor": INK, "xtick.color": INK,
        "ytick.color": INK, "text.color": INK,
    })
    FIGURES.mkdir(parents=True, exist_ok=True)


def num(x, digits=None):
    """Number with a decimal comma (Ukrainian convention)."""
    text = "{:g}".format(x) if digits is None else "{:.{}f}".format(x, digits)
    return text.replace(".", ",")


COMMA = FuncFormatter(lambda x, pos: num(x))


def style(ax, ylabel, xlabel=None, title=None, ylim0=True):
    if title:
        ax.set_title(title, loc="left")
    if xlabel:
        ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    if ylim0:
        ax.set_ylim(bottom=0)
    ax.grid(axis="y", color=GRID, linewidth=0.7)
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(COMMA)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def series(ax, x, med, q25, q75, mode, line=True, label=True, hollow=False, color=None):
    c, marker, name = MODE[mode]
    c = color or c
    ax.errorbar(x, med, yerr=[np.asarray(med) - np.asarray(q25), np.asarray(q75) - np.asarray(med)],
                color=c, marker=marker, markersize=5, linewidth=1.8 if line else 0,
                linestyle="-" if line else "none", capsize=2.5, elinewidth=0.9,
                markerfacecolor="white" if hollow else c, markeredgewidth=1.2,
                label=name if label else None)


def save(fig, name):
    fig.savefig(FIGURES / (name + ".png"), dpi=300)
    fig.savefig(FIGURES / (name + ".pdf"))
    plt.close(fig)
    print("saved", FIGURES / (name + ".png"))


# --------------------------------------------------------------------------- pilot

def fig_pilot():
    df = pd.read_csv(RESULTS / "runs.csv")
    df = df[(df["experiment"] == "pilot_null") & (~df["is_warmup"])].copy()
    p = df["params"].apply(json.loads)
    df = df[p.apply(lambda x: x["join"]) == "left"].copy()
    df["null_pct"] = p.apply(lambda x: x["null_share"] * 100)
    df["task_s"] = df["join_task_duration_ms_max"] / 1000
    g = df.groupby(["mode", "null_pct"])
    q = {name: g[col].quantile([0.25, 0.5, 0.75]).unstack()
         for name, col in (("task", "task_s"), ("wall", "elapsed_seconds"))}

    fig, axes = plt.subplots(1, 2, figsize=(7.2, 3.0))
    for ax, key, ylabel in ((axes[0], "task", "Найдовша задача join-стадії, с"),
                            (axes[1], "wall", "Час виконання запиту, с")):
        for mode in ("aqe_off", "aqe_on"):
            s = q[key].loc[mode]
            series(ax, s.index, s[0.5], s[0.25], s[0.75], mode)
        style(ax, ylabel, NULL_X)
        ax.xaxis.set_major_formatter(COMMA)
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Пілот: LEFT JOIN, 55 млн рядків, 200 shuffle-партицій ({})".format(IQR_NOTE),
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "pilot_null")


# --------------------------------------------------------------------------- A

def fig_exp_a():
    s = pd.read_csv(RESULTS / "exp_a_summary.csv")
    s = s[s["rows"] == s["rows"].min()]
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.7))
    for ax, prefix, ylabel in ((axes[0], "longest_join_task_s", "Найдовша задача join-стадії, с"),
                               (axes[1], "wall_s", "Час виконання запиту, с")):
        for mode in ("aqe_off", "aqe_on", "aqe_on_skew64m"):
            m = s[s["mode"] == mode].sort_values("null_share")
            series(ax, m["null_share"] * 100, m[prefix + "_median"], m[prefix + "_q25"],
                   m[prefix + "_q75"], mode)
        style(ax, ylabel, NULL_X)
        ax.xaxis.set_major_formatter(COMMA)
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Експеримент A: 13,7 млн рядків (shuffle ≈ 1,5 ГіБ), LEFT JOIN ({})".format(IQR_NOTE),
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "exp_a")


# --------------------------------------------------------------------------- B

def fig_exp_b():
    s = pd.read_csv(RESULTS / "exp_b_summary.csv")
    ticks = sorted(s["shuffle_partitions"].unique())
    pos = {t: i for i, t in enumerate(ticks)}
    groups = (
        ("aqe_on", s[(s["mode"] == "aqe_on") & (s["accurate_skew_factor"] < 0)], True),
        ("aqe_off", s[s["mode"] == "aqe_off"], False),
        ("aqe_on_accskew", s[(s["mode"] == "aqe_on") & (s["accurate_skew_factor"] > 0)], False),
    )
    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.3), gridspec_kw={"width_ratios": [1.3, 1, 1]})
    panels = (("longest_join_task_s", "Найдовша задача join-стадії, с"),
              ("join_stage_s", "Тривалість join-стадії, с"),
              ("wall_s", "Час виконання запиту, с"))
    for ax, (prefix, ylabel) in zip(axes, panels):
        for mode, m, line in groups:
            m = m.sort_values("shuffle_partitions")
            # The line is broken at the 2000/2001 boundary (different MapStatus).
            for part, first in ((m[m["shuffle_partitions"] <= 2000], True),
                                (m[m["shuffle_partitions"] > 2000], False)):
                if part.empty:
                    continue
                series(ax, part["shuffle_partitions"].map(pos), part[prefix + "_median"],
                       part[prefix + "_q25"], part[prefix + "_q75"], mode, line=line,
                       label=first or m[m["shuffle_partitions"] <= 2000].empty)
        boundary = (pos[2000] + pos[2001]) / 2
        ax.axvline(boundary, color="#52514e", linestyle="--", linewidth=0.9)
        ax.set_xticks(range(len(ticks)))
        ax.set_xticklabels([str(t) for t in ticks])
        ax.set_xlim(-0.5, len(ticks) - 0.5)
        style(ax, ylabel, "Кількість shuffle-партицій")
    for ax in axes:
        ax.text((pos[2000] + pos[2001]) / 2 + 0.08, ax.get_ylim()[1], "> 2000:\nHighlyCompressed-\nMapStatus",
                fontsize=6.5, va="top", color="#52514e")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="lower center", ncol=3)
    fig.suptitle("Експеримент B: 55 млн рядків, 20 % NULL, LEFT JOIN ({})".format(IQR_NOTE),
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.08, 1, 1))
    save(fig, "exp_b")


# --------------------------------------------------------------------------- C2

C2_MODES = ("aqe_on", "aqe_on_nocoalesce", "aqe_on_rule2")
# lz4 is measured at all five levels (joined by lines); zstd only at three,
# so it is drawn as hollow markers without lines.
CODEC_STYLE = {"lz4": ("-", False), "zstd": ("none", True)}   # line style, hollow marker


def fig_exp_c2():
    from matplotlib.lines import Line2D

    s = pd.read_csv(RESULTS / "exp_c2_summary.csv")
    off = s[s["mode"] == "aqe_off"].set_index(["payload_random_fraction", "codec"])
    s = s.join(off[["wall_s_median", "ratio_overall"]].rename(
        columns={"wall_s_median": "off_wall", "ratio_overall": "off_ratio"}),
        on=["payload_random_fraction", "codec"])
    s["rel_wall"] = s["wall_s_median"] / s["off_wall"]
    s["rel_q25"] = s["wall_s_q25"] / s["off_wall"]
    s["rel_q75"] = s["wall_s_q75"] / s["off_wall"]
    s["parts_after"] = s["partitions_after"].astype(str).str.split("|").str[0].astype(float)
    s["skew_share"] = s["skew_runs"].str.split("/").apply(lambda x: int(x[0]) / int(x[1]))

    fig, axes = plt.subplots(2, 2, figsize=(9.0, 6.4))
    panels = (
        (axes[0, 0], "rel_wall", C2_MODES, "Час запиту відносно AQE вимкнено"),
        (axes[0, 1], "parts_after", C2_MODES, "Партицій у join-стадії"),
        (axes[1, 0], "spill_disk_mib_median", ("aqe_off",) + C2_MODES, "Spill на диск, МіБ"),
        (axes[1, 1], "skew_share", ("aqe_off",) + C2_MODES, "Частка прогонів із skew join"),
    )
    for ax, col, modes, ylabel in panels:
        for mode in modes:
            for codec, (ls, hollow) in CODEC_STYLE.items():
                m = s[(s["mode"] == mode) & (s["codec"] == codec)].sort_values("off_ratio")
                if m.empty:
                    continue
                color, marker, _ = MODE[mode]
                if col == "rel_wall":
                    ax.errorbar(m["off_ratio"], m["rel_wall"],
                                yerr=[m["rel_wall"] - m["rel_q25"], m["rel_q75"] - m["rel_wall"]],
                                color=color, marker=marker, linestyle=ls, linewidth=1.5,
                                markersize=5, capsize=2, elinewidth=0.8,
                                markerfacecolor="white" if hollow else color)
                else:
                    ax.plot(m["off_ratio"], m[col], color=color, marker=marker, linestyle=ls,
                            linewidth=1.5, markersize=5,
                            markerfacecolor="white" if hollow else color)
        ax.set_xscale("log")
        # zstd at the lowest level (1.27) sits next to lz4 (1.17): one tick for both.
        ticks = [t for t in sorted(off["ratio_overall"].unique()) if not 1.2 < t < 1.3]
        ax.set_xticks(ticks)
        ax.set_xticklabels([num(t, 1) for t in ticks])
        ax.minorticks_off()
        style(ax, ylabel, "Виміряний коефіцієнт стиснення shuffle, лог. шкала")
    axes[0, 0].axhline(1, color="#52514e", linewidth=0.8)
    axes[1, 1].set_ylim(-0.05, 1.05)

    handles = [Line2D([], [], color=MODE[m][0], marker=MODE[m][1], label=MODE[m][2])
               for m in ("aqe_off",) + C2_MODES]
    handles += [Line2D([], [], color="#52514e", linestyle="-", marker="o", label="lz4"),
                Line2D([], [], color="#52514e", linestyle="none", marker="o",
                       markerfacecolor="white", label="zstd")]
    fig.legend(handles=handles, frameon=False, loc="lower center", ncol=3)
    fig.suptitle("Експеримент C2: 55 млн рядків, 8 % NULL, LEFT JOIN, 200 партицій "
                 "(медіана; планки на першій панелі — міжквартильний розмах)",
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.09, 1, 0.97))
    save(fig, "exp_c2")


# --------------------------------------------------------------------------- D

D_MODES = ("aqe_off", "aqe_on", "aqe_on_hints", "aqe_on_rules12")
STRATEGY = (("SortMergeJoin", C["blue"], "Sort-merge join"),
            ("BroadcastHashJoin", C["orange"], "Broadcast hash join"),
            ("ShuffledHashJoin", C["aqua"], "Shuffled hash join"))
MODE_SHORT = {"aqe_off": "AQE\nвимк.", "aqe_on": "AQE\nувімк.", "aqe_on_hints": "+ підказки",
              "aqe_on_rules12": "+ правила\n1 і 2"}


def fig_exp_d():
    s = pd.read_csv(RESULTS / "exp_d_summary.csv")
    s["scenario"] = s.apply(
        lambda r: "{} n={}{}".format(r["join"].upper(), int(r["n_dims"]),
                                     ", стрес" if r["null_share"] > 0 else ""), axis=1)
    order = ["INNER n=5", "LEFT n=5", "INNER n=20", "LEFT n=20", "LEFT n=5, стрес"]
    rows = []
    for scen in order:
        for mode in D_MODES:
            r = s[(s["scenario"] == scen) & (s["mode"] == mode)]
            if not r.empty:
                rows.append(r.iloc[0])
    d = pd.DataFrame(rows).reset_index(drop=True)
    x, labels, groups, pos = [], [], [], 0.0
    prev = None
    for _, r in d.iterrows():
        if prev is not None and r["scenario"] != prev:
            pos += 0.6
        x.append(pos)
        labels.append(MODE_SHORT[r["mode"]])
        prev = r["scenario"]
        pos += 1
    d["x"] = x

    fig, axes = plt.subplots(2, 1, figsize=(10.0, 6.0), sharex=True)
    bottom = np.zeros(len(d))
    for key, color, name in STRATEGY:
        counts = d["join_strategies"].apply(lambda v: json.loads(v.split(" || ")[0]).get(key, 0))
        if counts.sum() == 0:
            continue
        axes[0].bar(d["x"], counts, bottom=bottom, color=color, width=0.8, label=name,
                    edgecolor="white", linewidth=1)
        bottom += counts.values
    style(axes[0], "Кількість з'єднань у фінальному плані")
    axes[0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axes[0].legend(frameon=False, loc="upper left")
    for _, r in d[bottom == 0].iterrows():
        axes[0].text(r["x"], 0.3, "0: план\nEmpty-\nRelation", ha="center",
                     va="bottom", fontsize=6, color="#52514e")

    # Mode is named under each bar; one neutral colour avoids clashing with
    # the strategy colours of the upper panel.
    axes[1].bar(d["x"], d["wall_s_median"], color="#8a8984", width=0.8,
                yerr=[d["wall_s_median"] - d["wall_s_q25"], d["wall_s_q75"] - d["wall_s_median"]],
                capsize=2, error_kw={"elinewidth": 0.8})
    style(axes[1], "Час виконання запиту, с\n(медіана; планки — IQR)")
    axes[1].set_xticks(d["x"])
    axes[1].set_xticklabels(labels, rotation=0, fontsize=7)
    for scen in order:
        xs = d[d["scenario"] == scen]["x"]
        if len(xs):
            axes[1].annotate(scen, (xs.mean(), 0), xytext=(0, -34), textcoords="offset points",
                             xycoords=("data", "axes fraction"), ha="center", fontsize=8)
    fig.suptitle("Експеримент D: послідовні з'єднання facts з n таблицями вимірів "
                 "(режим AQE під кожним стовпцем)", fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "exp_d")


# --------------------------------------------------------------------------- batch 4

def grouped_bars(ax, groups, modes, value, q25=None, q75=None, width=0.8):
    """Bars per group (x position) and mode; missing (group, mode) pairs are skipped."""
    n = len(modes)
    w = width / n
    for i, mode in enumerate(modes):
        xs, ys, lo, hi = [], [], [], []
        for gx, rows in enumerate(groups):
            r = rows.get(mode)
            if r is None:
                continue
            xs.append(gx - width / 2 + w * (i + 0.5))
            ys.append(r[value])
            if q25:
                lo.append(r[value] - r[q25])
                hi.append(r[q75] - r[value])
        if not xs:
            continue
        color, _, name = MODE[mode]
        ax.bar(xs, ys, width=w * 0.92, color=color, label=name,
               yerr=[lo, hi] if q25 else None, capsize=1.8, error_kw={"elinewidth": 0.7})


def fig_exp_d2():
    s = pd.read_csv(RESULTS / "exp_d2_summary.csv")
    s = s[s["experiment"] == "exp_d2_inner"].copy()
    modes = ("aqe_off", "aqe_on", "aqe_on_hints")
    scen = [(5, "INNER, n = 5"), (20, "INNER, n = 20")]
    d = []
    for n, name in scen:
        for mode in modes:
            r = s[(s["n_dims"] == n) & (s["mode"] == mode)]
            if not r.empty:
                d.append(dict(r.iloc[0], scenario=name))
    d = pd.DataFrame(d)
    x, pos, prev = [], 0.0, None
    for _, r in d.iterrows():
        if prev is not None and r["scenario"] != prev:
            pos += 0.6
        x.append(pos)
        prev = r["scenario"]
        pos += 1
    d["x"] = x

    fig, axes = plt.subplots(1, 2, figsize=(9.0, 3.4))
    bottom = np.zeros(len(d))
    for key, color, name in STRATEGY:
        counts = d["join_strategies"].apply(lambda v: json.loads(v.split(" || ")[0]).get(key, 0))
        if counts.sum() == 0:
            continue
        axes[0].bar(d["x"], counts, bottom=bottom, color=color, width=0.8, label=name,
                    edgecolor="white", linewidth=1)
        bottom += counts.values
    style(axes[0], "Кількість з'єднань у фінальному плані")
    axes[0].yaxis.set_major_locator(MaxNLocator(integer=True))
    axes[0].legend(frameon=False, loc="upper left")
    axes[0].set_ylim(0, bottom.max() * 1.25)

    axes[1].bar(d["x"], d["wall_s_median"], color="#8a8984", width=0.8,
                yerr=[d["wall_s_median"] - d["wall_s_q25"], d["wall_s_q75"] - d["wall_s_median"]],
                capsize=2, error_kw={"elinewidth": 0.8})
    style(axes[1], "Час виконання запиту, с\n(медіана; планки — IQR)")
    for _, r in d.iterrows():
        share = r["output_rows_median"] / r["rows"] * 100
        axes[1].annotate("{} %".format(num(share, 1)), (r["x"], r["wall_s_q75"]),
                         xytext=(0, 3), textcoords="offset points", ha="center", fontsize=6.5,
                         color="#52514e")
    for ax in axes:
        ax.set_xticks(d["x"])
        ax.set_xticklabels([MODE_SHORT[m] for m in d["mode"]], fontsize=7)
        for name in d["scenario"].unique():
            xs = d[d["scenario"] == name]["x"]
            ax.annotate(name, (xs.mean(), 0), xytext=(0, -30), textcoords="offset points",
                        xycoords=("data", "axes fraction"), ha="center", fontsize=8)
    fig.suptitle("Експеримент D2: INNER-конвеєр, 3 млн рядків facts; над стовпцями — "
                 "частка рядків facts у результаті", fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "exp_d2")


STRESS_MODES = ("aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_rule2", "aqe_on_rules12")
MODE_SHORT.update({"aqe_on_rule1": "правило\n1", "aqe_on_rule2": "правило\n2"})


def fig_exp_d2_stress():
    s = pd.read_csv(RESULTS / "exp_d2_summary.csv")
    s = s[s["experiment"] == "exp_d2_stress"].set_index("mode").reindex(STRESS_MODES).dropna(how="all")
    x = np.arange(len(s))
    fig, axes = plt.subplots(1, 3, figsize=(11.5, 3.1))
    panels = (("k2_longest_task_s", "Найдовша задача стадії join за k2, с"),
              ("k2_stage_s", "Тривалість стадії join за k2, с"),
              ("wall_s", "Час виконання запиту, с"))
    for ax, (prefix, ylabel) in zip(axes, panels):
        med = s[prefix + "_median"]
        ax.bar(x, med, color=[MODE[m][0] for m in s.index], width=0.7,
               yerr=[med - s[prefix + "_q25"], s[prefix + "_q75"] - med],
               capsize=2, error_kw={"elinewidth": 0.8})
        ax.set_xticks(x)
        ax.set_xticklabels([MODE_SHORT[m] for m in s.index], fontsize=7)
        style(ax, ylabel)
    fig.suptitle("Експеримент D2, стрес: LEFT, n = 5, 20 % NULL у k2, 2001 партиція "
                 "(медіана; планки — IQR)", fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "exp_d2_stress")


def fig_exp_e():
    s = pd.read_csv(RESULTS / "exp_e_summary.csv")
    rows = (
        ("exp_e_volume", 0.2, "1x", 200, ("aqe_off", "aqe_on"),
         "2a: 20 % NULL, рівень стиснення 1x, 200 партицій"),
        ("exp_e_volume", 0.2, "8x", 2001, ("aqe_off", "aqe_on", "aqe_on_rules12"),
         "2b: 20 % NULL, рівень 8x, 2001 партиція"),
        ("exp_e_coalesce", 0.0, "8x", 2001, ("aqe_off", "aqe_on", "aqe_on_rule2"),
         "2c: без NULL, рівень 8x, 2001 партиція"),
    )
    fig, axes = plt.subplots(3, 2, figsize=(8.4, 8.0))
    for (exp, ns, level, sp, modes, title), axrow in zip(rows, axes):
        part = s[(s["experiment"] == exp) & (s["null_share"] == ns) & (s["level"] == level)
                 & (s["shuffle_partitions"] == sp)]
        for ax, prefix, ylabel in ((axrow[0], "longest_join_task_s", "Найдовша задача join-стадії, с"),
                                   (axrow[1], "wall_s", "Час виконання запиту, с")):
            for mode in modes:
                m = part[part["mode"] == mode].sort_values("rows")
                series(ax, m["rows"] / 1e6, m[prefix + "_median"], m[prefix + "_q25"],
                       m[prefix + "_q75"], mode)
            style(ax, ylabel, "Рядків facts, млн")
            ax.set_xticks(sorted(part["rows"].unique() / 1e6))
            ax.xaxis.set_major_formatter(COMMA)
        axrow[0].set_title(title, loc="left")
        axrow[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Експеримент E: обсяг даних, LEFT JOIN, broadcast вимкнено ({})".format(IQR_NOTE),
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "exp_e")


F_SCEN = (("null_key", "left", "NULL\nLEFT"), ("hot", "left", "гарячий\nключ, LEFT"),
          ("hot", "inner", "гарячий\nключ, INNER"), ("hot x5", "left", "5 гарячих\nключів, LEFT"))


def fig_exp_f():
    s = pd.read_csv(RESULTS / "exp_f_summary.csv")
    rows = (("1x", 200, ("aqe_off", "aqe_on"), "рівень 1x, 200 партицій"),
            ("8x", 2001, ("aqe_off", "aqe_on", "aqe_on_rules12"), "рівень 8x, 2001 партиція"))
    fig, axes = plt.subplots(2, 3, figsize=(12.0, 6.8))
    for (level, sp, modes, title), axrow in zip(rows, axes):
        part = s[(s["level"] == level) & (s["shuffle_partitions"] == sp)]
        groups, labels = [], []
        for src, join, label in F_SCEN:
            g = part[(part["skew_source"] == src) & (part["join"] == join)]
            if g.empty:
                continue
            groups.append({r["mode"]: r for _, r in g.iterrows()})
            labels.append(label)
        panels = (("longest_join_task_s_median", "longest_join_task_s_q25",
                   "longest_join_task_s_q75", "Найдовша задача join-стадії, с"),
                  ("wall_s_median", "wall_s_q25", "wall_s_q75", "Час виконання запиту, с"),
                  ("skewed_partitions_median", None, None, "Skewed-партицій (медіана)"))
        for ax, (value, q25, q75, ylabel) in zip(axrow, panels):
            grouped_bars(ax, groups, modes, value, q25, q75)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, fontsize=7)
            style(ax, ylabel)
        axrow[2].yaxis.set_major_locator(MaxNLocator(integer=True))
        axrow[0].set_title(title, loc="left")
    handles, labels = axes[1, 0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="lower center", ncol=3)
    fig.suptitle("Експеримент F: джерело перекосу (NULL або гарячий ключ, 20 % рядків), "
                 "55 млн рядків ({})".format(IQR_NOTE), fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "exp_f")


VERSION = {"413": (C["blue"], "Spark 4.1.3 (пакети 2–3)"), "359": (C["orange"], "Spark 3.5.9 (пакет 4)")}
S35_SHORT = {"aqe_off": "вимк.", "aqe_on": "увімк.", "aqe_on_rule1": "+пр. 1",
             "aqe_on_rule2": "+пр. 2", "aqe_on_rules12": "+пр. 1, 2"}


def fig_spark35():
    s = pd.read_csv(RESULTS / "spark35_summary.csv")

    def label(r):
        mode = S35_SHORT[r["mode"]]
        if r["experiment"] == "exp_b_partitions":
            if r["accurate_skew_factor"] > 0:
                mode = S35_SHORT["aqe_on_rule1"]
            return "{}\n{}".format(int(r["shuffle_partitions"]), mode)
        if r["experiment"] == "exp_c2_dose":
            return "{}\n{}".format(r["level"], mode)
        return mode

    panels = (("exp_b_partitions", "B: 20 % NULL, партиції"),
              ("exp_c2_dose", "C2: 8 % NULL, рівень стиснення lz4"),
              ("exp_rules", "Комбінована перевірка"))
    fig, axes = plt.subplots(2, 3, figsize=(12.0, 6.0),
                             gridspec_kw={"width_ratios": [6, 9, 5]})
    for col, (exp, title) in enumerate(panels):
        d = s[s["experiment"] == exp].copy()
        if exp == "exp_b_partitions":
            d = d.sort_values(["shuffle_partitions", "accurate_skew_factor", "mode"])
        elif exp == "exp_c2_dose":
            d["order"] = d["payload_random_fraction"].map({1.0: 0, 0.19: 1, 0.0: 2})
            d = d.sort_values(["order", "mode"])
        else:
            d["order"] = d["mode"].map({m: i for i, m in enumerate(STRESS_MODES)})
            d = d.sort_values("order")
        x = np.arange(len(d))
        for row, (prefix, ylabel) in enumerate((("wall_s", "Час виконання запиту, с"),
                                                ("longest_join_task_s", "Найдовша задача join-стадії, с"))):
            ax = axes[row, col]
            for i, ver in enumerate(("413", "359")):
                med = d["{}_median_{}".format(prefix, ver)]
                ax.bar(x - 0.2 + 0.4 * i, med, width=0.38, color=VERSION[ver][0],
                       label=VERSION[ver][1],
                       yerr=[med - d["{}_q25_{}".format(prefix, ver)],
                             d["{}_q75_{}".format(prefix, ver)] - med],
                       capsize=1.5, error_kw={"elinewidth": 0.7})
            ax.set_xticks(x)
            ax.set_xticklabels([label(r) for _, r in d.iterrows()], fontsize=6.5)
            style(ax, ylabel)
        axes[0, col].set_title(title, loc="left")
    handles, labels = axes[0, 0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="lower center", ncol=2)
    fig.suptitle("Spark 3.5.9 поруч із 4.1.3: 55 млн рядків, LEFT JOIN ({})".format(IQR_NOTE),
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    save(fig, "spark35")


# --------------------------------------------------------------------------- batch 5

def fig_tpc_ratios():
    sh = pd.read_csv(RESULTS / "tpc_shuffle_ratios.csv").dropna(subset=["ratio"])
    panels = (("tpch", "lz4", "TPC-H SF10, lz4"), ("tpch", "zstd", "TPC-H SF10, zstd"),
              ("tpcds", "lz4", "TPC-DS SF10, lz4"))
    bins = np.logspace(np.log10(0.25), np.log10(64), 25)
    fig, axes = plt.subplots(1, 3, figsize=(10.5, 3.2), sharey=False)
    for ax, (bench, codec, title) in zip(axes, panels):
        r = sh[(sh["bench"] == bench) & (sh["codec"] == codec)]["ratio"]
        ax.hist(r.clip(upper=63), bins=bins, color=C["blue"], edgecolor="white", linewidth=0.6)
        for x in (2, 4):
            ax.axvline(x, color="#52514e", linestyle="--", linewidth=0.8)
        ax.set_xscale("log")
        ticks = [0.25, 0.5, 1, 2, 4, 8, 16, 32]
        ax.set_xticks(ticks)
        ax.set_xticklabels([num(t) for t in ticks])
        ax.minorticks_off()
        style(ax, "Кількість shuffle", "Коефіцієнт стиснення (data size / bytes written)",
              "{} ({} shuffle)".format(title, len(r)))
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.suptitle("Розподіл коефіцієнтів стиснення окремих shuffle (AQE увімкнено, типові параметри; "
                 "пунктир — 2 і 4)", fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "tpc_ratios")


def fig_tpc_timing():
    t = pd.read_csv(RESULTS / "tpc_timing_summary.csv")
    t = t[t["mode"] == "aqe_on"].copy()
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 3.6), gridspec_kw={"width_ratios": [22, 30]})
    for ax, bench, title in ((axes[0], "tpch", "TPC-H SF10, 22 запити"),
                             (axes[1], "tpcds", "TPC-DS SF10, 30 запитів з найбільшим shuffle")):
        d = t[t["bench"] == bench].sort_values("rule2_over_on_round_median").reset_index(drop=True)
        x = np.arange(len(d))
        med = d["rule2_over_on_round_median"]
        ax.errorbar(x, med, yerr=[med - d["rule2_over_on_round_min"], d["rule2_over_on_round_max"] - med],
                    color=C["violet"], marker="D", markersize=4, linestyle="none", capsize=2,
                    elinewidth=0.8)
        ax.axhline(1, color="#52514e", linewidth=0.8)
        ax.axhline(1.1, color="#52514e", linewidth=0.8, linestyle="--")
        ax.set_xticks(x)
        ax.set_xticklabels(d["query"], rotation=90, fontsize=6.5)
        style(ax, "Час з правилом 2 / час з AQE (типові)", title=title, ylim0=False)
    fig.suptitle("Відношення часу запиту: правило 2 до AQE з типовими параметрами (медіана за 3 раундами; "
                 "планки — мін.–макс.; пунктир — 1,1)", fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "tpc_timing")


G_MODES = ("aqe_off", "aqe_on", "aqe_on_fixed8", "aqe_on_fixed16", "aqe_on_nocoalesce", "aqe_on_rule2")
MODE.update({
    "aqe_on_fixed8": ("#8a8984", "<", "AQE + фіксовані 8 МБ / 32 МБ"),
    "aqe_on_fixed16": ("#52514e", ">", "AQE + фіксовані 16 МБ / 64 МБ"),
})


def fig_exp_g():
    s = pd.read_csv(RESULTS / "exp_g_summary.csv")
    scen = ["S1", "S2", "S3", "S4", "S5"]
    labels = {"S1": "S1\n1x, 20 % NULL,\n200", "S2": "S2\n8x, 20 % NULL,\n200",
              "S3": "S3\n8x, 20 % NULL,\n2001", "S4": "S4\n8x, без NULL,\n2001",
              "S5": "S5\n1x, без NULL,\n2001"}
    groups = [{r["mode"]: r for _, r in s[s["scenario"] == sc].iterrows()} for sc in scen]
    fig, axes = plt.subplots(3, 1, figsize=(10.5, 8.4), sharex=True)
    panels = (("wall_s_median", "wall_s_q25", "wall_s_q75", "Час виконання запиту, с"),
              ("longest_join_task_s_median", "longest_join_task_s_q25", "longest_join_task_s_q75",
               "Найдовша задача join-стадії, с"),
              ("join_stage_tasks_median", None, None, "Задач у join-стадії"))
    for ax, (value, q25, q75, ylabel) in zip(axes, panels):
        grouped_bars(ax, groups, G_MODES, value, q25, q75)
        style(ax, ylabel)
    axes[2].set_yscale("log", nonpositive="clip")
    axes[2].set_ylim(5, 4000)
    axes[2].set_yticks([10, 100, 1000])
    axes[2].set_yticklabels(["10", "100", "1000"])
    axes[2].minorticks_off()
    axes[2].set_xticks(range(len(scen)))
    axes[2].set_xticklabels([labels[x] for x in scen], fontsize=7.5)
    handles, lab = axes[0].get_legend_handles_labels()
    fig.legend(handles, lab, frameon=False, loc="lower center", ncol=3)
    fig.suptitle("Крок 2: правило 2 проти простих альтернатив, 55 млн рядків, LEFT JOIN; у S3–S5 до всіх "
                 "AQE-режимів додано правило 1\n(медіана; планки — IQR; задачі — лог. шкала)",
                 fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    save(fig, "exp_g")


H_VARIANTS = (("aqe_off", "AQE\nвимк."), ("aqe_on", "AQE\nувімк."), ("rules12_roff", "правила 1, 2\nr з AQE off"),
              ("rules12_ron", "правила 1, 2\nr з AQE on"), ("rules12_rmax", "правила 1, 2\nr = макс."))


def fig_exp_h():
    st = pd.read_csv(RESULTS / "exp_h_stress_summary.csv").set_index("variant")
    k2 = pd.read_csv(RESULTS / "exp_h_stress_k2.csv").groupby("variant")["k2_longest_task_s"].median()
    st["k2_longest_task_s_median"] = k2
    it = pd.read_csv(RESULTS / "exp_h_iter.csv")
    se = pd.read_csv(RESULTS / "exp_h_sens_summary.csv").sort_values("rule2_factor")
    sa = pd.read_csv(RESULTS / "exp_h_sample.csv")
    fig, axes = plt.subplots(2, 2, figsize=(10.5, 6.8))
    ax = axes[0, 0]
    d = st.reindex([v for v, _ in H_VARIANTS])
    x = np.arange(len(d))
    for i, (col, name, color) in enumerate((("k2_longest_task_s_median", "найдовша задача join k2", C["blue"]),
                                             ("wall_s_median", "час запиту", C["orange"]))):
        ax.bar(x - 0.2 + 0.4 * i, d[col], width=0.38, color=color, label=name)
    ax.set_xticks(x)
    ax.set_xticklabels([l for _, l in H_VARIANTS], fontsize=6.5)
    style(ax, "с", title="3a: стрес-конвеєр, 7 млн рядків")
    ax.legend(frameon=False, loc="upper right", fontsize=7)

    ax = axes[0, 1]
    for rnd, g in it.groupby("round"):
        g = g.sort_values("iteration")
        ax.plot([0] + list(g["iteration"]), [g["r_used"].iloc[0]] + list(g["r_measured"]),
                marker="o", color=C["violet"], alpha=0.35 + 0.15 * rnd, linewidth=1.2)
    ax.set_xticks([0, 1, 2, 3])
    style(ax, "r", "Ітерація (0 — r з профілювання AQE on)", "3a: ітеративне правило, 4 раунди",
          ylim0=False)

    ax = axes[1, 0]
    med = se["wall_s_median"]
    ax.errorbar(se["rule2_factor"], med, yerr=[med - se["wall_s_q25"], se["wall_s_q75"] - med],
                color=C["yellow"], marker="v", capsize=2.5, linewidth=1.8, label="час запиту")
    med = se["longest_join_task_s_median"]
    ax.errorbar(se["rule2_factor"], med, yerr=[med - se["longest_join_task_s_q25"],
                                               se["longest_join_task_s_q75"] - med],
                color=C["blue"], marker="o", capsize=2.5, linewidth=1.8, label="найдовша задача join")
    ax.set_xscale("log")
    ax.set_xticks([0.5, 1, 2])
    ax.set_xticklabels(["0,5", "1", "2"])
    ax.minorticks_off()
    style(ax, "с", "Множник r", "3b: чутливість, правила 1 і 2")
    ax.legend(frameon=False, loc="upper left", fontsize=7)

    ax = axes[1, 1]
    lv = np.arange(len(sa))
    for i, (col, name, color) in enumerate((("r_sample_1pct", "вибірка 1 %", C["aqua"]),
                                             ("r_sample_10pct", "вибірка 10 %", C["violet"]),
                                             ("r_full", "повні дані", C["blue"]))):
        ax.bar(lv - 0.27 + 0.27 * i, sa[col], width=0.25, color=color, label=name)
    ax.set_xticks(lv)
    ax.set_xticklabels(sa["level"])
    style(ax, "r", "Рівень стиснення", "3c: r на вибірці facts")
    ax.legend(frameon=False, loc="upper left", fontsize=7)
    fig.suptitle("Крок 3: уточнення правила 2 (медіани; планки — IQR)", fontsize=9, x=0.01, ha="left")
    fig.tight_layout()
    save(fig, "exp_h")


REAL_MODES = ("aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_rule2", "aqe_on_rules12")
REAL_NAMES = {"taxi": "таксі", "hits": "hits", "pageviews": "pageviews"}


def fig_exp_real():
    s = pd.read_csv(RESULTS / "exp_real_summary.csv")
    panels = (("natural", 200), ("natural", 2001), ("lowcard", 200), ("lowcard", 2001))
    titles = {"natural": "Q-natural (ключ високої кардинальності, broadcast типовий)",
              "lowcard": "Q-lowcard (ключ низької кардинальності, broadcast вимкнено)"}
    fig, axes = plt.subplots(2, 4, figsize=(14.0, 6.4))
    for col, (query, sp) in enumerate(panels):
        d = s[(s["query"] == query) & (s["shuffle_partitions"] == sp)]
        groups = [{r["mode"]: r for _, r in d[d["dataset"] == ds].iterrows()} for ds in REAL_NAMES]
        for row, (prefix, ylabel) in enumerate((("wall_s", "Час виконання запиту, с"),
                                                ("longest_join_task_s", "Найдовша задача join-стадії, с"))):
            ax = axes[row, col]
            grouped_bars(ax, groups, REAL_MODES, prefix + "_median", prefix + "_q25", prefix + "_q75")
            ax.set_xticks(range(len(REAL_NAMES)))
            ax.set_xticklabels(list(REAL_NAMES.values()))
            style(ax, ylabel if col == 0 else "")
        axes[0, col].set_title("{},\n{} {}".format(titles[query].split(" (")[0], sp, "партиція" if sp == 2001 else "партицій"), loc="left")
    handles, labels = axes[0, 3].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="lower center", ncol=5)
    fig.suptitle("Крок 1c: реальні дані — таксі 2018 (102,9 млн), hits (100,0 млн), pageviews (143,4 млн); "
                 "Q-lowcard — broadcast вимкнено ({})".format(IQR_NOTE), fontsize=9, x=0.01, ha="left")
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    save(fig, "exp_real")


FIGS = {"pilot_null": fig_pilot, "exp_a": fig_exp_a, "exp_b": fig_exp_b,
        "exp_c2": fig_exp_c2, "exp_d": fig_exp_d, "exp_d2": fig_exp_d2,
        "exp_d2_stress": fig_exp_d2_stress, "exp_e": fig_exp_e, "exp_f": fig_exp_f,
        "spark35": fig_spark35, "tpc_ratios": fig_tpc_ratios, "tpc_timing": fig_tpc_timing,
        "exp_g": fig_exp_g, "exp_h": fig_exp_h, "exp_real": fig_exp_real}


def main():
    setup()
    names = sys.argv[1:] or list(FIGS)
    for name in names:
        FIGS[name]()


if __name__ == "__main__":
    main()
