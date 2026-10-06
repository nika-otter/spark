"""Thesis figures (Ukrainian labels), saved as PNG (300 dpi) and PDF.

  results/figures/pilot_null.{png,pdf}
  results/figures/exp_a.{png,pdf}
  results/figures/exp_b.{png,pdf}
  results/figures/exp_c2.{png,pdf}
  results/figures/exp_d.{png,pdf}

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


FIGS = {"pilot_null": fig_pilot, "exp_a": fig_exp_a, "exp_b": fig_exp_b,
        "exp_c2": fig_exp_c2, "exp_d": fig_exp_d}


def main():
    setup()
    names = sys.argv[1:] or list(FIGS)
    for name in names:
        FIGS[name]()


if __name__ == "__main__":
    main()
