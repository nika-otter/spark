"""Batch 5a (Spark 4.1.3): TPC-H / TPC-DS (step 1a, 1b), rule 2 against simple
alternatives (step 2), refining rule 2 (step 3a-3c). Step 1c (real datasets) is
batch 5b.

Phases, in order, all resumable (a finished job leaves results/batch5a/done/<job_id>):
  1a-profile  TPC-H, every query, one AQE-on run with lz4 and one with zstd
              (one application per query and codec); r(query) = overall ratio
              of the lz4 run
  1b-profile  TPC-DS, every query, one AQE-on run with lz4
  1a-timing   TPC-H, one application per query: 1 warm-up round + 3 measured
              rounds of aqe_off / aqe_on / aqe_on_rule2, modes interleaved
  1b-timing   TPC-DS, the 30 queries with the largest total shuffle bytes
              written in the lz4 profile: aqe_on / aqe_on_rule2, as above
  2           synthetic scenarios S1-S5 x 6 modes, 4 rounds (1 warm-up + 2
              measured per application); r from an AQE-on profiling run
  3a          stress pipeline: profiling runs (AQE off, AQE on), then 4 rounds of
              aqe_off, aqe_on, rules 1+2 with r from the AQE-off run / the AQE-on
              run / the maximum per-shuffle ratio of the AQE-on run, and the
              iterated rule (warm-up with r_on, then 3 iterations)
  3b          combined case, rules 1+2 with r x 0.5, 1, 2 (r = S3 profile), 4 rounds
  3c          r on 1 %, 10 % and 100 % of facts for levels 1x, 4x, 16x (one
              AQE-on run each)
The phase order is fixed; the job order inside a phase is randomized (seeded).
Profiles and r values: results/batch5a/ratios.json. Start or resume with
scripts/batch5a.sh (runs this under caffeinate).
"""

import csv
import json
import random
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch5a"
ROUNDS = 4
TPC_TIMING_RUNS = 3
TPCDS_TOP = 30
LEVELS = {"1x": 1.0, "4x": 0.19, "8x": 0.04, "16x": 0.0}
TPCH = ["q{}".format(i) for i in range(1, 23)]
TPCDS = sorted(p.stem for p in (ROOT / "bench" / "tpcds").glob("q*.sql"))

ALT_MODES = ["aqe_off", "aqe_on", "aqe_on_fixed8", "aqe_on_fixed16", "aqe_on_nocoalesce",
             "aqe_on_rule2"]
SCENARIOS = {   # step 2: name -> params (exp_g_alt), rule 1 added to AQE-on modes
    "S1": ({"null_share": 0.2, "payload_random_fraction": LEVELS["1x"], "shuffle_partitions": 200}, False),
    "S2": ({"null_share": 0.2, "payload_random_fraction": LEVELS["8x"], "shuffle_partitions": 200}, False),
    "S3": ({"null_share": 0.2, "payload_random_fraction": LEVELS["8x"], "shuffle_partitions": 2001}, True),
    "S4": ({"null_share": 0.0, "payload_random_fraction": LEVELS["8x"], "shuffle_partitions": 2001}, True),
    "S5": ({"null_share": 0.0, "payload_random_fraction": LEVELS["1x"], "shuffle_partitions": 2001}, True),
}
STRESS = {"rows": 7_000_000}
SENS_FACTORS = (0.5, 1.0, 2.0)
SAMPLES = (0.01, 0.1, 0.0)   # 0.0 = full data


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def submit(script, args, log_path):
    cmd = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/" + script] + args
    with open(log_path, "w") as f:
        return subprocess.run(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT).returncode


def csv_rows(path):
    with open(path) as f:
        return list(csv.DictReader(f))


class State:
    def __init__(self):
        self.path = OUT / "ratios.json"
        self.data = json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, key):
        return self.data.get(key)

    def put(self, key, value):
        self.data[key] = value
        self.path.write_text(json.dumps(self.data, indent=1, sort_keys=True))


def run_job(job_id, script, args, csv_name=None):
    """Run once; returns the CSV path (None on failure). Skips finished jobs."""
    csv_path = OUT / "jobs" / ((csv_name or job_id) + ".csv")
    if (OUT / "done" / job_id).exists():
        return csv_path
    if csv_path.exists():
        csv_path.unlink()
    start = time.time()
    rc = submit(script, args + ["--job-id", job_id, "--csv",
                                "/opt/project/results/batch5a/jobs/{}".format(csv_path.name)],
                OUT / "logs" / (job_id + ".log"))
    ok = rc == 0 and csv_path.exists()
    log("{} rc={} {:.0f}s".format(job_id, rc, time.time() - start))
    if ok:
        (OUT / "done" / job_id).touch()
        return csv_path
    return None


def shuffled(items, seed):
    items = list(items)
    random.Random(seed).shuffle(items)
    return items


# --------------------------------------------------------------------------- step 1

def tpc_profiles(state, bench, queries, codecs):
    for q, codec in shuffled([(q, c) for q in queries for c in codecs], "{}-profile".format(bench)):
        job = "1{}_profile_{}_{}_{}".format("a" if bench == "tpch" else "b", bench, q, codec)
        path = run_job(job, "run_bench.py", ["--bench", bench, "--query", q, "--profile",
                                             "--codec", codec])
        if path and codec == "lz4":
            row = csv_rows(path)[-1]
            r = float(row["compression_ratio_overall"]) if row["compression_ratio_overall"] else None
            state.put("{}_{}".format(bench, q), {
                "r_overall": r, "shuffle_write_bytes": int(row["shuffle_write_bytes"]),
                "profile_seconds": float(row["elapsed_seconds"])})


def tpc_timing(state, bench, queries, modes):
    for q in shuffled(queries, "{}-timing".format(bench)):
        prof = state.get("{}_{}".format(bench, q))
        if prof is None:
            log("no lz4 profile for {} {}, timing skipped".format(bench, q))
            continue
        r = prof["r_overall"] or 1.0   # no measurable shuffle: rule 2 = defaults
        job = "1{}_timing_{}_{}".format("a" if bench == "tpch" else "b", bench, q)
        run_job(job, "run_bench.py", ["--bench", bench, "--query", q, "--modes", ",".join(modes),
                                      "--rule2-ratio", "{:.4f}".format(r), "--warmup", "1",
                                      "--runs", str(TPC_TIMING_RUNS)])


def projection(state):
    """Projected hours of the remaining phases from the profiles (logged only)."""
    tpch = sum(state.get("tpch_" + q)["profile_seconds"] for q in TPCH if state.get("tpch_" + q))
    ds = sorted((state.get("tpcds_" + q) for q in TPCDS if state.get("tpcds_" + q)),
                key=lambda v: -v["shuffle_write_bytes"])[:TPCDS_TOP]
    tpcds = sum(v["profile_seconds"] for v in ds)
    t_tpch = 3 * (1 + TPC_TIMING_RUNS) * tpch + 25 * len(TPCH)
    t_tpcds = 2 * (1 + TPC_TIMING_RUNS) * tpcds + 25 * TPCDS_TOP
    log("projection: TPC-H timing {:.2f} h, TPC-DS timing {:.2f} h, steps 2-3 about 4.9 h".format(
        t_tpch / 3600, t_tpcds / 3600))


# --------------------------------------------------------------------------- step 2-3

def generate(experiment, params):
    key = experiment + "".join("_{}-{}".format(k, v) for k, v in sorted(params.items()))
    job = "gen_" + key
    if (OUT / "done" / job).exists():
        return
    rc = submit("run_experiment.py", ["--experiment", experiment, "--generate-only"]
                + param_args(params), OUT / "logs" / (job + ".log"))
    log("{} rc={}".format(job, rc))
    if rc != 0:
        sys.exit("generation failed: " + key)
    (OUT / "done" / job).touch()


def profile(state, key, experiment, params, mode):
    """One run (no warm-up) of `mode`; stores r_overall and the max per-shuffle ratio."""
    if state.get(key):
        return state.get(key)
    path = run_job("prof_" + key, "run_experiment.py",
                   ["--experiment", experiment, "--mode", mode, "--warmup", "0", "--runs", "1"]
                   + param_args(params))
    if path is None:
        sys.exit("profiling failed: " + key)
    row = csv_rows(path)[-1]
    rec = json.loads((OUT / "jobs" / row["json_path"]).resolve().read_text())
    ratios = [e["ratio"] for e in rec["metrics"]["exchanges_detail"] if e["ratio"]]
    value = {"r_overall": float(row["compression_ratio_overall"]),
             "r_facts": float(row["compression_ratio_facts"]),
             "r_max_per_shuffle": max(ratios), "mode": mode}
    state.put(key, value)
    log("profile {} {}".format(key, value))
    return value


def experiment_jobs(phase, items):
    """items: (label, experiment, params, mode, extra_args) -> rounds of randomized jobs."""
    jobs = []
    for rnd in range(1, ROUNDS + 1):
        for order, (label, experiment, params, mode, extra) in enumerate(
                shuffled(items, "{}-{}".format(phase, rnd))):
            jobs.append(("{}_r{}_{:02d}_{}".format(phase, rnd, order, label), experiment,
                         params, mode, extra))
    return jobs


def run_experiment_jobs(jobs):
    for job_id, experiment, params, mode, extra in jobs:
        run_job(job_id, "run_experiment.py",
                ["--experiment", experiment, "--mode", mode, "--warmup", "1", "--runs", "2"]
                + extra + param_args(params))


def step2(state):
    items = []
    for name, (params, rule1) in SCENARIOS.items():
        r = profile(state, "S_" + name, "exp_g_alt", params, "aqe_on")["r_overall"]
        for mode in ALT_MODES:
            p = dict(params)
            if rule1 and mode != "aqe_off":
                p["accurate_skew_factor"] = 5.0
            if mode == "aqe_on_rule2":
                p["rule2_ratio"] = round(r, 4)
            items.append(("{}_{}".format(name, mode), "exp_g_alt", p, mode, []))
    run_experiment_jobs(experiment_jobs("2", items))


def step3(state):
    off = profile(state, "stress_off", "exp_h_stress", STRESS, "aqe_off")
    on = profile(state, "stress_on", "exp_h_stress", STRESS, "aqe_on")
    rules = dict(STRESS, accurate_skew_factor=5.0)
    items = [("stress_aqe_off", "exp_h_stress", STRESS, "aqe_off", []),
             ("stress_aqe_on", "exp_h_stress", STRESS, "aqe_on", [])]
    for label, r in (("roff", off["r_overall"]), ("ron", on["r_overall"]),
                     ("rmax", on["r_max_per_shuffle"])):
        items.append(("stress_rules12_" + label, "exp_h_stress",
                      dict(rules, rule2_ratio=round(r, 4)), "aqe_on_rules12", []))
    items.append(("stress_iter", "exp_h_stress", dict(rules, rule2_ratio=round(on["r_overall"], 4)),
                  "aqe_on_rules12_iter", ["--iterate-rule2", "3"]))
    jobs = experiment_jobs("3a", items)
    # the iterated job runs 1 warm-up + 3 iterations instead of 1 + 2 runs
    for job_id, experiment, params, mode, extra in jobs:
        args = ["--experiment", experiment, "--mode", mode, "--warmup", "1"]
        args += extra if extra else ["--runs", "2"]
        run_job(job_id, "run_experiment.py", args + param_args(params))

    r = state.get("S_S3")["r_overall"]
    params, _ = SCENARIOS["S3"]
    items = [("sens_f{:g}".format(f), "exp_h_sens",
              {"accurate_skew_factor": 5.0, "rule2_ratio": round(r * f, 4), "rule2_factor": f},
              "aqe_on_rules12", []) for f in SENS_FACTORS]
    run_experiment_jobs(experiment_jobs("3b", items))

    for level in ("1x", "4x", "16x"):
        for sample in SAMPLES:
            params = {"payload_random_fraction": LEVELS[level], "facts_sample": sample}
            profile(state, "sample_{}_{:g}".format(level, sample), "exp_h_sample", params, "aqe_on")


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    state = State()
    log("start")
    tpc_profiles(state, "tpch", TPCH, ("lz4", "zstd"))
    tpc_profiles(state, "tpcds", TPCDS, ("lz4",))
    projection(state)
    tpc_timing(state, "tpch", TPCH, ["aqe_off", "aqe_on", "aqe_on_rule2"])
    profiled = [q for q in TPCDS if state.get("tpcds_" + q)]
    top = sorted(profiled, key=lambda q: -state.get("tpcds_" + q)["shuffle_write_bytes"])[:TPCDS_TOP]
    state.put("tpcds_top30", top)
    tpc_timing(state, "tpcds", top, ["aqe_on", "aqe_on_rule2"])

    for name, (params, _) in SCENARIOS.items():
        generate("exp_g_alt", params)
    generate("exp_h_stress", STRESS)
    for level in ("1x", "4x", "16x"):
        generate("exp_h_sample", {"payload_random_fraction": LEVELS[level]})
    step2(state)
    step3(state)
    failed = sorted(p.stem for p in (OUT / "logs").glob("*.log")
                    if not (OUT / "done" / p.stem).exists())
    log("finished, failed jobs: {}".format(failed or "none"))


if __name__ == "__main__":
    main()
