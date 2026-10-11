"""Batch 6 (Spark 4.1.3): operators other than joins, zstd on real data,
executor memory, open questions.

  python scripts/batch6.py prepare   step 0: one AQE-on trial per scenario (also
                                     the profiling run giving r for rule 2) and a
                                     runtime projection; nothing else runs
  python scripts/batch6.py rounds    step 1 (4 rounds), step 2 (STEP2_ROUNDS),
                                     step 3 (4 rounds), step 4a (one application
                                     per key), step 4c (4 rounds); each round runs
                                     every (point, mode) once in a randomized
                                     order as one application with 1 warm-up +
                                     2 measured runs

The script refuses to start unless the Mac is on AC power, and checks the
cluster (scripts/cluster_check.py) before and after every job; on failure the
job is not marked done and the batch stops. Resumable (done markers in
results/batch6/done). Start or resume with scripts/batch6.sh MODE.
"""

import csv
import json
import random
import re
import subprocess
import sys
import time
from pathlib import Path

import cluster_check

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch6"
# step 2 reduced from 3 to 2 rounds: projection 14.1 h > 12 h (user accepted ~13 h)
ROUNDS = {1: 4, 2: 2, 3: 4, "4c": 4}
LEVELS = {"1x": 1.0, "4x": 0.19, "8x": 0.04}
HEAPS = ["2g", "4g"]            # + the third level from results/batch6/heap.json (user: 3g)
RULE1_MODES = {"aqe_on_rule1", "aqe_on_rules12"}
RULE2_MODES = {"aqe_on_rule2", "aqe_on_rules12"}


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def submit(script, args, log_path):
    cmd = ["docker", "compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
           "/opt/project/jobs/" + script] + args
    with open(log_path, "w") as f:
        return subprocess.run(cmd, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT).returncode


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def heaps():
    path = OUT / "heap.json"
    extra = [json.loads(path.read_text())["max_safe"]] if path.exists() else []
    return HEAPS + [h for h in extra if h not in HEAPS]


def points():
    """(step, experiment, label, params, [(mode, extra params)], extra args)"""
    out = []
    for op in ("window", "agg"):
        for level in ("1x", "8x"):
            for sp in (200, 2001):
                rule1 = {"accurate_skew_factor": 5.0} if sp == 2001 else {}
                modes = [("aqe_off", {}), ("aqe_on", {}), ("aqe_on_fixed16", rule1), ("aqe_on_rule2", rule1)]
                out.append((1, "exp_op", "{}_{}_sp{}".format(op, level, sp),
                            {"operator": op, "payload_random_fraction": LEVELS[level],
                             "shuffle_partitions": sp}, modes, []))
    out.append((1, "exp_op", "window_hot_1x_sp200",
                {"operator": "window", "payload_random_fraction": 1.0, "hot_share": 0.2,
                 "shuffle_partitions": 200}, [("aqe_off", {}), ("aqe_on", {})], []))
    for name, key in (("taxi", "PULocationID"), ("hits", "RegionID"), ("pageviews", "project")):
        for sp in (200, 2001):
            if sp == 200:
                modes = [("aqe_off", {}), ("aqe_on", {}), ("aqe_on_fixed16", {}), ("aqe_on_rule2", {})]
            else:
                modes = [("aqe_off", {}), ("aqe_on", {}), ("aqe_on_fixed16", {"accurate_skew_factor": 5.0}),
                         ("aqe_on_rules12", {})]
            out.append((2, "exp_real_kt", "zstd_{}_{}_sp{}".format(name, key, sp),
                        {"dataset": name, "key": key, "shuffle_partitions": sp, "codec": "zstd"}, modes, []))
    for heap in heaps():
        out.append((3, "exp_g_alt", "mem{}_8x_null0_sp2001".format(heap),
                     {"null_share": 0.0, "payload_random_fraction": LEVELS["8x"], "shuffle_partitions": 2001,
                      "executor_memory": heap},
                     [("aqe_off", {}), ("aqe_on", {}), ("aqe_on_rules12", {})], []))
        out.append((3, "exp_c2_dose", "mem{}_4x_null0.08_sp200".format(heap),
                     {"payload_random_fraction": LEVELS["4x"], "executor_memory": heap},
                     [("aqe_off", {}), ("aqe_on", {}), ("aqe_on_rule2", {})], []))
    # hits: rule 1 vs the alternative alone; synthetic: both with rule 2, because
    # its NULL partition (172 MiB compressed) is below the default 256 MB
    # threshold even with exact block sizes (step-0 trial; user decision)
    out.append(("4c", "exp_real_kt", "hits_RegionID_sp2001",
                {"dataset": "hits", "key": "RegionID", "shuffle_partitions": 2001},
                [("aqe_on_rule1", {}), ("aqe_on", {"min_highly_compress": 2002})], ["--map-status-size"]))
    out.append(("4c", "exp_g_alt", "synthetic_8x_null0.2_sp2001",
                {"null_share": 0.2, "payload_random_fraction": LEVELS["8x"], "shuffle_partitions": 2001},
                [("aqe_on_rules12", {}), ("aqe_on_rule2", {"min_highly_compress": 2002})],
                ["--map-status-size"]))
    return out


def trial_label(step, label):
    """Trial of a point; step 3 has one trial per scenario, at the default heap 4g."""
    if step == 3:
        label = re.sub(r"^mem[^_]+_", "mem4g_", label)
    return "s{}_{}".format(step, label)


def run_job(job_id, script, args):
    csv_path = OUT / "jobs" / (job_id + ".csv")
    if (OUT / "done" / job_id).exists():
        return csv_path
    if csv_path.exists():
        csv_path.unlink()
    problems = cluster_check.check()
    if problems:
        log("cluster check failed before {}: {}".format(job_id, "; ".join(problems)))
        sys.exit("unhealthy cluster")
    start = time.time()
    rc = submit(script, args + ["--job-id", job_id, "--csv",
                                "/opt/project/results/batch6/jobs/" + csv_path.name],
                OUT / "logs" / (job_id + ".log"))
    log("{} rc={} {:.0f}s".format(job_id, rc, time.time() - start))
    problems = cluster_check.check()
    if problems:
        log("cluster check failed after {} (job not marked done): {}".format(job_id, "; ".join(problems)))
        sys.exit("unhealthy cluster")
    if rc == 0 and csv_path.exists():
        (OUT / "done" / job_id).touch()
        return csv_path
    return None


def prepare():
    trials_path = OUT / "trials.json"
    trials = json.loads(trials_path.read_text()) if trials_path.exists() else {}
    for step, experiment, label, params, modes, extra in points():
        if step == 3 and params.get("executor_memory") != "4g":
            continue
        t = trial_label(step, label)
        if t in trials:
            continue
        p = dict(params)
        if step == "4c":
            p["min_highly_compress"] = 2002   # confirm CompressedMapStatus at 2001 partitions
        path = run_job("trial_" + t, "run_experiment.py",
                       ["--experiment", experiment, "--mode", "aqe_on", "--warmup", "0", "--runs", "1",
                        "--map-status-size"] + param_args(p))
        if path is None:
            sys.exit("trial failed: " + t)
        with open(path) as f:
            row = list(csv.DictReader(f))[-1]
        trials[t] = {"r_overall": float(row["compression_ratio_overall"]) if row["compression_ratio_overall"] else None,
                     "seconds": float(row["elapsed_seconds"]), "json_path": row["json_path"]}
        trials_path.write_text(json.dumps(trials, indent=1, sort_keys=True))
    projection(trials)


def projection(trials):
    hours = {}
    for step, experiment, label, params, modes, extra in points():
        seconds = trials.get(trial_label(step, label), {}).get("seconds", 120)
        hours[step] = hours.get(step, 0) + ROUNDS[step] * len(modes) * (3 * seconds + 30) / 3600
    hours["4a"] = 2 * 600 / 3600
    log("projection (h): " + ", ".join("step {} {:.1f}".format(k, v) for k, v in hours.items())
        + "; total {:.1f}".format(sum(hours.values())))


def rounds():
    if not cluster_check.on_ac_power():
        sys.exit("refusing to start: not on AC power")
    trials = json.loads((OUT / "trials.json").read_text())
    rounds_by_step = dict(ROUNDS)
    override = OUT / "rounds.json"
    if override.exists():
        rounds_by_step.update({k if k == "4c" else int(k): v for k, v in json.loads(override.read_text()).items()})
    for step in (1, 2, 3, "4a", "4c"):
        if step == "4a":
            for key in ("CounterID", "RegionID"):
                job = "4a_shuffle_blocks_" + key
                if (OUT / "done" / job).exists():
                    continue
                problems = cluster_check.check()
                if problems:
                    sys.exit("unhealthy cluster: " + "; ".join(problems))
                rc = submit("shuffle_blocks.py", [key], OUT / "logs" / (job + ".log"))
                log("{} rc={}".format(job, rc))
                if rc == 0 and not cluster_check.check():
                    (OUT / "done" / job).touch()
            continue
        for rnd in range(1, rounds_by_step[step] + 1):
            items = [(e, l, p, m, mp, x) for s, e, l, p, modes, x in points() if s == step
                     for m, mp in modes]
            random.Random("6-{}-{}".format(step, rnd)).shuffle(items)
            for order, (experiment, label, p, mode, mode_params, extra) in enumerate(items):
                params = dict(p, **mode_params)
                if mode in RULE1_MODES:
                    params["accurate_skew_factor"] = 5.0
                if mode in RULE2_MODES:
                    t = trials[trial_label(step, label)]
                    params["rule2_ratio"] = round(t["r_overall"], 4)
                tag = mode + ("_minhc" if "min_highly_compress" in mode_params else "")
                run_job("s{}_r{}_{:02d}_{}_{}".format(step, rnd, order, label, tag), "run_experiment.py",
                        ["--experiment", experiment, "--mode", mode, "--warmup", "1", "--runs", "2"]
                        + extra + param_args(params))
    log("finished")


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    if not cluster_check.on_ac_power():
        sys.exit("refusing to start: not on AC power")
    {"prepare": prepare, "rounds": rounds}[sys.argv[1]]()


if __name__ == "__main__":
    main()
