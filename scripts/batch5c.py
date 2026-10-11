"""Batch 5c (Spark 4.1.3): skew on real data with materialized key tables
(step 2) and the structural limit of a join with an inline aggregate (step 3).

  python scripts/batch5c.py prepare   per-key tables (jobs/prepare_keytables.py),
                                      then one AQE-on trial per scenario (step 0
                                      validation; also the profiling run that
                                      gives r for rule 2), and a runtime
                                      projection; nothing else runs
  python scripts/batch5c.py rounds    step 2: 3 rounds; step 3: 4 rounds; each
                                      round runs every (point, mode) once in a
                                      randomized order as one application with
                                      1 warm-up + 2 measured runs

Before and after every job scripts/cluster_check.py verifies AC power, the
running containers and 3 alive workers; on failure the job is not marked done
and the batch stops. Resumable (done markers in results/batch5c/done). Start or resume with
scripts/batch5c.sh MODE (runs this under caffeinate).
"""

import csv
import json
import random
import subprocess
import sys
import time
from pathlib import Path

import cluster_check

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "results" / "batch5c"
STEP2_ROUNDS, STEP3_ROUNDS = 3, 4
REAL_KEYS = [("taxi", "PULocationID"), ("hits", "RegionID"), ("hits", "CounterID"),
             ("pageviews", "project")]
STEP2_MODES = {200: ["aqe_off", "aqe_on", "aqe_on_fixed16", "aqe_on_rule2"],
               2001: ["aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_fixed16", "aqe_on_rule2",
                      "aqe_on_rules12"]}
STEP3_MODES = ["aqe_off", "aqe_on", "aqe_on_force"]
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


def points():
    """(step, experiment, label, params, modes)"""
    out = []
    for name, key in REAL_KEYS:
        for sp in (200, 2001):
            out.append((2, "exp_real_kt", "{}_{}_sp{}".format(name, key, sp),
                        {"dataset": name, "key": key, "shuffle_partitions": sp}, STEP2_MODES[sp]))
    for variant in ("A", "B"):
        out.append((3, "exp_hot_kt", "hot_variant" + variant, {"variant": variant}, STEP3_MODES))
    return out


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
                                "/opt/project/results/batch5c/jobs/" + csv_path.name],
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
    tables = [["real", name, key] for name, key in REAL_KEYS]
    tables.append(["synthetic", "exp_hot_kt", "--param", "variant=B"])
    for args in tables:
        job = "keytable_" + "_".join(a for a in args if a != "--param")
        if (OUT / "done" / job).exists():
            continue
        start = time.time()
        rc = submit("prepare_keytables.py", args, OUT / "logs" / (job + ".log"))
        log("{} rc={} {:.0f}s".format(job, rc, time.time() - start))
        if rc:
            sys.exit("key table failed: " + job)
        (OUT / "done" / job).touch()

    trials_path = OUT / "trials.json"
    trials = json.loads(trials_path.read_text()) if trials_path.exists() else {}
    for step, experiment, label, params, modes in points():
        if label in trials:
            continue
        path = run_job("trial_" + label, "run_experiment.py",
                       ["--experiment", experiment, "--mode", "aqe_on", "--warmup", "0",
                        "--runs", "1"] + param_args(params))
        if path is None:
            sys.exit("trial failed: " + label)
        with open(path) as f:
            row = list(csv.DictReader(f))[-1]
        trials[label] = {"r_overall": float(row["compression_ratio_overall"]),
                         "r_facts": float(row["compression_ratio_facts"]),
                         "seconds": float(row["elapsed_seconds"]), "json_path": row["json_path"]}
        trials_path.write_text(json.dumps(trials, indent=1, sort_keys=True))
    hours = {2: 0.0, 3: 0.0}
    for step, experiment, label, params, modes in points():
        rounds = STEP2_ROUNDS if step == 2 else STEP3_ROUNDS
        hours[step] += rounds * len(modes) * (3 * trials[label]["seconds"] + 30) / 3600
    log("projection: step 2 {:.1f} h ({} rounds), step 3 {:.1f} h ({} rounds)".format(
        hours[2], STEP2_ROUNDS, hours[3], STEP3_ROUNDS))


def rounds():
    trials = json.loads((OUT / "trials.json").read_text())
    for step, n_rounds in ((2, STEP2_ROUNDS), (3, STEP3_ROUNDS)):
        for rnd in range(1, n_rounds + 1):
            items = [(e, l, p, m) for s, e, l, p, modes in points() if s == step for m in modes]
            random.Random(5300 + 10 * step + rnd).shuffle(items)
            for order, (experiment, label, p, mode) in enumerate(items):
                params = dict(p)
                if mode in RULE1_MODES or (mode == "aqe_on_fixed16" and p.get("shuffle_partitions") == 2001):
                    params["accurate_skew_factor"] = 5.0
                if mode in RULE2_MODES:
                    params["rule2_ratio"] = round(trials[label]["r_overall"], 4)
                run_job("s{}_r{}_{:02d}_{}_{}".format(step, rnd, order, label, mode), "run_experiment.py",
                        ["--experiment", experiment, "--mode", mode, "--warmup", "1", "--runs", "2"]
                        + param_args(params))
    log("finished")


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    {"prepare": prepare, "rounds": rounds}[sys.argv[1]]()


if __name__ == "__main__":
    main()
