"""Batch 5b (Spark 4.1.3): real datasets, step 1c.

  python scripts/batch5b.py prepare   copy data/raw into the Spark data volume,
                                      load the datasets (jobs/prepare_real.py),
                                      profile every point once with AQE on and
                                      defaults (r for rule 2), print a runtime
                                      projection for the rounds
  python scripts/batch5b.py rounds    4 rounds; each round runs every (point,
                                      mode) once in a randomized order as one
                                      application with 1 warm-up + 2 measured runs

Points: dataset (taxi, hits, pageviews) x query (natural, lowcard) x shuffle
partitions (200, 2001). Modes: 200 partitions: aqe_off, aqe_on, aqe_on_rule2
(rule 1 only changes HighlyCompressedMapStatus, used above 2000 partitions);
2001 partitions: aqe_off, aqe_on, aqe_on_rule1, aqe_on_rule2, aqe_on_rules12.
Before and after every job scripts/cluster_check.py verifies AC power, the
running containers and 3 alive workers; on failure the job is not marked done
and the batch stops. Resumable (done markers in results/batch5b/done). Start or resume with
scripts/batch5b.sh MODE (runs this under caffeinate).
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
OUT = ROOT / "results" / "batch5b"
ROUNDS = 4
DATASETS = ("taxi", "hits", "pageviews")
QUERIES = ("natural", "lowcard")
MODES = {200: ["aqe_off", "aqe_on", "aqe_on_rule2"],
         2001: ["aqe_off", "aqe_on", "aqe_on_rule1", "aqe_on_rule2", "aqe_on_rules12"]}
RULE1_MODES = {"aqe_on_rule1", "aqe_on_rules12"}
RULE2_MODES = {"aqe_on_rule2", "aqe_on_rules12"}


def log(msg):
    print("{} {}".format(time.strftime("%Y-%m-%d %H:%M:%S"), msg), flush=True)


def docker(*args, log_path=None):
    with open(log_path or "/dev/null", "a") as f:
        return subprocess.run(["docker"] + list(args), cwd=ROOT, stdout=f,
                              stderr=subprocess.STDOUT).returncode


def submit(script, args, log_path):
    return docker("compose", "exec", "-T", "spark-master", "/opt/spark/bin/spark-submit",
                  "/opt/project/jobs/" + script, *args, log_path=log_path)


def param_args(params):
    args = []
    for key, value in sorted(params.items()):
        args += ["--param", "{}={}".format(key, value)]
    return args


def points():
    return [{"dataset": d, "query": q, "shuffle_partitions": sp}
            for d in DATASETS for q in QUERIES for sp in (200, 2001)]


def key(p):
    return "{}_{}_sp{}".format(p["dataset"], p["query"], p["shuffle_partitions"])


def run_job(job_id, args):
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
    rc = submit("run_experiment.py", args + ["--job-id", job_id, "--csv",
                                             "/opt/project/results/batch5b/jobs/" + csv_path.name],
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
    for name in DATASETS:
        marker = OUT / "done" / ("copy_" + name)
        if not marker.exists():
            rc = docker("exec", "spark-master", "mkdir", "-p", "/opt/spark-data/raw")
            rc = rc or docker("cp", "data/raw/" + name, "spark-master:/opt/spark-data/raw/")
            rc = rc or docker("exec", "-u", "root", "spark-master", "chown", "-R", "185:185",
                              "/opt/spark-data/raw/" + name)
            if rc:
                sys.exit("copy failed: " + name)
            marker.touch()
            log("copied " + name)
        marker = OUT / "done" / ("prepare_" + name)
        if not marker.exists():
            start = time.time()
            rc = submit("prepare_real.py", [name], OUT / "logs" / ("prepare_" + name + ".log"))
            log("prepare {} rc={} {:.0f}s".format(name, rc, time.time() - start))
            if rc:
                sys.exit("prepare failed: " + name)
            marker.touch()
    ratios_path = OUT / "ratios.json"
    ratios = json.loads(ratios_path.read_text()) if ratios_path.exists() else {}
    for p in points():
        if key(p) in ratios:
            continue
        path = run_job("prof_" + key(p), ["--experiment", "exp_real", "--mode", "aqe_on",
                                          "--warmup", "0", "--runs", "1"] + param_args(p))
        if path is None:
            sys.exit("profiling failed: " + key(p))
        with open(path) as f:
            row = list(csv.DictReader(f))[-1]
        ratios[key(p)] = {"r_overall": float(row["compression_ratio_overall"]),
                          "r_facts": float(row["compression_ratio_facts"]),
                          "profile_seconds": float(row["elapsed_seconds"])}
        ratios_path.write_text(json.dumps(ratios, indent=1, sort_keys=True))
    # projection: one application = 1 warm-up + 2 measured runs + ~30 s overhead;
    # the AQE-on profile time stands for every mode of the point
    total = sum(len(MODES[p["shuffle_partitions"]]) * (3 * ratios[key(p)]["profile_seconds"] + 30)
                for p in points())
    log("projection: {:.1f} h per round, {:.1f} h for {} rounds".format(
        total / 3600, ROUNDS * total / 3600, ROUNDS))


def rounds():
    ratios = json.loads((OUT / "ratios.json").read_text())
    for rnd in range(1, ROUNDS + 1):
        items = [(p, m) for p in points() for m in MODES[p["shuffle_partitions"]]]
        random.Random(5200 + rnd).shuffle(items)
        for order, (p, mode) in enumerate(items):
            params = dict(p)
            if mode in RULE1_MODES:
                params["accurate_skew_factor"] = 5.0
            if mode in RULE2_MODES:
                params["rule2_ratio"] = round(ratios[key(p)]["r_overall"], 4)
            run_job("r{}_{:02d}_{}_{}".format(rnd, order, key(p), mode),
                    ["--experiment", "exp_real", "--mode", mode, "--warmup", "1", "--runs", "2"]
                    + param_args(params))
    log("finished")


def main():
    for sub in ("done", "logs", "jobs"):
        (OUT / sub).mkdir(parents=True, exist_ok=True)
    {"prepare": prepare, "rounds": rounds}[sys.argv[1]]()


if __name__ == "__main__":
    main()
