"""Health checks used by the batch scripts before and after every job.

A batch must stop instead of measuring on a degraded setup. Checks:
  - the Mac is on AC power (pmset), since sleep on battery pauses the VM;
  - every Spark container of the cluster is running (docker inspect), since a
    container killed by the OOM killer is not restarted;
  - the master reports the expected number of ALIVE workers (master UI JSON).
check() returns a list of problems (empty = healthy).
"""

import json
import subprocess
import urllib.request

CONTAINERS = ("spark-master", "spark-worker-1", "spark-worker-2", "spark-worker-3")
WORKERS = 3


def on_ac_power():
    out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True).stdout
    return "AC Power" in out


def container_state(name):
    r = subprocess.run(["docker", "inspect", "-f",
                        "{{.State.Running}} {{.State.OOMKilled}} {{.State.FinishedAt}}", name],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else "missing"


def alive_workers(url="http://localhost:8080/json/"):
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read()).get("aliveworkers")
    except Exception:
        return None


def check():
    problems = []
    if not on_ac_power():
        problems.append("not on AC power")
    for name in CONTAINERS:
        state = container_state(name)
        if not state.startswith("true"):
            problems.append("container {} not running ({})".format(name, state))
    workers = alive_workers()
    if workers != WORKERS:
        problems.append("master reports {} alive workers, expected {}".format(workers, WORKERS))
    return problems


if __name__ == "__main__":
    p = check()
    print("healthy" if not p else "\n".join(p))
