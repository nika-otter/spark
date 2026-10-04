# Spark AQE experiments — local cluster

Spark standalone cluster in Docker for experiments with Adaptive Query Execution.

- Image: `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu` (native `linux/arm64`)
- 1 master, 3 workers (3 cores / 6 GB container limit / 4 GB executor heap each), History Server
- Driver runs in client mode inside the master container (`spark.driver.memory=1g`)

## Requirements

Docker Desktop with **10 CPUs** and **22 GB** memory (Settings → Resources).

## Commands

| Command | What it does |
|---|---|
| `make up` | Start the cluster |
| `make down` | Stop the cluster (volumes with data and event logs are kept) |
| `make status` | Containers and their resource usage |
| `make logs` | Follow cluster logs |
| `make submit JOB=jobs/x.py ARGS="..."` | Run a PySpark job with spark-submit in the master container |
| `make smoke-generate` | Write smoke-test tables (10M and 1M rows) as Parquet |
| `make smoke` | Run the smoke join with AQE off, then on |
| `make history-check` | List applications known to the History Server |
| `make venv` | Create the project-local `.venv` used by `analysis/` |
| `make pilot-null` | Generate datasets and run the NULL-share pilot grid |
| `make pilot-null-summary` | Summary CSV and plot for the pilot |

To delete all data and event logs: `docker compose down -v`.

## UIs

- Master: http://localhost:8080
- Workers: http://localhost:8081, :8082, :8083
- Running application: http://localhost:4040
- History Server: http://localhost:18080

## Layout

| Path | Purpose |
|---|---|
| `docker-compose.yml` | Cluster definition, resource limits, volumes |
| `conf/spark-defaults.conf` | Infrastructure settings (master URL, executor sizing, event log) |
| `jobs/spark_conf.py` | Baseline experiment config (shuffle partitions, AQE, broadcast threshold, CBO off) and effective-config dump |
| `jobs/generate.py` | Deterministic data generator (facts, dim) with per-dataset manifest |
| `jobs/metrics.py` | Per-run metrics from the driver REST API -> JSON + `results/runs.csv` |
| `jobs/experiments.py` | Experiment definitions (datasets, query, conf overrides) |
| `jobs/run_experiment.py` | One app per (scenario, mode): 1 warm-up + 5 measured runs to noop |
| `jobs/smoke_*.py` | Smoke test |
| `scripts/run_pilot_null.sh` | Pilot grid driver |
| `analysis/pilot_null.py` | Pilot summary and plot (runs on the host in `.venv`) |
| `results/runs.csv` | One row per run; full record in `results/runs/<experiment>/<scenario>/<mode>/*.json` |
| `results/calibration/` | Calibration runs for the pilot |

## Generator and runner

```
make submit JOB=jobs/generate.py ARGS="facts --rows 1000000 --n-keys 1000 --null-share 0.1"
make submit JOB=jobs/run_experiment.py ARGS="--experiment pilot_null --mode aqe_on --param join=left --param null_share=0.12"
```

Datasets are written to `/opt/spark-data/datasets/<id>/data` with
`manifest.json` next to it; `<id>` encodes all parameters and existing
datasets are not regenerated.

## Storage

Named Docker volumes (inside the Docker VM, not on the macOS filesystem):

| Volume | Mount path | Content |
|---|---|---|
| `spark-data` | `/opt/spark-data` (all containers) | Parquet data |
| `spark-events` | `/opt/spark-events` | Event logs |
| `spark-local-master`, `spark-local-1..3` | `/opt/spark-local` | Spark local/shuffle/spill dirs |

Only the project directory is bind-mounted (`/opt/project` in the master).
