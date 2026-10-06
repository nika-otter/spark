"""Per-run metrics collection from the driver's monitoring REST API.

Each measured action is tagged with a job description (`run_label`); the SQL
execution with that description is located in /sql, and its jobs, stages and
tasks are read from /jobs, /stages and /stages/.../taskList.

Definitions and known limits:
- "join stage": the stage of the execution that reads shuffle data. For the
  single-join queries used here there is exactly one such stage; if there are
  several, the one with the largest shuffle read is taken and
  `join_stage_ambiguous` is set.
- Task "shuffle read bytes" = localBytesRead + remoteBytesRead.
- Per-partition map-output sizes (the input of AQE's skew decision) are not
  exposed by the REST API. As a proxy, `task_read_max_over_median` is computed
  from join-stage task read bytes: with AQE off one task reads exactly one
  shuffle partition of each side, so it equals the raw partition size ratio;
  with AQE on, tasks read post-AQE (coalesced/split) partitions.
- "facts scan stage": the completed stage with no shuffle input and the largest
  shuffle output (input bytes of Parquet scans were found to be under 1 MiB, so
  they are not used); its
  task count is the number of map tasks producing the facts shuffle.
- Stage duration = completionTime - submissionTime from the REST API.
- Compression ratio of a shuffle = Exchange "data size" (sum of UnsafeRow
  bytes) / Exchange "shuffle bytes written" (bytes after the codec). Both are
  parsed from the SQL metric strings, which Spark formats with 4 significant
  digits (e.g. "259.4 MiB"); the ratio is therefore exact to about 0.1 %.
  "facts" ratio = the exchange with the largest data size; "overall" =
  sum of data size / sum of bytes written over all shuffle exchanges.
- Quantiles: median as statistics.median, p95 as nearest-rank.
"""

import csv
import json
import math
import os
import re
import statistics
import time
import urllib.request
from datetime import datetime

JOIN_OPERATORS = [
    "SortMergeJoin",
    "BroadcastHashJoin",
    "ShuffledHashJoin",
    "BroadcastNestedLoopJoin",
    "CartesianProduct",
]

CSV_FIELDS = [
    "recorded_at", "job_id", "app_id", "experiment", "scenario_id", "mode", "run_index",
    "is_warmup", "params", "datasets", "elapsed_seconds", "sql_duration_ms",
    "join_strategies", "shuffle_exchanges", "num_stages", "compression_ratio_facts",
    "compression_ratio_overall", "skew_join_applied", "skewed_partitions", "skew_splits",
    "partitions_before", "partitions_after", "shuffle_write_bytes",
    "shuffle_read_bytes", "num_tasks", "gc_time_ms", "spill_memory_bytes",
    "spill_disk_bytes", "executor_run_time_ms", "facts_scan_map_tasks",
    "join_stage_id", "join_stage_tasks", "join_stage_duration_ms",
    "join_task_duration_ms_min", "join_task_duration_ms_median",
    "join_task_duration_ms_p95", "join_task_duration_ms_max",
    "join_task_read_bytes_min", "join_task_read_bytes_median",
    "join_task_read_bytes_p95", "join_task_read_bytes_max",
    "join_task_peak_mem_min", "join_task_peak_mem_median",
    "join_task_peak_mem_p95", "join_task_peak_mem_max",
    "task_read_max_over_median", "json_path",
]


class Rest:
    def __init__(self, spark):
        self.base = "{}/api/v1/applications/{}".format(
            spark.sparkContext.uiWebUrl, spark.sparkContext.applicationId)

    def get(self, path):
        with urllib.request.urlopen(self.base + path) as resp:
            return json.loads(resp.read())


def distribution(values):
    if not values:
        return None
    s = sorted(values)
    return {
        "min": s[0],
        "median": statistics.median(s),
        "p95": s[max(0, math.ceil(0.95 * len(s)) - 1)],
        "max": s[-1],
        "count": len(s),
    }


def _metric_number(value):
    """Parse a plain count metric such as '1,234'; None for composite values."""
    v = value.strip().replace(",", "")
    return int(v) if re.fullmatch(r"-?\d+", v) else None


def find_execution(rest, run_label, timeout_s=120):
    """Wait until the SQL execution tagged with run_label is COMPLETED."""
    deadline = time.time() + timeout_s
    while True:
        executions = rest.get("/sql?details=true&planDescription=true&length=100000")
        matches = [e for e in executions if e.get("description") == run_label]
        done = [e for e in matches if e["status"] in ("COMPLETED", "FAILED")]
        if done:
            if len(matches) > 1:
                raise RuntimeError("{} SQL executions labelled {}".format(len(matches), run_label))
            return done[0]
        if time.time() > deadline:
            raise TimeoutError("SQL execution {} not completed in REST API".format(run_label))
        time.sleep(0.5)


def final_plan_tree(plan):
    """Operator-tree part of the final plan (without the node details)."""
    if "== Final Plan ==" in plan:
        tree = plan.split("== Final Plan ==", 1)[1].split("== Initial Plan ==", 1)[0]
    else:
        tree = plan.split("\n\n", 1)[0]
    return tree


_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "TiB": 1024 ** 4}


def parse_size_total(value):
    """Total of a size metric string ('total (min, med, max ...)\n259.4 MiB (...)')."""
    if not value:
        return None
    line = value.strip().split("\n")[-1]
    m = re.match(r"([\d.,]+)\s*(B|KiB|MiB|GiB|TiB)", line)
    if not m:
        return None
    return float(m.group(1).replace(",", "")) * _UNITS[m.group(2)]


def shuffle_exchange_count(plan):
    """Shuffle exchanges in the final plan tree (BroadcastExchange excluded)."""
    return len(re.findall(r"(?<![A-Za-z])Exchange\b", final_plan_tree(plan)))


def compression_ratios(exchanges):
    sizes = [(parse_size_total(e["data_size"]), parse_size_total(e["shuffle_bytes_written"]))
             for e in exchanges]
    sizes = [(d, w) for d, w in sizes if d and w]
    if not sizes:
        return None, None
    facts = max(sizes)
    return facts[0] / facts[1], sum(d for d, _ in sizes) / sum(w for _, w in sizes)


def join_strategy_counts(plan):
    tree = final_plan_tree(plan)
    return {op: len(re.findall(r"\b{}\b".format(op), tree)) for op in JOIN_OPERATORS}


def _node_metrics(node):
    return {m["name"]: m["value"] for m in node.get("metrics", [])}


def aqe_actions(execution):
    """AQE decisions as reported by SQL metrics of the final plan."""
    readers, exchanges = [], []
    for node in execution.get("nodes", []):
        metrics = _node_metrics(node)
        if node["nodeName"].startswith("AQEShuffleRead"):
            readers.append({
                "node_id": node["nodeId"],
                "node_name": node["nodeName"],
                "partitions": _metric_number(metrics.get("number of partitions", "")),
                "coalesced_partitions": _metric_number(metrics.get("number of coalesced partitions", "")),
                "skewed_partitions": _metric_number(metrics.get("number of skewed partitions", "")),
                "skew_splits": _metric_number(metrics.get("number of skewed partition splits", "")),
                "empty_partitions": _metric_number(metrics.get("number of empty partitions", "")),
                "partition_data_size": metrics.get("partition data size"),
            })
        elif node["nodeName"] == "Exchange":
            exchanges.append({
                "node_id": node["nodeId"],
                "partitions": _metric_number(metrics.get("number of partitions", "")),
                "shuffle_bytes_written": metrics.get("shuffle bytes written"),
                "data_size": metrics.get("data size"),
                "records_written": metrics.get("shuffle records written"),
            })

    plan = execution.get("planDescription", "")
    skewed = sum(r["skewed_partitions"] or 0 for r in readers)
    splits = sum(r["skew_splits"] or 0 for r in readers)
    before = sorted({e["partitions"] for e in exchanges if e["partitions"] is not None})
    after = sorted({r["partitions"] for r in readers if r["partitions"] is not None})
    return {
        "skew_join_applied": skewed > 0 or "skew=true" in final_plan_tree(plan),
        "skewed_partitions": skewed,
        "skew_splits": splits,
        # No AQEShuffleRead (AQE off): partitions are read as produced.
        "partitions_before": before,
        "partitions_after": after if readers else before,
        "shuffle_readers": readers,
        "exchanges": exchanges,
    }


def _rest_time_ms(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%Z").timestamp() * 1000


def _stage_summary(stage):
    submitted = _rest_time_ms(stage.get("submissionTime"))
    completed = _rest_time_ms(stage.get("completionTime"))
    return {
        "stage_id": stage["stageId"],
        "attempt_id": stage["attemptId"],
        "name": stage["name"],
        "status": stage["status"],
        "num_tasks": stage["numTasks"],
        "num_complete_tasks": stage["numCompleteTasks"],
        "num_failed_tasks": stage["numFailedTasks"],
        "executor_run_time_ms": stage["executorRunTime"],
        "gc_time_ms": stage["jvmGcTime"],
        "input_bytes": stage["inputBytes"],
        "shuffle_read_bytes": stage["shuffleReadBytes"],
        "shuffle_write_bytes": stage["shuffleWriteBytes"],
        "spill_memory_bytes": stage["memoryBytesSpilled"],
        "spill_disk_bytes": stage["diskBytesSpilled"],
        "duration_ms": (completed - submitted) if submitted and completed else None,
    }


def stage_metrics(rest, execution):
    job_ids = execution.get("successJobIds", []) + execution.get("failedJobIds", [])
    stage_ids = set()
    for job_id in job_ids:
        stage_ids.update(rest.get("/jobs/{}".format(job_id))["stageIds"])
    stages = []
    for stage_id in sorted(stage_ids):
        for attempt in rest.get("/stages/{}?details=false".format(stage_id)):
            stages.append(_stage_summary(attempt))
    return stages


def join_stage_metrics(rest, stages):
    readers = [s for s in stages if s["status"] == "COMPLETE" and s["shuffle_read_bytes"] > 0]
    if not readers:
        return None
    stage = max(readers, key=lambda s: s["shuffle_read_bytes"])
    tasks = rest.get("/stages/{}/{}/taskList?length=1000000".format(
        stage["stage_id"], stage["attempt_id"]))
    ok = [t for t in tasks if t["status"] == "SUCCESS"]
    read = [t["taskMetrics"]["shuffleReadMetrics"]["localBytesRead"]
            + t["taskMetrics"]["shuffleReadMetrics"]["remoteBytesRead"] for t in ok]
    hosts = {}
    for t in ok:
        hosts[t["host"]] = hosts.get(t["host"], 0) + 1
    read_dist = distribution(read)
    return {
        "stage_id": stage["stage_id"],
        "attempt_id": stage["attempt_id"],
        "join_stage_ambiguous": len(readers) > 1,
        "stage_duration_ms": stage["duration_ms"],
        "tasks": len(ok),
        "tasks_per_host": hosts,
        "duration_ms": distribution([t["duration"] for t in ok]),
        "shuffle_read_bytes": read_dist,
        "peak_execution_memory_bytes": distribution(
            [t["taskMetrics"]["peakExecutionMemory"] for t in ok]),
        "spill_memory_bytes": distribution([t["taskMetrics"]["memoryBytesSpilled"] for t in ok]),
        "spill_disk_bytes": distribution([t["taskMetrics"]["diskBytesSpilled"] for t in ok]),
        "gc_time_ms": distribution([t["taskMetrics"]["jvmGcTime"] for t in ok]),
        "task_read_max_over_median": (read_dist["max"] / read_dist["median"]
                                      if read_dist and read_dist["median"] else None),
    }


def collect(spark, run_label):
    """All metrics of the SQL execution tagged run_label."""
    rest = Rest(spark)
    execution = find_execution(rest, run_label)
    stages = stage_metrics(rest, execution)
    completed = [s for s in stages if s["status"] == "COMPLETE"]
    # Map stages reading the source tables: no shuffle input, shuffle output.
    scans = [s for s in completed if s["shuffle_read_bytes"] == 0 and s["shuffle_write_bytes"] > 0]
    facts_scan = max(scans, key=lambda s: s["shuffle_write_bytes"]) if scans else None
    aqe = aqe_actions(execution)
    ratio_facts, ratio_overall = compression_ratios(aqe["exchanges"])
    return {
        "sql_execution_id": execution["id"],
        "sql_status": execution["status"],
        "sql_duration_ms": execution["duration"],
        "final_physical_plan": execution["planDescription"],
        "join_strategies": join_strategy_counts(execution["planDescription"]),
        "aqe": aqe,
        "shuffle_exchanges": shuffle_exchange_count(execution["planDescription"]),
        "num_stages": len(completed),
        "compression_ratio_facts": ratio_facts,
        "compression_ratio_overall": ratio_overall,
        "stages": stages,
        "totals": {
            "shuffle_write_bytes": sum(s["shuffle_write_bytes"] for s in completed),
            "shuffle_read_bytes": sum(s["shuffle_read_bytes"] for s in completed),
            "num_tasks": sum(s["num_complete_tasks"] for s in completed),
            "gc_time_ms": sum(s["gc_time_ms"] for s in completed),
            "spill_memory_bytes": sum(s["spill_memory_bytes"] for s in completed),
            "spill_disk_bytes": sum(s["spill_disk_bytes"] for s in completed),
            "executor_run_time_ms": sum(s["executor_run_time_ms"] for s in completed),
            "skipped_stages": sum(1 for s in stages if s["status"] == "SKIPPED"),
        },
        "facts_scan_stage_id": facts_scan["stage_id"] if facts_scan else None,
        "facts_scan_map_tasks": facts_scan["num_complete_tasks"] if facts_scan else None,
        "join_stage": join_stage_metrics(rest, stages),
    }


def _q(dist, key):
    return dist[key] if dist else None


def csv_row(record):
    m = record["metrics"]
    js = m["join_stage"] or {}
    dur, rd, mem = js.get("duration_ms"), js.get("shuffle_read_bytes"), js.get("peak_execution_memory_bytes")
    row = {
        "recorded_at": record["recorded_at"],
        "job_id": record.get("job_id"),
        "app_id": record["environment"]["application_id"],
        "experiment": record["experiment"],
        "scenario_id": record["scenario_id"],
        "mode": record["mode"],
        "run_index": record["run_index"],
        "is_warmup": record["is_warmup"],
        "params": json.dumps(record["params"], sort_keys=True),
        "datasets": ";".join(d["dataset_id"] for d in record["datasets"]),
        "elapsed_seconds": record["elapsed_seconds"],
        "sql_duration_ms": m["sql_duration_ms"],
        "join_strategies": json.dumps({k: v for k, v in m["join_strategies"].items() if v}),
        "shuffle_exchanges": m.get("shuffle_exchanges"),
        "num_stages": m.get("num_stages"),
        "compression_ratio_facts": m.get("compression_ratio_facts"),
        "compression_ratio_overall": m.get("compression_ratio_overall"),
        "skew_join_applied": m["aqe"]["skew_join_applied"],
        "skewed_partitions": m["aqe"]["skewed_partitions"],
        "skew_splits": m["aqe"]["skew_splits"],
        "partitions_before": "|".join(map(str, m["aqe"]["partitions_before"])),
        "partitions_after": "|".join(map(str, m["aqe"]["partitions_after"])),
        "join_stage_id": js.get("stage_id"),
        "join_stage_tasks": js.get("tasks"),
        "join_stage_duration_ms": js.get("stage_duration_ms"),
        "facts_scan_map_tasks": m["facts_scan_map_tasks"],
        "task_read_max_over_median": js.get("task_read_max_over_median"),
        "json_path": record["json_path"],
    }
    row.update({k: m["totals"][k] for k in (
        "shuffle_write_bytes", "shuffle_read_bytes", "num_tasks", "gc_time_ms",
        "spill_memory_bytes", "spill_disk_bytes", "executor_run_time_ms")})
    for prefix, dist in (("join_task_duration_ms", dur), ("join_task_read_bytes", rd),
                         ("join_task_peak_mem", mem)):
        for q in ("min", "median", "p95", "max"):
            row["{}_{}".format(prefix, q)] = _q(dist, q)
    return row


def save(record, json_path, csv_path):
    """Write the per-run JSON and append one row to the runs CSV."""
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    record["json_path"] = os.path.relpath(json_path, os.path.dirname(csv_path))
    with open(json_path, "w") as f:
        json.dump(record, f, indent=2, sort_keys=True)
    new_file = not os.path.exists(csv_path)
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(csv_row(record))
