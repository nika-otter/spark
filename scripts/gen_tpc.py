"""Generate TPC-H and TPC-DS data with DuckDB (host, project venv) as Parquet.

Usage:  .venv/bin/python scripts/gen_tpc.py tpch|tpcds [SF]

Writes data/<bench>_sf<SF>/<table>/*.parquet (one directory per table, several
files per large table) and data/<bench>_sf<SF>/manifest.json with row counts.
The directory is then copied into the Spark data volume
(/opt/spark-data/<bench>_sf<SF>) with `docker cp`.
"""

import json
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def main():
    bench = sys.argv[1]
    sf = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    out = ROOT / "data" / "{}_sf{}".format(bench, sf)
    out.mkdir(parents=True, exist_ok=True)
    db = ROOT / "data" / "{}_sf{}.duckdb".format(bench, sf)
    con = duckdb.connect(str(db))
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=8")
    con.execute("SET temp_directory='{}'".format(ROOT / "data" / "duckdb_tmp"))
    start = time.time()
    con.execute("INSTALL {0}; LOAD {0};".format(bench))
    if not con.execute("SELECT count(*) FROM information_schema.tables").fetchone()[0]:
        con.execute("CALL {}(sf={})".format("dbgen" if bench == "tpch" else "dsdgen", sf))
    gen_s = time.time() - start
    tables = [r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables ORDER BY 1").fetchall()]
    manifest = {"benchmark": bench, "scale_factor": sf, "duckdb_version": duckdb.__version__,
                "generate_seconds": round(gen_s, 1), "tables": {}}
    for t in tables:
        rows = con.execute("SELECT count(*) FROM {}".format(t)).fetchone()[0]
        target = out / t
        if not target.exists():
            # several files per large table (one per thread), row groups of 122880 rows
            con.execute("COPY {} TO '{}' (FORMAT parquet, PER_THREAD_OUTPUT true, "
                        "COMPRESSION snappy)".format(t, target))
        manifest["tables"][t] = {"rows": rows, "files": len(list(target.glob("*.parquet")))}
        print(t, rows, flush=True)
    manifest["total_seconds"] = round(time.time() - start, 1)
    (out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
