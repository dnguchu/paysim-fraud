"""
Ingest: raw PaySim CSV -> typed Parquet partitioned by day, plus a run manifest.

Design rules
- The raw CSV is immutable input. Everything written here can be deleted and rebuilt.
- Ingest only enforces types and adds derived columns. It does NOT clean data;
  judging quality is the DQ stage's job.
- Same input + same config + same code => same output, and a re-run is a no-op.
- Output is written to a temp folder and swapped in, so a crash never leaves half-written data.

Run:  python -m pipeline.ingest --sample        (steps 1..sample_max_step)
      python -m pipeline.ingest                 (full file)
      python -m pipeline.ingest --force         (ignore the skip check)
"""
import argparse
import logging
import math
import os
import shutil
import time
from pathlib import Path

import duckdb
import yaml

from pipeline import manifest as mf

log = logging.getLogger("ingest")

# Explicit schema: never rely on type inference at this scale.
SCHEMA = {
    "step": "INTEGER",
    "type": "VARCHAR",
    "amount": "DOUBLE",
    "nameOrig": "VARCHAR",
    "oldbalanceOrg": "DOUBLE",
    "newbalanceOrig": "DOUBLE",
    "nameDest": "VARCHAR",
    "oldbalanceDest": "DOUBLE",
    "newbalanceDest": "DOUBLE",
    "isFraud": "TINYINT",
    "isFlaggedFraud": "TINYINT",
}

# Columns in the staged output. txn_id is a surrogate key (see README note below).
STAGED_SELECT = """
    txn_id,
    step,
    CAST(((step - 1) // 24) + 1 AS SMALLINT) AS day,
    CAST((step - 1) % 24 AS TINYINT)        AS hour_of_day,
    type, amount,
    nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest,
    isFraud, isFlaggedFraud
"""


def load_config(path="config.yaml") -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def _columns_clause() -> str:
    return "{" + ", ".join(f"'{k}': '{v}'" for k, v in SCHEMA.items()) + "}"


def _stats(con, relation_sql: str) -> dict:
    n, amt, fraud, smin, smax = con.execute(
        f"SELECT count(*), sum(amount), sum(isFraud), min(step), max(step) FROM {relation_sql}"
    ).fetchone()
    return {"rows": n, "sum_amount": amt, "fraud_rows": int(fraud or 0), "min_step": smin, "max_step": smax}


def _atomic_swap(tmp: Path, final: Path) -> None:
    """Replace `final` with `tmp`. Two renames on the same filesystem; the gap is tiny
    and the previous output is kept until the new one is in place."""
    old = final.with_name(final.name + ".old")
    if old.exists():
        shutil.rmtree(old)
    if final.exists():
        os.rename(final, old)
    os.rename(tmp, final)
    if old.exists():
        shutil.rmtree(old)


def ingest(cfg: dict, sample: bool = False, force: bool = False) -> dict:
    mode = "sample" if sample else "full"
    max_step = int(cfg["ingest"]["sample_max_step"]) if sample else None

    raw = Path(cfg["paths"]["raw_csv"])
    if not raw.exists():
        found = [p.name for p in raw.parent.glob("*")] if raw.parent.exists() else []
        raise FileNotFoundError(f"{raw} not found. Files in {raw.parent}: {found}. Fix paths.raw_csv in config.yaml.")

    final_dir = Path(cfg["paths"]["staged_dir"]) / mode
    tmp_dir = Path(cfg["paths"]["staged_dir"]) / f".tmp_{mode}"
    man_dir = Path(cfg["paths"]["manifest_dir"])
    latest_path = man_dir / f"ingest_{mode}_latest.json"

    # 1. Fingerprint the inputs: source bytes, run settings, schema, and this file's code.
    t0 = time.time()
    started = mf.utc_now()
    src_sha = mf.sha256_file(raw)
    code_sha = mf.sha256_file(__file__)
    fingerprint = mf.sha256_json({
        "input_sha256": src_sha, "mode": mode, "max_step": max_step,
        "schema": SCHEMA, "code_sha256": code_sha,
    })

    # 2. Skip if this exact work was already done and the output is still there.
    prev = mf.read_json(latest_path)
    if (not force and prev and prev.get("status") == "success"
            and prev.get("fingerprint") == fingerprint and final_dir.exists()):
        log.info("Skip: %s output already built for this fingerprint (%s). Use --force to rebuild.",
                 mode, fingerprint[:12])
        return prev

    # 3. Build into a temp folder.
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.parent.mkdir(parents=True, exist_ok=True)

    d = cfg["duckdb"]
    con = duckdb.connect()  # in memory; Parquet is the storage layer
    con.execute(f"SET memory_limit='{d['memory_limit']}'")
    con.execute(f"SET threads={int(d['threads'])}")
    Path(d["temp_directory"]).mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{d['temp_directory']}'")

    # txn_id = 1-based position of the row in the source file. PaySim has no transaction ID,
    # so this is our surrogate key. It is computed before any sample filter, so a row keeps
    # the same txn_id in sample and full runs.
    con.execute(f"""
        CREATE TEMP VIEW src AS
        SELECT row_number() OVER () AS txn_id, *
        FROM read_csv('{raw.as_posix()}', header = true, columns = {_columns_clause()})
    """)
    where = f"WHERE step <= {max_step}" if max_step is not None else ""

    log.info("Writing %s Parquet to %s ...", mode, tmp_dir)
    con.execute(f"""
        COPY (SELECT {STAGED_SELECT} FROM src {where} ORDER BY txn_id)
        TO '{tmp_dir.as_posix()}'
        (FORMAT parquet, COMPRESSION zstd, PARTITION_BY (day), OVERWRITE_OR_IGNORE)
    """)

    # 4. Reconcile staged output against the source before publishing it.
    src_stats = _stats(con, f"(SELECT * FROM src {where})")
    out_stats = _stats(con, f"read_parquet('{tmp_dir.as_posix()}/*/*.parquet', hive_partitioning = true)")
    checks = {
        "rows_match": src_stats["rows"] == out_stats["rows"],
        "sum_amount_match": math.isclose(src_stats["sum_amount"], out_stats["sum_amount"], rel_tol=1e-9),
        "fraud_rows_match": src_stats["fraud_rows"] == out_stats["fraud_rows"],
    }
    if not all(checks.values()):
        shutil.rmtree(tmp_dir)
        raise RuntimeError(f"Reconciliation failed, nothing published: source={src_stats} staged={out_stats} checks={checks}")

    rows_per_day = dict(con.execute(
        f"SELECT day, count(*) FROM read_parquet('{tmp_dir.as_posix()}/*/*.parquet', hive_partitioning = true) "
        "GROUP BY day ORDER BY day").fetchall())
    files = list(tmp_dir.rglob("*.parquet"))
    size_bytes = sum(f.stat().st_size for f in files)
    con.close()

    # 5. Publish, then write the manifest (last, so a manifest always means a finished run).
    _atomic_swap(tmp_dir, final_dir)

    finished = mf.utc_now()
    record = {
        "stage": "ingest",
        "status": "success",
        "run_id": f"{started:%Y%m%dT%H%M%SZ}_{fingerprint[:8]}",
        "mode": mode,
        "fingerprint": fingerprint,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "duration_s": round(time.time() - t0, 2),
        "input": {"path": raw.as_posix(), "size_bytes": raw.stat().st_size, "sha256": src_sha},
        "settings": {"max_step": max_step, "schema": SCHEMA, "duckdb": d},
        "code": {"ingest_sha256": code_sha, "git": mf.git_info()},
        "output": {"path": final_dir.as_posix(), "partitions": len(rows_per_day),
                   "files": len(files), "size_bytes": size_bytes},
        "counts": {"source": src_stats, "staged": out_stats, "rows_per_day": rows_per_day},
        "reconciliation": checks,
    }
    mf.write_json_atomic(man_dir / f"ingest_{mode}_{record['run_id']}.json", record)
    mf.write_json_atomic(latest_path, record)
    log.info("Done: %s rows, %s partitions, %.1f MB in %.1fs",
             f"{out_stats['rows']:,}", len(rows_per_day), size_bytes / 1e6, record["duration_s"])
    return record


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", action="store_true", help="only steps 1..sample_max_step")
    ap.add_argument("--force", action="store_true", help="rebuild even if the fingerprint matches")
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ingest(load_config(args.config), sample=args.sample, force=args.force)


if __name__ == "__main__":
    main()