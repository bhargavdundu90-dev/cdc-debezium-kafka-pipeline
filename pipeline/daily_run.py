"""One daily run of the CDC pipeline, end to end, the way the orchestrator calls it.

    land new events in bronze
      -> build the next dimension version (full build on the first run, incremental after)
      -> data-quality checks on that version
      -> promote it only if no `error` check failed

Versions are written to `<table_root>/v=<run_id>/` and never overwritten. `_CURRENT` in
the table root holds the live version's folder name; readers resolve it with
`current_location()`. Locally this file plays the part of the Glue table location: on AWS,
`promote()` becomes an `UpdateTable` call that points the table at the new S3 prefix,
so Athena users switch over atomically and a failed run leaves yesterday's table live.

The incremental read starts at the oldest event_date partition this run landed, not at the
run date: a late event is partitioned by the day it happened, so starting at "today" would
skip it.

Run locally:
    python -m generator.cdc_events --snapshot 20 --changes 250 --duplicate-rate 0.05 --out data/output/in/all.jsonl
    python -m pipeline.split_days data/output/in/all.jsonl data/output/in      # one file per arrival day
    python -m pipeline.daily_run --input data/output/in/2026-10-01.jsonl --run-id 2026-10-01
    # the next file continues the offsets of the first one
    python -m pipeline.daily_run --input data/output/in/2026-10-02.jsonl --run-id 2026-10-02 \
        --start-offset $(wc -l < data/output/in/2026-10-01.jsonl)
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import SparkSession

from ingestion.bronze_writer import TABLE, JsonlSource, LocalSink, event_dates, run as land_bronze
from quality.dim_checks import run_checks, to_report
from transforms.scd2_incremental import merge_incremental, read_new_events
from transforms.scd2_merge import normalize, read_bronze, run as full_build

POINTER = "_CURRENT"


@dataclass
class RunResult:
    run_id: str
    mode: str                     # "full" | "incremental" | "skipped"
    landed: int
    rejected: int
    since: str | None
    version_path: str | None
    status: str                   # "PASS" | "FAIL" | "NO_DATA"
    promoted: bool
    live_path: str | None
    failed_checks: list = field(default_factory=list)


def current_location(table_root) -> str | None:
    pointer = Path(table_root) / POINTER
    if not pointer.exists():
        return None
    return str(Path(table_root) / pointer.read_text(encoding="utf-8").strip())


def promote(table_root, version: str) -> None:
    """Point the table at a new version; write-then-rename so a reader never sees half a pointer."""
    root = Path(table_root)
    tmp = root / f"{POINTER}.tmp"
    tmp.write_text(version, encoding="utf-8")
    tmp.replace(root / POINTER)


def daily_run(spark: SparkSession, input_file: str, bronze_root: str, table_root: str, run_id: str,
              start_offset: int = 0, batch_size: int = 500) -> RunResult:
    landed = land_bronze(JsonlSource(input_file, start_offset=start_offset), LocalSink(bronze_root),
                         batch_size=batch_size)
    bronze_table = str(Path(bronze_root) / TABLE)
    dates = event_dates(landed)
    live = current_location(table_root)

    if not dates:
        # nothing valid arrived: keep the live table, do not publish an identical copy
        return RunResult(run_id, "skipped", landed.written, landed.rejected, None, None,
                         "NO_DATA", False, live)

    version = f"v={run_id}"
    version_path = str(Path(table_root) / version)
    if live is None:
        mode, since = "full", None
        full_build(spark, bronze_table, version_path)
    else:
        mode, since = "incremental", dates[0]
        new = normalize(read_new_events(spark, bronze_table, since))
        merge_incremental(spark.read.parquet(live), new).write.mode("overwrite").parquet(version_path)

    # checks read the version back from storage, i.e. exactly what would be published
    results = run_checks(spark.read.parquet(version_path), normalize(read_bronze(spark, bronze_table)))
    report = to_report(results)
    dq_dir = Path(table_root) / "_dq"
    dq_dir.mkdir(parents=True, exist_ok=True)
    (dq_dir / f"{run_id}.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    promoted = report["status"] == "PASS"
    if promoted:
        promote(table_root, version)
    failed = [c["name"] for c in report["checks"] if not c["passed"] and c["severity"] == "error"]
    return RunResult(run_id, mode, landed.written, landed.rejected, since, version_path,
                     report["status"], promoted, current_location(table_root), failed)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Daily CDC run: bronze -> SCD2 version -> DQ -> promote")
    p.add_argument("--input", required=True, help="JSONL of change events for this run")
    p.add_argument("--run-id", required=True, help="usually the run date, e.g. 2026-10-02")
    p.add_argument("--bronze", default="data/output/bronze")
    p.add_argument("--table", default="data/output/silver/dim_customer_scd2")
    p.add_argument("--start-offset", type=int, default=0)
    a = p.parse_args(argv)
    spark = (SparkSession.builder.appName("cdc-daily-run")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    res = daily_run(spark, a.input, a.bronze, a.table, a.run_id, a.start_offset)
    spark.stop()
    print(json.dumps(res.__dict__, indent=2))
    return 1 if res.status == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
