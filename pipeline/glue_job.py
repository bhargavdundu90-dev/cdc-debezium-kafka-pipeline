"""Glue job entry point for the daily SCD2 build on AWS.

On AWS the bronze writer runs on its own (consuming MSK and writing to S3), so this job
starts from bronze that is already landed:

    read live version from the Glue table
      -> full build (first run, or --mode full for the weekly reconciliation)
         or incremental merge of event_date partitions >= run_date - lookback_days
      -> write s3://.../dim_customer_scd2/v=<run_id>/
      -> DQ checks, report to <table_root>/_dq/<run_id>.json
      -> UpdateTable to the new version only on PASS

Glue passes job arguments as `--key value`; they are parsed with argparse instead of
awsglue.utils.getResolvedOptions so the same entry point runs under local Spark in tests.

The lookback replaces "oldest partition landed by this run" from the local daily run:
landing is continuous here, so the job re-reads the last few event_date partitions to pick
up late events. Re-reading is safe because of the LSN dedupe. Anything later than the
lookback is picked up by the weekly full rebuild.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession

from pipeline.glue_catalog import GlueCatalog
from quality.dim_checks import run_checks, to_report
from transforms.scd2_incremental import merge_incremental, read_new_events
from transforms.scd2_merge import normalize, read_bronze, run as full_build


def since_for(run_date: str, lookback_days: int) -> str:
    return (date.fromisoformat(run_date) - timedelta(days=lookback_days)).isoformat()


def write_text(spark: SparkSession, path: str, body: str) -> None:
    """Write a small text file through Hadoop FS, so the same call works for file:// and s3://."""
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    out = fs.create(hpath, True)
    out.write(bytearray(body.encode("utf-8")))
    out.close()


def run_job(spark: SparkSession, catalog: GlueCatalog, bronze_path: str, table_root: str, run_id: str,
            mode: str = "auto", lookback_days: int = 2) -> dict:
    table_root = table_root.rstrip("/")
    live = catalog.current_location()
    version = f"v={run_id}"
    version_path = f"{table_root}/{version}"

    if live is None or mode == "full":
        used_mode, since = "full", None
        full_build(spark, bronze_path, version_path)
    else:
        used_mode, since = "incremental", since_for(run_id, lookback_days)
        new = normalize(read_new_events(spark, bronze_path, since))
        merge_incremental(spark.read.parquet(live), new).write.mode("overwrite").parquet(version_path)

    results = run_checks(spark.read.parquet(version_path), normalize(read_bronze(spark, bronze_path)))
    report = to_report(results)
    write_text(spark, f"{table_root}/_dq/{run_id}.json", json.dumps(report, indent=2))

    promoted = report["status"] == "PASS"
    if promoted:
        catalog.promote(version_path + "/", version, run_id)
    return {
        "run_id": run_id, "mode": used_mode, "since": since, "version_path": version_path,
        "status": report["status"], "promoted": promoted, "live": catalog.current_location(),
        "failed_checks": [c["name"] for c in report["checks"]
                          if not c["passed"] and c["severity"] == "error"],
    }


def parse_args(argv):
    p = argparse.ArgumentParser(description="Glue job: bronze -> SCD2 version -> DQ -> UpdateTable")
    p.add_argument("--bronze_path", required=True, help="s3://.../bronze/customers_cdc")
    p.add_argument("--table_root", required=True, help="s3://.../silver/dim_customer_scd2")
    p.add_argument("--database", required=True)
    p.add_argument("--table", default="dim_customer_scd2")
    p.add_argument("--run_id", default=None, help="defaults to today's UTC date")
    p.add_argument("--mode", choices=["auto", "full"], default="auto")
    p.add_argument("--lookback_days", type=int, default=2)
    # Glue adds its own arguments (--JOB_ID, --job-bookmark-option, ...); ignore them
    args, _ = p.parse_known_args(argv)
    return args


def main(argv=None) -> int:
    a = parse_args(sys.argv[1:] if argv is None else argv)
    run_id = a.run_id or date.today().isoformat()
    spark = (SparkSession.builder.appName("cdc-scd2-daily")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    res = run_job(spark, GlueCatalog(a.database, a.table), a.bronze_path, a.table_root, run_id,
                  a.mode, a.lookback_days)
    print(json.dumps(res, indent=2))
    # a failed DQ run fails the job so the trigger/alarm sees it; the old version stays live
    return 1 if res["status"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
