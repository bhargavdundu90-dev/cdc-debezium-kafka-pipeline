"""Glue job entry point under local Spark, with the catalog in moto and data on local disk."""
import boto3
import pytest
from moto import mock_aws

from generator.cdc_events import generate_events, write_jsonl
from ingestion.bronze_writer import TABLE, JsonlSource, LocalSink, run as land
from pipeline import glue_job as gj
from pipeline.glue_catalog import GlueCatalog
from tests.test_daily_run import _day
from tests.test_scd2_incremental import full, snapshot

pytestmark = pytest.mark.integration


def test_since_uses_lookback_from_run_date():
    assert gj.since_for("2026-10-02", 2) == "2026-09-30"
    assert gj.since_for("2026-03-01", 1) == "2026-02-28"


def test_glue_arguments_parse_and_ignore_glue_extras():
    a = gj.parse_args(["--JOB_ID", "j_1", "--bronze_path", "s3://b/bronze", "--table_root", "s3://b/silver/t",
                       "--database", "bank_cdc", "--job-bookmark-option", "job-bookmark-disable",
                       "--lookback_days", "3"])
    assert (a.bronze_path, a.database, a.table, a.mode, a.lookback_days) == \
        ("s3://b/bronze", "bank_cdc", "dim_customer_scd2", "auto", 3)


@pytest.fixture
def catalog():
    with mock_aws():
        glue = boto3.client("glue", region_name="ap-south-1")
        glue.create_database(DatabaseInput={"Name": "bank_cdc"})
        glue.create_table(DatabaseName="bank_cdc", TableInput={
            "Name": "dim_customer_scd2", "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {"Columns": [], "Location": "s3://placeholder/"}})
        yield GlueCatalog("bank_cdc", "dim_customer_scd2", glue)


def test_first_run_full_then_incremental_promotes_through_catalog(spark, tmp_path, catalog):
    events = generate_events(n_snapshot=12, n_changes=150, seed=5, duplicate_rate=0.05)
    day1 = [e for e in events if _day(e) == "2026-10-01"]
    day2 = [e for e in events if _day(e) != "2026-10-01"]
    bronze_root, table_root = tmp_path / "bronze", tmp_path / "dim"
    bronze = str(bronze_root / TABLE)

    land(JsonlSource(write_jsonl(day1, tmp_path / "d1.jsonl")), LocalSink(bronze_root))
    r1 = gj.run_job(spark, catalog, bronze, str(table_root), "2026-10-01")
    assert (r1["mode"], r1["status"], r1["promoted"]) == ("full", "PASS", True)
    assert catalog.live_version() == "v=2026-10-01"

    land(JsonlSource(write_jsonl(day2, tmp_path / "d2.jsonl"), start_offset=len(day1)), LocalSink(bronze_root))
    r2 = gj.run_job(spark, catalog, bronze, str(table_root), "2026-10-02")
    assert (r2["mode"], r2["since"], r2["promoted"]) == ("incremental", "2026-09-30", True)
    assert catalog.live_version() == "v=2026-10-02"
    assert (table_root / "_dq/2026-10-02.json").exists()

    live = spark.read.parquet(catalog.current_location())
    assert snapshot(live) == snapshot(full(spark, events))
