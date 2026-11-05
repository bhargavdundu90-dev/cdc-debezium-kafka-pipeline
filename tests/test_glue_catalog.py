"""Glue catalog promote / rollback against moto (no AWS account needed)."""
import boto3
import pytest
from moto import mock_aws

from pipeline.glue_catalog import GlueCatalog

COLUMNS = [{"Name": "customer_sk", "Type": "string"}, {"Name": "customer_id", "Type": "bigint"},
           {"Name": "valid_from", "Type": "timestamp"}, {"Name": "is_current", "Type": "boolean"}]
ROOT = "s3://lake/silver/dim_customer_scd2"


@pytest.fixture
def glue():
    with mock_aws():
        client = boto3.client("glue", region_name="ap-south-1")
        client.create_database(DatabaseInput={"Name": "bank_cdc"})
        client.create_table(DatabaseName="bank_cdc", TableInput={
            "Name": "dim_customer_scd2",
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": "parquet"},
            "StorageDescriptor": {
                "Columns": COLUMNS,
                "Location": f"{ROOT}/_empty/",
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {"SerializationLibrary":
                              "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
            },
        })
        yield client


def table(glue):
    return glue.get_table(DatabaseName="bank_cdc", Name="dim_customer_scd2")["Table"]


def test_placeholder_table_has_no_live_version(glue):
    assert GlueCatalog("bank_cdc", "dim_customer_scd2", glue).current_location() is None


def test_promote_moves_location_and_keeps_schema(glue):
    cat = GlueCatalog("bank_cdc", "dim_customer_scd2", glue)
    sent = cat.promote(f"{ROOT}/v=2026-10-01/", "v=2026-10-01", "2026-10-01")

    t = table(glue)
    assert t["StorageDescriptor"]["Location"] == f"{ROOT}/v=2026-10-01/"
    assert t["StorageDescriptor"]["Columns"] == COLUMNS
    assert t["Parameters"]["classification"] == "parquet"
    assert t["Parameters"]["cdc_live_version"] == "v=2026-10-01"
    assert cat.current_location() == f"{ROOT}/v=2026-10-01/"
    # read-only fields from GetTable must not be sent back
    assert not {"DatabaseName", "CreateTime", "UpdateTime", "CatalogId"} & set(sent)


def test_second_promote_records_previous_and_rollback_returns_to_it(glue):
    cat = GlueCatalog("bank_cdc", "dim_customer_scd2", glue)
    cat.promote(f"{ROOT}/v=2026-10-01/", "v=2026-10-01", "2026-10-01")
    cat.promote(f"{ROOT}/v=2026-10-02/", "v=2026-10-02", "2026-10-02")
    assert table(glue)["Parameters"]["cdc_previous_version"] == "v=2026-10-01"

    loc = cat.rollback(ROOT, "rollback-2026-10-02")
    assert loc == f"{ROOT}/v=2026-10-01/"
    assert cat.live_version() == "v=2026-10-01"
    assert table(glue)["StorageDescriptor"]["Location"] == loc


def test_rollback_without_history_fails_loudly(glue):
    cat = GlueCatalog("bank_cdc", "dim_customer_scd2", glue)
    cat.promote(f"{ROOT}/v=2026-10-01/", "v=2026-10-01", "2026-10-01")
    with pytest.raises(RuntimeError):
        cat.rollback(ROOT, "x")
