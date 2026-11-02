"""Incremental SCD2 merge: apply only the new CDC events to an existing dimension.

The full rebuild in scd2_merge.py re-reads all of bronze every run. Once bronze holds months
of events that gets slow and expensive, so the daily job reads just the new event_date
partitions and merges them into the existing dim_customer_scd2.

How it works
  1. normalize + dedupe the new events, and collect the customer_ids they touch
  2. split the existing dimension: untouched customers are copied through as-is
  3. for touched customers, turn their existing versions back into a change log:
       - every version becomes a change at (source_lsn, valid_from) with its values
       - a version that was ended by a delete also emits a 'd' change at
         (closed_by_lsn, valid_to)
  4. union that log with the new events, dedupe on (customer_id, lsn) again (a replayed
     batch overlaps what is already in the dim), and run the same build_scd2
  5. result = untouched rows + rebuilt rows for touched customers

Because the touched customers are rebuilt from their whole history, late events are not a
special case: an event whose LSN falls between two existing versions is slotted in, the
earlier version gets a new valid_to, and the later one keeps its own. Delete followed by
re-create (same customer_id) gives two separate version chains with a gap between them.

Known limit: updates that were dropped earlier as no-ops are not stored in the dimension,
so a late event that would make one of them a real change again cannot bring it back.
A periodic full rebuild from bronze (weekly, say) reconciles that rare case.

Run locally:
    python -m transforms.scd2_incremental --dim data/output/silver/dim_customer_scd2 \\
        --bronze data/output/bronze/customers_cdc --since 2026-10-02 \\
        --out data/output/silver/dim_customer_scd2_next
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from transforms.scd2_merge import BRONZE_SCHEMA, ROW_FIELDS, build_scd2, dedupe, normalize

DIM_COLUMNS = ["customer_sk", *ROW_FIELDS, "valid_from", "valid_to", "is_current",
               "source_lsn", "source_op", "closed_by_lsn", "ended_by_delete", "row_hash"]


def dim_to_changes(dim: DataFrame) -> DataFrame:
    """Rebuild the change log (same shape as normalize()) from SCD2 versions."""
    versions = dim.select(*ROW_FIELDS, F.col("source_op").alias("op"),
                          F.col("source_lsn").alias("lsn"), F.col("valid_from").alias("change_ts"))
    deletes = (dim.where(F.col("ended_by_delete"))
               .select(*ROW_FIELDS, F.lit("d").alias("op"),
                       F.col("closed_by_lsn").alias("lsn"), F.col("valid_to").alias("change_ts")))
    return versions.unionByName(deletes)


def merge_incremental(dim: DataFrame, new_changes: DataFrame) -> DataFrame:
    """Merge already-normalized new change rows into an existing SCD2 dimension."""
    new_changes = dedupe(new_changes)
    touched = new_changes.select("customer_id").distinct()

    untouched = dim.join(touched, "customer_id", "left_anti")
    affected = dim.join(touched, "customer_id", "left_semi")

    history = dim_to_changes(affected).unionByName(new_changes)
    rebuilt = build_scd2(dedupe(history))

    return untouched.select(*DIM_COLUMNS).unionByName(rebuilt.select(*DIM_COLUMNS))


def read_new_events(spark: SparkSession, bronze_path: str, since: str) -> DataFrame:
    """Only the event_date partitions on or after `since` (partition pruning on the path)."""
    return read_bronze_partitions(spark, bronze_path).where(F.col("event_date") >= since)


def read_bronze_partitions(spark: SparkSession, bronze_path: str) -> DataFrame:
    # copy, not BRONZE_SCHEMA.add(): StructType.add mutates the shared schema in place
    schema = T.StructType(BRONZE_SCHEMA.fields + [T.StructField("event_date", T.StringType())])
    return spark.read.schema(schema).option("basePath", bronze_path).json(bronze_path)


def run(spark: SparkSession, dim_path: str, bronze_path: str, since: str, out_path: str | None = None) -> DataFrame:
    dim = spark.read.parquet(dim_path)
    new = normalize(read_new_events(spark, bronze_path, since))
    merged = merge_incremental(dim, new)
    if out_path:
        # write to a new path and swap; overwriting the path we are reading from would lose data
        merged.write.mode("overwrite").parquet(out_path)
    return merged


def main(argv=None):
    p = argparse.ArgumentParser(description="Merge new bronze CDC events into dim_customer_scd2")
    p.add_argument("--dim", default="data/output/silver/dim_customer_scd2")
    p.add_argument("--bronze", default="data/output/bronze/customers_cdc")
    p.add_argument("--since", required=True, help="first event_date partition to read (YYYY-MM-DD)")
    p.add_argument("--out", default="data/output/silver/dim_customer_scd2_next")
    a = p.parse_args(argv)
    if a.out == a.dim:
        p.error("--out must differ from --dim (write next version, then swap)")
    spark = (SparkSession.builder.appName("cdc-scd2-incremental")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    merged = run(spark, a.dim, a.bronze, a.since, a.out)
    print(f"dim_customer_scd2: {merged.count()} versions, "
          f"{merged.where('is_current').count()} current -> {a.out}")
    spark.stop()


if __name__ == "__main__":
    main()
