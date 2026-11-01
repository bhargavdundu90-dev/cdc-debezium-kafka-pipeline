"""Build the SCD Type 2 customer dimension from bronze CDC events (PySpark).

Steps
  1. read bronze JSONL (one Debezium envelope per line + ingestion columns)
  2. pick the row image: `after` for r/c/u, `before` for d (after is null on delete)
  3. dedupe on (customer_id, lsn): Kafka is at-least-once, so the same change can land twice
  4. order each customer's changes by LSN, never by arrival order or Kafka offset
  5. drop "no-op" updates where none of the tracked columns changed
  6. derive valid_from / valid_to with a window: a version is valid until the next change
     (an update or a delete) for the same customer
  7. delete events close the open version and do not become a version themselves

This is a full rebuild from bronze, which is cheap at this size and always correct
because bronze is the complete replayable log. Incremental merge against an existing
dimension and late events after a run are handled separately.

Run locally:
    python -m transforms.scd2_merge --bronze data/output/bronze/customers_cdc --out data/output/silver/dim_customer_scd2
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

ROW_FIELDS = ["customer_id", "full_name", "email", "city", "branch_code", "risk_tier", "kyc_status", "updated_at"]
# columns whose change creates a new version; updated_at alone is not a business change
TRACKED = ["full_name", "email", "city", "branch_code", "risk_tier", "kyc_status"]
OPEN_END = "9999-12-31 23:59:59"

_ROW = T.StructType([T.StructField("customer_id", T.LongType())]
                    + [T.StructField(c, T.StringType()) for c in ROW_FIELDS[1:]])
BRONZE_SCHEMA = T.StructType([
    T.StructField("before", _ROW),
    T.StructField("after", _ROW),
    T.StructField("source", T.StructType([
        T.StructField("lsn", T.LongType()),
        T.StructField("txId", T.LongType()),
        T.StructField("ts_ms", T.LongType()),
        T.StructField("snapshot", T.StringType()),
    ])),
    T.StructField("op", T.StringType()),
    T.StructField("ts_ms", T.LongType()),
    T.StructField("_kafka_offset", T.LongType()),
    T.StructField("_ingested_at", T.StringType()),
])


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    # explicit schema: no inference pass over the files, and a missing field is null, not an error
    return spark.read.schema(BRONZE_SCHEMA).json(path)


def normalize(events: DataFrame) -> DataFrame:
    """One flat row per change event, keyed by customer_id and LSN."""
    image = F.when(F.col("op") == "d", F.col("before")).otherwise(F.col("after"))
    return events.select(
        *[image[c].alias(c) for c in ROW_FIELDS],
        F.col("op"),
        F.col("source.lsn").alias("lsn"),
        (F.col("source.ts_ms") / 1000).cast("timestamp").alias("change_ts"),
    ).where(F.col("customer_id").isNotNull())


def dedupe(changes: DataFrame) -> DataFrame:
    """At-least-once delivery means exact replays of a change; the LSN identifies a change."""
    return changes.dropDuplicates(["customer_id", "lsn"])


def build_scd2(changes: DataFrame) -> DataFrame:
    by_customer = Window.partitionBy("customer_id").orderBy("lsn")

    hashed = changes.withColumn(
        "row_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c), F.lit("")) for c in TRACKED]), 256))

    # drop updates that change nothing we track (e.g. only updated_at was touched)
    prev_hash = F.lag("row_hash").over(by_customer)
    prev_op = F.lag("op").over(by_customer)
    is_noop = (F.col("op") == "u") & (prev_op != "d") & (F.col("row_hash") == prev_hash)
    versions = hashed.withColumn("_noop", is_noop).where(~F.col("_noop")).drop("_noop")

    nxt = Window.partitionBy("customer_id").orderBy("lsn")
    versions = (versions
                .withColumn("valid_from", F.col("change_ts"))
                .withColumn("valid_to", F.lead("change_ts").over(nxt))
                .where(F.col("op") != "d"))  # a delete only closes the previous version

    return (versions
            .withColumn("is_current", F.col("valid_to").isNull())
            .withColumn("valid_to", F.coalesce(F.col("valid_to"), F.to_timestamp(F.lit(OPEN_END))))
            .withColumn("customer_sk", F.sha2(F.concat_ws(":", "customer_id", "lsn"), 256).substr(1, 16))
            .select("customer_sk", *ROW_FIELDS, "valid_from", "valid_to", "is_current",
                    F.col("lsn").alias("source_lsn"), F.col("op").alias("source_op"), "row_hash"))


def run(spark: SparkSession, bronze_path: str, out_path: str | None = None) -> DataFrame:
    dim = build_scd2(dedupe(normalize(read_bronze(spark, bronze_path))))
    if out_path:
        dim.write.mode("overwrite").parquet(out_path)
    return dim


def main(argv=None):
    p = argparse.ArgumentParser(description="Bronze CDC events -> SCD2 customer dimension")
    p.add_argument("--bronze", default="data/output/bronze/customers_cdc")
    p.add_argument("--out", default="data/output/silver/dim_customer_scd2")
    a = p.parse_args(argv)
    spark = (SparkSession.builder.appName("cdc-scd2-merge")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    dim = run(spark, a.bronze, a.out)
    total = dim.count()
    current = dim.where("is_current").count()
    print(f"dim_customer_scd2: {total} versions, {current} current customers -> {a.out}")
    spark.stop()


if __name__ == "__main__":
    main()
