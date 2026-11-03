import json
from datetime import datetime

from pyspark.sql import functions as F

from generator.cdc_events import generate_events
from quality.dim_checks import has_errors, main as dq_main, run_checks, to_report
from tests.test_scd2_merge import ev
from transforms.scd2_merge import BRONZE_SCHEMA, build_scd2, dedupe, normalize


def changes_of(spark, events):
    df = spark.createDataFrame([json.dumps(e) for e in events], "string")
    return normalize(spark.read.schema(BRONZE_SCHEMA).json(df.rdd.map(lambda r: r[0])))


def by_name(results):
    return {r.name: r for r in results}


BASE = [ev("r", 1, 10, 0), ev("u", 1, 20, 30, risk_tier="HIGH"),
        ev("r", 2, 11, 0), ev("d", 2, 30, 40),
        ev("c", 2, 40, 60, kyc_status="PENDING"),  # account re-opened with the same id
        ev("r", 3, 12, 0), ev("u", 3, 50, 70, updated_at="touched")]  # trailing no-op


def good_dim(spark):
    changes = changes_of(spark, BASE)
    return build_scd2(dedupe(changes)), changes


def test_clean_dimension_passes_every_check(spark):
    dim, changes = good_dim(spark)
    results = run_checks(dim, changes)
    assert [r.name for r in results if not r.passed] == []
    assert to_report(results)["status"] == "PASS"


def test_generated_stream_passes_every_check(spark):
    events = generate_events(n_snapshot=15, n_changes=120, seed=5, duplicate_rate=0.1, late_rate=0.1)
    changes = changes_of(spark, events)
    results = run_checks(build_scd2(dedupe(changes)), changes)
    assert not has_errors(results), [r for r in results if not r.passed]


def test_two_current_rows_for_one_customer(spark):
    dim, changes = good_dim(spark)
    broken = dim.withColumn("is_current", F.when(F.col("customer_id") == 1, True).otherwise(F.col("is_current")))
    r = by_name(run_checks(broken, changes))
    assert r["one_current_per_customer"].failed == 1
    assert r["one_current_per_customer"].sample == [{"customer_id": 1, "count": 2}]
    # the closed row now says current but still has a real end date
    assert r["current_flag_matches_end"].failed == 1
    assert has_errors(r.values())


def test_overlapping_versions(spark):
    dim, _ = good_dim(spark)
    # stretch customer 1's first version past the start of the second
    broken = dim.withColumn(
        "valid_to",
        F.when((F.col("customer_id") == 1) & ~F.col("is_current"), F.to_timestamp(F.lit("2026-10-01 12:00:00")))
         .otherwise(F.col("valid_to")))
    r = by_name(run_checks(broken))
    assert r["no_overlap"].failed == 1
    assert r["no_overlap"].sample[0]["customer_id"] == 1


def test_gap_is_only_allowed_after_a_delete(spark):
    dim, _ = good_dim(spark)
    assert by_name(run_checks(dim))["no_gap_without_delete"].passed  # customer 2 gap comes from the delete

    broken = dim.withColumn(
        "valid_to",
        F.when((F.col("customer_id") == 1) & ~F.col("is_current"), F.to_timestamp(F.lit("2026-10-01 09:10:00")))
         .otherwise(F.col("valid_to")))
    gap = by_name(run_checks(broken))["no_gap_without_delete"]
    assert gap.failed == 1 and gap.severity == "warn"
    assert not has_errors([gap])


def test_inverted_window_and_null_keys(spark):
    dim, _ = good_dim(spark)
    broken = (dim.withColumn("valid_from", F.when(F.col("customer_id") == 3, F.to_timestamp(F.lit("9999-12-31 23:59:59")))
                             .otherwise(F.col("valid_from")))
                 .withColumn("source_lsn", F.when(F.col("customer_id") == 2, F.lit(None).cast("long"))
                             .otherwise(F.col("source_lsn"))))
    r = by_name(run_checks(broken))
    assert r["valid_window"].failed == 1
    assert r["not_null_keys"].failed == 2


def test_duplicate_surrogate_key(spark):
    dim, _ = good_dim(spark)
    r = by_name(run_checks(dim.unionByName(dim.where("customer_id = 3"))))
    assert r["unique_customer_sk"].failed == 1


def test_deleted_version_still_current(spark):
    dim, _ = good_dim(spark)
    broken = dim.withColumn("is_current", F.col("is_current") | F.col("ended_by_delete"))
    assert by_name(run_checks(broken))["deleted_not_current"].failed == 1


def test_unknown_codes_are_a_warning(spark):
    dim, _ = good_dim(spark)
    broken = dim.withColumn("risk_tier", F.when(F.col("customer_id") == 3, "VERY_HIGH").otherwise(F.col("risk_tier")))
    r = by_name(run_checks(broken))
    assert r["accepted_values"].failed == 1 and r["accepted_values"].severity == "warn"
    assert not has_errors(r.values())


def test_reconcile_catches_missing_and_stale_current_rows(spark):
    dim, changes = good_dim(spark)
    # lost customer 1's current row, and customer 3 still shows an old value
    broken = (dim.where(~((F.col("customer_id") == 1) & F.col("is_current")))
                 .withColumn("city", F.when(F.col("customer_id") == 3, "Chennai").otherwise(F.col("city"))))
    rec = by_name(run_checks(broken, changes))["reconcile_with_source"]
    sides = sorted((s["customer_id"], s["side"]) for s in rec.sample)
    assert sides == [(1, "missing_in_dim"), (3, "stale_values")]


def test_cli_writes_report_and_exits_non_zero_on_errors(spark, tmp_path):
    dim, _ = good_dim(spark)
    good, bad = tmp_path / "good", tmp_path / "bad"
    dim.write.parquet(str(good))
    dim.unionByName(dim.where("customer_id = 3")).write.parquet(str(bad))

    report = tmp_path / "dq.json"
    assert dq_main(["--dim", str(good), "--report", str(report)], spark=spark) == 0
    assert json.loads(report.read_text())["status"] == "PASS"
    assert dq_main(["--dim", str(bad), "--report", str(report)], spark=spark) == 1
    failed = [c["name"] for c in json.loads(report.read_text())["checks"] if not c["passed"]]
    assert "unique_customer_sk" in failed and "one_current_per_customer" in failed
