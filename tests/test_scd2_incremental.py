import json
import random

import pytest

from generator.cdc_events import generate_events
from ingestion.bronze_writer import JsonlSource, LocalSink, run as land_bronze
from transforms.scd2_incremental import DIM_COLUMNS, merge_incremental, run as run_incremental
from transforms.scd2_merge import BRONZE_SCHEMA, build_scd2, dedupe, normalize, run as run_full
from tests.test_scd2_merge import ev


def changes_of(spark, events):
    df = spark.createDataFrame([json.dumps(e) for e in events], "string")
    return normalize(spark.read.schema(BRONZE_SCHEMA).json(df.rdd.map(lambda r: r[0])))


def full(spark, events):
    return build_scd2(dedupe(changes_of(spark, events)))


def incremental(spark, first, second):
    return merge_incremental(full(spark, first), changes_of(spark, second))


def snapshot(dim):
    """Comparable form of a dimension: sorted tuples of every column."""
    return sorted(tuple(r[c] for c in DIM_COLUMNS) for r in dim.collect())


def versions(dim, cid):
    return [r.asDict() for r in dim.where(f"customer_id = {cid}").orderBy("valid_from").collect()]


def test_new_update_closes_current_version(spark):
    first = [ev("r", 1, 10, 0), ev("r", 2, 11, 0)]
    second = [ev("u", 1, 20, 30, risk_tier="HIGH")]
    merged = incremental(spark, first, second)
    v = versions(merged, 1)
    assert [x["risk_tier"] for x in v] == ["LOW", "HIGH"]
    assert [x["is_current"] for x in v] == [False, True]
    assert v[0]["valid_to"] == v[1]["valid_from"]
    assert snapshot(merged) == snapshot(full(spark, first + second))


def test_untouched_customers_are_copied_as_is(spark):
    first = [ev("r", 1, 10, 0), ev("r", 2, 11, 0), ev("u", 2, 12, 5, city="Pune")]
    before = full(spark, first)
    merged = merge_incremental(before, changes_of(spark, [ev("u", 1, 20, 30, kyc_status="EXPIRED")]))
    assert versions(merged, 2) == versions(before, 2)


def test_late_event_is_slotted_between_existing_versions(spark):
    # yesterday's run saw LSN 10 and 30; LSN 20 was stuck in a retry and lands today
    first = [ev("r", 1, 10, 0), ev("u", 1, 30, 60, risk_tier="HIGH")]
    late = [ev("u", 1, 20, 30, kyc_status="EXPIRED")]
    merged = incremental(spark, first, late)
    v = versions(merged, 1)
    assert [x["source_lsn"] for x in v] == [10, 20, 30]
    # Debezium sends the full row image, so LSN 30 carries its own kyc_status; the late
    # change only lives between 20 and 30 and does not leak into the later version
    assert [(x["risk_tier"], x["kyc_status"]) for x in v] == [
        ("LOW", "VERIFIED"), ("LOW", "EXPIRED"), ("HIGH", "VERIFIED")]
    assert v[0]["valid_to"] == v[1]["valid_from"] and v[1]["valid_to"] == v[2]["valid_from"]
    assert snapshot(merged) == snapshot(full(spark, first + late))


def test_replayed_batch_is_idempotent(spark):
    first = [ev("r", 1, 10, 0), ev("u", 1, 20, 5, risk_tier="MEDIUM")]
    before = full(spark, first)
    merged = merge_incremental(before, changes_of(spark, first[1:]))  # same batch again
    assert snapshot(merged) == snapshot(before)


def test_delete_in_new_batch_closes_current_row(spark):
    merged = incremental(spark, [ev("r", 1, 10, 0)], [ev("d", 1, 20, 15)])
    v = versions(merged, 1)
    assert len(v) == 1 and not v[0]["is_current"] and v[0]["ended_by_delete"]
    assert v[0]["closed_by_lsn"] == 20


def test_delete_then_recreate_across_runs(spark):
    first = [ev("r", 1, 10, 0), ev("d", 1, 20, 15)]
    second = [ev("c", 1, 30, 120, kyc_status="PENDING")]
    merged = incremental(spark, first, second)
    v = versions(merged, 1)
    assert [x["source_op"] for x in v] == ["r", "c"]
    assert v[0]["ended_by_delete"] and not v[0]["is_current"]
    assert v[0]["valid_to"] < v[1]["valid_from"]  # gap while the customer did not exist
    assert v[1]["is_current"] and v[1]["kyc_status"] == "PENDING"
    assert snapshot(merged) == snapshot(full(spark, first + second))


def test_late_update_before_a_delete_keeps_the_delete(spark):
    first = [ev("r", 1, 10, 0), ev("d", 1, 30, 60)]
    late = [ev("u", 1, 20, 30, risk_tier="HIGH")]
    merged = incremental(spark, first, late)
    v = versions(merged, 1)
    assert [x["risk_tier"] for x in v] == ["LOW", "HIGH"]
    assert v[-1]["ended_by_delete"] and not any(x["is_current"] for x in v)
    assert snapshot(merged) == snapshot(full(spark, first + late))


def test_daily_runs_match_full_rebuild_on_generated_stream(spark, tmp_path):
    events = generate_events(n_snapshot=15, n_changes=120, seed=11, duplicate_rate=0.1)
    rng = random.Random(3)
    for i in range(len(events) - 1):  # late arrival inside the stream
        if rng.random() < 0.2:
            events[i], events[i + 1] = events[i + 1], events[i]
    cut = len(events) // 2
    day1, day2 = events[:cut], events[cut:]

    src1, src2 = tmp_path / "d1.jsonl", tmp_path / "d2.jsonl"
    src1.write_text("".join(json.dumps(e) + "\n" for e in day1))
    src2.write_text("".join(json.dumps(e) + "\n" for e in day2))
    bronze1, bronze2 = tmp_path / "b1", tmp_path / "b2"
    land_bronze(JsonlSource(src1), LocalSink(bronze1), batch_size=50)
    land_bronze(JsonlSource(src2), LocalSink(bronze2), batch_size=50)

    dim_path = tmp_path / "dim"
    run_full(spark, str(bronze1 / "customers_cdc"), str(dim_path))
    merged = run_incremental(spark, str(dim_path), str(bronze2 / "customers_cdc"), since="2000-01-01")

    expected = full(spark, events)
    assert merged.where("is_current").count() == expected.where("is_current").count()
    got = {r.customer_id: (r.risk_tier, r.kyc_status, r.city) for r in merged.where("is_current").collect()}
    want = {r.customer_id: (r.risk_tier, r.kyc_status, r.city) for r in expected.where("is_current").collect()}
    assert got == want
    assert snapshot(merged) == snapshot(expected)
