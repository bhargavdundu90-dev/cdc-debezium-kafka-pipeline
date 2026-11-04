"""Integration test: generator -> bronze -> full build -> incremental runs -> DQ -> promote.

Everything goes through files on disk the way the daily job does, so this catches problems
the unit tests cannot: partition paths, the --since choice, parquet round trips and the
promote step.
"""
import json
from datetime import datetime, timezone

import pytest

from generator.cdc_events import generate_events, write_jsonl
from pipeline import daily_run as dr
from tests.test_scd2_incremental import full, snapshot

pytestmark = pytest.mark.integration


def _day(ev):
    return datetime.fromtimestamp(ev["source"]["ts_ms"] / 1000, tz=timezone.utc).date().isoformat()


@pytest.fixture(scope="module")
def stream():
    """~2 days of changes with replays; split into what arrived on each run."""
    events = generate_events(n_snapshot=15, n_changes=220, seed=21, duplicate_rate=0.05)
    day1 = [e for e in events if _day(e) == "2026-10-01"]
    day2 = [e for e in events if _day(e) != "2026-10-01"]
    # the last three day-one changes were stuck in a retry and arrive with day two,
    # and two day-one events are redelivered after a consumer restart
    late = day1[-3:]
    day1 = day1[:-3]
    day2 = late + day1[:2] + day2
    assert day2 and day1
    return events, day1, day2


def test_two_daily_runs_promote_and_match_full_rebuild(spark, tmp_path, stream):
    events, day1, day2 = stream
    bronze, table = tmp_path / "bronze", tmp_path / "dim"
    f1 = write_jsonl(day1, tmp_path / "in/day1.jsonl")
    f2 = write_jsonl(day2, tmp_path / "in/day2.jsonl")

    r1 = dr.daily_run(spark, str(f1), str(bronze), str(table), "2026-10-01")
    assert (r1.mode, r1.status, r1.promoted) == ("full", "PASS", True)
    assert r1.live_path.endswith("v=2026-10-01")

    r2 = dr.daily_run(spark, str(f2), str(bronze), str(table), "2026-10-02", start_offset=len(day1))
    assert (r2.mode, r2.status, r2.promoted) == ("incremental", "PASS", True)
    # late day-one events are in the 2026-10-01 partition, so the merge must start there
    assert r2.since == "2026-10-01"
    assert r2.landed == len(day2)

    live = spark.read.parquet(dr.current_location(table))
    assert snapshot(live) == snapshot(full(spark, events))
    # the previous version is kept for rollback / time travel
    assert (table / "v=2026-10-01").exists()
    assert json.loads((table / "_dq/2026-10-02.json").read_text())["status"] == "PASS"


def test_rerunning_the_same_day_is_idempotent(spark, tmp_path, stream):
    _, day1, day2 = stream
    bronze, table = tmp_path / "bronze", tmp_path / "dim"
    f1 = write_jsonl(day1, tmp_path / "day1.jsonl")
    f2 = write_jsonl(day2, tmp_path / "day2.jsonl")
    dr.daily_run(spark, str(f1), str(bronze), str(table), "2026-10-01")
    dr.daily_run(spark, str(f2), str(bronze), str(table), "2026-10-02", start_offset=len(day1))
    once = snapshot(spark.read.parquet(dr.current_location(table)))

    # orchestrator retries day two after a timeout: same input, same offsets, new run id
    again = dr.daily_run(spark, str(f2), str(bronze), str(table), "2026-10-02-retry", start_offset=len(day1))
    assert again.status == "PASS"
    assert snapshot(spark.read.parquet(dr.current_location(table))) == once


def test_failed_checks_keep_yesterdays_table_live(spark, tmp_path, stream, monkeypatch):
    _, day1, day2 = stream
    bronze, table = tmp_path / "bronze", tmp_path / "dim"
    f1 = write_jsonl(day1, tmp_path / "day1.jsonl")
    f2 = write_jsonl(day2, tmp_path / "day2.jsonl")
    dr.daily_run(spark, str(f1), str(bronze), str(table), "2026-10-01")

    # simulate a merge bug that duplicates every current row
    real_merge = dr.merge_incremental
    monkeypatch.setattr(dr, "merge_incremental",
                        lambda dim, new: (lambda m: m.unionByName(m.where("is_current")))(real_merge(dim, new)))
    r2 = dr.daily_run(spark, str(f2), str(bronze), str(table), "2026-10-02", start_offset=len(day1))

    assert (r2.status, r2.promoted) == ("FAIL", False)
    assert "one_current_per_customer" in r2.failed_checks
    assert r2.live_path.endswith("v=2026-10-01")
    assert (table / "_dq/2026-10-02.json").exists()  # report kept for whoever is on call


def test_run_with_only_bad_events_keeps_table_and_skips_build(spark, tmp_path, stream):
    _, day1, _ = stream
    bronze, table = tmp_path / "bronze", tmp_path / "dim"
    dr.daily_run(spark, str(write_jsonl(day1, tmp_path / "day1.jsonl")), str(bronze), str(table), "2026-10-01")
    bad = tmp_path / "bad.jsonl"
    bad.write_text('{"op": "x"}\n{not json\n')
    r = dr.daily_run(spark, str(bad), str(bronze), str(table), "2026-10-02", start_offset=len(day1))
    assert (r.mode, r.status, r.rejected, r.promoted) == ("skipped", "NO_DATA", 2, False)
    assert r.live_path.endswith("v=2026-10-01")
    assert not (table / "v=2026-10-02").exists()


def test_promote_replaces_pointer(tmp_path):
    assert dr.current_location(tmp_path) is None
    dr.promote(tmp_path, "v=a")
    dr.promote(tmp_path, "v=b")
    assert dr.current_location(tmp_path) == str(tmp_path / "v=b")
    assert not (tmp_path / "_CURRENT.tmp").exists()


def test_split_days_writes_one_file_per_commit_date(tmp_path):
    from pipeline.split_days import split
    events = generate_events(n_snapshot=5, n_changes=220, seed=21)
    counts = split(write_jsonl(events, tmp_path / "all.jsonl"), tmp_path / "in")
    assert sum(counts.values()) == len(events)
    assert sorted(p.stem for p in (tmp_path / "in").glob("*.jsonl")) == list(counts)
