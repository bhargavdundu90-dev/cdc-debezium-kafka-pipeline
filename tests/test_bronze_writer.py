import json

import boto3
import pytest
from moto import mock_aws

from generator.cdc_events import generate_events, write_jsonl
from ingestion.bronze_writer import (InvalidEvent, JsonlSource, LocalSink, Record, S3Sink,
                                     run, validate, write_batch)

TOPIC = "bank.public.customers"


def _records(events, start=0):
    return [Record(TOPIC, 0, start + i, json.dumps(e)) for i, e in enumerate(events)]


def _read_rows(root):
    rows = []
    for f in sorted(root.glob("customers_cdc/event_date=*/*.jsonl")):
        rows += [json.loads(line) for line in f.read_text().splitlines()]
    return rows


def test_every_event_lands_including_duplicates(tmp_path):
    events = generate_events(n_snapshot=10, n_changes=50, seed=5, duplicate_rate=0.2)
    res = write_batch(_records(events), LocalSink(tmp_path))
    assert res.written == len(events) and res.rejected == 0
    assert len(_read_rows(tmp_path)) == len(events)  # bronze does not dedupe


def test_partitioned_by_event_date_and_enriched(tmp_path):
    events = generate_events(n_snapshot=5, n_changes=200, seed=6)
    write_batch(_records(events), LocalSink(tmp_path), ingested_at="2026-10-30T00:00:00+00:00")
    for f in tmp_path.glob("customers_cdc/event_date=*/*.jsonl"):
        day = f.parent.name.split("=")[1]
        for row in map(json.loads, f.read_text().splitlines()):
            assert row["_event_date"] == day
            assert row["_lsn"] == row["source"]["lsn"]
            assert row["_kafka_topic"] == TOPIC
            assert row["_customer_id"] == (row["after"] or row["before"])["customer_id"]


def test_delete_uses_before_image_for_key(tmp_path):
    events = [e for e in generate_events(n_changes=300, seed=4, delete_rate=0.2) if e["op"] == "d"]
    assert events
    write_batch(_records(events), LocalSink(tmp_path))
    rows = _read_rows(tmp_path)
    assert all(r["_customer_id"] == r["before"]["customer_id"] for r in rows)


def test_bad_events_go_to_rejected_not_dropped(tmp_path):
    good = generate_events(n_snapshot=3, n_changes=0)
    recs = _records(good) + [Record(TOPIC, 0, 3, "{not json"),
                             Record(TOPIC, 0, 4, json.dumps({"op": "x", "source": {}}))]
    res = write_batch(recs, LocalSink(tmp_path))
    assert (res.written, res.rejected) == (3, 2)
    rejected = (tmp_path / "_rejected/customers_cdc").glob("*.jsonl")
    lines = [json.loads(l) for f in rejected for l in f.read_text().splitlines()]
    assert [r["offset"] for r in lines] == [3, 4]


def test_reprocessing_same_batch_is_idempotent(tmp_path):
    recs = _records(generate_events(n_snapshot=5, n_changes=20, seed=9))
    first = write_batch(recs, LocalSink(tmp_path))
    second = write_batch(recs, LocalSink(tmp_path))
    assert first.files == second.files
    assert len(_read_rows(tmp_path)) == len(recs)


@pytest.mark.parametrize("event", [
    {"op": "u", "source": {"lsn": 1, "ts_ms": 1}, "after": None},
    {"op": "d", "source": {"lsn": 1, "ts_ms": 1}, "before": {"name": "no id"}},
    {"op": "c", "source": {"ts_ms": 1}, "after": {"customer_id": 1}},
])
def test_validate_rejects_broken_envelopes(event):
    with pytest.raises(InvalidEvent):
        validate(event)


def test_run_from_generator_file_in_batches(tmp_path):
    src = write_jsonl(generate_events(n_snapshot=10, n_changes=90, seed=3), tmp_path / "in.jsonl")
    res = run(JsonlSource(src), LocalSink(tmp_path / "bronze"), batch_size=25)
    assert res.written == 100 and res.rejected == 0
    offsets = sorted(r["_kafka_offset"] for r in _read_rows(tmp_path / "bronze"))
    assert offsets == list(range(100))


@mock_aws
def test_s3_sink_writes_under_prefix():
    s3 = boto3.client("s3", region_name="ap-south-1")
    s3.create_bucket(Bucket="bank-lake", CreateBucketConfiguration={"LocationConstraint": "ap-south-1"})
    res = write_batch(_records(generate_events(n_snapshot=4, n_changes=0)),
                      S3Sink("bank-lake", "bronze", client=s3))
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="bank-lake")["Contents"]]
    assert res.written == 4
    assert all(k.startswith("bronze/customers_cdc/event_date=") for k in keys)


# --- edge cases -------------------------------------------------------------

def test_kafka_bytes_values_are_accepted(tmp_path):
    events = generate_events(n_snapshot=3, n_changes=0)
    recs = [Record(TOPIC, 0, i, json.dumps(e).encode("utf-8")) for i, e in enumerate(events)]
    res = write_batch(recs, LocalSink(tmp_path))
    assert (res.written, res.rejected) == (3, 0)


def test_rejected_bytes_value_is_kept_as_text(tmp_path):
    res = write_batch([Record(TOPIC, 0, 7, b"{broken")], LocalSink(tmp_path))
    assert (res.written, res.rejected) == (0, 1)
    [f] = (tmp_path / "_rejected/customers_cdc").glob("*.jsonl")
    assert f.name == f"part-{TOPIC}-0-000000000007.jsonl"
    assert json.loads(f.read_text())["raw"] == "{broken"


def test_null_value_tombstone_is_rejected():
    # Debezium sends a null-value tombstone after each delete for log compaction
    res = write_batch([Record(TOPIC, 0, 0, None)], _NullSink())
    assert (res.written, res.rejected) == (0, 1)


def test_one_file_per_event_date_and_partition(tmp_path):
    day1 = ev_at(1, 100, "2026-10-01T23:59:00")
    day2 = ev_at(2, 108, "2026-10-02T00:01:00")
    recs = [Record(TOPIC, 0, 10, json.dumps(day1)), Record(TOPIC, 0, 11, json.dumps(day2)),
            Record(TOPIC, 1, 5, json.dumps(day1))]
    res = write_batch(recs, LocalSink(tmp_path))
    names = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*.jsonl"))
    assert names == [
        f"customers_cdc/event_date=2026-10-01/part-{TOPIC}-0-000000000010.jsonl",
        f"customers_cdc/event_date=2026-10-01/part-{TOPIC}-1-000000000005.jsonl",
        f"customers_cdc/event_date=2026-10-02/part-{TOPIC}-0-000000000011.jsonl",
    ]
    assert res.written == 3


def test_event_date_comes_from_commit_time_in_utc(tmp_path):
    # 23:30 UTC is already the next day in IST; partitions follow UTC, like the Spark session
    write_batch([Record(TOPIC, 0, 0, json.dumps(ev_at(1, 100, "2026-10-01T23:30:00")))], LocalSink(tmp_path))
    assert [p.name for p in (tmp_path / "customers_cdc").iterdir()] == ["event_date=2026-10-01"]


def test_offsets_committed_only_after_each_batch_is_written(tmp_path):
    log = []
    events = generate_events(n_snapshot=6, n_changes=0)
    source = _ListSource([_records(events[:3]), _records(events[3:], start=3)], log)
    run(source, _LoggingSink(LocalSink(tmp_path), log))
    assert log == ["write", "commit", "write", "commit"]


def test_no_commit_when_the_write_fails():
    log = []
    source = _ListSource([_records(generate_events(n_snapshot=2, n_changes=0))], log)
    with pytest.raises(OSError):
        run(source, _FailingSink())
    assert "commit" not in log  # batch is re-read after restart instead of being lost


# --- helpers ----------------------------------------------------------------

def ev_at(cid, lsn, iso_utc):
    from datetime import datetime, timezone
    ts = int(datetime.fromisoformat(iso_utc).replace(tzinfo=timezone.utc).timestamp() * 1000)
    row = {"customer_id": cid, "full_name": f"Customer {cid}"}
    return {"before": None, "after": row, "op": "c", "ts_ms": ts,
            "source": {"lsn": lsn, "ts_ms": ts}}


class _ListSource:
    def __init__(self, batches, log):
        self._batches, self.log = batches, log

    def batches(self, batch_size=500):
        yield from self._batches

    def commit(self):
        self.log.append("commit")


class _LoggingSink:
    def __init__(self, inner, log):
        self.inner, self.log = inner, log

    def write(self, key, body):
        self.log.append("write")
        return self.inner.write(key, body)


class _FailingSink:
    def write(self, key, body):
        raise OSError("disk full")


class _NullSink:
    def write(self, key, body):
        return key


# --- replaying several files locally ----------------------------------------

def test_second_replay_file_continues_offsets_and_keeps_day_one_files(tmp_path):
    from ingestion.bronze_writer import event_dates
    day1 = [ev_at(1, 100, "2026-10-01T22:00:00"), ev_at(2, 108, "2026-10-01T23:00:00")]
    late = [ev_at(3, 116, "2026-10-01T23:58:00"), ev_at(4, 124, "2026-10-02T08:00:00")]
    f1 = write_jsonl(day1, tmp_path / "day1.jsonl")
    f2 = write_jsonl(late, tmp_path / "day2.jsonl")
    bronze = tmp_path / "bronze"

    first = run(JsonlSource(f1), LocalSink(bronze))
    second = run(JsonlSource(f2, start_offset=len(day1)), LocalSink(bronze))

    assert len(_read_rows(bronze)) == 4  # day-one file in 2026-10-01 not overwritten by the late event
    assert sorted(r["_kafka_offset"] for r in _read_rows(bronze)) == [0, 1, 2, 3]
    assert event_dates(first) == ["2026-10-01"]
    assert event_dates(second) == ["2026-10-01", "2026-10-02"]


def test_event_dates_ignores_rejected_files(tmp_path):
    from ingestion.bronze_writer import event_dates
    res = write_batch([Record(TOPIC, 0, 0, "{bad"), Record(TOPIC, 0, 1, json.dumps(ev_at(1, 9, "2026-10-03T10:00:00")))],
                      LocalSink(tmp_path))
    assert res.rejected == 1 and event_dates(res) == ["2026-10-03"]


def test_cli_start_offset(tmp_path):
    from ingestion.bronze_writer import main
    src = write_jsonl(generate_events(n_snapshot=3, n_changes=0), tmp_path / "in.jsonl")
    main(["--from-file", str(src), "--out", str(tmp_path / "b"), "--start-offset", "100"])
    assert sorted(r["_kafka_offset"] for r in _read_rows(tmp_path / "b")) == [100, 101, 102]
