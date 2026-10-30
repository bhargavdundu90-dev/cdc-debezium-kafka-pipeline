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
