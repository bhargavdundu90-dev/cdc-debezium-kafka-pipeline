"""Land raw Debezium change events into the bronze layer.

Bronze keeps every event exactly as it arrived (duplicates and out-of-order
events included) plus a few ingestion columns, so the SCD2 job can always be
replayed from bronze. Layout:

    <root>/customers_cdc/event_date=YYYY-MM-DD/part-<topic>-<partition>-<first_offset>.jsonl

The file name is derived from the Kafka partition and the batch's first
offset, so re-processing the same batch after a crash overwrites the same file
instead of creating a second copy (idempotent landing).

Sources:
  * JsonlSource  - reads a generator file, assigns sequential offsets (local dev/tests)
  * KafkaSource  - confluent-kafka consumer, manual commits after a batch is written
Sinks:
  * LocalSink    - local directory
  * S3Sink       - boto3 put_object under s3://bucket/prefix
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

VALID_OPS = {"r", "c", "u", "d"}
TABLE = "customers_cdc"


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    value: bytes | str | None


class InvalidEvent(ValueError):
    pass


def validate(event: dict) -> None:
    """Reject anything that is not a usable Debezium envelope."""
    op = event.get("op")
    if op not in VALID_OPS:
        raise InvalidEvent(f"unknown op {op!r}")
    src = event.get("source") or {}
    if "lsn" not in src or "ts_ms" not in src:
        raise InvalidEvent("source.lsn / source.ts_ms missing")
    row = event.get("after") if op != "d" else event.get("before")
    if not row or row.get("customer_id") is None:
        raise InvalidEvent(f"op={op} has no row image with customer_id")


def to_bronze_row(rec: Record, ingested_at: str) -> dict:
    event = json.loads(rec.value)
    validate(event)
    key_row = event["after"] if event["op"] != "d" else event["before"]
    event_date = datetime.fromtimestamp(event["source"]["ts_ms"] / 1000, tz=timezone.utc).date().isoformat()
    return {
        **event,
        "_customer_id": key_row["customer_id"],
        "_lsn": event["source"]["lsn"],
        "_event_date": event_date,
        "_kafka_topic": rec.topic,
        "_kafka_partition": rec.partition,
        "_kafka_offset": rec.offset,
        "_ingested_at": ingested_at,
    }


class LocalSink:
    def __init__(self, root):
        self.root = Path(root)

    def write(self, key: str, body: str) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
        return str(path)


class S3Sink:
    def __init__(self, bucket: str, prefix: str = "bronze", client=None):
        import boto3

        self.bucket, self.prefix = bucket, prefix.strip("/")
        self.s3 = client or boto3.client("s3")

    def write(self, key: str, body: str) -> str:
        full = f"{self.prefix}/{key}" if self.prefix else key
        self.s3.put_object(Bucket=self.bucket, Key=full, Body=body.encode("utf-8"),
                           ContentType="application/x-ndjson")
        return f"s3://{self.bucket}/{full}"


@dataclass
class BatchResult:
    written: int
    rejected: int
    files: list


def write_batch(records: Iterable[Record], sink, ingested_at: str | None = None) -> BatchResult:
    """Validate, enrich and land one batch. Bad events go to a _rejected file, not dropped."""
    ingested_at = ingested_at or datetime.now(timezone.utc).isoformat()
    records = list(records)
    groups: dict[tuple, list] = defaultdict(list)
    rejected = []
    for rec in records:
        try:
            row = to_bronze_row(rec, ingested_at)
        except (InvalidEvent, json.JSONDecodeError, TypeError, KeyError) as exc:
            rejected.append({"topic": rec.topic, "partition": rec.partition, "offset": rec.offset,
                             "error": str(exc),
                             "raw": rec.value.decode() if isinstance(rec.value, bytes) else rec.value})
            continue
        groups[(row["_event_date"], rec.topic, rec.partition)].append(row)

    files = []
    for (event_date, topic, partition), rows in sorted(groups.items()):
        first = min(r["_kafka_offset"] for r in rows)
        key = f"{TABLE}/event_date={event_date}/part-{topic}-{partition}-{first:012d}.jsonl"
        files.append(sink.write(key, "".join(json.dumps(r) + "\n" for r in rows)))
    if rejected:
        first = min(r["offset"] for r in rejected)
        key = f"_rejected/{TABLE}/part-{rejected[0]['topic']}-{rejected[0]['partition']}-{first:012d}.jsonl"
        files.append(sink.write(key, "".join(json.dumps(r) + "\n" for r in rejected)))
    return BatchResult(written=sum(len(v) for v in groups.values()), rejected=len(rejected), files=files)


class JsonlSource:
    """Replay a generator file as if it came from one Kafka partition."""

    def __init__(self, path, topic="bank.public.customers", partition=0):
        self.path, self.topic, self.partition = Path(path), topic, partition

    def batches(self, batch_size=500) -> Iterator[list[Record]]:
        batch = []
        with self.path.open(encoding="utf-8") as fh:
            for offset, line in enumerate(fh):
                if not line.strip():
                    continue
                batch.append(Record(self.topic, self.partition, offset, line))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def commit(self):
        pass


class KafkaSource:  # pragma: no cover - needs a running broker
    def __init__(self, bootstrap, topic="bank.public.customers", group="bronze-writer"):
        from confluent_kafka import Consumer

        self.consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group,
                                  "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        self.consumer.subscribe([topic])

    def batches(self, batch_size=500, timeout=5.0, max_empty_polls=3):
        empty = 0
        while empty < max_empty_polls:
            msgs = self.consumer.consume(num_messages=batch_size, timeout=timeout)
            msgs = [m for m in msgs if m.error() is None and m.value() is not None]
            if not msgs:
                empty += 1
                continue
            empty = 0
            yield [Record(m.topic(), m.partition(), m.offset(), m.value()) for m in msgs]

    def commit(self):
        # commit only after the batch is safely in bronze -> at-least-once, never data loss
        self.consumer.commit(asynchronous=False)


def run(source, sink, batch_size=500) -> BatchResult:
    total = BatchResult(0, 0, [])
    for batch in source.batches(batch_size=batch_size):
        res = write_batch(batch, sink)
        source.commit()
        total.written += res.written
        total.rejected += res.rejected
        total.files += res.files
    return total


def main(argv=None):
    p = argparse.ArgumentParser(description="Land Debezium CDC events into bronze")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--from-file", help="generator JSONL file (local dev)")
    src.add_argument("--bootstrap", help="Kafka bootstrap servers, e.g. localhost:9092")
    p.add_argument("--topic", default="bank.public.customers")
    p.add_argument("--out", default="data/output/bronze", help="local bronze root")
    p.add_argument("--s3-bucket", help="write to S3 instead of --out")
    p.add_argument("--s3-prefix", default="bronze")
    p.add_argument("--batch-size", type=int, default=500)
    a = p.parse_args(argv)

    source = JsonlSource(a.from_file, a.topic) if a.from_file else KafkaSource(a.bootstrap, a.topic)
    sink = S3Sink(a.s3_bucket, a.s3_prefix) if a.s3_bucket else LocalSink(a.out)
    res = run(source, sink, a.batch_size)
    print(f"bronze: {res.written} events written, {res.rejected} rejected, {len(res.files)} files")


if __name__ == "__main__":
    main()
