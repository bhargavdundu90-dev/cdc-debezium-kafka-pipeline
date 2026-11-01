import json
import random
from datetime import datetime, timezone

import pytest

from generator.cdc_events import generate_events
from ingestion.bronze_writer import JsonlSource, LocalSink, run as land_bronze
from transforms.scd2_merge import BRONZE_SCHEMA, build_scd2, dedupe, normalize

T0 = 1790845200000  # 2026-10-01 09:00 UTC


def ev(op, cid, lsn, minutes, **attrs):
    row = {"customer_id": cid, "full_name": f"Customer {cid}", "email": f"c{cid}@example.com",
           "city": "Hyderabad", "branch_code": "HYD001", "risk_tier": "LOW",
           "kyc_status": "VERIFIED", "updated_at": "x"}
    row.update(attrs)
    ts = T0 + minutes * 60_000
    return {"before": row if op == "d" else None, "after": None if op == "d" else row,
            "source": {"lsn": lsn, "txId": lsn, "ts_ms": ts, "snapshot": "false"}, "op": op, "ts_ms": ts}


def dim_of(spark, events):
    df = spark.createDataFrame([json.dumps(e) for e in events], "string")
    parsed = spark.read.schema(BRONZE_SCHEMA).json(df.rdd.map(lambda r: r[0]))
    return build_scd2(dedupe(normalize(parsed)))


def rows(dim, cid):
    return [r.asDict() for r in dim.where(f"customer_id = {cid}").orderBy("valid_from").collect()]


def test_update_closes_previous_version(spark):
    dim = dim_of(spark, [ev("r", 1, 10, 0), ev("u", 1, 20, 30, risk_tier="HIGH")])
    v = rows(dim, 1)
    assert [x["risk_tier"] for x in v] == ["LOW", "HIGH"]
    assert v[0]["valid_to"] == v[1]["valid_from"]
    assert [x["is_current"] for x in v] == [False, True]
    assert v[1]["valid_to"] == datetime(9999, 12, 31, 23, 59, 59)


def test_duplicate_events_do_not_create_versions(spark):
    e = [ev("r", 1, 10, 0), ev("u", 1, 20, 5, kyc_status="EXPIRED")]
    dim = dim_of(spark, e + e + [e[1]])
    assert len(rows(dim, 1)) == 2


def test_arrival_order_does_not_matter(spark):
    e = [ev("r", 1, 10, 0), ev("u", 1, 20, 5, city="Pune", branch_code="PNQ003"),
         ev("u", 1, 30, 9, city="Pune", branch_code="PNQ003", risk_tier="MEDIUM")]
    in_order = rows(dim_of(spark, e), 1)
    shuffled = rows(dim_of(spark, [e[2], e[0], e[1]]), 1)
    assert in_order == shuffled
    assert shuffled[-1]["city"] == "Pune" and shuffled[-1]["risk_tier"] == "MEDIUM"


def test_noop_update_is_ignored(spark):
    dim = dim_of(spark, [ev("r", 1, 10, 0), ev("u", 1, 20, 5, updated_at="touched")])
    v = rows(dim, 1)
    assert len(v) == 1 and v[0]["is_current"]


def test_delete_closes_version_without_current_row(spark):
    dim = dim_of(spark, [ev("r", 1, 10, 0), ev("d", 1, 20, 15)])
    v = rows(dim, 1)
    assert len(v) == 1
    assert not v[0]["is_current"]
    assert v[0]["valid_to"] == datetime(2026, 10, 1, 9, 15)


def test_generated_stream_matches_source_state(spark, tmp_path):
    events = generate_events(n_snapshot=15, n_changes=80, seed=7, duplicate_rate=0.1)
    # deliver late: shuffle neighbours, like Kafka partitions interleaving
    rng = random.Random(1)
    for i in range(len(events) - 1):
        if rng.random() < 0.2:
            events[i], events[i + 1] = events[i + 1], events[i]
    src = tmp_path / "events.jsonl"
    src.write_text("".join(json.dumps(e) + "\n" for e in events))
    land_bronze(JsonlSource(src), LocalSink(tmp_path / "bronze"), batch_size=40)

    from transforms.scd2_merge import run
    dim = run(spark, str(tmp_path / "bronze" / "customers_cdc"))

    # expected live state: replay distinct events in LSN order
    live = {}
    for e in sorted({e["source"]["lsn"]: e for e in events}.values(), key=lambda e: e["source"]["lsn"]):
        if e["op"] == "d":
            live.pop(e["before"]["customer_id"], None)
        else:
            live[e["after"]["customer_id"]] = e["after"]

    current = {r.customer_id: r for r in dim.where("is_current").collect()}
    assert set(current) == set(live)
    for cid, row in live.items():
        assert current[cid].risk_tier == row["risk_tier"]
        assert current[cid].kyc_status == row["kyc_status"]
        assert current[cid].city == row["city"]

    # no customer has overlapping or zero-length versions
    for cid in {r.customer_id for r in dim.select("customer_id").collect()}:
        v = rows(dim, cid)
        for a, b in zip(v, v[1:]):
            assert a["valid_to"] == b["valid_from"] or b["source_op"] == "c"
            assert a["valid_from"] < a["valid_to"]
