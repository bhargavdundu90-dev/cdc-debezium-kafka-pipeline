"""Generate synthetic Debezium-style change events for a `customers` table.

The output mimics the Debezium Postgres connector envelope (value only, with
schemas disabled), so the downstream SCD2 job can be built and tested without
running Postgres or Kafka:

    {"before": {...} | null, "after": {...} | null,
     "source": {"connector": "postgresql", "db": "bank", "schema": "public",
                "table": "customers", "lsn": 123, "txId": 9, "ts_ms": ...},
     "op": "r" | "c" | "u" | "d", "ts_ms": ...}

Optional realism knobs:
  * duplicate_rate  - re-emit an event (at-least-once delivery from Kafka)
  * late_rate       - swap neighbouring events so arrival order != LSN order
"""
from __future__ import annotations

import argparse
import copy
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

RISK_TIERS = ["LOW", "MEDIUM", "HIGH"]
KYC_STATUSES = ["PENDING", "VERIFIED", "EXPIRED"]
CITIES = ["Hyderabad", "Bengaluru", "Chennai", "Mumbai", "Pune", "Vijayawada"]
BRANCHES = ["HYD001", "BLR004", "MAA002", "BOM010", "PNQ003", "VGA001"]
UPDATABLE = ["risk_tier", "kyc_status", "city", "branch_code", "email"]


def _envelope(op, before, after, lsn, tx_id, ts):
    ts_ms = int(ts.timestamp() * 1000)
    return {
        "before": before,
        "after": after,
        "source": {
            "connector": "postgresql",
            "db": "bank",
            "schema": "public",
            "table": "customers",
            "lsn": lsn,
            "txId": tx_id,
            "ts_ms": ts_ms,
            "snapshot": "true" if op == "r" else "false",
        },
        "op": op,
        "ts_ms": ts_ms,
    }


def _new_customer(rng, customer_id, ts):
    city_idx = rng.randrange(len(CITIES))
    return {
        "customer_id": customer_id,
        "full_name": f"Customer {customer_id}",
        "email": f"cust{customer_id}@example.com",
        "city": CITIES[city_idx],
        "branch_code": BRANCHES[city_idx],
        "risk_tier": rng.choice(RISK_TIERS),
        "kyc_status": rng.choice(KYC_STATUSES),
        "updated_at": ts.isoformat(),
    }


def generate_events(
    n_snapshot=20,
    n_changes=100,
    seed=42,
    start=datetime(2026, 10, 1, 9, 0, tzinfo=timezone.utc),
    delete_rate=0.05,
    duplicate_rate=0.0,
    late_rate=0.0,
):
    """Return a list of change events in arrival order.

    Events are first produced in true commit (LSN) order; duplicates and
    out-of-order arrival are then injected so consumers can be tested against
    realistic delivery.
    """
    rng = random.Random(seed)
    ts = start
    lsn, tx_id = 1000, 500
    live: dict[int, dict] = {}
    events = []

    # 1) initial snapshot (op = r), as Debezium does on first start
    for cid in range(1, n_snapshot + 1):
        row = _new_customer(rng, cid, ts)
        live[cid] = row
        lsn += 8
        events.append(_envelope("r", None, row, lsn, tx_id, ts))
    next_id = n_snapshot + 1

    # 2) streaming changes
    for _ in range(n_changes):
        ts += timedelta(seconds=rng.randint(5, 900))
        lsn += rng.randint(8, 64)
        tx_id += 1
        roll = rng.random()
        if not live or roll < 0.2:
            row = _new_customer(rng, next_id, ts)
            live[next_id] = row
            next_id += 1
            events.append(_envelope("c", None, row, lsn, tx_id, ts))
        elif roll < 0.2 + delete_rate:
            cid = rng.choice(sorted(live))
            before = live.pop(cid)
            events.append(_envelope("d", before, None, lsn, tx_id, ts))
        else:
            cid = rng.choice(sorted(live))
            before = live[cid]
            after = copy.deepcopy(before)
            field = rng.choice(UPDATABLE)
            if field == "risk_tier":
                after[field] = rng.choice([t for t in RISK_TIERS if t != before[field]])
            elif field == "kyc_status":
                after[field] = rng.choice([s for s in KYC_STATUSES if s != before[field]])
            elif field in ("city", "branch_code"):
                idx = rng.choice([i for i, c in enumerate(CITIES) if c != before["city"]])
                after["city"], after["branch_code"] = CITIES[idx], BRANCHES[idx]
            else:
                after["email"] = f"cust{cid}.{lsn}@example.com"
            after["updated_at"] = ts.isoformat()
            live[cid] = after
            events.append(_envelope("u", before, after, lsn, tx_id, ts))

    # 3) delivery realism: at-least-once duplicates and late arrivals
    delivered = []
    for ev in events:
        delivered.append(ev)
        if rng.random() < duplicate_rate:
            delivered.append(copy.deepcopy(ev))
    for i in range(len(delivered) - 1):
        if rng.random() < late_rate:
            delivered[i], delivered[i + 1] = delivered[i + 1], delivered[i]
    return delivered


def write_jsonl(events, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return path


def main(argv=None):
    p = argparse.ArgumentParser(description="Generate Debezium-style CDC events for bank customers")
    p.add_argument("--snapshot", type=int, default=20)
    p.add_argument("--changes", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--duplicate-rate", type=float, default=0.0)
    p.add_argument("--late-rate", type=float, default=0.0)
    p.add_argument("--out", default="data/sample/customers_cdc.jsonl")
    a = p.parse_args(argv)
    events = generate_events(a.snapshot, a.changes, a.seed,
                             duplicate_rate=a.duplicate_rate, late_rate=a.late_rate)
    out = write_jsonl(events, a.out)
    print(f"wrote {len(events)} events to {out}")


if __name__ == "__main__":
    main()
