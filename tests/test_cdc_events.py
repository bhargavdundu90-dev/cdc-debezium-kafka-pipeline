import json
from collections import Counter

from generator.cdc_events import generate_events, main


def test_snapshot_comes_first_and_is_op_r():
    events = generate_events(n_snapshot=10, n_changes=30, seed=1)
    assert [e["op"] for e in events[:10]] == ["r"] * 10
    assert all(e["source"]["snapshot"] == "true" for e in events[:10])


def test_envelope_shape_per_op():
    for e in generate_events(n_snapshot=10, n_changes=200, seed=2, delete_rate=0.1):
        if e["op"] in ("r", "c"):
            assert e["before"] is None and e["after"] is not None
        elif e["op"] == "u":
            assert e["before"]["customer_id"] == e["after"]["customer_id"]
            assert e["before"] != e["after"]
        else:
            assert e["op"] == "d" and e["after"] is None and e["before"] is not None


def test_lsn_strictly_increasing_without_disorder():
    lsns = [e["source"]["lsn"] for e in generate_events(seed=3)]
    assert lsns == sorted(lsns) and len(set(lsns)) == len(lsns)


def test_deterministic_for_same_seed():
    assert generate_events(seed=7) == generate_events(seed=7)
    assert generate_events(seed=7) != generate_events(seed=8)


def test_no_update_or_delete_after_delete():
    deleted = set()
    for e in generate_events(n_changes=300, seed=4, delete_rate=0.15):
        cid = (e["after"] or e["before"])["customer_id"]
        assert cid not in deleted, "event for a customer that was already deleted"
        if e["op"] == "d":
            deleted.add(cid)


def test_duplicates_and_late_arrivals_are_injected():
    clean = generate_events(seed=5)
    noisy = generate_events(seed=5, duplicate_rate=0.2, late_rate=0.2)
    assert len(noisy) > len(clean)
    counts = Counter(e["source"]["lsn"] for e in noisy)
    assert max(counts.values()) >= 2
    lsns = [e["source"]["lsn"] for e in noisy]
    assert lsns != sorted(lsns)


def test_cli_writes_jsonl(tmp_path):
    out = tmp_path / "cdc.jsonl"
    main(["--snapshot", "3", "--changes", "5", "--out", str(out)])
    lines = out.read_text().splitlines()
    assert len(lines) == 8
    assert json.loads(lines[0])["op"] == "r"
