"""Split a generated change stream into one input file per day (UTC commit date).

Used to feed pipeline.daily_run one day at a time when there is no Kafka running.

    python -m pipeline.split_days data/output/in/all.jsonl data/output/in
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def split(src, out_dir) -> dict[str, int]:
    days: dict[str, list[str]] = defaultdict(list)
    with open(src, encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                ts = json.loads(line)["source"]["ts_ms"]
                days[datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date().isoformat()].append(line)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for day, lines in days.items():
        (out / f"{day}.jsonl").write_text("".join(lines), encoding="utf-8")
    return {d: len(v) for d, v in sorted(days.items())}


def main(argv=None):
    p = argparse.ArgumentParser(description="Split a CDC JSONL stream into per-day files")
    p.add_argument("src")
    p.add_argument("out_dir")
    a = p.parse_args(argv)
    for day, n in split(a.src, a.out_dir).items():
        print(f"{day}: {n} events")


if __name__ == "__main__":
    main()
