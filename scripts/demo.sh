#!/usr/bin/env bash
# Two simulated days end to end on local files: generate -> split by day -> daily_run day 1 -> daily_run day 2.
# Prints the live version, the DQ verdicts and a few rows of history for one customer.
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=data/output
rm -rf "$OUT/in" "$OUT/bronze" "$OUT/silver"

python -m generator.cdc_events --snapshot 20 --changes 200 --duplicate-rate 0.05 --late-rate 0.05 \
    --out "$OUT/in/all.jsonl"
python -m pipeline.split_days "$OUT/in/all.jsonl" "$OUT/in"

first=$(ls "$OUT"/in/20*.jsonl | head -1)
second=$(ls "$OUT"/in/20*.jsonl | sed -n 2p)

python -m pipeline.daily_run --input "$first" --run-id day1
python -m pipeline.daily_run --input "$second" --run-id day2 --start-offset "$(wc -l < "$first")"

DIM="$OUT/silver/dim_customer_scd2"
echo
echo "live version: $(cat "$DIM/_CURRENT")"
for report in "$DIM"/_dq/*.json; do
  python - "$report" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
print(f"{sys.argv[1].rsplit('/', 1)[-1]}: {r.get('status')}")
PY
done

python - "$DIM" <<'PY'
import sys
from pyspark.sql import SparkSession
dim = sys.argv[1]
live = open(f"{dim}/_CURRENT").read().strip()
spark = SparkSession.builder.master("local[1]").appName("demo-peek").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet(f"{dim}/{live}")
cid = df.groupBy("customer_id").count().orderBy("count", ascending=False).first()["customer_id"]
print(f"\nhistory of customer {cid}:")
(df.where(df.customer_id == cid)
   .select("valid_from", "valid_to", "is_current", "risk_tier", "kyc_status", "source_op")
   .orderBy("valid_from").show(truncate=False))
spark.stop()
PY
