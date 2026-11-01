# cdc-debezium-kafka-pipeline

Change data capture (CDC) from a Postgres core-banking database with **Debezium + Kafka**, landed on S3 and
merged into an **SCD Type 2** customer dimension with **PySpark**, queryable in **Athena**.

> 🚧 Work in progress – built milestone by milestone. See [Roadmap](#roadmap).

## Business scenario
A retail bank keeps customer master data (KYC status, risk tier, address, branch) in an OLTP Postgres database.
Compliance and risk teams need to answer *"what was this customer's risk tier and KYC status on the day the loan
was approved?"*, a point-in-time question that a nightly full snapshot cannot answer, because intra-day changes
are lost and history is overwritten.

This project streams every insert/update/delete from Postgres as a change event and keeps a full,
auditable history (SCD Type 2: `valid_from`, `valid_to`, `is_current`) in the lakehouse.

## Architecture
```mermaid
flowchart LR
    PG[(Postgres<br/>core banking<br/>WAL / logical decoding)] --> DBZ[Debezium<br/>Postgres connector<br/>Kafka Connect]
    DBZ -->|change events<br/>op c/u/d/r| K[(Kafka topic<br/>bank.public.customers)]
    K --> RAW[Bronze: raw CDC events<br/>S3 JSON / Parquet]
    RAW --> SPARK[PySpark job<br/>dedupe by LSN + SCD2 merge]
    SPARK --> SCD[Silver: dim_customer_scd2<br/>S3 Parquet / Iceberg]
    SCD --> CAT[Glue Data Catalog]
    CAT --> ATH[Athena<br/>point-in-time queries]
```

## Generate sample change events (no database needed)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m generator.cdc_events --snapshot 20 --changes 100 --duplicate-rate 0.05 --late-rate 0.05
pytest -q        # Spark tests need Java 11 or 17 on PATH
```
Each line is a Debezium-style envelope (`before`, `after`, `source.lsn`, `op` = r/c/u/d). The
`--duplicate-rate` and `--late-rate` flags simulate Kafka's at-least-once delivery and out-of-order
arrival, which the SCD2 job must handle. A seeded sample lives in `data/sample/customers_cdc.jsonl`.

## Run the real CDC stack locally (Docker)
```bash
docker compose -f docker/docker-compose.yml up -d     # Postgres + Kafka (KRaft) + Debezium Connect
./docker/connect/register-connector.sh                 # registers bank-customers-cdc
docker exec -it cdc-postgres psql -U bank -d bank \
  -c "UPDATE customers SET kyc_status='EXPIRED', updated_at=now() WHERE customer_id=2;"
```
Details of each service and connector setting are in [docker/README.md](docker/README.md).

## Land change events in bronze
```bash
# from the generator file (no Docker needed)
python -m ingestion.bronze_writer --from-file data/sample/customers_cdc.jsonl --out data/output/bronze

# from the local Kafka topic
python -m ingestion.bronze_writer --bootstrap localhost:9092 --out data/output/bronze

# or straight to S3
python -m ingestion.bronze_writer --bootstrap localhost:9092 --s3-bucket my-lake-bucket --s3-prefix bronze
```
Output layout:
```
bronze/customers_cdc/event_date=2026-10-01/part-bank.public.customers-0-000000000000.jsonl
bronze/_rejected/customers_cdc/part-...jsonl      # events that failed validation, kept with the error
```
How the bronze writer behaves:
- **Keeps duplicates and out-of-order events.** Bronze is the replayable raw log; dedupe by LSN happens in the SCD2 job.
- **Adds ingestion columns:** `_customer_id`, `_lsn`, `_event_date`, `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_ingested_at`.
- **Idempotent landing:** the file name is `topic-partition-first_offset`, so re-running a batch after a crash overwrites the same file.
- **At-least-once:** Kafka offsets are committed only after the batch is written.
- **Deletes** are keyed from the `before` image, because `after` is null for `op=d`.

## Build the SCD2 dimension (PySpark)
```bash
python -m transforms.scd2_merge --bronze data/output/bronze/customers_cdc --out data/output/silver/dim_customer_scd2
```
How [`transforms/scd2_merge.py`](transforms/scd2_merge.py) turns the raw change log into history:

| Step | Why |
|------|-----|
| Take `after` for r/c/u and `before` for d | `after` is null on a delete, so the key and last values come from `before` |
| Dedupe on `(customer_id, lsn)` | Kafka is at-least-once; the LSN is the identity of a change, so replays collapse |
| Order by LSN inside each customer | arrival order and Kafka offset can be wrong after retries; LSN is commit order |
| Drop no-op updates (row hash of tracked columns unchanged) | touching `updated_at` alone should not create a new version |
| `valid_to = lead(change_ts)` over the customer window | a version lives until the next change for that customer |
| Delete events close the open version and are then dropped | a deleted customer has history but no `is_current` row |

Output columns: `customer_sk, customer_id, full_name, email, city, branch_code, risk_tier, kyc_status,
updated_at, valid_from, valid_to (9999-12-31 for open rows), is_current, source_lsn, source_op, row_hash`.

Point-in-time question from the scenario, once the table is in the Glue catalog:
```sql
SELECT d.customer_id, d.risk_tier, d.kyc_status
FROM loans l
JOIN dim_customer_scd2 d
  ON d.customer_id = l.customer_id
 AND l.approved_at >= d.valid_from
 AND l.approved_at <  d.valid_to;
```
Right now the job rebuilds the dimension from all of bronze. That is simple and always correct while bronze
is small; the incremental merge (only new events against the existing dimension) comes next.

## Repository layout
```
generator/      synthetic Debezium-style change events (no database needed for tests)
docker/         docker-compose: Postgres + Kafka + Kafka Connect/Debezium
ingestion/      Kafka / file -> bronze landing (local or S3)
transforms/     bronze -> SCD2 silver (PySpark)
quality/        data-quality checks on the dimension
terraform/      S3, Glue, MSK/IAM mapping
tests/          pytest (local Spark, no AWS)
```

## Roadmap
- [x] Repo skeleton, scenario and architecture
- [x] Synthetic CDC event generator
- [x] Local infra (docker-compose: Postgres, Kafka, Debezium)
- [x] Ingestion to bronze
- [x] SCD2 merge transform
- [ ] Deletes, late / out-of-order events
- [ ] Data-quality checks
- [ ] Unit + integration tests
- [ ] Terraform
- [ ] GitHub Actions CI
- [ ] Design decisions, AWS mapping and FAQ
