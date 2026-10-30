# Local CDC stack

| Service | Image | Port | Role |
|---------|-------|------|------|
| postgres | postgres:16 | 5432 | Source OLTP DB, `wal_level=logical`, `customers` table with `REPLICA IDENTITY FULL` |
| kafka | apache/kafka:3.8.0 (KRaft, no ZooKeeper) | 9092 | Change-event log |
| connect | quay.io/debezium/connect:2.7 | 8083 | Kafka Connect running the Debezium Postgres connector |

```bash
docker compose -f docker/docker-compose.yml up -d
./docker/connect/register-connector.sh

# watch change events (topic = <topic.prefix>.<schema>.<table>)
docker exec -it cdc-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:29092 --topic bank.public.customers --from-beginning

# make a change and see the "u" event arrive
docker exec -it cdc-postgres psql -U bank -d bank \
  -c "UPDATE customers SET risk_tier='HIGH', updated_at=now() WHERE customer_id=1;"

docker compose -f docker/docker-compose.yml down -v
```

Notes
- `pgoutput` is the logical decoding plugin built into Postgres 10+, so no extra extension is needed.
- `REPLICA IDENTITY FULL` makes update/delete events carry the full `before` row.
- Converters run with `schemas.enable=false`, so each message value is the plain Debezium envelope
  (`before`, `after`, `source`, `op`, `ts_ms`), the same shape the generator writes.
- `tombstones.on.delete=false`: deletes arrive as a single `op=d` event, no null tombstone after it.
