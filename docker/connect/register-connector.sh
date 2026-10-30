#!/usr/bin/env bash
# Register (or update) the Debezium Postgres connector on the local Kafka Connect.
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"
NAME="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["name"])' "$DIR/customers-connector.json")"

echo "waiting for Kafka Connect at $CONNECT_URL ..."
until curl -sf "$CONNECT_URL/connectors" >/dev/null; do sleep 3; done

# PUT /config is idempotent: creates the connector or updates it in place
python3 -c 'import json,sys;print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$DIR/customers-connector.json" \
  | curl -sf -X PUT -H "Content-Type: application/json" --data @- "$CONNECT_URL/connectors/$NAME/config"
echo
curl -sf "$CONNECT_URL/connectors/$NAME/status"
echo
