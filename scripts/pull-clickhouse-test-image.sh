#!/usr/bin/env bash
set -euo pipefail

: "${OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE:?OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE is required}"
if [[ ! "$OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
  printf 'OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE must use an exact sha256 digest\n' >&2
  exit 1
fi

for attempt in 1 2 3; do
  if docker pull "$OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"; then
    exit 0
  fi
  if [[ "$attempt" -eq 3 ]]; then
    exit 1
  fi
  sleep "$((attempt * 5))"
done
