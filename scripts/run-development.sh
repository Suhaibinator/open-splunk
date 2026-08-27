#!/bin/sh

set -eu
LC_ALL=C
export LC_ALL

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <development-env-file>" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
environment_file=$1
case "$environment_file" in
    /*) ;;
    *) environment_file="$repository_root/$environment_file" ;;
esac
if [ ! -f "$environment_file" ]; then
    echo "development environment is missing; run 'make dev-clickhouse' first" >&2
    exit 1
fi

# The generated file contains only generator-validated shell assignments.
# shellcheck disable=SC1090
. "$environment_file"

state_root=${OPEN_SPLUNK_DEVELOPMENT_STATE_ROOT:-"$repository_root/data/development"}
private_state="$state_root/private"
export_root=${OPEN_SPLUNK_DEVELOPMENT_EXPORT_ROOT:-"$repository_root/exports/development"}
private_exports="$export_root/private"
mkdir -p "$private_state" "$private_exports"
chmod 0700 "$state_root" "$private_state" "$export_root" "$private_exports"

server_binary=${OPEN_SPLUNK_DEVELOPMENT_SERVER_BINARY:-"$repository_root/build/open-splunk-server"}
if [ ! -x "$server_binary" ]; then
    echo "development server binary is missing; run 'make build-server'" >&2
    exit 1
fi

: "${OPEN_SPLUNK_ADMINISTRATOR_TOKEN:?development environment has no administrator token}"
: "${OPEN_SPLUNK_CLICKHOUSE_PASSWORD:?development environment has no ClickHouse password}"
: "${OPEN_SPLUNK_CLICKHOUSE_USERNAME:?development environment has no ClickHouse username}"
export OPEN_SPLUNK_ADMINISTRATOR_TOKEN OPEN_SPLUNK_CLICKHOUSE_PASSWORD

http_port=${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}
clickhouse_address=${OPEN_SPLUNK_CLICKHOUSE_ADDRESS:-127.0.0.1:9000}
export OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH="$private_state/open-splunk-server-open_splunk.server.lock"

echo "Open Splunk development server: http://127.0.0.1:$http_port/signin/"
echo "Administrator token is stored in $environment_file"

exec "$server_binary" \
    -http-address "127.0.0.1:$http_port" \
    -control-db "$private_state/open-splunk.db" \
    -master-key "$private_state/master.key" \
    -export-artifact-dir "$private_exports" \
    -clickhouse-address "$clickhouse_address" \
    -clickhouse-username "$OPEN_SPLUNK_CLICKHOUSE_USERNAME"
