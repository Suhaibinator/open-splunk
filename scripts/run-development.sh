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

: "${OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN:?development environment has no administrator token}"
: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD:?development environment has no ClickHouse password}"
: "${OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME:?development environment has no ClickHouse username}"

http_port=${OPEN_SPLUNK_DEPLOY_HTTP_PORT:-8080}
OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS="127.0.0.1:$http_port"
OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE="$private_state/open-splunk.db"
OPEN_SPLUNK_SERVER_MASTER_KEY_FILE="$private_state/master.key"
OPEN_SPLUNK_SERVER_LOCK_FILE="$private_state/open-splunk-server-open_splunk.server.lock"
OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY="$private_exports"
export OPEN_SPLUNK_SERVER_ADMINISTRATOR_TOKEN
export OPEN_SPLUNK_SERVER_CLICKHOUSE_ADDRESS
export OPEN_SPLUNK_SERVER_CLICKHOUSE_PASSWORD
export OPEN_SPLUNK_SERVER_CLICKHOUSE_USERNAME
export OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS
export OPEN_SPLUNK_SERVER_CONTROL_DATABASE_FILE
export OPEN_SPLUNK_SERVER_MASTER_KEY_FILE
export OPEN_SPLUNK_SERVER_LOCK_FILE
export OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY

echo "Open Splunk development server: http://127.0.0.1:$http_port/signin/"
echo "Administrator token is stored in $environment_file"

exec "$server_binary"
