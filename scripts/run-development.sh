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

# Raw bootstrap credentials are Compose inputs only. The host-native server
# receives the three least-privilege password files and must never inherit the
# corresponding values through its environment.
unset \
    OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD \
    OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD \
    OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD \
    OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD \
    OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD \
    OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD

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

: "${OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE:?development environment has no administrator token file}"
: "${OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE:?development environment has no migration password file}"
: "${OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE:?development environment has no runtime password file}"
: "${OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE:?development environment has no deletion password file}"
: "${OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE:?development environment has no ClickHouse CA file}"

http_port=${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}
clickhouse_port=${OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT:-9440}
export OPEN_SPLUNK_SERVER_SINGLETON_LOCK_PATH="$private_state/open-splunk-server-open_splunk.server.lock"

echo "Open Splunk development server: http://127.0.0.1:$http_port/signin/"
echo "Administrator token: $OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE"

exec "$server_binary" \
    -http-address "127.0.0.1:$http_port" \
    -control-db "$private_state/open-splunk.db" \
    -master-key "$private_state/master.key" \
    -administrator-token-file "$OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE" \
    -export-artifact-dir "$private_exports" \
    -clickhouse-address "127.0.0.1:$clickhouse_port" \
    -clickhouse-secure \
    -clickhouse-ca-cert "$OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE" \
    -clickhouse-server-name "${OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME:-clickhouse}" \
    -clickhouse-migration-password-file "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE" \
    -clickhouse-runtime-password-file "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE" \
    -clickhouse-deletion-password-file "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE"
