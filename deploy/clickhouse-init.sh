#!/bin/sh

set -eu

validate_secret() {
    secret_name=$1
    secret_value=$2
    if [ "${#secret_value}" -ne 64 ]; then
        echo "$secret_name must contain exactly 64 lowercase hexadecimal characters" >&2
        exit 1
    fi
    case "$secret_value" in
        *[!0-9a-f]*)
            echo "$secret_name must contain exactly 64 lowercase hexadecimal characters" >&2
            exit 1
            ;;
    esac
}

validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD \
    "$CLICKHOUSE_PASSWORD"
validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD \
    "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD"
validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD \
    "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD"
validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD \
    "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD"

if [ "$CLICKHOUSE_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD" ] ||
    [ "$CLICKHOUSE_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD" ] ||
    [ "$CLICKHOUSE_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD" ] ||
    [ "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD" ] ||
    [ "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD" ] ||
    [ "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD" = "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD" ]; then
    echo "ClickHouse bootstrap, migration, runtime, and deletion passwords must be distinct" >&2
    exit 1
fi

expected_server_version=26.3.17.4
server_version=$(
    clickhouse-client \
        --host 127.0.0.1 \
        --user "$CLICKHOUSE_USER" \
        --password "$CLICKHOUSE_PASSWORD" \
        --query "SELECT version()"
)
if [ "$server_version" != "$expected_server_version" ]; then
    echo "ClickHouse server version $server_version is unsupported; expected $expected_server_version" >&2
    exit 1
fi

clickhouse-client \
    --host 127.0.0.1 \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery <<SQL
CREATE USER IF NOT EXISTS open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD';
ALTER USER open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD';
GRANT CREATE DATABASE ON open_splunk.* TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator;
GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX ON open_splunk.events TO open_splunk_migrator;
GRANT SELECT ON system.tables TO open_splunk_migrator;
GRANT SELECT, INSERT ON open_splunk.schema_migrations TO open_splunk_migrator;
SQL

for migration in /open-splunk-migrations/*.sql; do
    clickhouse-client \
        --host 127.0.0.1 \
        --user open_splunk_migrator \
        --password "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD" \
        --multiquery <"$migration"
done

clickhouse-client \
    --host 127.0.0.1 \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --multiquery <<SQL
CREATE USER IF NOT EXISTS open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD';
ALTER USER open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD';
GRANT SELECT, INSERT ON open_splunk.events TO open_splunk_runtime;
GRANT SELECT(database, table, active, rows, bytes_on_disk) ON system.parts TO open_splunk_runtime;

CREATE USER IF NOT EXISTS open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD';
ALTER USER open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD';
GRANT ALTER DELETE, SELECT(tenant_id, index_name) ON open_splunk.events TO open_splunk_deletion;
GRANT SELECT ON system.tables TO open_splunk_deletion;
GRANT SELECT ON system.mutations TO open_splunk_deletion;
SQL
