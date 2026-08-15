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
validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD \
    "$OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD"
validate_secret \
    OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD \
    "$OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"

require_distinct_secrets() {
    while [ "$#" -gt 1 ]; do
        first_secret=$1
        shift
        for other_secret in "$@"; do
            if [ "$first_secret" = "$other_secret" ]; then
                echo "ClickHouse bootstrap, migration, runtime, deletion, backup, and restore passwords must be distinct" >&2
                exit 1
            fi
        done
    done
}

require_distinct_secrets \
    "$CLICKHOUSE_PASSWORD" \
    "$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD" \
    "$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD" \
    "$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD" \
    "$OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD" \
    "$OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD"

# The official image creates fresh volume roots with permissive modes. This
# script runs as the fixed ClickHouse UID, so tighten both durable roots before
# provisioning work and fail closed if either mount is not owned by that UID.
chmod 0700 /var/lib/clickhouse /var/log/clickhouse-server

expected_server_version=26.3.17.56
server_version=$(
    clickhouse-client \
        --host 127.0.0.1 \
        --user "$CLICKHOUSE_USER" \
        --query "SELECT version()"
)
if [ "$server_version" != "$expected_server_version" ]; then
    echo "ClickHouse server version $server_version is unsupported; expected $expected_server_version" >&2
    exit 1
fi

clickhouse-client \
    --host 127.0.0.1 \
    --user "$CLICKHOUSE_USER" \
    --multiquery <<SQL
CREATE USER IF NOT EXISTS open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD';
ALTER USER open_splunk_migrator
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD';
REVOKE ALL ON *.* FROM open_splunk_migrator;
REVOKE ALL FROM open_splunk_migrator;
GRANT CREATE DATABASE ON open_splunk.* TO open_splunk_migrator;
GRANT SHOW TABLES ON open_splunk.* TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.schema_migrations TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.events TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.recovery_sets TO open_splunk_migrator;
GRANT CREATE TABLE ON open_splunk.recovery_archive_markers TO open_splunk_migrator;
GRANT ALTER ADD COLUMN, ALTER ADD CONSTRAINT, ALTER ADD INDEX ON open_splunk.events TO open_splunk_migrator;
GRANT SELECT ON system.tables TO open_splunk_migrator;
GRANT SELECT, INSERT ON open_splunk.schema_migrations TO open_splunk_migrator;

CREATE USER IF NOT EXISTS open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD';
ALTER USER open_splunk_runtime
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD';
REVOKE ALL ON *.* FROM open_splunk_runtime;
REVOKE ALL FROM open_splunk_runtime;
GRANT SELECT, INSERT ON open_splunk.events TO open_splunk_runtime;
GRANT SELECT(database, table, active, rows, bytes_on_disk) ON system.parts TO open_splunk_runtime;

CREATE USER IF NOT EXISTS open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD';
ALTER USER open_splunk_deletion
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD';
REVOKE ALL ON *.* FROM open_splunk_deletion;
REVOKE ALL FROM open_splunk_deletion;
GRANT ALTER DELETE, SELECT(tenant_id, index_name) ON open_splunk.events TO open_splunk_deletion;
GRANT SELECT ON system.tables TO open_splunk_deletion;
GRANT SELECT ON system.mutations TO open_splunk_deletion;

CREATE USER IF NOT EXISTS open_splunk_backup
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD';
ALTER USER open_splunk_backup
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD';
REVOKE ALL ON *.* FROM open_splunk_backup;
REVOKE ALL FROM open_splunk_backup;
GRANT BACKUP, SHOW TABLES ON open_splunk.* TO open_splunk_backup;
GRANT SELECT ON open_splunk.schema_migrations TO open_splunk_backup;
GRANT SELECT(visibility_seq) ON open_splunk.events TO open_splunk_backup;
GRANT INSERT, SELECT, TRUNCATE ON open_splunk.recovery_archive_markers TO open_splunk_backup;
GRANT SELECT ON system.databases TO open_splunk_backup;
GRANT SELECT ON system.mutations TO open_splunk_backup;
GRANT SELECT ON system.tables TO open_splunk_backup;

CREATE USER IF NOT EXISTS open_splunk_restore
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD';
ALTER USER open_splunk_restore
    IDENTIFIED WITH sha256_password BY '$OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD';
REVOKE ALL ON *.* FROM open_splunk_restore;
REVOKE ALL FROM open_splunk_restore;
GRANT CREATE DATABASE, SHOW TABLES ON open_splunk.* TO open_splunk_restore;
GRANT CREATE TABLE, INSERT, SELECT(visibility_seq) ON open_splunk.events TO open_splunk_restore;
GRANT CREATE TABLE, INSERT, SELECT(name, version) ON open_splunk.schema_migrations TO open_splunk_restore;
GRANT CREATE TABLE, INSERT, SELECT, TRUNCATE ON open_splunk.recovery_archive_markers TO open_splunk_restore;
GRANT CREATE TABLE, INSERT, SELECT(database_uuid, deployment_manifest_sha256, events_table_uuid, recovery_archive_markers_table_uuid, recovery_set_id, recovery_sets_table_uuid, restored_at, schema_migrations_table_uuid, slot), TRUNCATE ON open_splunk.recovery_sets TO open_splunk_restore;
GRANT SELECT ON system.databases TO open_splunk_restore;
GRANT SELECT ON system.disks TO open_splunk_restore;
GRANT SELECT ON system.mutations TO open_splunk_restore;
GRANT SELECT ON system.tables TO open_splunk_restore;
GRANT SHOW DATABASES ON *.* TO open_splunk_restore;
SQL
