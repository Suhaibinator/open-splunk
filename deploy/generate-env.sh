#!/bin/sh

set -eu
LC_ALL=C
export LC_ALL

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
generation_mode=source
published_server_image=
case "${1:-}" in
    --development)
        generation_mode=development
        shift
        ;;
    --server-image)
        generation_mode=published
        shift
        if [ "$#" -eq 0 ]; then
            echo "usage: $0 [--development | --server-image IMAGE] [output-file]" >&2
            exit 2
        fi
        published_server_image=$1
        shift
        ;;
esac
env_file=${1:-"$script_dir/.env"}

if [ "$#" -gt 1 ]; then
    echo "usage: $0 [--development | --server-image IMAGE] [output-file]" >&2
    exit 2
fi

env_directory=$(CDPATH= cd -- "$(dirname -- "$env_file")" && pwd)
env_file="$env_directory/$(basename -- "$env_file")"
tls_directory="${env_file}.tls"

case "$env_file" in
    *[!A-Za-z0-9_./\ -]*)
        echo "output path contains shell-unsafe characters: $env_file" >&2
        exit 1
        ;;
esac

if [ -e "$env_file" ]; then
    echo "refusing to overwrite existing $env_file" >&2
    exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
    echo "openssl is required to generate credentials and the TLS identity" >&2
    exit 1
fi
if [ "$generation_mode" = source ] && ! command -v git >/dev/null 2>&1; then
    echo "git is required to record the release source revision" >&2
    exit 1
fi

host_operating_system=$(uname -s)
clear_inherited_acl() {
    if [ "$host_operating_system" != Darwin ]; then
        return 0
    fi
    if ! chmod -N "$1"; then
        echo "failed to remove inherited access controls from $1" >&2
        return 1
    fi
}

source_revision=
image_created=
source_date_epoch=
server_image=
development_clickhouse_port=${OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT:-9440}
development_http_port=${OPEN_SPLUNK_SERVER_HTTP_PORT:-8080}
if [ "$generation_mode" = source ]; then
    source_revision=$(git -C "$script_dir/.." rev-parse --verify HEAD)
    case "${#source_revision}:$source_revision" in
        40:*|64:*) ;;
        *)
            echo "repository HEAD is not a full Git object ID" >&2
            exit 1
            ;;
    esac
    case "$source_revision" in
        *[!0-9a-f]*)
            echo "repository HEAD is not a lowercase Git object ID" >&2
            exit 1
            ;;
    esac
    image_created=$(git -C "$script_dir/.." show -s --format=%cI "$source_revision")
    source_date_epoch=$(git -C "$script_dir/.." show -s --format=%ct "$source_revision")
    case "$source_date_epoch" in
        ""|*[!0-9]*)
            echo "repository HEAD has an invalid commit timestamp" >&2
            exit 1
            ;;
    esac
    case "$image_created" in
        ""|*[!0-9TZ:+-]*)
            echo "repository HEAD has an invalid RFC 3339 commit timestamp" >&2
            exit 1
            ;;
    esac
    server_image="open-splunk-server:$source_revision"
elif [ "$generation_mode" = development ]; then
    for development_port in "$development_clickhouse_port" "$development_http_port"; do
        case "$development_port" in
            ""|*[!0-9]*)
                echo "development ports must be decimal integers from 1 through 65535" >&2
                exit 1
                ;;
        esac
        if [ "$development_port" -lt 1 ] || [ "$development_port" -gt 65535 ]; then
            echo "development ports must be decimal integers from 1 through 65535" >&2
            exit 1
        fi
    done
    server_image=open-splunk-server:development
else
    case "$published_server_image" in
        ""|/*|*//*|*..*|*[!A-Za-z0-9_./:-]*)
            echo "published server image is not a safe tagged image reference" >&2
            exit 1
            ;;
    esac
    image_name=${published_server_image%:*}
    image_tag=${published_server_image##*:}
    if [ "$image_name" = "$published_server_image" ] ||
        [ -z "$image_name" ] || [ -z "$image_tag" ] ||
        [ "$image_name" = "$image_tag" ]; then
        echo "published server image must include an explicit nonempty tag" >&2
        exit 1
    fi
    server_image=$published_server_image
fi

umask 077
tmp_file=
remove_final_tls=0
cleanup() {
    if [ -n "$tmp_file" ]; then
        rm -f -- "$tmp_file"
    fi
    if [ "$remove_final_tls" -eq 1 ]; then
        rm -rf -- "$tls_directory"
    fi
}
trap cleanup EXIT HUP INT TERM

if ! mkdir -m 0700 -- "$tls_directory"; then
    echo "refusing to overwrite existing $tls_directory" >&2
    exit 1
fi
remove_final_tls=1
clear_inherited_acl "$tls_directory"
tmp_file=$(mktemp "${env_file}.tmp.XXXXXX")
clear_inherited_acl "$tmp_file"

cat >"$tls_directory/ca.conf" <<'EOF'
[req]
distinguished_name = distinguished_name
x509_extensions = v3_ca
prompt = no

[distinguished_name]
CN = Open Splunk local deployment CA

[v3_ca]
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
EOF

issue_server_certificate() {
    certificate_identity=$1
    certificate_primary_dns=$2
    certificate_config="$tls_directory/$certificate_identity.conf"
    certificate_key="$tls_directory/$certificate_identity.key"
    certificate_request="$tls_directory/$certificate_identity.csr"
    certificate_output="$tls_directory/$certificate_identity.crt"

    cat >"$certificate_config" <<EOF
[req]
distinguished_name = distinguished_name
req_extensions = v3_request
prompt = no

[distinguished_name]
CN = $certificate_primary_dns

[v3_request]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = @subject_alt_names
subjectKeyIdentifier = hash

[v3_server]
basicConstraints = critical, CA:false
keyUsage = critical, digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = @subject_alt_names
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer

[subject_alt_names]
DNS.1 = $certificate_primary_dns
DNS.2 = localhost
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

    openssl genpkey \
        -algorithm EC \
        -pkeyopt ec_paramgen_curve:P-256 \
        -pkeyopt ec_param_enc:named_curve \
        -out "$certificate_key"
    openssl req \
        -new \
        -sha256 \
        -key "$certificate_key" \
        -config "$certificate_config" \
        -out "$certificate_request"
    if [ -e "$tls_directory/ca.srl" ]; then
        set -- -CAserial "$tls_directory/ca.srl"
    else
        set -- -CAcreateserial
    fi
    openssl x509 \
        -req \
        -sha256 \
        -days 825 \
        -in "$certificate_request" \
        -CA "$tls_directory/ca.crt" \
        -CAkey "$tls_directory/ca.key" \
        "$@" \
        -extfile "$certificate_config" \
        -extensions v3_server \
        -out "$certificate_output"
}

openssl genpkey \
    -algorithm EC \
    -pkeyopt ec_paramgen_curve:P-256 \
    -pkeyopt ec_param_enc:named_curve \
    -out "$tls_directory/ca.key"
openssl req \
    -new \
    -x509 \
    -sha256 \
    -days 3650 \
    -key "$tls_directory/ca.key" \
    -config "$tls_directory/ca.conf" \
    -out "$tls_directory/ca.crt"
issue_server_certificate server clickhouse
issue_server_certificate open-splunk-server open-splunk-server

openssl rand -base64 48 >"$tls_directory/administrator.token"

rm -f -- \
    "$tls_directory/ca.conf" \
    "$tls_directory/ca.key" \
    "$tls_directory/ca.srl" \
	"$tls_directory/open-splunk-server.conf" \
	"$tls_directory/open-splunk-server.csr" \
    "$tls_directory/server.conf" \
    "$tls_directory/server.csr"
chmod 0644 \
    "$tls_directory/ca.crt" \
	"$tls_directory/open-splunk-server.crt" \
	"$tls_directory/open-splunk-server.key" \
    "$tls_directory/server.crt" \
    "$tls_directory/server.key"
if [ "$generation_mode" = development ]; then
    chmod 0600 "$tls_directory/administrator.token"
else
    chmod 0644 "$tls_directory/administrator.token"
fi

bootstrap_password=$(openssl rand -hex 32)
migration_password=$(openssl rand -hex 32)
runtime_password=$(openssl rand -hex 32)
deletion_password=$(openssl rand -hex 32)
backup_password=$(openssl rand -hex 32)
restore_password=$(openssl rand -hex 32)
printf '%s' "$migration_password" >"$tls_directory/clickhouse-migration.password"
printf '%s' "$runtime_password" >"$tls_directory/clickhouse-runtime.password"
printf '%s' "$deletion_password" >"$tls_directory/clickhouse-deletion.password"
printf '%s' "$backup_password" >"$tls_directory/clickhouse-backup.password"
printf '%s' "$restore_password" >"$tls_directory/clickhouse-restore.password"
if [ "$generation_mode" = development ]; then
    chmod 0600 "$tls_directory"/clickhouse-*.password
else
    chmod 0644 "$tls_directory"/clickhouse-*.password
fi
{
    echo "# Generated by deploy/generate-env.sh; do not commit this file."
	if [ "$generation_mode" = source ]; then
		echo "OPEN_SPLUNK_SOURCE_REVISION=$source_revision"
		echo "OPEN_SPLUNK_IMAGE_CREATED=$image_created"
		echo "OPEN_SPLUNK_SOURCE_DATE_EPOCH=$source_date_epoch"
	fi
	echo "OPEN_SPLUNK_SERVER_IMAGE=$server_image"
	if [ "$generation_mode" = development ]; then
		echo "OPEN_SPLUNK_CLICKHOUSE_SECURE_NATIVE_PORT=$development_clickhouse_port"
		echo "OPEN_SPLUNK_SERVER_HTTP_PORT=$development_http_port"
	fi
    echo "OPEN_SPLUNK_CLICKHOUSE_BOOTSTRAP_PASSWORD=$bootstrap_password"
    echo "OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD=$migration_password"
    echo "OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD=$runtime_password"
    echo "OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD=$deletion_password"
	echo "OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD=$backup_password"
	echo "OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD=$restore_password"
	echo "OPEN_SPLUNK_CLICKHOUSE_MIGRATION_PASSWORD_FILE=\"$tls_directory/clickhouse-migration.password\""
	echo "OPEN_SPLUNK_CLICKHOUSE_RUNTIME_PASSWORD_FILE=\"$tls_directory/clickhouse-runtime.password\""
	echo "OPEN_SPLUNK_CLICKHOUSE_DELETION_PASSWORD_FILE=\"$tls_directory/clickhouse-deletion.password\""
	echo "OPEN_SPLUNK_CLICKHOUSE_BACKUP_PASSWORD_FILE=\"$tls_directory/clickhouse-backup.password\""
	echo "OPEN_SPLUNK_CLICKHOUSE_RESTORE_PASSWORD_FILE=\"$tls_directory/clickhouse-restore.password\""
    echo "OPEN_SPLUNK_CLICKHOUSE_TLS_CA_FILE=\"$tls_directory/ca.crt\""
    echo "OPEN_SPLUNK_CLICKHOUSE_TLS_CERT_FILE=\"$tls_directory/server.crt\""
    echo "OPEN_SPLUNK_CLICKHOUSE_TLS_KEY_FILE=\"$tls_directory/server.key\""
    echo "OPEN_SPLUNK_CLICKHOUSE_TLS_SERVER_NAME=clickhouse"
	echo "OPEN_SPLUNK_SERVER_TLS_CA_FILE=\"$tls_directory/ca.crt\""
	echo "OPEN_SPLUNK_SERVER_TLS_CERT_FILE=\"$tls_directory/open-splunk-server.crt\""
	echo "OPEN_SPLUNK_SERVER_TLS_KEY_FILE=\"$tls_directory/open-splunk-server.key\""
	echo "OPEN_SPLUNK_SERVER_TLS_SERVER_NAME=open-splunk-server"
	echo "OPEN_SPLUNK_ADMINISTRATOR_TOKEN_FILE=\"$tls_directory/administrator.token\""
} >"$tmp_file"

# The adjacent hard link is an atomic no-overwrite publication. Once the
# complete TLS directory is retained, interruption can leave either no env
# file or an env file that always references a complete identity.
remove_final_tls=0
if ! ln -- "$tmp_file" "$env_file"; then
    remove_final_tls=1
    echo "refusing to overwrite existing $env_file" >&2
    exit 1
fi
rm -f -- "$tmp_file"
tmp_file=
trap - EXIT HUP INT TERM

echo "wrote $env_file and $tls_directory with permissions restricted by umask 077"
