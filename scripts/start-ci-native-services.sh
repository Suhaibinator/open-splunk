#!/usr/bin/env bash
set -euo pipefail

# These disposable services are only for GitHub's isolated Linux test runners.
# They never seed application users, memberships or browser credentials.
if [[ -z "${OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE:-}" ]]; then
  echo '::error::Set repository variable OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE to an approved exact MinIO image before running native database tests.' >&2
  exit 1
fi
ci_minio_image="$OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE"
ci_minio_tag="${ci_minio_image##*/}"
if [[ "$ci_minio_image" == -* || "$ci_minio_image" =~ [[:space:]] ||
      ( "$ci_minio_image" == *@* && ! "$ci_minio_image" =~ ^.+@sha256:[a-f0-9]{64}$ ) ||
      ( "$ci_minio_image" != *@* && ( ! "$ci_minio_tag" =~ ^.+:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$ || "${ci_minio_tag,,}" == *:latest ) ) ]]; then
  echo '::error::OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE must identify an exact approved tag or digest, without latest or implicit tags.' >&2
  exit 1
fi
: "${GITHUB_ENV:?GitHub environment output is required}"
: "${GITHUB_PATH:?GitHub PATH output is required}"
: "${RUNNER_TEMP:?An isolated runner temporary directory is required}"
command -v docker >/dev/null
command -v curl >/dev/null
command -v openssl >/dev/null

ci_postgres_image='postgres:18-alpine@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873'
ci_suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}-$RANDOM"
ci_postgres_name="open-splunk-ci-postgres-$ci_suffix"
ci_minio_name="open-splunk-ci-minio-$ci_suffix"
ci_postgres_secret="$(openssl rand -hex 32)"
ci_minio_secret="$(openssl rand -hex 32)"
printf '::add-mask::%s\n' "$ci_postgres_secret" "$ci_minio_secret"
ci_fixture_root="$RUNNER_TEMP/open-splunk-native-$ci_suffix"
mkdir -m 0700 "$ci_fixture_root"
ci_ready=false
ci_complete=false
cleanup_failed_start() {
  if [[ "$ci_complete" != true ]]; then
    docker rm --force "$ci_postgres_name" "$ci_minio_name" >/dev/null 2>&1 || true
  fi
}
trap cleanup_failed_start EXIT

docker pull "$ci_postgres_image"
docker pull "$ci_minio_image"
POSTGRES_PASSWORD="$ci_postgres_secret" docker run --detach --rm \
  --name "$ci_postgres_name" \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_PASSWORD --env POSTGRES_USER=postgres --env POSTGRES_DB=postgres \
  "$ci_postgres_image" >/dev/null
MINIO_ROOT_PASSWORD="$ci_minio_secret" docker run --detach --rm \
  --name "$ci_minio_name" \
  --publish 127.0.0.1:59000:9000 \
  --env MINIO_ROOT_USER=open-splunk-test --env MINIO_ROOT_PASSWORD \
  "$ci_minio_image" server /data >/dev/null

for attempt in {1..60}; do
  if docker exec "$ci_postgres_name" pg_isready -U postgres -d postgres >/dev/null 2>&1 &&
     curl --silent --fail http://127.0.0.1:59000/minio/health/live >/dev/null; then
    ci_ready=true
    break
  fi
  sleep 2
done
if [[ "$ci_ready" != true ]]; then
  echo '::error::Disposable PostgreSQL 18 or approved private MinIO failed to become ready.' >&2
  exit 1
fi

# Extract the pinned client and its full musl dependency closure. Execute the
# loader on the host so native archive inspection retains its inherited FD3;
# a new container cannot inherit that pinned descriptor from the Go process.
ci_client_root="$ci_fixture_root/postgres-tools"
mkdir -m 0700 "$ci_client_root"
docker run --rm --interactive --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$ci_client_root,dst=/native-tools" \
  --entrypoint sh "$ci_postgres_image" -es <<'COPY_TOOLS'
    mkdir -p /native-tools/usr/local/bin
    cp /usr/local/bin/pg_dump /usr/local/bin/pg_restore /native-tools/usr/local/bin/
    ldd /usr/local/bin/pg_dump /usr/local/bin/pg_restore |
      awk '$2 == "=>" && substr($3, 1, 1) == "/" {print $3} substr($1, 1, 1) == "/" {print $1}' |
      sort -u > /native-tools/libraries
    while IFS= read -r library; do
      mkdir -p "/native-tools$(dirname "$library")"
      cp -L "$library" "/native-tools$library"
    done < /native-tools/libraries
COPY_TOOLS
ci_loaders=("$ci_client_root"/lib/ld-musl-*.so.1)
if [[ "${#ci_loaders[@]}" != 1 || ! -x "${ci_loaders[0]}" ]]; then
  echo '::error::Pinned PostgreSQL 18 image did not provide one executable musl loader.' >&2
  exit 1
fi
for ci_tool in pg_dump pg_restore; do
  printf '#!/usr/bin/env bash\nset -euo pipefail\nci_loader=%q\nci_client_root=%q\nci_tool=%q\n' \
    "${ci_loaders[0]}" "$ci_client_root" "$ci_tool" \
    > "$ci_fixture_root/$ci_tool"
  cat >> "$ci_fixture_root/$ci_tool" <<'WRAPPER'
exec "$ci_loader" --library-path "$ci_client_root/lib:$ci_client_root/usr/lib:$ci_client_root/usr/local/lib" \
  "$ci_client_root/usr/local/bin/$ci_tool" "$@"
WRAPPER
  chmod 0700 "$ci_fixture_root/$ci_tool"
  "$ci_fixture_root/$ci_tool" --version | grep -E 'PostgreSQL\) 18\.'
done

printf '%s\n' "$ci_fixture_root" >> "$GITHUB_PATH"
{
  printf 'OPEN_SPLUNK_TEST_POSTGRES_DSN=postgres://postgres:%s@127.0.0.1:55432/postgres?sslmode=disable\n' "$ci_postgres_secret"
  printf 'OPEN_SPLUNK_TEST_MINIO_ENDPOINT=127.0.0.1:59000\n'
  printf 'OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY=open-splunk-test\n'
  printf 'OPEN_SPLUNK_TEST_MINIO_SECRET_KEY=%s\n' "$ci_minio_secret"
  printf 'OPEN_SPLUNK_TEST_MINIO_USE_TLS=false\n'
  printf 'OPEN_SPLUNK_RECOVERY_DRILL_MINIO_IMAGE=%s\n' "$ci_minio_image"
} >> "$GITHUB_ENV"
ci_complete=true
