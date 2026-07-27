#!/usr/bin/env bash
set -euo pipefail
umask 022

if [[ -n "${OPEN_SPLUNK_REPOSITORY_ROOT:-}" ]]; then
  REPO_ROOT="$(cd "$OPEN_SPLUNK_REPOSITORY_ROOT" && pwd -P)"
else
  REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
fi
unset OPEN_SPLUNK_REPOSITORY_ROOT

if [[ $# -ne 0 ]]; then
  echo "error: build-release does not accept arguments" >&2
  exit 2
fi

: "${OPEN_SPLUNK_APPLICATION_VERSION:?OPEN_SPLUNK_APPLICATION_VERSION is required}"
: "${OPEN_SPLUNK_SOURCE_REVISION:?OPEN_SPLUNK_SOURCE_REVISION is required}"
if [[ ${#OPEN_SPLUNK_APPLICATION_VERSION} -gt 64 ||
      ! "$OPEN_SPLUNK_APPLICATION_VERSION" =~ ^[0-9A-Za-z.+-]+$ ]]; then
  echo "error: OPEN_SPLUNK_APPLICATION_VERSION contains unsafe or unsupported characters" >&2
  exit 1
fi

git_repo() {
  env \
    -u GIT_ALTERNATE_OBJECT_DIRECTORIES \
    -u GIT_COMMON_DIR \
    -u GIT_DIR \
    -u GIT_INDEX_FILE \
    -u GIT_NAMESPACE \
    -u GIT_OBJECT_DIRECTORY \
    -u GIT_PREFIX \
    -u GIT_WORK_TREE \
    GIT_CONFIG_GLOBAL=/dev/null \
    GIT_CONFIG_NOSYSTEM=1 \
    GIT_NO_REPLACE_OBJECTS=1 \
    GIT_OPTIONAL_LOCKS=0 \
    git \
      -c core.fsmonitor=false \
      -c core.hooksPath=/dev/null \
      -C "$REPO_ROOT" \
      "$@"
}

resolved_root="$(git_repo rev-parse --show-toplevel)"
resolved_root="$(cd "$resolved_root" && pwd -P)"
if [[ "$resolved_root" != "$REPO_ROOT" ]]; then
  echo "error: Git repository root $resolved_root does not match $REPO_ROOT" >&2
  exit 1
fi

HEAD_REVISION="$(git_repo rev-parse --verify HEAD)"
if [[ "$OPEN_SPLUNK_SOURCE_REVISION" == "development" ||
      "$OPEN_SPLUNK_SOURCE_REVISION" != "$HEAD_REVISION" ]]; then
  echo "error: OPEN_SPLUNK_SOURCE_REVISION must equal the current HEAD commit $HEAD_REVISION" >&2
  exit 1
fi

WORK_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/open-splunk-release.XXXXXX")"
WORK_ROOT="$(cd "$WORK_ROOT" && pwd -P)"
SOURCE_ROOT="$WORK_ROOT/source"
HOME_ROOT="$WORK_ROOT/home"
TEMP_ROOT="$WORK_ROOT/tmp"
STATUS_FILE="$WORK_ROOT/git-status"
PUBLISH_ROOT=""
PREVIOUS_BUILD=""
FAILED_BUILD=""
RELEASE_LOCK=""
RELEASE_LOCK_OWNER=""
published=false
publish_renamed=false

rename_exact() {
  env -i \
    PATH="$PATH" \
    HOME="$HOME_ROOT" \
    TMPDIR="$TEMP_ROOT" \
    NODE_OPTIONS="" \
    node -e '
      const fs = require("node:fs");
      if (process.argv.length !== 3) {
        throw new Error("rename_exact requires source and destination");
      }
      fs.renameSync(process.argv[1], process.argv[2]);
    ' "$1" "$2"
}

remove_private_tree() {
  local tree=$1
  local description=$2
  local failed=0

  if [[ ! -e "$tree" && ! -L "$tree" ]]; then
    return 0
  fi
  if [[ -L "$tree" || ! -d "$tree" ]]; then
    echo "error: $description changed before cleanup: $tree" >&2
    return 1
  fi
  # Go's module cache and prior artifacts may contain read-only directories.
  # Restore owner traversal/write permission without following symlinks.
  if ! find "$tree" -type d -exec chmod u+rwx {} +; then
    echo "error: could not make $description removable: $tree" >&2
    failed=1
  fi
  if ! rm -rf "$tree"; then
    echo "error: could not remove $description: $tree" >&2
    failed=1
  fi
  if [[ -e "$tree" || -L "$tree" ]]; then
    echo "error: $description remains after cleanup: $tree" >&2
    failed=1
  fi
  return "$failed"
}

cleanup() {
  local status=$?
  local rollback_failed=0
  local failed_candidate=""
  trap - EXIT
  set +e

  if [[ $status -ne 0 && "$published" != true ]]; then
    if [[ "$publish_renamed" == true && ( -e "$REPO_ROOT/build" || -L "$REPO_ROOT/build" ) ]]; then
      failed_candidate="$(mktemp -d "$REPO_ROOT/.cache/release-failed.XXXXXX")"
      rmdir "$failed_candidate"
      if rename_exact "$REPO_ROOT/build" "$failed_candidate"; then
        FAILED_BUILD="$failed_candidate"
      else
        echo "error: could not preserve the failed published build during rollback" >&2
        rollback_failed=1
      fi
    elif [[ -n "$PREVIOUS_BUILD" && -d "$PREVIOUS_BUILD" &&
            "$publish_renamed" != true &&
            ( -e "$REPO_ROOT/build" || -L "$REPO_ROOT/build" ) ]]; then
      echo "error: release rollback destination is occupied by a concurrently created path" >&2
      rollback_failed=1
    fi

    if [[ -n "$PREVIOUS_BUILD" && -d "$PREVIOUS_BUILD" ]]; then
      if [[ ! -e "$REPO_ROOT/build" && ! -L "$REPO_ROOT/build" ]]; then
        if rename_exact "$PREVIOUS_BUILD" "$REPO_ROOT/build"; then
          PREVIOUS_BUILD=""
        else
          echo "error: could not restore the previous release build" >&2
          rollback_failed=1
        fi
      else
        rollback_failed=1
      fi
    fi
  fi

  if [[ -n "$PUBLISH_ROOT" ]]; then
    if ! remove_private_tree "$PUBLISH_ROOT" "unpublished release output"; then
      status=1
    fi
  fi
  if [[ "$published" == true && -n "$PREVIOUS_BUILD" ]]; then
    if ! remove_private_tree "$PREVIOUS_BUILD" "previous release build"; then
      status=1
    fi
  fi
  if [[ $rollback_failed -eq 0 && -n "$FAILED_BUILD" ]]; then
    if ! remove_private_tree "$FAILED_BUILD" "failed published build"; then
      status=1
    fi
  elif [[ $rollback_failed -ne 0 ]]; then
    echo "error: release rollback is incomplete" >&2
    if [[ -n "$PREVIOUS_BUILD" && -d "$PREVIOUS_BUILD" ]]; then
      echo "error: previous build preserved at $PREVIOUS_BUILD" >&2
    fi
    if [[ -n "$FAILED_BUILD" && -d "$FAILED_BUILD" ]]; then
      echo "error: failed published build preserved at $FAILED_BUILD" >&2
    fi
  fi
  if ! remove_private_tree "$WORK_ROOT" "release work root"; then
    status=1
  fi
  if [[ -n "$RELEASE_LOCK" ]]; then
    if [[ -L "$RELEASE_LOCK" || ! -d "$RELEASE_LOCK" ]]; then
      echo "error: repository release lock changed while held: $RELEASE_LOCK" >&2
      status=1
    else
      rm -f "$RELEASE_LOCK_OWNER"
      if ! rmdir "$RELEASE_LOCK"; then
        echo "error: could not release repository release lock $RELEASE_LOCK" >&2
        status=1
      fi
    fi
  fi
  exit "$status"
}
trap cleanup EXIT

if ! git_repo status --porcelain=v1 -z --untracked-files=all >"$STATUS_FILE"; then
  echo "error: could not inspect the release worktree" >&2
  exit 1
fi
if [[ -s "$STATUS_FILE" ]]; then
  echo "error: release builds require a clean worktree; commit or remove local changes first" >&2
  exit 1
fi

mkdir -p "$HOME_ROOT" "$TEMP_ROOT"
MATERIALIZER_PATH="scripts/materialize-git-snapshot.mjs"
MATERIALIZER_ENTRY="$(git_repo ls-tree "$HEAD_REVISION" -- "$MATERIALIZER_PATH")"
MATERIALIZER_HEADER="${MATERIALIZER_ENTRY%%$'\t'*}"
MATERIALIZER_TREE_PATH="${MATERIALIZER_ENTRY#*$'\t'}"
if [[ "$MATERIALIZER_TREE_PATH" != "$MATERIALIZER_PATH" ||
      "$MATERIALIZER_HEADER" != 100755\ blob\ * ]]; then
  echo "error: HEAD must contain executable regular file $MATERIALIZER_PATH" >&2
  exit 1
fi
MATERIALIZER_OBJECT="${MATERIALIZER_HEADER##* }"
if [[ ! "$MATERIALIZER_OBJECT" =~ ^[0-9a-f]{40}([0-9a-f]{24})?$ ]]; then
  echo "error: HEAD contains an invalid object ID for $MATERIALIZER_PATH" >&2
  exit 1
fi
MATERIALIZER_BOOTSTRAP="$WORK_ROOT/materialize-git-snapshot.mjs"
git_repo cat-file blob "$MATERIALIZER_OBJECT" >"$MATERIALIZER_BOOTSTRAP"
chmod 0555 "$MATERIALIZER_BOOTSTRAP"
env -i \
  PATH="$PATH" \
  HOME="$HOME_ROOT" \
  TMPDIR="$TEMP_ROOT" \
  LANG=C \
  LC_ALL=C \
  NODE_OPTIONS="" \
  TZ=UTC \
  node "$MATERIALIZER_BOOTSTRAP" "$REPO_ROOT" "$HEAD_REVISION" "$SOURCE_ROOT"

SOURCE_DATE_EPOCH="$(git_repo show -s --format=%ct "$HEAD_REVISION")"
if [[ ! "$SOURCE_DATE_EPOCH" =~ ^[0-9]+$ ]]; then
  echo "error: invalid commit timestamp for $HEAD_REVISION" >&2
  exit 1
fi

RELEASE_ENVIRONMENT=(
  env -i
  "PATH=$PATH"
  "HOME=$HOME_ROOT"
  "TMPDIR=$TEMP_ROOT"
  "CI=1"
  "LANG=C"
  "LC_ALL=C"
  "NODE_ENV=production"
  "NODE_OPTIONS="
  "NEXT_TELEMETRY_DISABLED=1"
  "SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH"
  "TZ=UTC"
  "GOCACHE=$WORK_ROOT/go-build-cache"
  "GOMODCACHE=$WORK_ROOT/go-module-cache"
  "GOAUTH=off"
  "GOINSECURE="
  "GONOPROXY="
  "GONOSUMDB="
  "GOPRIVATE="
  "GOPROXY=https://proxy.golang.org,direct"
  "GOSUMDB=sum.golang.org"
  "GOVCS=*:git|hg"
  "GIT_CONFIG_GLOBAL=/dev/null"
  "GIT_CONFIG_NOSYSTEM=1"
  "GIT_CEILING_DIRECTORIES=$SOURCE_ROOT"
  "GIT_NO_REPLACE_OBJECTS=1"
  "GIT_OPTIONAL_LOCKS=0"
  "GIT_TERMINAL_PROMPT=0"
  "npm_config_audit=false"
  "npm_config_cache=$WORK_ROOT/npm-cache"
  "npm_config_fund=false"
  "npm_config_globalconfig=/dev/null"
  "npm_config_include=dev"
  "npm_config_ignore_scripts=false"
  "npm_config_registry=https://registry.npmjs.org/"
  "npm_config_update_notifier=false"
)

"${RELEASE_ENVIRONMENT[@]}" make -C "$SOURCE_ROOT" proto-tools release-go-deps

"${RELEASE_ENVIRONMENT[@]}" make -C "$SOURCE_ROOT" build build-loggen \
    OPEN_SPLUNK_APPLICATION_VERSION="$OPEN_SPLUNK_APPLICATION_VERSION" \
    OPEN_SPLUNK_DATA_MODE=backend \
    OPEN_SPLUNK_SOURCE_REVISION="$HEAD_REVISION"

for artifact in open-splunk-server open-splunk-collector open-splunk-loggen; do
  if [[ -L "$SOURCE_ROOT/build/$artifact" || ! -f "$SOURCE_ROOT/build/$artifact" ]]; then
    echo "error: release build did not produce regular artifact build/$artifact" >&2
    exit 1
  fi
done
if [[ -L "$SOURCE_ROOT/out/asset-manifest.json" || ! -f "$SOURCE_ROOT/out/asset-manifest.json" ]]; then
  echo "error: release build did not produce a regular canonical asset manifest" >&2
  exit 1
fi

if [[ -L "$REPO_ROOT/.cache" ]]; then
  echo "error: repository .cache path must not be a symbolic link" >&2
  exit 1
fi
mkdir -p "$REPO_ROOT/.cache"
if [[ ! -d "$REPO_ROOT/.cache" ]]; then
  echo "error: repository .cache path is not a directory" >&2
  exit 1
fi
if [[ -L "$REPO_ROOT/build" || ( -e "$REPO_ROOT/build" && ! -d "$REPO_ROOT/build" ) ]]; then
  echo "error: repository build path must be absent or a real directory" >&2
  exit 1
fi
release_lock_candidate="$REPO_ROOT/.cache/release.lock"
if ! mkdir "$release_lock_candidate"; then
  echo "error: another release is publishing, or a stale release lock exists at $release_lock_candidate" >&2
  exit 1
fi
RELEASE_LOCK="$release_lock_candidate"
RELEASE_LOCK_OWNER="$RELEASE_LOCK/owner.$$"
if ! : >"$RELEASE_LOCK_OWNER"; then
  rmdir "$RELEASE_LOCK"
  RELEASE_LOCK=""
  RELEASE_LOCK_OWNER=""
  echo "error: could not initialize repository release lock" >&2
  exit 1
fi

PUBLISH_ROOT="$(mktemp -d "$REPO_ROOT/.cache/release-output.XXXXXX")"
install -m 0755 "$SOURCE_ROOT/build/open-splunk-server" "$PUBLISH_ROOT/open-splunk-server"
install -m 0755 "$SOURCE_ROOT/build/open-splunk-collector" "$PUBLISH_ROOT/open-splunk-collector"
install -m 0755 "$SOURCE_ROOT/build/open-splunk-loggen" "$PUBLISH_ROOT/open-splunk-loggen"
install -m 0644 "$SOURCE_ROOT/out/asset-manifest.json" "$PUBLISH_ROOT/asset-manifest.json"

EXPECTED_IDENTITY="$WORK_ROOT/expected-identity.txt"
printf \
  'application_version=%s\nsource_revision=%s\n' \
  "$OPEN_SPLUNK_APPLICATION_VERSION" \
  "$HEAD_REVISION" >"$EXPECTED_IDENTITY"
"$PUBLISH_ROOT/open-splunk-server" -verify-embedded-release >"$PUBLISH_ROOT/release-verification.txt"
sed -n '1,2p' "$PUBLISH_ROOT/release-verification.txt" >"$WORK_ROOT/server-identity.txt"
cmp "$EXPECTED_IDENTITY" "$WORK_ROOT/server-identity.txt"
"$PUBLISH_ROOT/open-splunk-collector" version >"$WORK_ROOT/collector-identity.txt"
cmp "$EXPECTED_IDENTITY" "$WORK_ROOT/collector-identity.txt"
"$PUBLISH_ROOT/open-splunk-loggen" -version >"$WORK_ROOT/loggen-identity.txt"
cmp "$EXPECTED_IDENTITY" "$WORK_ROOT/loggen-identity.txt"
EXPECTED_BINARY_IDENTITIES="$WORK_ROOT/expected-binary-identities.txt"
{
  printf 'open-splunk-server\n'
  cat "$WORK_ROOT/server-identity.txt"
  printf 'open-splunk-collector\n'
  cat "$WORK_ROOT/collector-identity.txt"
  printf 'open-splunk-loggen\n'
  cat "$WORK_ROOT/loggen-identity.txt"
} >"$EXPECTED_BINARY_IDENTITIES"
install -m 0644 "$EXPECTED_BINARY_IDENTITIES" "$PUBLISH_ROOT/binary-identities.txt"
chmod 0644 "$PUBLISH_ROOT/release-verification.txt"

# A slower build must not overwrite a newer release that committed and
# published while this process was preparing its private snapshot. Sample HEAD
# on both sides of status so a ref update during the inspection also fails.
if ! PUBLISH_HEAD_BEFORE="$(git_repo rev-parse --verify HEAD)"; then
  echo "error: could not revalidate repository HEAD before release publication" >&2
  exit 1
fi
if ! git_repo status --porcelain=v1 -z --untracked-files=all >"$STATUS_FILE"; then
  echo "error: could not revalidate the release worktree before publication" >&2
  exit 1
fi
if ! PUBLISH_HEAD_AFTER="$(git_repo rev-parse --verify HEAD)"; then
  echo "error: could not revalidate repository HEAD before release publication" >&2
  exit 1
fi
if [[ "$PUBLISH_HEAD_BEFORE" != "$HEAD_REVISION" ||
      "$PUBLISH_HEAD_AFTER" != "$HEAD_REVISION" ]]; then
  echo "error: repository HEAD changed after release snapshot $HEAD_REVISION; refusing stale publication" >&2
  exit 1
fi
if [[ -s "$STATUS_FILE" ]]; then
  echo "error: repository worktree changed after release snapshot; refusing publication" >&2
  exit 1
fi

if [[ -d "$REPO_ROOT/build" ]]; then
  previous_candidate="$(mktemp -d "$REPO_ROOT/.cache/release-previous.XXXXXX")"
  rmdir "$previous_candidate"
  if ! rename_exact "$REPO_ROOT/build" "$previous_candidate"; then
    echo "error: could not move the previous build into the release transaction" >&2
    exit 1
  fi
  PREVIOUS_BUILD="$previous_candidate"
fi
if ! rename_exact "$PUBLISH_ROOT" "$REPO_ROOT/build"; then
  echo "error: could not atomically publish the verified release build" >&2
  exit 1
fi
PUBLISH_ROOT=""
publish_renamed=true

for artifact in \
  open-splunk-server \
  open-splunk-collector \
  open-splunk-loggen \
  asset-manifest.json \
  release-verification.txt \
  binary-identities.txt; do
  if [[ -L "$REPO_ROOT/build/$artifact" || ! -f "$REPO_ROOT/build/$artifact" ]]; then
    echo "error: published release artifact is missing or irregular: build/$artifact" >&2
    exit 1
  fi
done
cmp "$SOURCE_ROOT/out/asset-manifest.json" "$REPO_ROOT/build/asset-manifest.json"
cmp "$EXPECTED_BINARY_IDENTITIES" "$REPO_ROOT/build/binary-identities.txt"
"$REPO_ROOT/build/open-splunk-server" -verify-embedded-release >"$WORK_ROOT/published-verification.txt"
cmp "$REPO_ROOT/build/release-verification.txt" "$WORK_ROOT/published-verification.txt"
"$REPO_ROOT/build/open-splunk-collector" version >"$WORK_ROOT/published-collector-identity.txt"
cmp "$EXPECTED_IDENTITY" "$WORK_ROOT/published-collector-identity.txt"
"$REPO_ROOT/build/open-splunk-loggen" -version >"$WORK_ROOT/published-loggen-identity.txt"
cmp "$EXPECTED_IDENTITY" "$WORK_ROOT/published-loggen-identity.txt"
published=true

echo "release built from committed snapshot $HEAD_REVISION"
