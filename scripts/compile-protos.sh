#!/bin/sh
set -eu
umask 022

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_DIR="$REPO_ROOT/proto"
GEN_OUT="$REPO_ROOT/gen"
BUF_BIN="$REPO_ROOT/node_modules/.bin/buf"
export BUF_CACHE_DIR="${BUF_CACHE_DIR:-$REPO_ROOT/.cache/buf}"

case "$#:${1:-}" in
  0:) set -- go ts ;;
  1:--go-only) set -- go ;;
  1:--typescript-only) set -- ts ;;
  *)
    echo "usage: $0 [--go-only | --typescript-only]" >&2
    exit 1
    ;;
esac

if [ ! -x "$BUF_BIN" ]; then
  echo "error: Buf is missing; run 'npm ci --include=dev'" >&2
  exit 1
fi
for language do
  case "$language" in
    go)
      if [ ! -x "$REPO_ROOT/.cache/proto-tools/protoc-gen-go" ] ||
        [ ! -x "$REPO_ROOT/.cache/proto-tools/protoc-gen-go-grpc" ]; then
        echo "error: Go protobuf plugins are missing; run 'make proto-tools'" >&2
        exit 1
      fi
      ;;
    ts)
      if [ ! -x "$REPO_ROOT/node_modules/.bin/protoc-gen-ts_proto" ]; then
        echo "error: ts-proto is missing; run 'npm ci --include=dev'" >&2
        exit 1
      fi
      ;;
  esac
done

first_proto="$(find "$PROTO_DIR" -type f -name '*.proto' -print -quit)"
if [ -z "$first_proto" ]; then
  echo "No protobuf sources found under $PROTO_DIR; nothing to generate."
  exit 0
fi

if [ -L "$GEN_OUT" ]; then
  echo "error: protobuf output directory is a symbolic link: $GEN_OUT" >&2
  exit 1
fi
if [ -e "$GEN_OUT" ] && [ ! -d "$GEN_OUT" ]; then
  echo "error: protobuf output path is not a directory: $GEN_OUT" >&2
  exit 1
fi
mkdir -p "$GEN_OUT"

# Buf resolves relative plugin paths and inputs from the working directory.
cd "$REPO_ROOT"

# Validate all selected outputs before publishing any of them. Retain
# handwritten files and restore the old trees if a directory swap fails.
GENERATION_LOCK=""
TRANSACTION_ROOT="$(mktemp -d "$GEN_OUT/.proto-generation.XXXXXX")"
STAGED_ROOT="$TRANSACTION_ROOT/staged"
PUBLISHING=0

cleanup() {
  exit_status=$?
  rollback_failed=0
  trap - EXIT
  set +e
  if [ "$exit_status" -ne 0 ] && [ "$PUBLISHING" -eq 1 ]; then
    for language in ts go; do
      old_output="$TRANSACTION_ROOT/old-$language"
      output="$GEN_OUT/$language"
      if [ ! -d "$old_output" ]; then
        continue
      fi
      if [ -d "$output" ] && ! mv "$output" "$TRANSACTION_ROOT/failed-$language"; then
        echo "error: could not move the partially published protobuf tree at $output" >&2
        rollback_failed=1
      fi
      if [ -e "$output" ] || ! mv "$old_output" "$output"; then
        echo "error: could not restore the previous protobuf tree at $output" >&2
        rollback_failed=1
      fi
    done
  fi
  if [ "$rollback_failed" -eq 0 ]; then
    rm -rf "$TRANSACTION_ROOT"
  else
    echo "error: protobuf rollback is incomplete; preserved recovery data at $TRANSACTION_ROOT" >&2
  fi
  if [ -n "$GENERATION_LOCK" ]; then
    if [ -L "$GENERATION_LOCK" ] || [ ! -d "$GENERATION_LOCK" ]; then
      echo "error: protobuf generation lock changed while held: $GENERATION_LOCK" >&2
      exit_status=1
    else
      rm -f "$GENERATION_LOCK/owner.$$"
      if ! rmdir "$GENERATION_LOCK"; then
        echo "error: could not release protobuf generation lock $GENERATION_LOCK" >&2
        exit_status=1
      fi
    fi
  fi
  exit "$exit_status"
}
trap cleanup EXIT

lock_candidate="$GEN_OUT/.proto-generation.lock"
if ! mkdir "$lock_candidate"; then
  echo "error: another protobuf generation is running, or a stale lock exists at $lock_candidate" >&2
  exit 1
fi
GENERATION_LOCK="$lock_candidate"
if ! : >"$GENERATION_LOCK/owner.$$"; then
  rmdir "$GENERATION_LOCK"
  GENERATION_LOCK=""
  echo "error: could not initialize protobuf generation lock" >&2
  exit 1
fi

validate_messages() {
  find "$PROTO_DIR" -type f -name '*.proto' -exec sh -ec '
    proto_dir=$1
    staged=$2
    extension=$3
    shift 3
    for proto_file do
      relative_proto=${proto_file#"$proto_dir/"}
      stem=${relative_proto%.proto}
      if [ ! -f "$staged/$stem$extension" ]; then
        echo "error: missing generated $extension message file for $relative_proto" >&2
        exit 1
      fi
      if [ "$extension" = .pb.go ] &&
        grep -Eq "^[[:space:]]*service[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*[{]" "$proto_file" &&
        [ ! -f "$staged/${stem}_grpc.pb.go" ]; then
        echo "error: missing generated Go gRPC file for $relative_proto" >&2
        exit 1
      fi
    done
  ' sh "$PROTO_DIR" "$1" "$2" {} +
}

sanitize_typescript() {
  find "$1" -type f -name '*.ts' -exec sh -ec '
    filtered_file=$1
    lint_header=$2
    shift 2
    for generated_file do
      if ! awk -v lint_header="$lint_header" '\''$0 != lint_header'\'' "$generated_file" >"$filtered_file"; then
        echo "error: could not remove generated TypeScript lint header from $generated_file" >&2
        exit 1
      fi
      cp "$filtered_file" "$generated_file"
    done
  ' sh "$TRANSACTION_ROOT/typescript-without-lint-header" '/* eslint-''disable */' {} +
}

stage_output() (
  language=$1
  output="$GEN_OUT/$language"
  staged="$STAGED_ROOT/gen/$language"
  next="$TRANSACTION_ROOT/next-$language"

  # Take the lock before inspecting the output, which another publisher
  # temporarily removes during its directory swap.
  if [ -L "$output" ]; then
    echo "error: protobuf output directory is a symbolic link: $output" >&2
    exit 1
  fi
  if [ -e "$output" ] && [ ! -d "$output" ]; then
    echo "error: protobuf output path is not a directory: $output" >&2
    exit 1
  fi
  mkdir -p "$output"
  if [ -n "$(find "$output" -type l -print -quit)" ]; then
    echo "error: protobuf output trees must not contain symbolic links" >&2
    exit 1
  fi

  case "$language" in
    go) template=buf.gen.yaml; extension=.pb.go ;;
    ts) template=buf.gen.ts.yaml; extension=.ts ;;
  esac
  "$BUF_BIN" generate --template "$REPO_ROOT/$template" --output "$STAGED_ROOT"
  if [ ! -d "$staged" ]; then
    echo "error: protobuf generator did not create $staged" >&2
    exit 1
  fi
  if [ -n "$(find "$staged" -type l -print -quit)" ]; then
    echo "error: protobuf generator produced a symbolic link" >&2
    exit 1
  fi
  validate_messages "$staged" "$extension"

  if [ "$language" = ts ]; then
    for expected in \
      google/protobuf/duration.ts \
      google/protobuf/field_mask.ts \
      google/protobuf/timestamp.ts \
      index.google.protobuf.ts \
      index.google.ts \
      index.open_splunk.ts \
      index.ts; do
      if [ ! -f "$staged/$expected" ]; then
        echo "error: missing generated TypeScript support file $expected" >&2
        exit 1
      fi
    done
    sanitize_typescript "$staged"
  fi

  mkdir "$next"
  find "$output" -type f ! -name "*$extension" -exec sh -ec '
    source_root=$1
    destination_root=$2
    shift 2
    for source_path do
      relative_path=${source_path#"$source_root/"}
      destination="$destination_root/$relative_path"
      mkdir -p "$(dirname "$destination")"
      cp -p "$source_path" "$destination"
    done
  ' sh "$output" "$next" {} +
  cp -R "$staged/." "$next/"
)

for language do
  stage_output "$language"
done

PUBLISHING=1
for language do
  mv "$GEN_OUT/$language" "$TRANSACTION_ROOT/old-$language"
  mv "$TRANSACTION_ROOT/next-$language" "$GEN_OUT/$language"
done
PUBLISHING=0
