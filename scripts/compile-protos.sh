#!/usr/bin/env bash
set -euo pipefail
umask 022

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$REPO_ROOT/proto"
GEN_OUT="$REPO_ROOT/gen"
GO_OUT="$REPO_ROOT/gen/go"
TS_OUT="$REPO_ROOT/gen/ts"
BUF_BIN="$REPO_ROOT/node_modules/.bin/buf"
GO_PLUGIN="$REPO_ROOT/.cache/proto-tools/protoc-gen-go"
GO_GRPC_PLUGIN="$REPO_ROOT/.cache/proto-tools/protoc-gen-go-grpc"
export BUF_CACHE_DIR="${BUF_CACHE_DIR:-$REPO_ROOT/.cache/buf}"

if [[ $# -ne 0 ]]; then
  echo "error: partial protobuf compilation is not supported" >&2
  echo "hint: run 'make proto' to regenerate every contract" >&2
  exit 1
fi

if [[ ! -x "$BUF_BIN" ||
  ! -x "$REPO_ROOT/node_modules/.bin/protoc-gen-ts_proto" ||
  ! -x "$GO_PLUGIN" ||
  ! -x "$GO_GRPC_PLUGIN" ]]; then
  echo "error: pinned protobuf tools are not installed in the repository" >&2
  echo "hint: run 'make proto-tools'" >&2
  exit 1
fi

PROTO_FILES=()
while IFS= read -r proto_file; do
  PROTO_FILES+=("$proto_file")
done < <(find "$PROTO_DIR" -type f -name '*.proto' | sort)

if [[ ${#PROTO_FILES[@]} -eq 0 ]]; then
  echo "No protobuf sources found under $PROTO_DIR; nothing to generate."
  exit 0
fi

if [[ -L "$GEN_OUT" ]]; then
  echo "error: protobuf output directory is a symbolic link: $GEN_OUT" >&2
  exit 1
fi
if [[ -e "$GEN_OUT" && ! -d "$GEN_OUT" ]]; then
  echo "error: protobuf output path is not a directory: $GEN_OUT" >&2
  exit 1
fi
mkdir -p "$GEN_OUT"

# Buf resolves its inputs, relative plugin paths, and template outputs from the
# current working directory. Always anchor those inputs to the repository even
# when this script is invoked by absolute path from elsewhere.
cd "$REPO_ROOT"

# Generate and validate a complete replacement before touching the checked-in
# trees. The final directory swaps retain hand-written files and roll back both
# languages if publication fails after either swap.
GENERATION_LOCK=""
GENERATION_LOCK_OWNER=""
TRANSACTION_ROOT="$(mktemp -d "$GEN_OUT/.proto-generation.XXXXXX")"
STAGED_ROOT="$TRANSACTION_ROOT/staged"
STAGED_GO="$STAGED_ROOT/gen/go"
STAGED_TS="$STAGED_ROOT/gen/ts"
NEXT_GO="$TRANSACTION_ROOT/next-go"
NEXT_TS="$TRANSACTION_ROOT/next-ts"
OLD_GO="$TRANSACTION_ROOT/old-go"
OLD_TS="$TRANSACTION_ROOT/old-ts"
PUBLISHING=0

cleanup() {
  local exit_status=$?
  local rollback_failed=0
  trap - EXIT
  set +e
  if [[ $exit_status -ne 0 && $PUBLISHING -eq 1 ]]; then
    if [[ -d "$OLD_TS" ]]; then
      if [[ -d "$TS_OUT" ]]; then
        if ! mv "$TS_OUT" "$TRANSACTION_ROOT/failed-ts"; then
          echo "error: could not move the partially published TypeScript tree during rollback" >&2
          rollback_failed=1
        fi
      fi
      if [[ ! -e "$TS_OUT" ]]; then
        if ! mv "$OLD_TS" "$TS_OUT"; then
          echo "error: could not restore the previous TypeScript protobuf tree" >&2
          rollback_failed=1
        fi
      else
        echo "error: TypeScript protobuf rollback destination is still occupied" >&2
        rollback_failed=1
      fi
    fi
    if [[ -d "$OLD_GO" ]]; then
      if [[ -d "$GO_OUT" ]]; then
        if ! mv "$GO_OUT" "$TRANSACTION_ROOT/failed-go"; then
          echo "error: could not move the partially published Go tree during rollback" >&2
          rollback_failed=1
        fi
      fi
      if [[ ! -e "$GO_OUT" ]]; then
        if ! mv "$OLD_GO" "$GO_OUT"; then
          echo "error: could not restore the previous Go protobuf tree" >&2
          rollback_failed=1
        fi
      else
        echo "error: Go protobuf rollback destination is still occupied" >&2
        rollback_failed=1
      fi
    fi
  fi
  if [[ $rollback_failed -eq 0 ]]; then
    rm -rf "$TRANSACTION_ROOT"
  else
    echo "error: protobuf rollback is incomplete; preserved recovery data at $TRANSACTION_ROOT" >&2
  fi
  if [[ -n "$GENERATION_LOCK" ]]; then
    if [[ -L "$GENERATION_LOCK" || ! -d "$GENERATION_LOCK" ]]; then
      echo "error: protobuf generation lock changed while held: $GENERATION_LOCK" >&2
      exit_status=1
    else
      rm -f "$GENERATION_LOCK_OWNER"
      if ! rmdir "$GENERATION_LOCK"; then
        echo "error: could not release protobuf generation lock $GENERATION_LOCK" >&2
        exit_status=1
      fi
    fi
  fi
  exit "$exit_status"
}
trap cleanup EXIT

generation_lock_candidate="$GEN_OUT/.proto-generation.lock"
if ! mkdir "$generation_lock_candidate"; then
  echo "error: another protobuf generation is running, or a stale lock exists at $generation_lock_candidate" >&2
  exit 1
fi
GENERATION_LOCK="$generation_lock_candidate"
GENERATION_LOCK_OWNER="$GENERATION_LOCK/owner.$$"
if ! : >"$GENERATION_LOCK_OWNER"; then
  rmdir "$GENERATION_LOCK"
  GENERATION_LOCK=""
  GENERATION_LOCK_OWNER=""
  echo "error: could not initialize protobuf generation lock" >&2
  exit 1
fi

# No process may inspect or create a language output tree before holding the
# lock: a publisher temporarily removes each tree while atomically swapping it.
for output_directory in "$GO_OUT" "$TS_OUT"; do
  if [[ -L "$output_directory" ]]; then
    echo "error: protobuf output directory is a symbolic link: $output_directory" >&2
    exit 1
  fi
  if [[ -e "$output_directory" && ! -d "$output_directory" ]]; then
    echo "error: protobuf output path is not a directory: $output_directory" >&2
    exit 1
  fi
  mkdir -p "$output_directory"
done

if [[ -n "$(find "$GO_OUT" "$TS_OUT" -type l -print -quit)" ]]; then
  echo "error: protobuf output trees must not contain symbolic links" >&2
  exit 1
fi

"$BUF_BIN" generate \
  --template "$REPO_ROOT/buf.gen.yaml" \
  --output "$STAGED_ROOT"

if [[ ! -d "$STAGED_GO" || ! -d "$STAGED_TS" ]]; then
  echo "error: protobuf generators did not create both output trees" >&2
  exit 1
fi
if [[ -n "$(find "$STAGED_GO" "$STAGED_TS" -type l -print -quit)" ]]; then
  echo "error: protobuf generators produced a symbolic link" >&2
  exit 1
fi

for proto_file in "${PROTO_FILES[@]}"; do
  relative_proto="${proto_file#"$PROTO_DIR/"}"
  generated_stem="${relative_proto%.proto}"
  if [[ ! -f "$STAGED_GO/$generated_stem.pb.go" ]]; then
    echo "error: missing generated Go message file for $relative_proto" >&2
    exit 1
  fi
  if [[ ! -f "$STAGED_TS/$generated_stem.ts" ]]; then
    echo "error: missing generated TypeScript message file for $relative_proto" >&2
    exit 1
  fi
  if grep -Eq '^[[:space:]]*service[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*[{]' "$proto_file" &&
    [[ ! -f "$STAGED_GO/$generated_stem"_grpc.pb.go ]]; then
    echo "error: missing generated Go gRPC file for $relative_proto" >&2
    exit 1
  fi
done
for expected_typescript_output in \
  "google/protobuf/duration.ts" \
  "google/protobuf/field_mask.ts" \
  "google/protobuf/timestamp.ts" \
  "index.google.protobuf.ts" \
  "index.google.ts" \
  "index.open_splunk.ts" \
  "index.ts"; do
  if [[ ! -f "$STAGED_TS/$expected_typescript_output" ]]; then
    echo "error: missing generated TypeScript support file $expected_typescript_output" >&2
    exit 1
  fi
done

strip_typescript_lint_headers() {
  local generated_file filtered_file lint_header
  lint_header='/* eslint-''disable */'

  while IFS= read -r -d '' generated_file; do
    filtered_file="$TRANSACTION_ROOT/typescript-without-lint-header"
    if ! awk -v lint_header="$lint_header" '$0 != lint_header' "$generated_file" >"$filtered_file"; then
      echo "error: could not remove generated TypeScript lint header from $generated_file" >&2
      return 1
    fi
    if ! cp "$filtered_file" "$generated_file"; then
      echo "error: could not publish sanitized generated TypeScript file $generated_file" >&2
      return 1
    fi
    rm -f "$filtered_file"
  done < <(find "$STAGED_TS" -type f -name '*.ts' -print0)
}

strip_typescript_lint_headers

copy_preserved_files() {
  local source_root=$1
  local destination_root=$2
  local generated_pattern=$3
  local source_path relative_path destination_path

  while IFS= read -r -d '' source_path; do
    relative_path="${source_path#"$source_root/"}"
    destination_path="$destination_root/$relative_path"
    mkdir -p "$(dirname "$destination_path")"
    cp -p "$source_path" "$destination_path"
  done < <(find "$source_root" -type f ! -name "$generated_pattern" -print0)
}

mkdir "$NEXT_GO" "$NEXT_TS"
copy_preserved_files "$GO_OUT" "$NEXT_GO" '*.pb.go'
copy_preserved_files "$TS_OUT" "$NEXT_TS" '*.ts'
cp -R "$STAGED_GO/." "$NEXT_GO/"
cp -R "$STAGED_TS/." "$NEXT_TS/"

PUBLISHING=1
mv "$GO_OUT" "$OLD_GO"
mv "$NEXT_GO" "$GO_OUT"
mv "$TS_OUT" "$OLD_TS"
mv "$NEXT_TS" "$TS_OUT"
PUBLISHING=0
