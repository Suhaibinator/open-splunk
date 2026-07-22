#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="$REPO_ROOT/proto"
GO_OUT="$REPO_ROOT/gen/go"
TS_OUT="$REPO_ROOT/gen/ts"
TS_PLUGIN="$REPO_ROOT/node_modules/.bin/protoc-gen-ts_proto"

# Keep these options aligned with GradeThis's browser protobuf contract so the
# two TypeScript codebases share the same representation for dates, int64s,
# enums, JSON conversion, partial construction, and generated barrel files.
# Open Splunk additionally uses discriminated unions for oneofs and does not
# generate RPC clients: the browser transport is SRouter/WebSocket, while gRPC
# service stubs are generated for Go separately below.
TS_PROTO_OPTIONS=(
  "esModuleInterop=true"
  "forceLong=bigint"
  "exportCommonSymbols=false"
  "snakeToCamel=true"
  "oneof=unions-value"
  "outputEncodeMethods=true"
  "outputJsonMethods=true"
  "outputPartialMethods=true"
  "outputServices=none"
  "stringEnums=false"
  "useDate=true"
  "outputIndex=true"
)
TS_PROTO_OPT="$(IFS=,; echo "${TS_PROTO_OPTIONS[*]}")"

PROTOC_BIN="$(command -v protoc || true)"
PROTOC_INCLUDE=""

if [[ $# -ne 0 ]]; then
  echo "error: partial protobuf compilation is not supported" >&2
  echo "hint: run 'make proto' to regenerate every contract" >&2
  exit 1
fi

for command_name in protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "error: $command_name is not installed or not on PATH" >&2
    echo "hint: run 'make proto-tools'; protoc itself must also be installed" >&2
    exit 1
  fi
done

if [[ -z "$PROTOC_BIN" ]]; then
  echo "error: protoc is not installed or not on PATH" >&2
  echo "hint: install protoc, then run 'make proto' again" >&2
  exit 1
fi

# Supplying --proto_path disables reliance on platform-specific implicit
# include behavior. Locate protoc's well-known type sources explicitly so the
# same command works with Homebrew and Linux distribution packages.
for include_dir in \
  "$(dirname "$PROTOC_BIN")/../include" \
  "/usr/local/include" \
  "/usr/include"; do
  if [[ -f "$include_dir/google/protobuf/timestamp.proto" ]]; then
    PROTOC_INCLUDE="$include_dir"
    break
  fi
done

if [[ -z "$PROTOC_INCLUDE" ]]; then
  echo "error: protoc well-known types were not found" >&2
  echo "hint: install the protobuf compiler and development files" >&2
  exit 1
fi

if [[ ! -x "$TS_PLUGIN" ]]; then
  echo "error: ts-proto is not installed at $TS_PLUGIN" >&2
  echo "hint: run 'npm ci' or 'make proto-tools'" >&2
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

mkdir -p "$GO_OUT" "$TS_OUT"

# Generation is all-or-nothing because ts-proto index files span the complete
# source set. Preserve hand-written README files while removing stale outputs.
find "$GO_OUT" -type f -name '*.pb.go' -delete
find "$TS_OUT" -type f -name '*.ts' -delete

protoc \
  --proto_path="$PROTO_DIR" \
  --proto_path="$PROTOC_INCLUDE" \
  "${PROTO_FILES[@]}" \
  --go_out="$GO_OUT" \
  --go_opt=paths=source_relative \
  --go-grpc_out="$GO_OUT" \
  --go-grpc_opt=paths=source_relative \
  --plugin="protoc-gen-ts_proto=$TS_PLUGIN" \
  --ts_proto_out="$TS_OUT" \
  --ts_proto_opt="$TS_PROTO_OPT"
