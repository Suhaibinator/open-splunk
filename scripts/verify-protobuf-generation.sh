#!/usr/bin/env bash
set -euo pipefail

snapshot="$(mktemp -d "${TMPDIR:-/tmp}/open-splunk-protobuf-snapshot.XXXXXX")"
trap 'rm -rf -- "$snapshot"' EXIT HUP INT TERM
cp -R proto gen "$snapshot"
make proto
diff -ru "$snapshot/proto" proto
diff -ru "$snapshot/gen" gen
