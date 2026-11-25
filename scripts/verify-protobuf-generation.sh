#!/usr/bin/env bash
set -euo pipefail

test "$#" -gt 0
snapshot="$(mktemp -d "${TMPDIR:-/tmp}/open-splunk-protobuf-snapshot.XXXXXX")"
trap 'rm -rf -- "$snapshot"' EXIT HUP INT TERM
cp -R proto "$snapshot/proto"
cp -R gen/go "$snapshot/go"
"$@"
diff -ru "$snapshot/proto" proto
diff -ru "$snapshot/go" gen/go
