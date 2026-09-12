#!/usr/bin/env bash
# Opt-in disposable recovery drill. Run from the prepared release source tree.
set -euo pipefail
if [[ "${OPEN_SPLUNK_DEPLOYMENT_RECOVERY_DRILL:-}" != 1 ]]; then
  echo 'Set OPEN_SPLUNK_DEPLOYMENT_RECOVERY_DRILL=1 to authorize the disposable Docker drill.' >&2
  exit 1
fi
: "${OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE:?set the exact locally built server image}"
repository=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$repository"
# The static test helper embeds the same already-built UI/migration manifest as
# the release binary. Never substitute an unverified or empty UI for this drill.
identity=$(node --input-type=module -e '
import { readFileSync } from "node:fs";
const manifest = JSON.parse(readFileSync("out/asset-manifest.json", "utf8"));
if (!/^([0-9a-f]{40}|[0-9a-f]{64})$/.test(manifest.source_revision)) throw Error("invalid source revision");
if (manifest.product_version && !/^0\.[0-9]+\.[0-9]+$/.test(manifest.product_version)) throw Error("invalid product version");
console.log("source_revision=" + manifest.source_revision);
if (manifest.product_version) console.log("product_version=" + manifest.product_version);
')
# Resolve the supplied local image once; every later process uses its immutable ID.
OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE=$(docker image inspect --format '{{.Id}}' "$OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE")
export OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE
image_identity=$(docker run --rm --network none "$OPEN_SPLUNK_RECOVERY_DRILL_SERVER_IMAGE" version)
if [[ "$identity" != "$image_identity" ]]; then
  echo 'The server image and prepared source tree must have the same release identity.' >&2
  exit 1
fi
work=$(mktemp -d "${TMPDIR:-/tmp}/open-splunk-recovery-drill.XXXXXXXX")
trap 'rm -rf -- "$work"' EXIT
revision=${identity%%$'\n'*}
revision=${revision#source_revision=}
if [[ "$(git rev-parse HEAD)" != "$revision" ]] || ! git diff --quiet HEAD -- .; then
  echo 'The drill requires a clean committed source tree matching the release manifest.' >&2
  exit 1
fi
if [[ -n "$(git ls-files --others --exclude-standard -- cmd internal app lib migrations proto deploy scripts gen)" ]]; then
  echo 'Commit source and drill files before running exact-release qualification.' >&2
  exit 1
fi
version=''
if [[ "$identity" == *$'\nproduct_version='* ]]; then
  version=${identity##*$'\nproduct_version='}
fi
package=github.com/Suhaibinator/open-splunk/internal/buildinfo
CGO_ENABLED=0 go test -p 2 -c \
  -ldflags "-X $package.sourceRevision=$revision -X $package.productVersion=$version" \
  -o "$work/helper" ./cmd/open-splunk-server
export OPEN_SPLUNK_RECOVERY_DRILL_HELPER_BINARY="$work/helper"
cd -- "$repository/cmd/open-splunk-server"
"$work/helper" -test.run '^TestDeploymentRecoveryDrill$' -test.timeout 15m -test.v
