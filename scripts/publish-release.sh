#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ "${GITHUB_ACTIONS:-}" != true || "${GITHUB_EVENT_NAME:-}" != release ]]; then
  echo "error: official releases may be published only by a GitHub release workflow" >&2
  exit 1
fi
if [[ $# -ne 0 ]]; then
  echo "error: publish-release does not accept arguments" >&2
  exit 2
fi
: "${GITHUB_EVENT_PATH:?GitHub release event payload is required}"
: "${GITHUB_SHA:?GitHub release commit is required}"
: "${GITHUB_REPOSITORY_OWNER:?GitHub repository owner is required}"
: "${GITHUB_REF_NAME:?GitHub release tag is required}"

event_metadata=$(node -e '
  const event = require(process.env.GITHUB_EVENT_PATH);
  if (event.action !== "published" || event.release?.draft || event.release?.prerelease) process.exit(1);
  process.stdout.write(String(event.release.tag_name ?? ""));
') || {
  echo "error: release publication requires one published, non-draft, non-prerelease GitHub Release" >&2
  exit 1
}
if [[ "$event_metadata" != "$GITHUB_REF_NAME" ||
      ! "$GITHUB_REF_NAME" =~ ^v0\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
  echo "error: GitHub Release tag must be canonical v0.MINOR.PATCH" >&2
  exit 1
fi
product_version=${GITHUB_REF_NAME#v}

repository_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$repository_root"
if [[ -n "$(git status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "error: release publication requires a clean checkout" >&2
  exit 1
fi
head_revision=$(git rev-parse --verify HEAD)
if [[ "$head_revision" != "$GITHUB_SHA" ]]; then
  echo "error: checked-out release commit does not match GITHUB_SHA" >&2
  exit 1
fi
tag_revision=$(git rev-list -n 1 "$GITHUB_REF_NAME")
if [[ "$tag_revision" != "$head_revision" ]]; then
  echo "error: GitHub Release tag does not resolve to the checked-out commit" >&2
  exit 1
fi
git fetch --no-tags origin main
if ! git merge-base --is-ancestor "$head_revision" origin/main; then
  echo "error: GitHub Release commit is not reachable from origin/main" >&2
  exit 1
fi

owner=$(printf '%s' "$GITHUB_REPOSITORY_OWNER" | tr '[:upper:]' '[:lower:]')
server_repository="ghcr.io/$owner/open-splunk-server"
collector_repository="ghcr.io/$owner/open-splunk-collector"
server_image="$server_repository:$product_version"
collector_image="$collector_repository:$product_version"
for image in "$server_image" "$collector_image"; do
  if docker buildx imagetools inspect "$image" >/dev/null 2>&1; then
    echo "error: immutable release image already exists: $image" >&2
    exit 1
  fi
done

image_created=$(git show -s --format=%cI HEAD)
source_date_epoch=$(git show -s --format=%ct HEAD)
common_build_arguments=(
  --platform linux/amd64,linux/arm64
  --build-arg "OPEN_SPLUNK_SOURCE_REVISION=$head_revision"
  --build-arg "OPEN_SPLUNK_PRODUCT_VERSION=$product_version"
  --build-arg "OPEN_SPLUNK_IMAGE_CREATED=$image_created"
  --build-arg "OPEN_SPLUNK_SOURCE_DATE_EPOCH=$source_date_epoch"
  --build-arg "OPEN_SPLUNK_EXPECTED_TARGETOS=linux"
  --build-arg "SOURCE_DATE_EPOCH=$source_date_epoch"
  --provenance=false
  --sbom=false
  --push
)
docker buildx build "${common_build_arguments[@]}" --target server --tag "$server_image" .
docker buildx build "${common_build_arguments[@]}" --target collector --tag "$collector_image" .

verify_multiarch_image() {
  local image=$1
  docker buildx imagetools inspect --raw "$image" | node -e '
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", chunk => { input += chunk; });
    process.stdin.on("end", () => {
      const manifest = JSON.parse(input);
      const platforms = new Set((manifest.manifests ?? []).map(
        item => `${item.platform?.os}/${item.platform?.architecture}`,
      ));
      for (const required of ["linux/amd64", "linux/arm64"]) {
        if (!platforms.has(required)) {
          throw new Error(`published image is missing ${required}`);
        }
      }
    });
  '
}
verify_multiarch_image "$server_image"
verify_multiarch_image "$collector_image"

# Mutable convenience tags advance only after both immutable images exist.
docker buildx imagetools create --tag "$server_repository:latest" "$server_image"
docker buildx imagetools create --tag "$collector_repository:latest" "$collector_image"

work_root=$(mktemp -d "${RUNNER_TEMP:-/tmp}/open-splunk-release.XXXXXX")
trap 'rm -rf "$work_root"' EXIT
archives=()
for architecture in amd64 arm64; do
  artifact_root="$work_root/linux-$architecture"
  mkdir -p "$artifact_root"
  docker buildx build \
    --platform "linux/$architecture" \
    --target artifacts \
    --build-arg "OPEN_SPLUNK_SOURCE_REVISION=$head_revision" \
    --build-arg "OPEN_SPLUNK_PRODUCT_VERSION=$product_version" \
    --build-arg "OPEN_SPLUNK_IMAGE_CREATED=$image_created" \
    --build-arg "OPEN_SPLUNK_SOURCE_DATE_EPOCH=$source_date_epoch" \
    --build-arg "OPEN_SPLUNK_EXPECTED_TARGETOS=linux" \
    --build-arg "OPEN_SPLUNK_EXPECTED_TARGETARCH=$architecture" \
    --build-arg "SOURCE_DATE_EPOCH=$source_date_epoch" \
    --output "type=local,dest=$artifact_root" \
    .
  for binary in open-splunk-server open-splunk-collector open-splunk-loggen; do
    if ! binary_metadata=$(GOTOOLCHAIN=local go version -m "$artifact_root/$binary"); then
      echo "error: could not inspect release binary metadata: $binary ($architecture)" >&2
      exit 1
    fi
    if ! grep -Eq '^[[:space:]]*build[[:space:]]+GOOS=linux$' <<<"$binary_metadata"; then
      echo "error: release binary is not built for Linux: $binary ($architecture)" >&2
      exit 1
    fi
    if ! grep -Eq "^[[:space:]]*build[[:space:]]+GOARCH=$architecture$" <<<"$binary_metadata"; then
      echo "error: release binary has the wrong architecture: $binary ($architecture)" >&2
      exit 1
    fi
  done
  archive="$work_root/open-splunk_${product_version}_linux_${architecture}.tar.gz"
  tar \
    --format=gnu \
    --sort=name \
    --mtime="@$source_date_epoch" \
    --owner=0 --group=0 --numeric-owner \
    --mode='u+rwX,go+rX,go-w' \
    -C "$artifact_root" \
    -cf - \
    open-splunk-server open-splunk-collector open-splunk-loggen | gzip -n >"$archive"
  archives+=("$archive")
done
checksums="$work_root/open-splunk_${product_version}_SHA256SUMS"
(
  cd "$work_root"
  shasum -a 256 "$(basename "${archives[0]}")" "$(basename "${archives[1]}")"
) >"$checksums"
(
  cd "$work_root"
  shasum -a 256 -c "$(basename "$checksums")"
)
gh release upload "$GITHUB_REF_NAME" "${archives[@]}" "$checksums"
