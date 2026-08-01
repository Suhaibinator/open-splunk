#!/usr/bin/env bash
set -euo pipefail
umask 077

if [[ -n "${OPEN_SPLUNK_REPOSITORY_ROOT:-}" ]]; then
  REPO_ROOT="$(cd "$OPEN_SPLUNK_REPOSITORY_ROOT" && pwd -P)"
else
  REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
fi
unset OPEN_SPLUNK_REPOSITORY_ROOT
export LANG=C
export LC_ALL=C
export TZ=UTC

if [[ $# -ne 0 ]]; then
  echo "error: build-oci does not accept arguments" >&2
  exit 2
fi

: "${OPEN_SPLUNK_APPLICATION_VERSION:?OPEN_SPLUNK_APPLICATION_VERSION is required}"
: "${OPEN_SPLUNK_SOURCE_REVISION:?OPEN_SPLUNK_SOURCE_REVISION is required}"

SEMANTIC_VERSION_PATTERN='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(\.(0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*))?(\+[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
if [[ ${#OPEN_SPLUNK_APPLICATION_VERSION} -gt 64 ||
      ! "$OPEN_SPLUNK_APPLICATION_VERSION" =~ $SEMANTIC_VERSION_PATTERN ]]; then
  echo "error: OPEN_SPLUNK_APPLICATION_VERSION must be a semantic version" >&2
  exit 1
fi
if [[ ! "$OPEN_SPLUNK_SOURCE_REVISION" =~ ^[0-9a-f]{40}([0-9a-f]{24})?$ ]]; then
  echo "error: OPEN_SPLUNK_SOURCE_REVISION must be a full lowercase Git hash" >&2
  exit 1
fi

DEFAULT_IMAGE_TAG="${OPEN_SPLUNK_APPLICATION_VERSION//+/_}"
SERVER_IMAGE_INPUT="${OPEN_SPLUNK_SERVER_IMAGE:-open-splunk-server:$DEFAULT_IMAGE_TAG}"
COLLECTOR_IMAGE_INPUT="${OPEN_SPLUNK_COLLECTOR_IMAGE:-open-splunk-collector:$DEFAULT_IMAGE_TAG}"
OCI_PLATFORM="${OPEN_SPLUNK_OCI_PLATFORM:-linux/amd64}"
OCI_NO_CACHE="${OPEN_SPLUNK_OCI_NO_CACHE:-}"
IMAGE_REFERENCE_PATTERN='^[a-z0-9]+([._-][a-z0-9]+)*([/:][a-z0-9]+([._-][a-z0-9]+)*)*:[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$'
if [[ ! "$SERVER_IMAGE_INPUT" =~ $IMAGE_REFERENCE_PATTERN ]]; then
  echo "error: OPEN_SPLUNK_SERVER_IMAGE is not a tagged server image reference" >&2
  exit 1
fi
if [[ ! "$COLLECTOR_IMAGE_INPUT" =~ $IMAGE_REFERENCE_PATTERN ]]; then
  echo "error: OPEN_SPLUNK_COLLECTOR_IMAGE is not a tagged collector image reference" >&2
  exit 1
fi
if [[ ! "$OCI_PLATFORM" =~ ^linux/(amd64|arm64)$ ]]; then
  echo "error: OCI platform OPEN_SPLUNK_OCI_PLATFORM must be linux/amd64 or linux/arm64" >&2
  exit 1
fi
OCI_TARGET_OS="${OCI_PLATFORM%%/*}"
OCI_TARGET_ARCH="${OCI_PLATFORM##*/}"
if [[ -n "$OCI_NO_CACHE" && "$OCI_NO_CACHE" != 1 ]]; then
  echo "error: OPEN_SPLUNK_OCI_NO_CACHE must be empty or 1" >&2
  exit 1
fi

normalize_image_reference() {
  local reference=$1
  local name=${reference%:*}
  local tag=${reference##*:}
  local first_component
  local registry
  local repository

  if [[ "$name" == */* ]]; then
    first_component=${name%%/*}
    if [[ "$first_component" == *.* ||
          "$first_component" == *:* ||
          "$first_component" == localhost ]]; then
      registry=$first_component
      repository=${name#*/}
    else
      registry=docker.io
      repository=$name
    fi
  else
    registry=docker.io
    repository=$name
  fi
  if [[ "$registry" == index.docker.io ]]; then
    registry=docker.io
  fi
  if [[ "$registry" == docker.io && "$repository" != */* ]]; then
    repository="library/$repository"
  fi
  printf '%s/%s:%s\n' "$registry" "$repository" "$tag"
}

SERVER_IMAGE="$(normalize_image_reference "$SERVER_IMAGE_INPUT")"
COLLECTOR_IMAGE="$(normalize_image_reference "$COLLECTOR_IMAGE_INPUT")"
if [[ "$SERVER_IMAGE" == "$COLLECTOR_IMAGE" ]]; then
  echo "error: server and collector image references must be distinct Docker names" >&2
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

RESOLVED_ROOT="$(git_repo rev-parse --show-toplevel)"
RESOLVED_ROOT="$(cd "$RESOLVED_ROOT" && pwd -P)"
if [[ "$RESOLVED_ROOT" != "$REPO_ROOT" ]]; then
  echo "error: Git repository root $RESOLVED_ROOT does not match $REPO_ROOT" >&2
  exit 1
fi
HEAD_REVISION="$(git_repo rev-parse --verify HEAD)"
if [[ "$OPEN_SPLUNK_SOURCE_REVISION" != "$HEAD_REVISION" ]]; then
  echo "error: OPEN_SPLUNK_SOURCE_REVISION must equal the current HEAD commit $HEAD_REVISION" >&2
  exit 1
fi

IMAGE_CREATED="$(git_repo show -s --format=%cI "$HEAD_REVISION")"
SOURCE_DATE_EPOCH="$(git_repo show -s --format=%ct "$HEAD_REVISION")"
if [[ ! "$SOURCE_DATE_EPOCH" =~ ^[0-9]+$ || -z "$IMAGE_CREATED" ]]; then
  echo "error: current HEAD has invalid release timestamps" >&2
  exit 1
fi

WORK_ROOT=""
WORK_ROOT_OWNER=""
WORK_ROOT_OWNER_TOKEN=""
SOURCE_ROOT=""
HOME_ROOT=""
TEMP_ROOT=""
STATUS_FILE=""
LOCK_OWNER_TOKEN=""
declare -a PUBLICATION_LOCKS=()
declare -a PUBLICATION_LOCK_IDS=()
declare -a PUBLICATION_LOCK_ID_FILES=()
declare -a PUBLICATION_LOCK_REFERENCES=()
declare -a PUBLICATION_LOCK_STATES=()
TEMPORARY_SERVER_IMAGE=""
TEMPORARY_COLLECTOR_IMAGE=""
TEMPORARY_SERVER_IMAGE_ID=""
TEMPORARY_COLLECTOR_IMAGE_ID=""
PREVIOUS_SERVER_IMAGE_ID=""
PREVIOUS_COLLECTOR_IMAGE_ID=""
PUBLICATION_ROLLBACK_REQUIRED=false
SIGNALS_DEFERRED=false
DEFERRED_SIGNAL_STATUS=0
docker_started=false

path_owner_uid() {
  local target=$1
  if stat -c '%u' "$target" >/dev/null 2>&1; then
    stat -c '%u' "$target"
  else
    stat -f '%u' "$target"
  fi
}

validate_owned_directory() {
  local directory=$1
  local expected_path=$2
  local description=$3
  local physical_path
  local owner_uid

  if [[ -L "$directory" || ! -d "$directory" ]]; then
    echo "error: $description is not a real directory: $directory" >&2
    return 1
  fi
  physical_path="$(cd "$directory" && pwd -P)" || return 1
  if [[ "$physical_path" != "$expected_path" ]]; then
    echo "error: $description resolved to $physical_path, want $expected_path" >&2
    return 1
  fi
  owner_uid="$(path_owner_uid "$directory")" || {
    echo "error: could not inspect ownership of $description: $directory" >&2
    return 1
  }
  if [[ "$owner_uid" != "$(id -u)" ]]; then
    echo "error: $description is not owned by the current user: $directory" >&2
    return 1
  fi
}

remove_private_work_root() {
  local failed=0
  local actual_owner

  if [[ -z "$WORK_ROOT" || ( ! -e "$WORK_ROOT" && ! -L "$WORK_ROOT" ) ]]; then
    return 0
  fi
  if ! validate_owned_directory "$WORK_ROOT" "$WORK_ROOT" "OCI work root"; then
    return 1
  fi
  if [[ -L "$WORK_ROOT_OWNER" || ! -f "$WORK_ROOT_OWNER" ]]; then
    echo "error: OCI work-root ownership marker changed before cleanup: $WORK_ROOT_OWNER" >&2
    return 1
  fi
  actual_owner="$(<"$WORK_ROOT_OWNER")"
  if [[ "$actual_owner" != "$WORK_ROOT_OWNER_TOKEN" ]]; then
    echo "error: OCI work-root ownership marker does not match this process" >&2
    return 1
  fi
  if ! find "$WORK_ROOT" -type d -exec chmod u+rwx {} +; then
    echo "error: could not make OCI work root removable: $WORK_ROOT" >&2
    failed=1
  fi
  if ! rm -rf "$WORK_ROOT"; then
    echo "error: could not remove OCI work root: $WORK_ROOT" >&2
    failed=1
  fi
  if [[ -e "$WORK_ROOT" || -L "$WORK_ROOT" ]]; then
    echo "error: OCI work root remains after cleanup: $WORK_ROOT" >&2
    failed=1
  fi
  return "$failed"
}

INSPECTED_LOCK_ID=""
INSPECTED_LOCK_OWNER=""
INSPECTED_LOCK_REFERENCE=""

inspect_publication_lock() {
  local lock_name=$1
  local inspection
  local remainder

  if ! inspection="$(
    docker container inspect \
      --format '{{.Id}}|{{ index .Config.Labels "org.open-splunk.oci-publication-lock.owner" }}|{{ index .Config.Labels "org.open-splunk.oci-publication-lock.reference" }}' \
      "$lock_name" 2>/dev/null
  )"; then
    return 1
  fi
  INSPECTED_LOCK_ID=${inspection%%|*}
  remainder=${inspection#*|}
  INSPECTED_LOCK_OWNER=${remainder%%|*}
  INSPECTED_LOCK_REFERENCE=${remainder#*|}
  if [[ ! "$INSPECTED_LOCK_ID" =~ ^[0-9a-f]{64}$ ||
        ! "$INSPECTED_LOCK_OWNER" =~ ^[0-9a-f]{64}$ ||
        -z "$INSPECTED_LOCK_REFERENCE" ||
        "$INSPECTED_LOCK_REFERENCE" == *'|'* ]]; then
    return 1
  fi
}

verify_publication_lock() {
  local index=$1
  local lock_name=${PUBLICATION_LOCKS[$index]}
  local expected_id=${PUBLICATION_LOCK_IDS[$index]}
  local reference=${PUBLICATION_LOCK_REFERENCES[$index]}

  if [[ "${PUBLICATION_LOCK_STATES[$index]}" != acquired ||
        ! "$expected_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: OCI publication lock was not fully acquired: $lock_name" >&2
    return 1
  fi
  if ! inspect_publication_lock "$lock_name"; then
    echo "error: OCI publication lock cannot be inspected: $lock_name" >&2
    return 1
  fi
  if [[ "$INSPECTED_LOCK_ID" != "$expected_id" ||
        "$INSPECTED_LOCK_OWNER" != "$LOCK_OWNER_TOKEN" ||
        "$INSPECTED_LOCK_REFERENCE" != "$reference" ]]; then
    echo "error: OCI publication lock identity changed: $lock_name" >&2
    return 1
  fi
}

verify_publication_locks() {
  local index
  for ((index=0; index < ${#PUBLICATION_LOCKS[@]}; index++)); do
    verify_publication_lock "$index" || return 1
  done
}

release_publication_locks() {
  local failed=0
  local index
  local lock_id_file
  local lock_name
  local lock_state
  local expected_id
  local reference

  for ((index=${#PUBLICATION_LOCKS[@]} - 1; index >= 0; index--)); do
    lock_name=${PUBLICATION_LOCKS[$index]}
    lock_state=${PUBLICATION_LOCK_STATES[$index]}
    reference=${PUBLICATION_LOCK_REFERENCES[$index]}
    expected_id=${PUBLICATION_LOCK_IDS[$index]}
    lock_id_file=${PUBLICATION_LOCK_ID_FILES[$index]}
    if [[ -z "$expected_id" && -f "$lock_id_file" && ! -L "$lock_id_file" ]]; then
      IFS= read -r expected_id <"$lock_id_file" || true
    fi
    if ! inspect_publication_lock "$lock_name"; then
      if [[ "$lock_state" == acquired ]]; then
        echo "error: OCI publication lock disappeared while held: $lock_name" >&2
        failed=1
      fi
      continue
    fi
    if [[ "$INSPECTED_LOCK_OWNER" != "$LOCK_OWNER_TOKEN" ||
          "$INSPECTED_LOCK_REFERENCE" != "$reference" ]]; then
      if [[ "$lock_state" == acquired ]]; then
        echo "error: OCI publication lock ownership changed while held: $lock_name" >&2
        failed=1
      fi
      continue
    fi
    if [[ -n "$expected_id" && "$INSPECTED_LOCK_ID" != "$expected_id" ]]; then
      echo "error: OCI publication lock container ID changed while held: $lock_name" >&2
      failed=1
      continue
    fi
    if ! docker container rm --force "$INSPECTED_LOCK_ID" >/dev/null; then
      echo "error: could not release OCI publication lock $lock_name for $reference" >&2
      failed=1
    fi
  done
  PUBLICATION_LOCKS=()
  PUBLICATION_LOCK_IDS=()
  PUBLICATION_LOCK_ID_FILES=()
  PUBLICATION_LOCK_REFERENCES=()
  PUBLICATION_LOCK_STATES=()
  return "$failed"
}

resolve_single_image_id() {
  local reference=$1
  local filter_reference=$reference
  local image_ids

  # Docker accepts canonical Docker Hub references for tag/inspect, but older
  # Linux daemons match `image ls --filter reference=...` by familiar names
  # (for example `image:tag`, not `docker.io/library/image:tag`). Keep canonical
  # names for locking and mutation, and translate only this lookup.
  if [[ "$filter_reference" == docker.io/library/* ]]; then
    filter_reference=${filter_reference#docker.io/library/}
  elif [[ "$filter_reference" == docker.io/* ]]; then
    filter_reference=${filter_reference#docker.io/}
  fi
  if ! image_ids="$(
    docker image ls \
      --quiet \
      --no-trunc \
      --filter "reference=$filter_reference"
  )"; then
    echo "error: could not inspect Docker image reference $reference" >&2
    return 1
  fi
  if [[ "$image_ids" == *$'\n'* ]]; then
    echo "error: Docker image reference resolves to more than one image: $reference" >&2
    return 1
  fi
  printf '%s' "$image_ids"
}

restore_image_reference() {
  local reference=$1
  local previous_image_id=$2
  local description=$3
  local current_image_id

  if [[ -n "$previous_image_id" ]]; then
    verify_publication_locks || return 1
    if ! docker image tag "$previous_image_id" "$reference"; then
      echo "error: could not restore the previous $description image tag" >&2
      return 1
    fi
    return 0
  fi
  if ! current_image_id="$(resolve_single_image_id "$reference")"; then
    return 1
  fi
  if [[ -n "$current_image_id" ]]; then
    verify_publication_locks || return 1
    if ! docker image rm --force "$reference"; then
      echo "error: could not remove the partially published $description image tag" >&2
      return 1
    fi
  fi
}

rollback_publication() {
  local accept_complete_pair=${1:-false}
  local current_collector_image_id
  local current_server_image_id
  local failed=0

  if [[ "$PUBLICATION_ROLLBACK_REQUIRED" != true ]]; then
    return 0
  fi

  # A signal can arrive after Docker committed the collector tag but before Bash
  # executes the next assignment. In that case the intended pair is complete.
  if [[ "$accept_complete_pair" == true ]]; then
    if ! verify_publication_locks; then
      return 1
    fi
    if ! current_server_image_id="$(resolve_single_image_id "$SERVER_IMAGE")"; then
      return 1
    fi
    if ! current_collector_image_id="$(resolve_single_image_id "$COLLECTOR_IMAGE")"; then
      return 1
    fi
    if [[ -n "$current_server_image_id" &&
          "$current_server_image_id" == "$TEMPORARY_SERVER_IMAGE_ID" &&
          -n "$current_collector_image_id" &&
          "$current_collector_image_id" == "$TEMPORARY_COLLECTOR_IMAGE_ID" ]]; then
      PUBLICATION_ROLLBACK_REQUIRED=false
      return 0
    fi
  fi

  if ! restore_image_reference \
    "$SERVER_IMAGE" \
    "$PREVIOUS_SERVER_IMAGE_ID" \
    server; then
    failed=1
  fi
  restore_image_reference \
    "$COLLECTOR_IMAGE" \
    "$PREVIOUS_COLLECTOR_IMAGE_ID" \
    collector || failed=1
  if [[ "$failed" -ne 0 ]]; then
    return 1
  fi
  PUBLICATION_ROLLBACK_REQUIRED=false
}

verify_published_pair() {
  local current_collector_image_id
  local current_server_image_id

  if ! current_server_image_id="$(resolve_single_image_id "$SERVER_IMAGE")" ||
     ! current_collector_image_id="$(resolve_single_image_id "$COLLECTOR_IMAGE")"; then
    return 1
  fi
  if [[ "$current_server_image_id" != "$TEMPORARY_SERVER_IMAGE_ID" ||
        "$current_collector_image_id" != "$TEMPORARY_COLLECTOR_IMAGE_ID" ]]; then
    echo "error: published OCI image tags do not resolve to the built image pair" >&2
    return 1
  fi
}

cleanup() {
  local status=$?
  trap - EXIT
  trap '' HUP INT TERM
  set +e
  if ! rollback_publication true; then
    status=1
  fi
  if ! release_publication_locks; then
    status=1
  fi
  if [[ "$docker_started" == true ]]; then
    docker image rm --force \
      "$TEMPORARY_SERVER_IMAGE" \
      "$TEMPORARY_COLLECTOR_IMAGE" >/dev/null 2>&1 || true
  fi
  if ! remove_private_work_root; then
    status=1
  fi
  exit "$status"
}

handle_signal() {
  local status=$1
  if [[ "$SIGNALS_DEFERRED" == true ]]; then
    DEFERRED_SIGNAL_STATUS=$status
    return 0
  fi
  exit "$status"
}

trap cleanup EXIT
trap 'handle_signal 129' HUP
trap 'handle_signal 130' INT
trap 'handle_signal 143' TERM

work_root_candidate="$(mktemp -d "${TMPDIR:-/tmp}/open-splunk-oci.XXXXXX")"
WORK_ROOT="$(cd "$work_root_candidate" && pwd -P)"
if ! validate_owned_directory "$WORK_ROOT" "$WORK_ROOT" "OCI work root"; then
  exit 1
fi
chmod 0700 "$WORK_ROOT"
WORK_ROOT_OWNER="$WORK_ROOT/owner"
WORK_ROOT_OWNER_TOKEN="$HEAD_REVISION:$$"
printf '%s\n' "$WORK_ROOT_OWNER_TOKEN" >"$WORK_ROOT_OWNER"
chmod 0600 "$WORK_ROOT_OWNER"
SOURCE_ROOT="$WORK_ROOT/source"
HOME_ROOT="$WORK_ROOT/home"
TEMP_ROOT="$WORK_ROOT/tmp"
STATUS_FILE="$WORK_ROOT/git-status"
mkdir -m 0700 "$HOME_ROOT" "$TEMP_ROOT"
LOCK_OWNER_TOKEN="$(
  env -i \
    PATH="$PATH" \
    HOME="$HOME_ROOT" \
    TMPDIR="$TEMP_ROOT" \
    LANG=C \
    LC_ALL=C \
    NODE_OPTIONS= \
    TZ=UTC \
    node -e \
      'const { randomBytes } = require("node:crypto"); process.stdout.write(randomBytes(32).toString("hex"));'
)"
if [[ ! "$LOCK_OWNER_TOKEN" =~ ^[0-9a-f]{64}$ ]]; then
  echo "error: could not generate a safe OCI publication-lock owner" >&2
  exit 1
fi

if ! git_repo status --porcelain=v1 -z --untracked-files=all >"$STATUS_FILE"; then
  echo "error: could not inspect the OCI build worktree" >&2
  exit 1
fi
if [[ -s "$STATUS_FILE" ]]; then
  echo "error: OCI builds require a clean worktree; commit or remove local changes first" >&2
  exit 1
fi

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
chmod 0500 "$MATERIALIZER_BOOTSTRAP"
env -i \
  PATH="$PATH" \
  HOME="$HOME_ROOT" \
  TMPDIR="$TEMP_ROOT" \
  LANG=C \
  LC_ALL=C \
  NODE_OPTIONS= \
  TZ=UTC \
  node "$MATERIALIZER_BOOTSTRAP" "$REPO_ROOT" "$HEAD_REVISION" "$SOURCE_ROOT"

for required_path in Dockerfile .dockerignore oci/rootfs/etc/passwd oci/rootfs/etc/group; do
  if [[ -L "$SOURCE_ROOT/$required_path" || ! -f "$SOURCE_ROOT/$required_path" ]]; then
    echo "error: committed OCI build input must be a regular file: $required_path" >&2
    exit 1
  fi
done

TEMPORARY_TAG_TOKEN="${WORK_ROOT##*.}"
if [[ ! "$TEMPORARY_TAG_TOKEN" =~ ^[A-Za-z0-9]+$ ]]; then
  echo "error: OCI work root produced an unsafe temporary image token" >&2
  exit 1
fi
TEMPORARY_TAG_PREFIX="open-splunk-build:${HEAD_REVISION:0:12}-$TEMPORARY_TAG_TOKEN"
TEMPORARY_SERVER_IMAGE="$TEMPORARY_TAG_PREFIX-server"
TEMPORARY_COLLECTOR_IMAGE="$TEMPORARY_TAG_PREFIX-collector"

build_target() {
  local target=$1
  local temporary_image=$2
  local -a build_arguments=(
    --file "$SOURCE_ROOT/Dockerfile"
    --platform "$OCI_PLATFORM"
    --target "$target"
    --tag "$temporary_image"
    --build-arg "OPEN_SPLUNK_APPLICATION_VERSION=$OPEN_SPLUNK_APPLICATION_VERSION"
    --build-arg "OPEN_SPLUNK_SOURCE_REVISION=$OPEN_SPLUNK_SOURCE_REVISION"
    --build-arg "OPEN_SPLUNK_IMAGE_CREATED=$IMAGE_CREATED"
    --build-arg "OPEN_SPLUNK_SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH"
    --build-arg "OPEN_SPLUNK_EXPECTED_TARGETOS=$OCI_TARGET_OS"
    --build-arg "OPEN_SPLUNK_EXPECTED_TARGETARCH=$OCI_TARGET_ARCH"
    --build-arg "SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH"
  )
  if [[ "$OCI_NO_CACHE" == 1 ]]; then
    build_arguments=(--no-cache "${build_arguments[@]}")
  fi
  docker_started=true
  DOCKER_BUILDKIT=1 SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" docker build \
    "${build_arguments[@]}" \
    "$SOURCE_ROOT"
}

build_target server "$TEMPORARY_SERVER_IMAGE"
build_target collector "$TEMPORARY_COLLECTOR_IMAGE"

validate_repository_stability() {
  local head_before
  local head_after
  if ! head_before="$(git_repo rev-parse --verify HEAD)"; then
    echo "error: could not revalidate repository HEAD before OCI publication" >&2
    return 1
  fi
  if ! git_repo status --porcelain=v1 -z --untracked-files=all >"$STATUS_FILE"; then
    echo "error: could not revalidate the OCI build worktree" >&2
    return 1
  fi
  if ! head_after="$(git_repo rev-parse --verify HEAD)"; then
    echo "error: could not revalidate repository HEAD before OCI publication" >&2
    return 1
  fi
  if [[ "$head_before" != "$HEAD_REVISION" || "$head_after" != "$HEAD_REVISION" ]]; then
    echo "error: repository HEAD changed after OCI snapshot $HEAD_REVISION; refusing stale publication" >&2
    return 1
  fi
  if [[ -s "$STATUS_FILE" ]]; then
    echo "error: repository worktree changed after OCI snapshot; refusing publication" >&2
    return 1
  fi
}

acquire_publication_lock() {
  local reference=$1
  local attempt
  local create_status
  local lock_id
  local lock_id_file
  local lock_key
  local lock_name
  local lock_index

  lock_key="$(
    env -i \
      PATH="$PATH" \
      HOME="$HOME_ROOT" \
      TMPDIR="$TEMP_ROOT" \
      LANG=C \
      LC_ALL=C \
      NODE_OPTIONS= \
      TZ=UTC \
      node -e \
        'const { createHash } = require("node:crypto"); process.stdout.write(createHash("sha256").update(process.argv[1], "utf8").digest("hex"));' \
        "$reference"
  )"
  if [[ ! "$lock_key" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: could not derive a safe OCI publication-lock key" >&2
    return 1
  fi
  lock_name="open-splunk-oci-publication-lock-$lock_key"

  # Container names are allocated atomically by the effective Docker daemon.
  # This coordinates independent clones and users without conflating separate
  # Docker contexts/endpoints. Record the attempt before creation so signal
  # cleanup can remove a container created just before Bash receives a trap.
  lock_index=${#PUBLICATION_LOCKS[@]}
  lock_id_file="$WORK_ROOT/publication-lock-$lock_index.id"
  PUBLICATION_LOCKS+=("$lock_name")
  PUBLICATION_LOCK_IDS+=("")
  PUBLICATION_LOCK_ID_FILES+=("$lock_id_file")
  PUBLICATION_LOCK_REFERENCES+=("$reference")
  PUBLICATION_LOCK_STATES+=(pending)
  DEFERRED_SIGNAL_STATUS=0
  SIGNALS_DEFERRED=true
  if docker container create \
    --name "$lock_name" \
    --label "org.open-splunk.oci-publication-lock.owner=$LOCK_OWNER_TOKEN" \
    --label "org.open-splunk.oci-publication-lock.reference=$reference" \
    --network none \
    --platform "$OCI_PLATFORM" \
    --entrypoint /usr/local/bin/open-splunk-server \
    "$TEMPORARY_SERVER_IMAGE" >"$lock_id_file"; then
    create_status=0
  else
    create_status=$?
  fi
  lock_id=""
  if [[ ! -L "$lock_id_file" && -f "$lock_id_file" ]]; then
    IFS= read -r lock_id <"$lock_id_file" || true
  fi

  # A terminal can interrupt the Docker CLI after the daemon accepted create.
  # Reconcile the exact named/labelled container for a bounded interval before
  # allowing EXIT cleanup to decide that no lock belongs to this process.
  if [[ ! "$lock_id" =~ ^[0-9a-f]{64}$ ]]; then
    lock_id=""
    for ((attempt=0; attempt < 10; attempt++)); do
      if inspect_publication_lock "$lock_name"; then
        if [[ "$INSPECTED_LOCK_OWNER" == "$LOCK_OWNER_TOKEN" &&
              "$INSPECTED_LOCK_REFERENCE" == "$reference" ]]; then
          lock_id=$INSPECTED_LOCK_ID
        fi
        break
      fi
      sleep 0.1
    done
  fi
  if [[ "$lock_id" =~ ^[0-9a-f]{64}$ ]]; then
    PUBLICATION_LOCK_IDS[$lock_index]=$lock_id
    PUBLICATION_LOCK_STATES[$lock_index]=acquired
    printf '%s\n' "$lock_id" >"$lock_id_file"
  fi

  SIGNALS_DEFERRED=false
  if [[ "$DEFERRED_SIGNAL_STATUS" -ne 0 ]]; then
    exit "$DEFERRED_SIGNAL_STATUS"
  fi
  if [[ "$create_status" -ne 0 ]]; then
    echo "error: another OCI publication is active, or stale daemon lock $lock_name exists for $reference; inspect that container before removing it" >&2
    return 1
  fi
  if [[ "$lock_id" != "${PUBLICATION_LOCK_IDS[$lock_index]}" ||
        ! "$lock_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "error: Docker returned an invalid container ID for publication lock $lock_name" >&2
    return 1
  fi
  verify_publication_lock "$lock_index"
}

validate_repository_stability
if [[ "$SERVER_IMAGE" < "$COLLECTOR_IMAGE" ]]; then
  acquire_publication_lock "$SERVER_IMAGE"
  acquire_publication_lock "$COLLECTOR_IMAGE"
else
  acquire_publication_lock "$COLLECTOR_IMAGE"
  acquire_publication_lock "$SERVER_IMAGE"
fi
validate_repository_stability
verify_publication_locks

if [[ "$(git_repo rev-parse --verify HEAD)" != "$HEAD_REVISION" ]]; then
  echo "error: repository changed during the OCI build; refusing image publication" >&2
  exit 1
fi

if ! PREVIOUS_SERVER_IMAGE_ID="$(resolve_single_image_id "$SERVER_IMAGE")"; then
  exit 1
fi
if [[ -n "$PREVIOUS_SERVER_IMAGE_ID" &&
      ! "$PREVIOUS_SERVER_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "error: final server image reference has an invalid image ID" >&2
  exit 1
fi
if ! PREVIOUS_COLLECTOR_IMAGE_ID="$(resolve_single_image_id "$COLLECTOR_IMAGE")"; then
  exit 1
fi
if [[ -n "$PREVIOUS_COLLECTOR_IMAGE_ID" &&
      ! "$PREVIOUS_COLLECTOR_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "error: final collector image reference has an invalid image ID" >&2
  exit 1
fi
TEMPORARY_SERVER_IMAGE_ID="$(
  docker image inspect --format '{{.Id}}' "$TEMPORARY_SERVER_IMAGE"
)"
if [[ ! "$TEMPORARY_SERVER_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "error: temporary server image has an invalid image ID" >&2
  exit 1
fi
TEMPORARY_COLLECTOR_IMAGE_ID="$(
  docker image inspect --format '{{.Id}}' "$TEMPORARY_COLLECTOR_IMAGE"
)"
if [[ ! "$TEMPORARY_COLLECTOR_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "error: temporary collector image has an invalid image ID" >&2
  exit 1
fi

PUBLICATION_ROLLBACK_REQUIRED=true
verify_publication_locks
docker image tag "$TEMPORARY_SERVER_IMAGE" "$SERVER_IMAGE"
verify_publication_locks
if ! docker image tag "$TEMPORARY_COLLECTOR_IMAGE" "$COLLECTOR_IMAGE"; then
  if ! rollback_publication false; then
    echo "error: collector image publication failed and the previous publication state could not be restored" >&2
    exit 1
  fi
  echo "error: collector image publication failed; restored the previous publication state" >&2
  exit 1
fi
if ! verify_publication_locks || ! verify_published_pair; then
  if ! rollback_publication false; then
    echo "error: OCI publication verification failed and the previous publication state could not be restored" >&2
    exit 1
  fi
  echo "error: OCI publication verification failed; restored the previous publication state" >&2
  exit 1
fi
PUBLICATION_ROLLBACK_REQUIRED=false

echo "built $SERVER_IMAGE and $COLLECTOR_IMAGE from committed snapshot $HEAD_REVISION"
