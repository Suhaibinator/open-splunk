#!/usr/bin/env bash
set -euo pipefail

readonly max_catalog_shards=16
readonly runnable_pattern='^(Test|Example|Fuzz)[A-Za-z0-9_]*$'

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/run-go-race-shard.sh core
  scripts/run-go-race-shard.sh catalog SHARD_INDEX SHARD_COUNT
EOF
  exit 64
}

fail() {
  printf 'run-go-race-shard: %s\n' "$1" >&2
  exit 65
}

append_unique() {
  local candidate=$1
  shift
  local existing
  for existing in "$@"; do
    [[ $candidate != "$existing" ]] || fail "duplicate inventory entry: $candidate"
  done
}

catalog_package=''
discover_catalog_package() {
  catalog_package=$(go list -race ./internal/knowledgecatalog)
  [[ $catalog_package =~ ^[A-Za-z0-9._~/-]+$ ]] ||
    fail "catalog package discovery returned an invalid import path"
}

run_core() {
  [[ $# -eq 0 ]] || usage
  discover_catalog_package

  local package_output package found_catalog=0
  local -a packages=()
  local -a selected=()
  package_output=$(go list -race ./...)
  [[ -n $package_output ]] || fail "repository package inventory is empty"

  while IFS= read -r package; do
    [[ $package =~ ^[A-Za-z0-9._~/-]+$ ]] ||
      fail "package inventory contains an invalid import path"
    if ((${#packages[@]} > 0)); then
      append_unique "$package" "${packages[@]}"
    fi
    packages+=("$package")
    if [[ $package == "$catalog_package" ]]; then
      found_catalog=$((found_catalog + 1))
    else
      selected+=("$package")
    fi
  done <<< "$package_output"

  [[ $found_catalog -eq 1 ]] ||
    fail "catalog package must occur exactly once in the repository inventory"
  [[ ${#selected[@]} -gt 0 ]] || fail "core package selection is empty"
  [[ ${#selected[@]} -eq $((${#packages[@]} - 1)) ]] ||
    fail "core package selection did not reconstruct the repository inventory"

  printf 'race_scope=core\n'
  printf 'repository_package_count=%d\n' "${#packages[@]}"
  printf 'excluded_package=%s\n' "$catalog_package"
  printf 'selected_package_count=%d\n' "${#selected[@]}"
  printf 'selected_package=%s\n' "${selected[@]}"

  exec go test -p=1 -race -shuffle=on -count=1 -parallel=2 -timeout=40m "${selected[@]}"
}

validate_nonnegative_integer() {
  [[ $1 =~ ^(0|[1-9][0-9]*)$ ]]
}

run_catalog() {
  [[ $# -eq 2 ]] || usage
  local shard_index=$1
  local shard_count=$2
  validate_nonnegative_integer "$shard_index" || fail "shard index must be a canonical non-negative integer"
  validate_nonnegative_integer "$shard_count" || fail "shard count must be a canonical non-negative integer"
  ((shard_count >= 2)) || fail "shard count must be at least two"
  ((shard_count <= max_catalog_shards)) || fail "shard count exceeds the bounded maximum"
  ((shard_index < shard_count)) || fail "shard index must be less than shard count"
  discover_catalog_package

  local inventory_output line runnable previous='' assigned shard ordinal
  local trailer_count=0
  local -a discovered=()
  local -a runnables=()
  local -a selected=()
  local -a reconstructed=()
  local -a shard_sizes=()

  inventory_output=$(go test -race -list '^(Test|Example|Fuzz)' ./internal/knowledgecatalog)
  while IFS= read -r line; do
    if [[ $line =~ $runnable_pattern ]]; then
      ((trailer_count == 0)) || fail "catalog discovery emitted a runnable after its summary"
      if ((${#discovered[@]} > 0)); then
        append_unique "$line" "${discovered[@]}"
      fi
      discovered+=("$line")
      continue
    fi
    if [[ -n $line ]]; then
      local status='' reported_package='' duration='' extra=''
      read -r status reported_package duration extra <<< "$line"
      [[ $status == ok && $reported_package == "$catalog_package" && -n $duration && -z $extra ]] ||
        fail "catalog discovery emitted an unexpected line"
      [[ $duration == '(cached)' || $duration =~ ^[0-9]+([.][0-9]+)?s$ ]] ||
        fail "catalog discovery emitted an invalid summary duration"
      ((trailer_count == 0)) || fail "catalog discovery emitted more than one summary"
      trailer_count=1
    fi
  done <<< "$inventory_output"
  ((trailer_count == 1)) || fail "catalog discovery did not emit its final summary"
  [[ ${#discovered[@]} -gt 0 ]] || fail "catalog runnable inventory is empty"

  local sorted_output
  sorted_output=$(printf '%s\n' "${discovered[@]}" | LC_ALL=C sort)
  while IFS= read -r runnable; do
    [[ -n $runnable ]] && runnables+=("$runnable")
  done <<< "$sorted_output"
  [[ ${#runnables[@]} -eq ${#discovered[@]} ]] ||
    fail "catalog sort changed the runnable inventory size"

  for ((shard = 0; shard < shard_count; shard++)); do
    shard_sizes[$shard]=0
  done
  for ((ordinal = 0; ordinal < ${#runnables[@]}; ordinal++)); do
    assigned=$((ordinal % shard_count))
    shard_sizes[$assigned]=$((${shard_sizes[$assigned]} + 1))
    printf 'catalog_assignment shard=%d runnable=%s\n' "$assigned" "${runnables[$ordinal]}"
    if ((assigned == shard_index)); then
      selected+=("${runnables[$ordinal]}")
    fi
  done
  for ((shard = 0; shard < shard_count; shard++)); do
    ((${shard_sizes[$shard]} > 0)) || fail "catalog partition contains an empty shard"
    for ((ordinal = shard; ordinal < ${#runnables[@]}; ordinal += shard_count)); do
      reconstructed+=("${runnables[$ordinal]}")
    done
  done

  [[ ${#reconstructed[@]} -eq ${#runnables[@]} ]] ||
    fail "catalog shard reconstruction has the wrong size"
  local reconstructed_sorted
  reconstructed_sorted=$(printf '%s\n' "${reconstructed[@]}" | LC_ALL=C sort)
  [[ $reconstructed_sorted == "$(printf '%s\n' "${runnables[@]}")" ]] ||
    fail "catalog shards do not reconstruct the runnable inventory exactly"
  for runnable in "${runnables[@]}"; do
    [[ $runnable != "$previous" ]] || fail "catalog sorted inventory contains a duplicate"
    previous=$runnable
  done
  [[ ${#selected[@]} -eq ${shard_sizes[$shard_index]} ]] ||
    fail "selected catalog shard has the wrong size"

  local separator='' run_pattern='^('
  for runnable in "${selected[@]}"; do
    run_pattern+="$separator$runnable"
    separator='|'
  done
  run_pattern+=')$'

  printf 'race_scope=catalog\n'
  printf 'catalog_package=%s\n' "$catalog_package"
  printf 'catalog_runnable_count=%d\n' "${#runnables[@]}"
  printf 'catalog_shard_index=%d\n' "$shard_index"
  printf 'catalog_shard_count=%d\n' "$shard_count"
  printf 'catalog_selected_count=%d\n' "${#selected[@]}"
  printf 'catalog_run_pattern=%s\n' "$run_pattern"

  # Each selected top-level runnable retains all of its dynamic subtests. The
  # shard still co-runs dozens of t.Parallel tests under one race binary.
  exec go test -race -shuffle=on -count=1 -parallel=2 -timeout=40m \
    -run "$run_pattern" ./internal/knowledgecatalog
}

[[ $# -ge 1 ]] || usage
scope=$1
shift
case $scope in
  core)
    run_core "$@"
    ;;
  catalog)
    run_catalog "$@"
    ;;
  *)
    usage
    ;;
esac
