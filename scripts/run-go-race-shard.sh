#!/usr/bin/env bash
set -euo pipefail

readonly max_race_shards=16
readonly runnable_pattern='^(Test|Example|Fuzz)[A-Za-z0-9_]*$'

usage() {
  cat >&2 <<'EOF'
usage:
  scripts/run-go-race-shard.sh core SHARD_INDEX SHARD_COUNT
  scripts/run-go-race-shard.sh catalog SHARD_INDEX SHARD_COUNT
EOF
  exit 64
}

fail() {
  printf 'run-go-race-shard: %s\n' "$1" >&2
  exit 65
}

catalog_package=''
discover_catalog_package() {
  catalog_package=$(go list -race ./internal/knowledgecatalog)
  [[ $catalog_package =~ ^[A-Za-z0-9._~/-]+$ ]] ||
    fail "catalog package discovery returned an invalid import path"
}

validate_shard_coordinates() {
  local shard_index=$1
  local shard_count=$2
  validate_nonnegative_integer "$shard_index" || fail "shard index must be a canonical non-negative integer"
  validate_nonnegative_integer "$shard_count" || fail "shard count must be a canonical non-negative integer"
  ((${#shard_index} <= ${#max_race_shards})) || fail "shard index exceeds the bounded maximum"
  ((${#shard_count} <= ${#max_race_shards})) || fail "shard count exceeds the bounded maximum"
  ((shard_count >= 2)) || fail "shard count must be at least two"
  ((shard_count <= max_race_shards)) || fail "shard count exceeds the bounded maximum"
  ((shard_index < shard_count)) || fail "shard index must be less than shard count"
}

partitioned_inventory=()
selected_partition=()
partition_inventory() {
  local scope=$1
  local item_label=$2
  local shard_index=$3
  local shard_count=$4
  shift 4
  local item sorted_output previous='' assigned shard ordinal reconstructed_sorted
  local -a inventory=("$@")
  local -a reconstructed=()
  local -a shard_sizes=()

  [[ ${#inventory[@]} -gt 0 ]] || fail "$scope $item_label inventory is empty"
  sorted_output=$(printf '%s\n' "${inventory[@]}" | LC_ALL=C sort)
  partitioned_inventory=()
  while IFS= read -r item; do
    [[ -n $item ]] && partitioned_inventory+=("$item")
  done <<< "$sorted_output"
  [[ ${#partitioned_inventory[@]} -eq ${#inventory[@]} ]] ||
    fail "$scope sort changed the $item_label inventory size"
  for item in "${partitioned_inventory[@]}"; do
    [[ $item != "$previous" ]] || fail "$scope sorted inventory contains a duplicate $item_label"
    previous=$item
  done

  selected_partition=()
  for ((shard = 0; shard < shard_count; shard++)); do
    shard_sizes[$shard]=0
  done
  for ((ordinal = 0; ordinal < ${#partitioned_inventory[@]}; ordinal++)); do
    assigned=$((ordinal % shard_count))
    shard_sizes[$assigned]=$((${shard_sizes[$assigned]} + 1))
    printf '%s_assignment shard=%d %s=%s\n' \
      "$scope" "$assigned" "$item_label" "${partitioned_inventory[$ordinal]}"
    if ((assigned == shard_index)); then
      selected_partition+=("${partitioned_inventory[$ordinal]}")
    fi
  done
  for ((shard = 0; shard < shard_count; shard++)); do
    ((${shard_sizes[$shard]} > 0)) || fail "$scope partition contains an empty shard"
    for ((ordinal = shard; ordinal < ${#partitioned_inventory[@]}; ordinal += shard_count)); do
      reconstructed+=("${partitioned_inventory[$ordinal]}")
    done
  done

  [[ ${#reconstructed[@]} -eq ${#partitioned_inventory[@]} ]] ||
    fail "$scope shard reconstruction has the wrong size"
  reconstructed_sorted=$(printf '%s\n' "${reconstructed[@]}" | LC_ALL=C sort)
  [[ $reconstructed_sorted == "$(printf '%s\n' "${partitioned_inventory[@]}")" ]] ||
    fail "$scope shards do not reconstruct the $item_label inventory exactly"
  [[ ${#selected_partition[@]} -eq ${shard_sizes[$shard_index]} ]] ||
    fail "selected $scope shard has the wrong size"
}

run_core() {
  [[ $# -eq 2 ]] || usage
  local shard_index=$1
  local shard_count=$2
  validate_shard_coordinates "$shard_index" "$shard_count"
  discover_catalog_package

  local package_output package found_catalog=0
  local -a packages=()
  local -a core_packages=()
  local -a selected=()
  package_output=$(go list -race ./...)
  [[ -n $package_output ]] || fail "repository package inventory is empty"

  while IFS= read -r package; do
    [[ $package =~ ^[A-Za-z0-9._~/-]+$ ]] ||
      fail "package inventory contains an invalid import path"
    packages+=("$package")
    if [[ $package == "$catalog_package" ]]; then
      found_catalog=$((found_catalog + 1))
    else
      core_packages+=("$package")
    fi
  done <<< "$package_output"

  [[ $found_catalog -eq 1 ]] ||
    fail "catalog package must occur exactly once in the repository inventory"
  [[ ${#core_packages[@]} -eq $((${#packages[@]} - 1)) ]] ||
    fail "core package selection did not reconstruct the repository inventory"

  partition_inventory core package "$shard_index" "$shard_count" "${core_packages[@]}"
  core_packages=("${partitioned_inventory[@]}")
  selected=("${selected_partition[@]}")

  printf 'race_scope=core\n'
  printf 'repository_package_count=%d\n' "${#packages[@]}"
  printf 'excluded_package=%s\n' "$catalog_package"
  printf 'core_package_count=%d\n' "${#core_packages[@]}"
  printf 'core_shard_index=%d\n' "$shard_index"
  printf 'core_shard_count=%d\n' "$shard_count"
  printf 'core_selected_count=%d\n' "${#selected[@]}"
  printf 'core_selected_package=%s\n' "${selected[@]}"

  exec go test -p=1 -race -shuffle=on -count=1 -parallel=2 -timeout=40m "${selected[@]}"
}

validate_nonnegative_integer() {
  [[ $1 =~ ^(0|[1-9][0-9]*)$ ]]
}

run_catalog() {
  [[ $# -eq 2 ]] || usage
  local shard_index=$1
  local shard_count=$2
  validate_shard_coordinates "$shard_index" "$shard_count"
  discover_catalog_package

  local inventory_output line runnable
  local trailer_count=0
  local -a discovered=()
  local -a runnables=()
  local -a selected=()

  inventory_output=$(go test -race -list '^(Test|Example|Fuzz)' ./internal/knowledgecatalog)
  while IFS= read -r line; do
    if [[ $line =~ $runnable_pattern ]]; then
      ((trailer_count == 0)) || fail "catalog discovery emitted a runnable after its summary"
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
  partition_inventory catalog runnable "$shard_index" "$shard_count" "${discovered[@]}"
  runnables=("${partitioned_inventory[@]}")
  selected=("${selected_partition[@]}")

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
