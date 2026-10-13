package common_util

import (
	"math"

	"github.com/lib/pq"
)

// ApplyToEach executes its operation in the shared utility flow used by service and persistence layers.
func ApplyToEach[T any, V any](arr []T, fn func(T) V) []V {
	result := make([]V, len(arr))
	for i := range arr {
		result[i] = fn(arr[i])
	}
	return result
}

// ApplyToEachFiltered executes its operation in the shared utility flow used by service and persistence layers.
func ApplyToEachFiltered[T any, V any](arr []T, fn func(T) V, includeFn func(T) bool, preallocSize *int) []V {
	prealloc := len(arr)
	if preallocSize != nil {
		prealloc = max(*preallocSize, 0)
	}
	result := make([]V, 0, prealloc)
	for _, item := range arr {
		if !includeFn(item) {
			continue
		}
		result = append(result, fn(item))
	}
	return result
}

// Deduplicate returns a new slice containing the first occurrence of each value
// in arr, preserving input order.
func Deduplicate[T comparable](arr []T) []T {
	seen := make(map[T]struct{}, len(arr))
	result := make([]T, 0, len(arr))
	for _, v := range arr {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	return result
}

// DeduplicateNonZero returns a new slice containing the first occurrence of
// each non-zero value in arr, preserving input order. Zero values are dropped.
func DeduplicateNonZero[T comparable](arr []T) []T {
	var zero T
	seen := make(map[T]struct{}, len(arr))
	result := make([]T, 0, len(arr))
	for _, v := range arr {
		if v == zero {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		result = append(result, v)
	}
	return result
}

// ApplyToEachFilteredOnResult executes its operation in the shared utility flow used by service and persistence layers.
func ApplyToEachFilteredOnResult[T any, V any](arr []T, fn func(T) V, includeFn func(V) bool, preallocSize *int) []V {
	prealloc := len(arr)
	if preallocSize != nil {
		prealloc = max(*preallocSize, 0)
	}
	result := make([]V, 0, prealloc)
	for _, item := range arr {
		currentResult := fn(item)
		if !includeFn(currentResult) {
			continue
		}
		result = append(result, currentResult)
	}
	return result
}

// ConvertPqInt64ArrayToUint64Slice executes its operation in the shared utility flow used by service and persistence layers.
func ConvertPqInt64ArrayToUint64Slice(pqArray pq.Int64Array) []uint64 {
	if pqArray == nil {
		return nil
	}
	return ApplyToEach(pqArray, func(val int64) uint64 {
		if val < 0 {
			return 0
		}
		return uint64(val)
	})
}

// ConvertUint64SliceToPqInt64Array executes its operation in the shared utility flow used by service and persistence layers.
func ConvertUint64SliceToPqInt64Array(slice []uint64) pq.Int64Array {
	if slice == nil {
		return nil
	}
	return ApplyToEach(slice, func(val uint64) int64 {
		if val > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(val)
	})
}
