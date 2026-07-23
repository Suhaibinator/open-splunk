// Package eventfields defines the event-root namespaces shared by ingestion,
// storage, planning, and query compilation.
package eventfields

import (
	"slices"
	"strings"
)

var canonicalSPLFieldNames = []string{
	"event_id",
	"index",
	"_time",
	"_indextime",
	"host",
	"source",
	"sourcetype",
	"service",
	"severity",
	"level",
	"message",
	"_raw",
	"trace_id",
	"span_id",
	"collector_id",
	"batch_id",
}

// These names are storage columns, server-owned security metadata, or public
// event containers. A root dynamic field with any of these names would be
// hidden by, or confused with, authoritative event metadata.
var additionalReservedDynamicRootNames = []string{
	"tenant_id",
	"index_name",
	"event_time",
	"index_time",
	"collected_at",
	"event_time_source",
	"body",
	"raw",
	"raw_encoding",
	"fields",
	"field_names",
	"batch_sequence",
	"expires_at",
	"visibility_seq",
}

// These source-format conveniences are consumed only by the first-party
// collector. They must not become canonical SPL names or globally reserve the
// same ordinary dynamic names on the native ingestion protocol.
var collectorAliasRootNames = []string{
	"timestamp",
	"ts",
	"time",
	"@timestamp",
	"severity_text",
	"msg",
	"traceid",
	"spanid",
}

var (
	canonicalSPLFields   = exactSet(canonicalSPLFieldNames)
	reservedDynamicRoots = foldedSet(join(canonicalSPLFieldNames, additionalReservedDynamicRootNames))
	collectorAliasRoots  = foldedSet(collectorAliasRootNames)
)

// CanonicalSPLFieldNames returns the exact, case-sensitive field names backed
// by promoted columns in an event scan. The returned slice is independent.
func CanonicalSPLFieldNames() []string {
	return slices.Clone(canonicalSPLFieldNames)
}

// ReservedDynamicRootNames returns the case-insensitive dynamic root
// namespace. The returned names are the lowercase canonical spellings and the
// slice is independent.
func ReservedDynamicRootNames() []string {
	return join(canonicalSPLFieldNames, additionalReservedDynamicRootNames)
}

// IsCanonicalSPLField reports whether name has an exact canonical SPL
// spelling. SPL field names remain case-sensitive.
func IsCanonicalSPLField(name string) bool {
	_, ok := canonicalSPLFields[name]
	return ok
}

// IsReservedDynamicRoot reports whether an untrusted root dynamic field would
// collide with canonical, physical, security, or public-container metadata.
// Root collision checks are deliberately case-insensitive.
func IsReservedDynamicRoot(name string) bool {
	folded := strings.ToLower(name)
	return isReservedDynamicRootFolded(folded)
}

func isReservedDynamicRootFolded(folded string) bool {
	if strings.HasPrefix(folded, "__os_") {
		return true
	}
	_, ok := reservedDynamicRoots[folded]
	return ok
}

// IsCollectorReservedRoot extends IsReservedDynamicRoot with source-format
// aliases extracted by the first-party collector. Alias extraction itself
// remains owned by the collector and is intentionally not defined here.
func IsCollectorReservedRoot(name string) bool {
	folded := strings.ToLower(name)
	if isReservedDynamicRootFolded(folded) {
		return true
	}
	_, ok := collectorAliasRoots[folded]
	return ok
}

func exactSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
}

func foldedSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[strings.ToLower(name)] = struct{}{}
	}
	return set
}

func join(groups ...[]string) []string {
	size := 0
	for _, group := range groups {
		size += len(group)
	}
	joined := make([]string, 0, size)
	for _, group := range groups {
		joined = append(joined, group...)
	}
	return joined
}
