package eventfields

import (
	"slices"
	"strings"
	"testing"
)

func TestCanonicalSPLFieldNamespace(t *testing.T) {
	t.Parallel()

	want := []string{
		"event_id", "index", "_time", "_indextime", "host", "source", "sourcetype", "service",
		"severity", "level", "message", "_raw", "trace_id", "span_id", "collector_id", "batch_id",
	}
	if got := CanonicalSPLFieldNames(); !slices.Equal(got, want) {
		t.Fatalf("CanonicalSPLFieldNames() = %#v, want %#v", got, want)
	}
	for _, name := range want {
		if !IsCanonicalSPLField(name) {
			t.Errorf("IsCanonicalSPLField(%q) = false", name)
		}
		variant := strings.ToUpper(name)
		if variant != name && IsCanonicalSPLField(variant) {
			t.Errorf("IsCanonicalSPLField(%q) = true; canonical SPL spellings must remain exact", variant)
		}
		if !IsReservedDynamicRoot(variant) {
			t.Errorf("IsReservedDynamicRoot(%q) = false", variant)
		}
	}

	got := CanonicalSPLFieldNames()
	got[0] = "mutated"
	if IsCanonicalSPLField("mutated") || CanonicalSPLFieldNames()[0] != "event_id" {
		t.Fatal("CanonicalSPLFieldNames exposed mutable package state")
	}
}

func TestReservedDynamicRootsCoverPhysicalSecurityAndContainers(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"tenant_id", "index_name", "event_time", "index_time", "collected_at", "event_time_source",
		"body", "raw", "raw_encoding", "fields", "field_names", "batch_sequence", "expires_at",
		"visibility_seq",
	} {
		if !IsReservedDynamicRoot(name) || !IsReservedDynamicRoot(strings.ToUpper(name)) {
			t.Errorf("reserved root %q or its case variant was not recognized", name)
		}
	}
	for _, name := range []string{"__os_fields", "__OS_SORT_TIME", "__Os_Future_Internal"} {
		if !IsReservedDynamicRoot(name) {
			t.Errorf("compiler-private root %q was not reserved", name)
		}
	}
	for _, name := range []string{"status", "http", "origin", "timestamp", "msg", "severity_text"} {
		if IsReservedDynamicRoot(name) {
			t.Errorf("ordinary/native-protocol dynamic root %q was globally reserved", name)
		}
	}

	names := ReservedDynamicRootNames()
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, duplicate := seen[name]; duplicate {
			t.Errorf("ReservedDynamicRootNames contains duplicate %q", name)
		}
		seen[name] = struct{}{}
		if !IsReservedDynamicRoot(name) {
			t.Errorf("ReservedDynamicRootNames contains unrecognized %q", name)
		}
	}
	for _, name := range CanonicalSPLFieldNames() {
		if _, ok := seen[name]; !ok {
			t.Errorf("canonical field %q is absent from ReservedDynamicRootNames", name)
		}
	}
}

func TestCollectorReservedRootsAddAliasesWithoutChangingCanonicalSPL(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"timestamp", "ts", "time", "@timestamp", "severity_text", "msg", "traceId", "spanId",
	} {
		if !IsCollectorReservedRoot(name) || !IsCollectorReservedRoot(strings.ToUpper(name)) {
			t.Errorf("collector alias %q or its case variant was not recognized", name)
		}
		if IsCanonicalSPLField(name) {
			t.Errorf("collector alias %q became a canonical SPL field", name)
		}
	}
	if IsCollectorReservedRoot("request_id") {
		t.Fatal("ordinary collector field request_id was reserved")
	}
}
