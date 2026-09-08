package parserconfig

import (
	"strings"
	"testing"
)

func TestExplicitMappingsAcceptSameRoleCollectorAliases(t *testing.T) {
	// These spellings are independently enumerated from the collector's public
	// canonical extraction contract, not generated from roleAlias.
	for _, test := range []struct {
		role    string
		aliases []string
	}{
		{"timestamp", []string{"timestamp", "ts", "time", "@timestamp", "Timestamp", "TIME"}},
		{"message", []string{"message", "msg", "body", "Message", "BODY"}},
		{"level", []string{"level", "severity", "severity_text", "Level", "LEVEL", "Severity_Text"}},
		{"trace_id", []string{"trace_id", "traceId", "traceid", "TRACE_ID", "TraceId"}},
		{"span_id", []string{"span_id", "spanId", "spanid", "SPAN_ID", "SpanId"}},
	} {
		for _, source := range test.aliases {
			t.Run(test.role+"/"+source, func(t *testing.T) {
				options := &Options{Fields: map[string]string{test.role: source}}
				compiled, err := Compile("logfmt", options)
				if err != nil {
					t.Fatal(err)
				}
				if compiled.SourceField(test.role) != source {
					t.Fatalf("source spelling changed: got %q want %q", compiled.SourceField(test.role), source)
				}
				if compiled.CanonicalField(source) != test.role {
					t.Fatalf("explicit source %q did not map to %q", source, test.role)
				}
				variant := strings.ToUpper(source)
				if variant == source {
					variant = strings.ToLower(source)
				}
				if compiled.CanonicalField(variant) != "" {
					t.Fatalf("mapping unexpectedly matched unconfigured case variant %q", variant)
				}
			})
		}
	}
}

func TestExplicitMappingsRejectCrossRoleAliasesAndTrustedMetadata(t *testing.T) {
	for _, test := range []struct{ role, source string }{
		{"timestamp", "Body"}, {"timestamp", "LEVEL"}, {"message", "Timestamp"},
		{"level", "MESSAGE"}, {"trace_id", "spanId"}, {"span_id", "traceId"},
		{"message", "tenant_id"}, {"message", "TENANT_ID"}, {"message", "Host"},
		{"level", "Source"}, {"timestamp", "INDEX_NAME"}, {"trace_id", "Event_ID"},
		{"span_id", "__OS_private"}, {"message", "COLLECTOR_ID"}, {"timestamp", "Collected_At"},
	} {
		t.Run(test.role+"/"+test.source, func(t *testing.T) {
			if _, err := Compile("logfmt", &Options{Fields: map[string]string{test.role: test.source}}); err == nil {
				t.Fatal("accepted protected or cross-role field mapping")
			}
		})
	}
}

func TestCanonicalMappingRoleNamesRemainCaseSensitive(t *testing.T) {
	for _, role := range []string{"Timestamp", "MESSAGE", "Level", "traceId", "spanId"} {
		if _, err := Compile("logfmt", &Options{Fields: map[string]string{role: "custom"}}); err == nil {
			t.Fatalf("accepted noncanonical role %q", role)
		}
	}
}
