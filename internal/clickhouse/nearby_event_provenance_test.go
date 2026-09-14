package clickhouse

import (
	"bytes"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestNearbyEventProvenanceIsCompilerAuthenticated(t *testing.T) {
	compiled := compileSPL(t, `index=gradethis | search level="error" | sort -_time`)
	output, ok := compiled.NearbyEventOutput()
	if !ok {
		t.Fatal("compiled event result has no authenticated nearby-event source provenance")
	}
	want := NearbyEventOutput{TimeIndex: 0, IndexIndex: 2, HostIndex: 3, SourceIndex: 4}
	if output != want {
		t.Fatalf("nearby event output = %#v, want %#v", output, want)
	}

	clone, ok := compiled.CloneForExecution()
	if !ok {
		t.Fatal("clone refused valid nearby-event authority")
	}
	clone.NearbyEvent.HostIndex = clone.NearbyEvent.SourceIndex
	if _, ok := clone.NearbyEventOutput(); ok || clone.HasValidExecutionSeal() {
		t.Fatal("tampered nearby-event output retained compiler authority")
	}
	if _, ok := compiled.NearbyEventOutput(); !ok {
		t.Fatal("tampering a clone changed the original nearby-event authority")
	}
	if !slices.Equal(compiled.OutputFields, []string{
		"_time", "_raw", "index", "host", "source", "sourcetype", "service",
		"level", "message", "trace_id", "span_id", "event_id", "_indextime", "fields",
	}) {
		t.Fatalf("ordinary event outputs changed = %v", compiled.OutputFields)
	}
}

func TestNearbyEventProvenanceParticipatesInKnowledgeInputAuthority(t *testing.T) {
	state := compileState{visible: map[string]fieldState{"host": canonicalState("host")}}
	original, err := compileKnowledgeFieldInputStateAuthority(state, []string{"host"})
	if err != nil {
		t.Fatal(err)
	}
	field := state.visible["host"]
	field.originalEventField = originalEventFieldNone
	state.visible["host"] = field
	overwritten, err := compileKnowledgeFieldInputStateAuthority(state, []string{"host"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(original[:], overwritten[:]) {
		t.Fatal("knowledge continuation authority omitted original-event provenance")
	}
}

func TestNearbyGeneratedWherePreservesLiteralStringsAndExactNumbers(t *testing.T) {
	text := "wild*card 'quote' \\ slash\nnewline"
	for _, field := range []string{"index", "host", "source", "trace_id"} {
		compiled := compileSPL(t, `index=gradethis | where '`+field+`' = `+strconv.Quote(text))
		if !slices.ContainsFunc(compiled.Args, func(value any) bool { return value == text }) {
			t.Fatalf("%s literal comparison args omit exact string %#v: %#v", field, text, compiled.Args)
		}
		for _, argument := range compiled.Args {
			if candidate, ok := argument.(string); ok && strings.Contains(candidate, "[[:alnum:]]") {
				t.Fatalf("%s where string was lowered as wildcard regex: %q", field, candidate)
			}
		}
	}

	for _, literal := range []string{
		"18446744073709551615",
		"18446744073709551616.0",
		"9007199254740993.000001",
		"1e-400",
	} {
		compiled := compileSPL(t, `index=gradethis | where 'ratio' = `+literal)
		if !strings.Contains(compiled.SQL, "__os_exact_order_text") {
			t.Fatalf("exact numeric %q did not use exact ordering key:\n%s", literal, compiled.SQL)
		}
		key := parseExactNumericLiteralKey(literal)
		if !key.eligible || !strings.Contains(compiled.SQL, `CAST('`+key.coefficient+`' AS String)`) ||
			!strings.Contains(compiled.SQL, strconv.FormatInt(key.decimalOrder, 10)) {
			t.Fatalf("exact numeric %q lost its ordering key %+v:\n%s", literal, key, compiled.SQL)
		}
	}
}

func TestNearbyEventProvenanceTracksOnlyUnchangedRequiredOutputs(t *testing.T) {
	for _, test := range []struct {
		name      string
		source    string
		available bool
	}{
		{name: "raw event", source: `index=gradethis`, available: true},
		{name: "filters sort and limit", source: `index=gradethis | search host="api" | sort source | head 2`, available: true},
		{name: "explicit required table", source: `index=gradethis | table source host index _time`, available: true},
		{name: "unrelated calculated field", source: `index=gradethis | eval correlation=trace_id`, available: true},
		{name: "source removed", source: `index=gradethis | fields - source`},
		{name: "host overwritten", source: `index=gradethis | eval host="calculated"`},
		{name: "index renamed", source: `index=gradethis | rename index AS original_index`},
		{name: "time bucketed", source: `index=gradethis | bin _time span=1m`},
		{name: "source extracted", source: `index=gradethis | rex field=_raw "(?<source>.*)"`},
		{name: "transforming", source: `index=gradethis | stats count BY host`},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSPL(t, test.source)
			_, available := compiled.NearbyEventOutput()
			if available != test.available {
				t.Fatalf("NearbyEventOutput() available = %t, want %t; outputs %v", available, test.available, compiled.OutputFields)
			}
		})
	}
}
