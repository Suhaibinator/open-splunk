package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// TestGradeThisCompatibilityCorpus keeps the product plan's ten initial SPL
// searches progressing through the complete parse, plan, and compile contract.
// Pinned ClickHouse integration tests separately exercise the emitted query
// primitives and result transport.
func TestGradeThisCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		source             string
		unsupportedCommand string
	}{
		{
			name:   "follow one request",
			source: `index=gradethis trace_id="<trace-id>" | sort _time | table _time level layer logger message`,
		},
		{
			name:   "errors and warnings",
			source: `index=gradethis (level=ERROR OR level=WARN) | sort -_time`,
		},
		{
			name:   "known raw error fragment",
			source: `index=gradethis "connection refused" | table _time level logger message trace_id`,
		},
		{
			name:   "severity counts",
			source: `index=gradethis | stats count by level | sort -count`,
		},
		{
			name:   "frequent errors",
			source: `index=gradethis level=ERROR | stats count by logger, message | sort -count | head 20`,
		},
		{
			name:   "volume by severity",
			source: `index=gradethis | timechart span=5m count by level`,
		},
		{
			name:   "server errors by route",
			source: `index=gradethis message="Request metrics" status>=500 | timechart span=5m count by path`,
		},
		{
			name:   "responses by route and status",
			source: `index=gradethis message="Request metrics" | stats count by path, status | sort -count`,
		},
		{
			name: "slow routes",
			source: `index=gradethis message="Request metrics"
| eval duration_ms=tonumber(replace(duration, "ms$", ""))
| stats count p95(duration_ms) as p95_ms by path
| where p95_ms > 500`,
		},
		{
			name:   "common messages",
			source: `index=gradethis | top limit=20 message`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.unsupportedCommand == "" {
				compiled := compileSPL(t, test.source)
				if compiled.SQL == "" || len(compiled.OutputFields) == 0 {
					t.Fatalf("compiled query is incomplete: %#v", compiled)
				}
				return
			}

			_, err := spl.Parse(test.source)
			if err == nil {
				t.Fatalf("Parse succeeded; remove the expected %q diagnostic and exercise compilation", test.unsupportedCommand)
			}
			diagnostic, ok := err.(*spl.Diagnostic)
			if !ok {
				t.Fatalf("diagnostic = %T, want *spl.Diagnostic", err)
			}
			if diagnostic.Code != "SPL_UNSUPPORTED_COMMAND" ||
				!strings.Contains(diagnostic.Message, `unsupported command "`+test.unsupportedCommand+`"`) {
				t.Fatalf("diagnostic = %#v, want unsupported %q", err, test.unsupportedCommand)
			}
		})
	}
}

func TestRexCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	queries := []string{
		`index=gradethis | rex "method=(?<raw_method>[A-Z]+)\s+path=(?<raw_path>\S+)\s+status=(?<raw_status>\d+)" | table raw_method raw_path raw_status`,
		`index=gradethis message="Request summary statistics" | rex field=duration "^(?<duration_value>\d+(?:\.\d+)?)(?<duration_unit>µs|ms)$" | stats count BY duration_unit`,
		`index=gradethis | rex field=path "^/api/v1/(?<area>[^/?]+)(?:/(?<resource>[^/?]+))?" | stats count BY area, resource | sort -count`,
		`index=gradethis message="GORM slow query" | rex field=sql "^\s*(?<sql_verb>[A-Za-z]+)\b" | stats count BY sql_verb`,
	}
	for _, source := range queries {
		compiled := compileSPL(t, source)
		if compiled.SQL == "" || len(compiled.OutputFields) == 0 ||
			strings.Count(compiled.SQL, "extractGroups(") != 1 {
			t.Fatalf("rex corpus query is incomplete for %q: %#v", source, compiled)
		}
	}
}

func TestBinCompatibilityCorpus(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		source   string
		required string
	}{
		{`index=gradethis | bin _time span=5m | stats count BY _time`, "fromUnixTimestamp64Nano("},
		{`index=gradethis | bucket span=1h _time AS hour | table _time hour message`, "fromUnixTimestamp64Nano("},
		{`index=gradethis | bin severity span=10 | stats count BY severity`, `toUInt64("severity")`},
		{`index=gradethis | eval latency=-11.5 | bucket span=10 latency AS band | table latency band`, UnsupportedNumericBinValueMarker},
		{`index=gradethis | stats count | bin count span=10`, `toUInt64("count")`},
	} {
		compiled := compileSPL(t, test.source)
		if compiled.SQL == "" || len(compiled.OutputFields) == 0 ||
			!strings.Contains(compiled.SQL, test.required) {
			t.Fatalf("bin corpus query is incomplete for %q: %#v", test.source, compiled)
		}
	}
}
