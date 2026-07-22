package clickhouse

import (
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// TestGradeThisCompatibilityCorpus keeps the product plan's ten initial SPL
// searches executable as one contract. Unsupported entries remain explicit so
// each implementation increment turns one expected diagnostic into a compile.
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
			name:               "volume by severity",
			source:             `index=gradethis | timechart span=5m count by level`,
			unsupportedCommand: "timechart",
		},
		{
			name:               "server errors by route",
			source:             `index=gradethis message="Request metrics" status>=500 | timechart span=5m count by path`,
			unsupportedCommand: "timechart",
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
			unsupportedCommand: "eval",
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
			if !ok || diagnostic.Code != "SPL_UNSUPPORTED_COMMAND" ||
				!strings.Contains(diagnostic.Message, `unsupported command "`+test.unsupportedCommand+`"`) {
				t.Fatalf("diagnostic = %#v, want unsupported %q", err, test.unsupportedCommand)
			}
		})
	}
}
