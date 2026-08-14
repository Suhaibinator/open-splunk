package queryexec

import (
	"context"
	"errors"
	"strings"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// The v0.3 compiler deliberately fails hostile runtime values with stable,
// payload-free ClickHouse markers. This table is separate from the compiler
// tests: accepting a marker in generated SQL is not sufficient unless the
// executor maps it to the correct public category and redacts the backend
// exception, authored field contents, and generated SQL fragment.
func TestV03RuntimeMarkersAreCompletelyClassifiedAndRedacted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		marker string
		want   error
	}{
		{name: "makemv incompatible value", marker: clickhouse.UnsupportedMakeMVValueMarker, want: searchjobs.ErrUnsupportedValue},
		{name: "makemv per-row members", marker: clickhouse.MakeMVRowMembersLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "makemv per-row member bytes", marker: clickhouse.MakeMVRowBytesLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "makemv whole-result members", marker: clickhouse.MakeMVResultMembersLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "makemv whole-result member bytes", marker: clickhouse.MakeMVResultBytesLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "makemv retained relation bytes", marker: clickhouse.MakeMVRetainedBytesLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "mvexpand incompatible value", marker: clickhouse.UnsupportedMVExpandValueMarker, want: searchjobs.ErrUnsupportedValue},
		{name: "mvexpand per-input members", marker: clickhouse.MVExpandRowMembersLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "mvexpand stage rows", marker: clickhouse.MVExpandStageRowsLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "mvexpand query rows", marker: clickhouse.MVExpandQueryRowsLimitMarker, want: searchjobs.ErrExecutionLimit},
		{name: "mvexpand retained bytes", marker: clickhouse.MVExpandRetainedBytesLimitMarker, want: searchjobs.ErrExecutionLimit},
	}

	seen := make(map[string]string, len(tests))
	for _, test := range tests {
		if test.marker == "" || !strings.HasPrefix(test.marker, "open-splunk: ") {
			t.Fatalf("runtime marker %q = %q, want a stable Open Splunk marker", test.name, test.marker)
		}
		if previous, duplicate := seen[test.marker]; duplicate {
			t.Fatalf("runtime marker %q is shared by %q and %q", test.marker, previous, test.name)
		}
		seen[test.marker] = test.name
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const secret = "AUTHORED_SECRET_VALUE SELECT private_storage_column"
			backend := &clickhousedriver.Exception{
				Code:    395,
				Name:    "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
				Message: test.marker + "; while executing " + secret,
			}
			classified := classifyQueryError(context.Background(), backend)
			if !errors.Is(classified, test.want) {
				t.Fatalf("classification = %v, want %v", classified, test.want)
			}
			other := searchjobs.ErrExecutionLimit
			if errors.Is(test.want, searchjobs.ErrExecutionLimit) {
				other = searchjobs.ErrUnsupportedValue
			}
			if errors.Is(classified, other) {
				t.Fatalf("classification = %v, unexpectedly also matches %v", classified, other)
			}
			if strings.Contains(classified.Error(), test.marker) ||
				strings.Contains(classified.Error(), secret) ||
				strings.Contains(classified.Error(), backend.Name) {
				t.Fatalf("classified error leaked backend detail: %v", classified)
			}
		})
	}
}
