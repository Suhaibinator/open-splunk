package searchjobproto

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestDiagnosticsPublishesOnlyCompleteForwardSourceRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		diagnostic searchjobs.Diagnostic
		wantRange  bool
	}{
		{
			name: "valid zero-based byte offset",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: 0, Line: 1, Column: 1,
				EndByteOffset: 3, EndLine: 1, EndColumn: 4,
			},
			wantRange: true,
		},
		{name: "positionless"},
		{
			name: "negative offset",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: -1, Line: 1, Column: 1,
				EndByteOffset: 3, EndLine: 1, EndColumn: 4,
			},
		},
		{
			name: "reversed offsets",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: 4, Line: 1, Column: 1,
				EndByteOffset: 3, EndLine: 1, EndColumn: 4,
			},
		},
		{
			name: "incomplete position",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: 0, Line: 0, Column: 1,
				EndByteOffset: 3, EndLine: 1, EndColumn: 4,
			},
		},
		{
			name: "reversed lines",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: 0, Line: 2, Column: 1,
				EndByteOffset: 3, EndLine: 1, EndColumn: 4,
			},
		},
		{
			name: "reversed columns",
			diagnostic: searchjobs.Diagnostic{
				ByteOffset: 0, Line: 1, Column: 4,
				EndByteOffset: 3, EndLine: 1, EndColumn: 3,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got := Diagnostics([]searchjobs.Diagnostic{test.diagnostic})
			if len(got) != 1 {
				t.Fatalf("Diagnostics() length = %d, want 1", len(got))
			}
			if hasRange := got[0].GetSourceRange() != nil; hasRange != test.wantRange {
				t.Fatalf("Diagnostics() source range = %+v, want present %t", got[0].GetSourceRange(), test.wantRange)
			}
		})
	}
}
