package knowledgevalidation

import (
	"context"
	"reflect"
	"testing"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"google.golang.org/protobuf/proto"
)

func FuzzCalculatedRangeRebase(f *testing.F) {
	f.Add(" \n\tlower(host) \r\n", uint32(0), uint32(len("lower(host)")))
	f.Add("\t😀value\n", uint32(0), uint32(len("😀")))
	f.Add("  value  ", uint32(len("value")), uint32(len("value")))
	f.Fuzz(func(t *testing.T, raw string, start, end uint32) {
		if len(raw) > 1<<16 || !utf8.ValidString(raw) {
			return
		}
		left, right := asciiTrimBounds(raw)
		canonical := raw[left:right]
		if canonical == "" {
			return
		}
		submitted := calculatedDefinition("calculated-fuzz", raw)
		normalized := calculatedDefinition("calculated-fuzz", canonical)
		issue := knowledgeprogram.Issue{
			FieldPath: "calculated_field.expression",
			Code:      "SPL_FUZZ",
			Message:   "fuzz",
			Range: &knowledgeprogram.ScalarRange{
				StartByteOffset: start,
				EndByteOffset:   end,
			},
		}
		projected, err := rebaseProgramRange(submitted, normalized, issue)
		if err != nil {
			return
		}
		if projected.source != raw || projected.start > projected.end || projected.end > uint64(len(raw)) ||
			!utf8.ValidString(raw[:projected.start]) || !utf8.ValidString(raw[:projected.end]) {
			t.Fatalf("accepted invalid rebase: %+v in %q", projected, raw)
		}
		if _, err := publicRange(context.Background(), *projected); err != nil {
			t.Fatalf("accepted rebase cannot be publicly projected: %v", err)
		}
	})
}

func FuzzIssueCanonicalizationDeterministic(f *testing.F) {
	f.Add("path", "CODE", "message", "suggestion")
	f.Add("", "SPL_EXAMPLE", "é", "😀")
	f.Fuzz(func(t *testing.T, path, code, message, suggestion string) {
		if len(path)+len(code)+len(message)+len(suggestion) > 16<<10 {
			return
		}
		fieldInput := []fieldIssue{{path: path, code: code, message: message}}
		firstFields, firstFieldsTruncated, firstFieldErr := canonicalFieldViolations(context.Background(), fieldInput)
		secondFields, secondFieldsTruncated, secondFieldErr := canonicalFieldViolations(context.Background(), fieldInput)
		if (firstFieldErr == nil) != (secondFieldErr == nil) || firstFieldsTruncated != secondFieldsTruncated ||
			!reflect.DeepEqual(firstFields, secondFields) {
			t.Fatal("field canonicalization is nondeterministic")
		}

		diagnosticInput := []diagnosticIssue{{
			path: path, code: code, message: message,
			severity:    opensplunkv1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
			suggestions: []string{suggestion, suggestion},
		}}
		firstDiagnostics, _, firstTruncated, firstErr := canonicalDiagnosticsWithSources(context.Background(), diagnosticInput)
		secondDiagnostics, _, secondTruncated, secondErr := canonicalDiagnosticsWithSources(context.Background(), diagnosticInput)
		if (firstErr == nil) != (secondErr == nil) || firstTruncated != secondTruncated ||
			len(firstDiagnostics) != len(secondDiagnostics) {
			t.Fatal("diagnostic canonicalization status is nondeterministic")
		}
		for index := range firstDiagnostics {
			if !proto.Equal(firstDiagnostics[index], secondDiagnostics[index]) {
				t.Fatal("diagnostic canonicalization value is nondeterministic")
			}
		}
	})
}
