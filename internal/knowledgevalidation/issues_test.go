package knowledgevalidation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestFieldViolationCanonicalizationAndBounds(t *testing.T) {
	input := []fieldIssue{
		{path: "z", code: "B", message: "two"},
		{path: "a", code: "B", message: "one"},
		{path: "a", code: "A", message: "three"},
		{path: "a", code: "A", message: "three"},
	}
	values, truncated, err := canonicalFieldViolations(context.Background(), input)
	if err != nil || truncated || len(values) != 3 {
		t.Fatalf("canonical field violations = %d/%t/%v", len(values), truncated, err)
	}
	if values[0].GetFieldPath() != "a" || values[0].GetCode() != "A" ||
		values[1].GetFieldPath() != "a" || values[1].GetCode() != "B" ||
		values[2].GetFieldPath() != "z" {
		t.Fatalf("field violation order = %+v", values)
	}

	exact := make([]fieldIssue, 64)
	for index := range exact {
		exact[index] = fieldIssue{
			path: fmt.Sprintf("p%03d", index),
			code: "C", message: strings.Repeat("m", 4091),
		}
	}
	values, truncated, err = canonicalFieldViolations(context.Background(), exact)
	if err != nil || truncated || len(values) != len(exact) {
		t.Fatalf("exact field text budget = %d/%t/%v", len(values), truncated, err)
	}
	exact[len(exact)-1].message += "x"
	values, truncated, err = canonicalFieldViolations(context.Background(), exact)
	if err != nil || !truncated || len(values) != len(exact)-1 {
		t.Fatalf("field text budget +1 = %d/%t/%v", len(values), truncated, err)
	}

	tiny := make([]fieldIssue, MaximumIssues+1)
	for index := range tiny {
		tiny[index] = fieldIssue{path: fmt.Sprintf("c%03d", index), code: "C", message: "m"}
	}
	values, truncated, err = canonicalFieldViolations(context.Background(), tiny)
	if err != nil || !truncated || len(values) != MaximumIssues {
		t.Fatalf("field count cap = %d/%t/%v", len(values), truncated, err)
	}
}

func TestDiagnosticCanonicalizationTotalOrderAndSuggestions(t *testing.T) {
	input := []diagnosticIssue{
		{path: "z", code: "W", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING, message: "warning"},
		{path: "a", code: "E2", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR, message: "located", rangeValue: &byteRange{start: 1, end: 2, source: "abc"}},
		{path: "a", code: "E1", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR, message: "absent", suggestions: []string{"z", "a", "z"}},
		{path: "a", code: "E1", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR, message: "absent", suggestions: []string{"a", "z"}},
		{path: "a", code: "I", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO, message: "info"},
	}
	values, _, truncated, err := canonicalDiagnosticsWithSources(context.Background(), input)
	if err != nil || truncated || len(values) != 4 {
		t.Fatalf("canonical diagnostics = %d/%t/%v", len(values), truncated, err)
	}
	if values[0].GetDiagnostic().GetCode() != "E1" || values[0].GetDiagnostic().GetSourceRange() != nil ||
		values[1].GetDiagnostic().GetCode() != "E2" || values[1].GetDiagnostic().GetSourceRange() == nil ||
		values[2].GetDiagnostic().GetSeverity() != opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING ||
		values[3].GetDiagnostic().GetSeverity() != opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO {
		t.Fatalf("diagnostic order = %+v", values)
	}
	suggestions := values[0].GetDiagnostic().GetSuggestions()
	if len(suggestions) != 2 || suggestions[0] != "a" || suggestions[1] != "z" {
		t.Fatalf("canonical suggestions = %v", suggestions)
	}
}

func TestDiagnosticBoundsRetainErrorBeforeWarningSaturation(t *testing.T) {
	input := make([]diagnosticIssue, 192, 194)
	for index := range input {
		input[index] = diagnosticIssue{
			path: fmt.Sprintf("w%03d", index), code: "W",
			severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
			message:  strings.Repeat("w", 4091),
		}
	}
	errorIssue := diagnosticIssue{
		path: "z", code: "E",
		severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
		message:  "invalid",
	}
	input = append(input, errorIssue, errorIssue)
	values, _, truncated, err := canonicalDiagnosticsWithSources(context.Background(), input)
	if err != nil || !truncated || len(values) != 192 ||
		values[0].GetDiagnostic().GetSeverity() != opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR {
		t.Fatalf("warning saturation = %d/%t/%v first=%+v", len(values), truncated, err, values[0])
	}

	tiny := make([]diagnosticIssue, MaximumIssues+1)
	for index := range tiny {
		tiny[index] = diagnosticIssue{
			path: fmt.Sprintf("c%03d", index), code: "C", message: "m",
			severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
		}
	}
	values, _, truncated, err = canonicalDiagnosticsWithSources(context.Background(), tiny)
	if err != nil || !truncated || len(values) != MaximumIssues {
		t.Fatalf("diagnostic count cap = %d/%t/%v", len(values), truncated, err)
	}
}

func TestIssueEntryValidationFailsClosed(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	if _, _, err := canonicalFieldViolations(context.Background(), []fieldIssue{{path: invalidUTF8, code: "C", message: "m"}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("invalid field UTF-8 error = %v", err)
	}
	if _, _, err := canonicalFieldViolations(context.Background(), []fieldIssue{{code: "", message: "m"}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("empty field code error = %v", err)
	}
	tooManySuggestions := make([]string, maximumSuggestions+1)
	for index := range tooManySuggestions {
		tooManySuggestions[index] = fmt.Sprintf("s%02d", index)
	}
	if _, _, _, err := canonicalDiagnosticsWithSources(context.Background(), []diagnosticIssue{{
		code: "C", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR,
		message: "m", suggestions: tooManySuggestions,
	}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("too many suggestions error = %v", err)
	}
	if _, _, _, err := canonicalDiagnosticsWithSources(context.Background(), []diagnosticIssue{{
		code: "C", severity: opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_UNSPECIFIED,
		message: "m",
	}}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("unspecified severity error = %v", err)
	}
	if _, err := publicRange(context.Background(), byteRange{start: 1, end: 2, source: "😀"}); !errors.Is(err, ErrInvariant) {
		t.Fatalf("mid-codepoint range error = %v", err)
	}
	if err := issueDescriptorContract(); err != nil {
		t.Fatalf("issue descriptor contract: %v", err)
	}
}

func TestRangeProjectionExactUTF8Boundaries(t *testing.T) {
	source := "α\rβ\n😀z"
	start := uint64(strings.Index(source, "😀"))
	end := start + uint64(len("😀"))
	value, err := publicRange(context.Background(), byteRange{start: start, end: end, source: source})
	if err != nil {
		t.Fatal(err)
	}
	if value.GetStart().GetByteOffset() != start || value.GetStart().GetLine() != 2 || value.GetStart().GetColumn() != 1 ||
		value.GetEnd().GetByteOffset() != end || value.GetEnd().GetLine() != 2 || value.GetEnd().GetColumn() != 2 ||
		!utf8.ValidString(source[:start]) || source[start:end] != "😀" {
		t.Fatalf("projected range = %+v", value)
	}
}
