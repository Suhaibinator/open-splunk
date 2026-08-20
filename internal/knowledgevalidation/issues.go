package knowledgevalidation

import (
	"cmp"
	"context"
	"slices"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

const (
	maximumFieldPathBytes  = 1 << 10
	maximumIssueCodeBytes  = 128
	maximumIssueMessage    = 4 << 10
	maximumSuggestions     = 32
	maximumSuggestionBytes = 1 << 10
)

type fieldIssue struct {
	path    string
	code    string
	message string
}

type diagnosticIssue struct {
	path        string
	code        string
	severity    opensplunk.DiagnosticSeverity
	message     string
	rangeValue  *byteRange
	suggestions []string
}

type byteRange struct {
	start  uint64
	end    uint64
	source string
}

type diagnosticSource struct {
	present   bool
	fieldPath string
	value     string
}

type projectedDiagnostic struct {
	value  *opensplunk.KnowledgeValidationDiagnostic
	source diagnosticSource
}

func canonicalFieldViolations(ctx context.Context, input []fieldIssue) ([]*opensplunk.FieldViolation, bool, error) {
	values := make([]*opensplunk.FieldViolation, 0, len(input))
	for index, issue := range input {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return nil, false, err
			}
		}
		if !validPath(issue.path) || !validIssueScalar(issue.code, maximumIssueCodeBytes) ||
			!validIssueScalar(issue.message, maximumIssueMessage) {
			return nil, false, ErrInvariant
		}
		values = append(values, &opensplunk.FieldViolation{
			FieldPath: strings.Clone(issue.path),
			Code:      strings.Clone(issue.code),
			Message:   strings.Clone(issue.message),
		})
	}
	slices.SortFunc(values, compareFieldViolations)
	values = slices.CompactFunc(values, func(left, right *opensplunk.FieldViolation) bool {
		return left.GetFieldPath() == right.GetFieldPath() && left.GetCode() == right.GetCode() &&
			left.GetMessage() == right.GetMessage()
	})
	textBytes := 0
	for index, value := range values {
		if index == MaximumIssues {
			clear(values[index:])
			return values[:index:index], true, nil
		}
		charge := len(value.GetFieldPath()) + len(value.GetCode()) + len(value.GetMessage())
		if charge > MaximumFieldViolationTextBytes-textBytes {
			clear(values[index:])
			return values[:index:index], true, nil
		}
		textBytes += charge
	}
	return values, false, nil
}

func canonicalDiagnosticsWithSources(
	ctx context.Context,
	input []diagnosticIssue,
) ([]*opensplunk.KnowledgeValidationDiagnostic, []diagnosticSource, bool, error) {
	projected := make([]projectedDiagnostic, 0, len(input))
	for index, issue := range input {
		if index%64 == 0 {
			if err := contextError(ctx); err != nil {
				return nil, nil, false, err
			}
		}
		value, err := projectDiagnostic(ctx, issue)
		if err != nil {
			return nil, nil, false, err
		}
		source := diagnosticSource{}
		if issue.rangeValue != nil {
			source = diagnosticSource{
				present: true, fieldPath: strings.Clone(issue.path), value: strings.Clone(issue.rangeValue.source),
			}
		}
		projected = append(projected, projectedDiagnostic{value: value, source: source})
	}
	slices.SortFunc(projected, func(left, right projectedDiagnostic) int {
		return compareDiagnostics(left.value, right.value)
	})
	compacted := projected[:0]
	for _, current := range projected {
		if len(compacted) == 0 || !proto.Equal(compacted[len(compacted)-1].value, current.value) {
			compacted = append(compacted, current)
			continue
		}
		previous := compacted[len(compacted)-1].source
		if previous.present != current.source.present || previous.fieldPath != current.source.fieldPath ||
			previous.value != current.source.value {
			return nil, nil, false, ErrInvariant
		}
	}
	projected = compacted
	textBytes := 0
	retained := len(projected)
	truncated := false
	for index, projectedValue := range projected {
		if index == MaximumIssues {
			retained, truncated = index, true
			break
		}
		value := projectedValue.value
		diagnostic := value.GetDiagnostic()
		charge := len(value.GetFieldPath()) + len(diagnostic.GetCode()) + len(diagnostic.GetMessage())
		for _, suggestion := range diagnostic.GetSuggestions() {
			charge += len(suggestion)
		}
		if charge > MaximumDiagnosticTextBytes-textBytes {
			retained, truncated = index, true
			break
		}
		textBytes += charge
	}
	values := make([]*opensplunk.KnowledgeValidationDiagnostic, retained)
	sources := make([]diagnosticSource, retained)
	for index := range retained {
		values[index] = projected[index].value
		sources[index] = projected[index].source
	}
	return values, sources, truncated, nil
}

func projectDiagnostic(ctx context.Context, issue diagnosticIssue) (*opensplunk.KnowledgeValidationDiagnostic, error) {
	if !validPath(issue.path) || !validIssueScalar(issue.code, maximumIssueCodeBytes) ||
		!validIssueScalar(issue.message, maximumIssueMessage) || severityRank(issue.severity) < 0 {
		return nil, ErrInvariant
	}
	suggestions := make([]string, 0, min(len(issue.suggestions), maximumSuggestions+1))
	seen := make(map[string]struct{}, min(len(issue.suggestions), maximumSuggestions+1))
	for index, suggestion := range issue.suggestions {
		if index%32 == 0 {
			if err := contextError(ctx); err != nil {
				return nil, err
			}
		}
		if !validIssueScalar(suggestion, maximumSuggestionBytes) {
			return nil, ErrInvariant
		}
		if _, duplicate := seen[suggestion]; duplicate {
			continue
		}
		if len(seen) == maximumSuggestions {
			return nil, ErrInvariant
		}
		seen[suggestion] = struct{}{}
		suggestions = append(suggestions, strings.Clone(suggestion))
	}
	slices.Sort(suggestions)

	diagnostic := &opensplunk.Diagnostic{
		Code:        strings.Clone(issue.code),
		Severity:    issue.severity,
		Message:     strings.Clone(issue.message),
		Suggestions: suggestions,
	}
	if issue.rangeValue != nil {
		sourceRange, err := publicRange(ctx, *issue.rangeValue)
		if err != nil {
			return nil, err
		}
		diagnostic.SourceRange = sourceRange
	}
	return &opensplunk.KnowledgeValidationDiagnostic{
		FieldPath:  strings.Clone(issue.path),
		Diagnostic: diagnostic,
	}, nil
}

func publicRange(ctx context.Context, value byteRange) (*opensplunk.SourceRange, error) {
	if !utf8.ValidString(value.source) || value.start > value.end || value.end > uint64(len(value.source)) ||
		!utf8.ValidString(value.source[:value.start]) || !utf8.ValidString(value.source[:value.end]) {
		return nil, ErrInvariant
	}
	start, err := sourcePosition(ctx, value.source, value.start)
	if err != nil {
		return nil, err
	}
	end, err := sourcePosition(ctx, value.source, value.end)
	if err != nil {
		return nil, err
	}
	return &opensplunk.SourceRange{Start: start, End: end}, nil
}

func sourcePosition(ctx context.Context, source string, offset uint64) (*opensplunk.SourcePosition, error) {
	if offset > uint64(len(source)) || !utf8.ValidString(source[:offset]) {
		return nil, ErrInvariant
	}
	end := int(offset) // #nosec G115 -- offset was bounded by len(source) above.
	line, column := uint32(1), uint32(1)
	for index := 0; index < end; {
		if index&4095 == 0 {
			if err := contextError(ctx); err != nil {
				return nil, err
			}
		}
		character, width := utf8.DecodeRuneInString(source[index:end])
		if character == utf8.RuneError && width == 1 {
			return nil, ErrInvariant
		}
		index += width
		if character == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return &opensplunk.SourcePosition{ByteOffset: offset, Line: line, Column: column}, nil
}

func compareFieldViolations(left, right *opensplunk.FieldViolation) int {
	if order := cmp.Compare(left.GetFieldPath(), right.GetFieldPath()); order != 0 {
		return order
	}
	if order := cmp.Compare(left.GetCode(), right.GetCode()); order != 0 {
		return order
	}
	return cmp.Compare(left.GetMessage(), right.GetMessage())
}

func compareDiagnostics(left, right *opensplunk.KnowledgeValidationDiagnostic) int {
	leftDiagnostic, rightDiagnostic := left.GetDiagnostic(), right.GetDiagnostic()
	if order := cmp.Compare(severityRank(leftDiagnostic.GetSeverity()), severityRank(rightDiagnostic.GetSeverity())); order != 0 {
		return order
	}
	if order := cmp.Compare(left.GetFieldPath(), right.GetFieldPath()); order != 0 {
		return order
	}
	leftRange, rightRange := leftDiagnostic.GetSourceRange(), rightDiagnostic.GetSourceRange()
	if (leftRange == nil) != (rightRange == nil) {
		if leftRange == nil {
			return -1
		}
		return 1
	}
	if leftRange != nil {
		if order := cmp.Compare(leftRange.GetStart().GetByteOffset(), rightRange.GetStart().GetByteOffset()); order != 0 {
			return order
		}
		if order := cmp.Compare(leftRange.GetEnd().GetByteOffset(), rightRange.GetEnd().GetByteOffset()); order != 0 {
			return order
		}
	}
	if order := cmp.Compare(leftDiagnostic.GetCode(), rightDiagnostic.GetCode()); order != 0 {
		return order
	}
	if order := cmp.Compare(leftDiagnostic.GetMessage(), rightDiagnostic.GetMessage()); order != 0 {
		return order
	}
	if leftRange != nil {
		for _, positions := range [][2]*opensplunk.SourcePosition{
			{leftRange.GetStart(), rightRange.GetStart()},
			{leftRange.GetEnd(), rightRange.GetEnd()},
		} {
			if order := cmp.Compare(positions[0].GetLine(), positions[1].GetLine()); order != 0 {
				return order
			}
			if order := cmp.Compare(positions[0].GetColumn(), positions[1].GetColumn()); order != 0 {
				return order
			}
		}
	}
	leftSuggestions, rightSuggestions := leftDiagnostic.GetSuggestions(), rightDiagnostic.GetSuggestions()
	for index := 0; index < min(len(leftSuggestions), len(rightSuggestions)); index++ {
		if order := cmp.Compare(leftSuggestions[index], rightSuggestions[index]); order != 0 {
			return order
		}
	}
	return cmp.Compare(len(leftSuggestions), len(rightSuggestions))
}

func severityRank(severity opensplunk.DiagnosticSeverity) int {
	switch severity {
	case opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_ERROR:
		return 0
	case opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING:
		return 1
	case opensplunk.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_INFO:
		return 2
	default:
		return -1
	}
}

func validPath(value string) bool {
	return len(value) <= maximumFieldPathBytes && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validIssueScalar(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
