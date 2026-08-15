// Package splcompataudit performs a bounded, read-only inventory of stored SPL
// that may change meaning at the v0.2 operator-character transition. Reports
// deliberately contain only object identities and source locations, never SPL
// text or fragments.
package splcompataudit

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	CompatibilityVersion                 = "0.2"
	FindingKindAmbiguousUnspacedOperator = "ambiguous_unspaced_scalar_operator"
	MaximumAuditFindings                 = 100_000
	MaximumAuditSourceBytes              = 2 << 20
)

// Finding identifies a possible compatibility transition without retaining or
// returning the authored source. SourceLocation is a stable path plus one-based
// line and column; ObjectID is a saved-search ID or repository-relative ID.
type Finding struct {
	ObjectID       string `json:"object_id"`
	SourceLocation string `json:"source_location"`
	Kind           string `json:"kind"`
}

// Report is safe for logs and command output. It contains no authored SPL.
type Report struct {
	CompatibilityVersion string    `json:"compatibility_version"`
	ScannedObjects       uint64    `json:"scanned_objects"`
	Findings             []Finding `json:"findings"`
}

func newReport() Report {
	return Report{CompatibilityVersion: CompatibilityVersion}
}

func mergeReports(reports ...Report) (Report, error) {
	result := newReport()
	for _, report := range reports {
		if report.CompatibilityVersion != CompatibilityVersion {
			return Report{}, errors.New("merge SPL compatibility audit: report version is invalid")
		}
		if ^uint64(0)-result.ScannedObjects < report.ScannedObjects {
			return Report{}, errors.New("merge SPL compatibility audit: object count overflows")
		}
		result.ScannedObjects += report.ScannedObjects
		if len(report.Findings) > MaximumAuditFindings-len(result.Findings) {
			return Report{}, fmt.Errorf("merge SPL compatibility audit: findings exceed %d", MaximumAuditFindings)
		}
		result.Findings = append(result.Findings, report.Findings...)
	}
	slices.SortFunc(result.Findings, func(left, right Finding) int {
		if compared := strings.Compare(left.ObjectID, right.ObjectID); compared != 0 {
			return compared
		}
		if compared := strings.Compare(left.SourceLocation, right.SourceLocation); compared != 0 {
			return compared
		}
		return strings.Compare(left.Kind, right.Kind)
	})
	return result, nil
}

// AuditSource returns only source ranges for ambiguous unspaced scalar
// operators. It scans scalar command stages independently of a successful
// v0.2 parse because a legacy spelling may itself make the new parser fail.
// Both intended identifier arithmetic and a legacy punctuation-bearing field
// are candidates: no data-independent parser can distinguish them.
func AuditSource(source string) ([]spl.Range, error) {
	return auditSource(source, MaximumAuditFindings)
}

func auditSource(source string, maximumFindings int) ([]spl.Range, error) {
	if maximumFindings < 0 || maximumFindings > MaximumAuditFindings {
		return nil, errors.New("scan SPL compatibility audit source: invalid finding limit")
	}
	if len(source) > MaximumAuditSourceBytes {
		return nil, fmt.Errorf(
			"scan SPL compatibility audit source: source exceeds %d bytes",
			MaximumAuditSourceBytes,
		)
	}
	if source == "" {
		return nil, nil
	}
	if !utf8.ValidString(source) {
		return nil, errors.New("scan SPL compatibility audit source: source is not valid UTF-8")
	}
	offsets := make([]int, 0)
	for _, stage := range scalarAuditStages(source) {
		stageOffsets, err := ambiguousStageOperatorOffsets(
			source,
			stage.start,
			stage.end,
			maximumFindings-len(offsets),
		)
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, stageOffsets...)
	}
	return sourceRangesForSortedOffsets(source, offsets), nil
}

type auditStage struct{ start, end int }

func scalarAuditStages(source string) []auditStage {
	boundaries := []int{0}
	quote := byte(0)
	escaped := false
	for index := 0; index < len(source); index++ {
		character := source[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		if character == '\'' || character == '"' {
			quote = character
			continue
		}
		if character == '|' {
			boundaries = append(boundaries, index+1)
		}
	}
	boundaries = append(boundaries, len(source)+1)

	stages := make([]auditStage, 0, len(boundaries)-1)
	for index := 0; index+1 < len(boundaries); index++ {
		start := boundaries[index]
		end := boundaries[index+1] - 1
		for start < end {
			character, width := utf8.DecodeRuneInString(source[start:end])
			if !unicode.IsSpace(character) {
				break
			}
			start += width
		}
		commandStart := start
		for start < end {
			character, width := utf8.DecodeRuneInString(source[start:end])
			if unicode.IsSpace(character) {
				break
			}
			start += width
		}
		command := strings.ToLower(source[commandStart:start])
		if command == "eval" || command == "where" {
			stages = append(stages, auditStage{start: start, end: end})
		}
	}
	return stages
}

func ambiguousStageOperatorOffsets(source string, start, end, maximumFindings int) ([]int, error) {
	offsets := make([]int, 0)
	for index := start; index < end; {
		if source[index] == '\'' || source[index] == '"' {
			index = skipAuditQuoted(source, index, end)
			continue
		}
		if scalarSegmentDelimiter(source[index]) {
			index++
			continue
		}
		segmentStart := index
		for index < end && source[index] != '\'' && source[index] != '"' &&
			!scalarSegmentDelimiter(source[index]) {
			index++
		}
		segmentEnd := index
		segment := source[segmentStart:segmentEnd]
		if numericArithmeticSegment(segment) {
			continue
		}
		protected := numericLiteralOperatorOffsets(segment)
		for relative := 0; relative < len(segment); relative++ {
			character := segment[relative]
			if !strings.ContainsRune("+-*/%", rune(character)) || protected[relative] {
				continue
			}
			operator := segmentStart + relative
			left, leftOK := boundaryRuneBefore(source, operator)
			right, rightOK := boundaryRuneAfter(source, operator+1)
			if (!leftOK || unicode.IsSpace(left)) && (!rightOK || unicode.IsSpace(right)) {
				continue
			}
			if (!leftOK || !identifierBoundary(left)) && (!rightOK || !identifierBoundary(right)) {
				continue
			}
			if len(offsets) == maximumFindings {
				return nil, fmt.Errorf(
					"scan SPL compatibility audit source: findings exceed %d",
					MaximumAuditFindings,
				)
			}
			offsets = append(offsets, operator)
		}
	}
	return offsets, nil
}

func skipAuditQuoted(source string, start, end int) int {
	quote := source[start]
	escaped := false
	for index := start + 1; index < end; index++ {
		if escaped {
			escaped = false
			continue
		}
		if source[index] == '\\' {
			escaped = true
			continue
		}
		if source[index] == quote {
			return index + 1
		}
	}
	return end
}

func scalarSegmentDelimiter(character byte) bool {
	if character >= utf8.RuneSelf {
		return false
	}
	return character == ' ' || character == '\t' || character == '\r' || character == '\n' ||
		strings.ContainsRune(",()=<>!&|\"'", rune(character))
}

func numericArithmeticSegment(segment string) bool {
	parser := numericSegmentParser{source: segment}
	return segment != "" && parser.parseExpression() && parser.offset == len(segment)
}

// numericLiteralOperatorOffsets marks only unary and exponent signs that are
// part of complete numeric tokens inside an otherwise nonnumeric segment. The
// segment's other operators remain audit candidates. For example, the minus in
// 1e-3+field is numeric structure while the plus is a transition candidate.
func numericLiteralOperatorOffsets(segment string) []bool {
	protected := make([]bool, len(segment))
	for index := 0; index < len(segment); {
		unarySign := false
		numberStart := index
		if (segment[index] == '+' || segment[index] == '-') &&
			(index == 0 || arithmeticOperatorByte(segment[index-1])) {
			unarySign = true
			numberStart++
		}
		if numberStart >= len(segment) || !numericLiteralStart(segment, numberStart) ||
			(numberStart > 0 && numberStart == index && !arithmeticOperatorByte(segment[numberStart-1])) {
			index++
			continue
		}
		numberEnd, exponentSign, ok := scanUnsignedNumericLiteral(segment, numberStart)
		if !ok || (numberEnd < len(segment) && identifierBoundaryByte(segment[numberEnd])) {
			index++
			continue
		}
		if unarySign {
			protected[index] = true
		}
		if exponentSign >= 0 {
			protected[exponentSign] = true
		}
		index = numberEnd
	}
	return protected
}

func numericLiteralStart(source string, offset int) bool {
	if offset < 0 || offset >= len(source) {
		return false
	}
	return source[offset] >= '0' && source[offset] <= '9' ||
		(source[offset] == '.' && offset+1 < len(source) &&
			source[offset+1] >= '0' && source[offset+1] <= '9')
}

func scanUnsignedNumericLiteral(source string, offset int) (int, int, bool) {
	start := offset
	digits := 0
	for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
		offset++
		digits++
	}
	if offset < len(source) && source[offset] == '.' {
		offset++
		for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
			offset++
			digits++
		}
	}
	if digits == 0 {
		return start, -1, false
	}
	exponentSign := -1
	if offset < len(source) && (source[offset] == 'e' || source[offset] == 'E') {
		exponentStart := offset
		offset++
		if offset < len(source) && (source[offset] == '+' || source[offset] == '-') {
			exponentSign = offset
			offset++
		}
		digitsStart := offset
		for offset < len(source) && source[offset] >= '0' && source[offset] <= '9' {
			offset++
		}
		if offset == digitsStart {
			return exponentStart, -1, true
		}
	}
	return offset, exponentSign, true
}

func arithmeticOperatorByte(character byte) bool {
	return character == '+' || character == '-' || character == '*' ||
		character == '/' || character == '%'
}

func identifierBoundaryByte(character byte) bool {
	if character >= utf8.RuneSelf {
		return true
	}
	return character == '_' || character == '$' || character == '.' ||
		character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9'
}

type numericSegmentParser struct {
	source string
	offset int
}

func (parser *numericSegmentParser) parseExpression() bool {
	if !parser.parseTerm() {
		return false
	}
	for parser.offset < len(parser.source) &&
		(parser.source[parser.offset] == '+' || parser.source[parser.offset] == '-') {
		parser.offset++
		if !parser.parseTerm() {
			return false
		}
	}
	return true
}

func (parser *numericSegmentParser) parseTerm() bool {
	if !parser.parseUnary() {
		return false
	}
	for parser.offset < len(parser.source) && strings.ContainsRune("*/%", rune(parser.source[parser.offset])) {
		parser.offset++
		if !parser.parseUnary() {
			return false
		}
	}
	return true
}

func (parser *numericSegmentParser) parseUnary() bool {
	for parser.offset < len(parser.source) &&
		(parser.source[parser.offset] == '+' || parser.source[parser.offset] == '-') {
		parser.offset++
	}
	return parser.parseNumber()
}

func (parser *numericSegmentParser) parseNumber() bool {
	end, _, ok := scanUnsignedNumericLiteral(parser.source, parser.offset)
	parser.offset = end
	return ok
}

func boundaryRuneBefore(source string, offset int) (rune, bool) {
	if offset <= 0 || offset > len(source) {
		return 0, false
	}
	character, _ := utf8.DecodeLastRuneInString(source[:offset])
	return character, character != utf8.RuneError
}

func boundaryRuneAfter(source string, offset int) (rune, bool) {
	if offset < 0 || offset >= len(source) {
		return 0, false
	}
	character, _ := utf8.DecodeRuneInString(source[offset:])
	return character, character != utf8.RuneError
}

func identifierBoundary(character rune) bool {
	return character == '_' || character == '$' || character == '.' || character == '\\' ||
		unicode.IsLetter(character) || unicode.IsNumber(character) || unicode.IsMark(character)
}

func sourceRangesForSortedOffsets(source string, offsets []int) []spl.Range {
	if len(offsets) == 0 {
		return nil
	}
	ranges := make([]spl.Range, 0, len(offsets))
	position := spl.Position{Line: 1, Column: 1}
	for _, target := range offsets {
		for position.Offset < target {
			character, width := utf8.DecodeRuneInString(source[position.Offset:])
			position.Offset += width
			if character == '\n' {
				position.Line++
				position.Column = 1
			} else {
				position.Column++
			}
		}
		start := position
		end := start
		end.Offset++
		end.Column++
		ranges = append(ranges, spl.Range{Start: start, End: end})
	}
	return ranges
}

func finding(objectID, locationPrefix string, sourceRange spl.Range) Finding {
	return Finding{
		ObjectID: objectID,
		SourceLocation: fmt.Sprintf(
			"%s:%d:%d",
			filepath.ToSlash(locationPrefix),
			sourceRange.Start.Line,
			sourceRange.Start.Column,
		),
		Kind: FindingKindAmbiguousUnspacedOperator,
	}
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("SPL compatibility audit: context is nil")
	}
	return ctx.Err()
}
