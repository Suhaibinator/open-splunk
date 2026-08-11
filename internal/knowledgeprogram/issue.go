package knowledgeprogram

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

// IssueCode is a stable candidate-authored semantic validation category.
// Calculated-expression parser failures retain their existing SPL_* code by
// converting it to IssueCode; the constants below cover knowledge-specific
// compilation failures that do not originate in the SPL parser.
type IssueCode string

const (
	IssueCodeRegexInvalid          IssueCode = "KNOWLEDGE_REGEX_INVALID"
	IssueCodeRegexResourceLimit    IssueCode = "KNOWLEDGE_REGEX_RESOURCE_LIMIT"
	IssueCodeRegexCaptureMismatch  IssueCode = "KNOWLEDGE_REGEX_CAPTURE_MISMATCH"
	IssueCodeJSONPathInvalid       IssueCode = "KNOWLEDGE_JSON_PATH_INVALID"
	IssueCodeJSONPathUnsupported   IssueCode = "KNOWLEDGE_JSON_PATH_UNSUPPORTED"
	IssueCodeJSONPathResourceLimit IssueCode = "KNOWLEDGE_JSON_PATH_RESOURCE_LIMIT"
	IssueCodeCalculatedBoolean     IssueCode = "KNOWLEDGE_CALCULATED_BOOLEAN_RESULT"
)

// ScalarRange is one half-open UTF-8 byte range within the canonical scalar
// named by Issue.FieldPath. It is not a range within a serialized definition.
// In particular, calculated expressions are ASCII-trimmed by definition
// normalization; a response adapter must independently prove and reapply the
// submitted scalar's leading trim before deriving public line/column values.
type ScalarRange struct {
	StartByteOffset uint32
	EndByteOffset   uint32
}

// Issue is one detached, candidate-actionable semantic compilation failure.
// FieldPath is relative to KnowledgeObjectDefinition. It deliberately carries
// no object identity, catalog authority, source text, or cohort information.
// Compile remains fail-fast, so one returned error carries at most one Issue.
type Issue struct {
	FieldPath   string
	Code        IssueCode
	Message     string
	Range       *ScalarRange
	Suggestions []string
}

// authoredSemanticError is produced only for locally attributable definition
// semantics. Its Error text is the legacy appendObject text; it never unwraps
// parser/compiler implementation errors into the public errors.Is taxonomy.
type authoredSemanticError struct {
	text  string
	issue Issue
}

func (err *authoredSemanticError) Error() string {
	if err == nil {
		return ""
	}
	return err.text
}

// candidateIssueError preserves Compile's exact legacy Error text and root
// sentinel while privately binding the issue to one input position. No such
// wrapper is emitted by Prepare.
type candidateIssueError struct {
	text       string
	inputIndex uint32
	issue      Issue
}

func (err *candidateIssueError) Error() string {
	if err == nil {
		return ""
	}
	return err.text
}

func (err *candidateIssueError) Unwrap() error {
	if err == nil {
		return nil
	}
	return ErrInvalidProgram
}

// CandidateIssueFromError returns a detached issue only when the privately
// recorded Compile input position equals candidateInputIndex. The index check
// is attribution, not authorization: callers must independently prove that
// the indexed object is the exact submitted candidate. A validation adapter
// should prefer a singleton candidate Compile and index zero, and must never
// project issues from a complete winner-cohort compilation.
func CandidateIssueFromError(err error, candidateInputIndex uint32) (Issue, bool) {
	var detailed *candidateIssueError
	if !errors.As(err, &detailed) || detailed == nil ||
		detailed.inputIndex != candidateInputIndex {
		return Issue{}, false
	}
	return cloneIssue(detailed.issue), true
}

func newAuthoredSemanticError(text string, issue Issue) error {
	return &authoredSemanticError{text: text, issue: cloneIssue(issue)}
}

func candidateInvalid(index int, err *authoredSemanticError) error {
	return &candidateIssueError{
		text:       fmt.Sprintf("%v: object %d %s", ErrInvalidProgram, index, err.Error()),
		inputIndex: uint32(index), // #nosec G115 -- index comes from the MaximumObjects-bounded input slice.
		issue:      cloneIssue(err.issue),
	}
}

func cloneIssue(issue Issue) Issue {
	result := Issue{
		FieldPath: strings.Clone(issue.FieldPath),
		Code:      IssueCode(strings.Clone(string(issue.Code))),
		Message:   strings.Clone(issue.Message),
	}
	if issue.Range != nil {
		rangeCopy := *issue.Range
		result.Range = &rangeCopy
	}
	if issue.Suggestions != nil {
		result.Suggestions = make([]string, len(issue.Suggestions))
		for index := range issue.Suggestions {
			result.Suggestions[index] = strings.Clone(issue.Suggestions[index])
		}
	}
	return result
}

func regexCompilationIssue(err error) (Issue, bool) {
	issue := Issue{FieldPath: "field_extraction.regex.pattern"}
	switch {
	case splregex.IsExtractionComplexityError(err):
		issue.Code = IssueCodeRegexResourceLimit
		issue.Message = "pattern exceeds extraction complexity limits"
	case errors.Is(err, splregex.ErrNoNamedCapture):
		issue.Code = IssueCodeRegexInvalid
		issue.Message = "pattern must contain at least one named capture"
	case errors.Is(err, splregex.ErrDuplicateNamedCapture):
		issue.Code = IssueCodeRegexInvalid
		issue.Message = "pattern must use each named capture name once"
	case errors.Is(err, splregex.ErrInvalidExtractionPattern):
		issue.Code = IssueCodeRegexInvalid
		issue.Message = "pattern is not a supported RE2 extraction expression"
	default:
		return Issue{}, false
	}
	return issue, true
}

func jsonPathCompilationIssue(source string, err error) (Issue, bool) {
	var pathErr *splpath.Error
	if !errors.As(err, &pathErr) || pathErr == nil ||
		!validIssueText(pathErr.Message) {
		return Issue{}, false
	}
	var code IssueCode
	switch pathErr.Kind {
	case splpath.ErrorKindInvalid:
		code = IssueCodeJSONPathInvalid
	case splpath.ErrorKindUnsupported:
		code = IssueCodeJSONPathUnsupported
	case splpath.ErrorKindTooComplex:
		code = IssueCodeJSONPathResourceLimit
	default:
		return Issue{}, false
	}
	sourceRange, ok := scalarPointRange(source, pathErr.Offset)
	if !ok {
		return Issue{}, false
	}
	return Issue{
		FieldPath: "field_extraction.json.path",
		Code:      code,
		Message:   pathErr.Message,
		Range:     sourceRange,
	}, true
}

func calculatedExpressionCompilationIssue(source string, err error) (Issue, bool) {
	var diagnostic *spl.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic == nil ||
		!strings.HasPrefix(diagnostic.Code, "SPL_") ||
		!validIssueText(diagnostic.Code) || !validIssueText(diagnostic.Message) {
		return Issue{}, false
	}
	sourceRange, ok := scalarDiagnosticRange(source, diagnostic.Range)
	if !ok {
		return Issue{}, false
	}
	for _, suggestion := range diagnostic.Suggestions {
		if !validIssueText(suggestion) {
			return Issue{}, false
		}
	}
	return Issue{
		FieldPath:   "calculated_field.expression",
		Code:        IssueCode(diagnostic.Code),
		Message:     diagnostic.Message,
		Range:       sourceRange,
		Suggestions: diagnostic.Suggestions,
	}, true
}

func scalarDiagnosticRange(source string, sourceRange spl.Range) (*ScalarRange, bool) {
	start, end := sourceRange.Start.Offset, sourceRange.End.Offset
	if start < 0 || end < start || end > len(source) ||
		!utf8.ValidString(source) || !utf8.ValidString(source[:start]) ||
		!utf8.ValidString(source[:end]) {
		return nil, false
	}
	return &ScalarRange{
		StartByteOffset: uint32(start), // #nosec G115 -- definition input is bounded far below MaxUint32.
		EndByteOffset:   uint32(end),   // #nosec G115 -- definition input is bounded far below MaxUint32.
	}, true
}

func scalarPointRange(source string, offset int) (*ScalarRange, bool) {
	if offset < 0 || offset > len(source) || !utf8.ValidString(source) ||
		!utf8.ValidString(source[:offset]) {
		return nil, false
	}
	end := offset
	if offset < len(source) {
		_, width := utf8.DecodeRuneInString(source[offset:])
		if width < 1 {
			return nil, false
		}
		end += width
	}
	return &ScalarRange{
		StartByteOffset: uint32(offset), // #nosec G115 -- definition input is bounded far below MaxUint32.
		EndByteOffset:   uint32(end),    // #nosec G115 -- definition input is bounded far below MaxUint32.
	}, true
}

func validIssueText(value string) bool {
	return value != "" && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
