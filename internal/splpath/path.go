// Package splpath parses the bounded explicit JSON location paths accepted by
// Open Splunk's first spath compatibility slice.
package splpath

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumPathBytes bounds parser work and the aggregate size of bound
	// ClickHouse path arguments.
	MaximumPathBytes = 4 << 10
	// MaximumPathSteps aligns explicit extraction with the ingestion field-path
	// depth ceiling.
	MaximumPathSteps = 17
	// MaximumKeyBytes aligns one JSON location step with one normalized event
	// field segment.
	MaximumKeyBytes = 256
	// MaximumArraySelectors bounds the extra full-document container checks
	// required because ClickHouse integer path arguments also index objects by
	// member position.
	MaximumArraySelectors = 4
	// MaximumEvaluationWorkUnits bounds cumulative JSON parser invocations per
	// row. One stage costs terminal type + raw extraction + typed-leaf decode,
	// plus one container-type check for each fixed array selector.
	MaximumEvaluationWorkUnits = 32
	// MaximumArrayIndex leaves room to translate Splunk's zero-based index to
	// a positive signed-32-bit ClickHouse index. The pinned server wraps larger
	// integer path arguments instead of treating them as out of range.
	MaximumArrayIndex = uint64(math.MaxInt32 - 1)
)

// Step is one case-sensitive JSON object key and an optional zero-based array
// index applied to that key's value.
type Step struct {
	Key      string
	HasIndex bool
	Index    uint64
}

// EvaluationWorkUnits returns the worst-case JSON parser invocations needed to
// evaluate one validated explicit path.
func EvaluationWorkUnits(steps []Step) int {
	units := 3
	for _, step := range steps {
		if step.HasIndex {
			units++
		}
	}
	return units
}

// ErrorKind distinguishes malformed input, deliberately unsupported SPL
// surface, and bounded-complexity failures.
type ErrorKind uint8

const (
	ErrorKindInvalid ErrorKind = iota + 1
	ErrorKindUnsupported
	ErrorKindTooComplex
)

// Error is a byte-located path diagnostic. Offset is relative to the unescaped
// path value rather than the complete SPL source.
type Error struct {
	Kind    ErrorKind
	Offset  int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("spath JSON path byte %d: %s", e.Offset, e.Message)
}

// ParseJSON parses one explicit SPL JSON datapath. Auto-extraction, XML
// attributes, array wildcards, escaped key separators, and multiple indexes
// per location step are intentionally outside this slice.
func ParseJSON(path string) ([]Step, error) {
	if path == "" {
		return nil, invalid(0, "path is empty")
	}
	if !utf8.ValidString(path) {
		return nil, invalid(firstInvalidUTF8(path), "path must be valid UTF-8")
	}
	if len(path) > MaximumPathBytes {
		return nil, tooComplex(MaximumPathBytes, fmt.Sprintf("path exceeds %d UTF-8 bytes", MaximumPathBytes))
	}

	steps := make([]Step, 0, 4)
	arraySelectors := 0
	start := 0
	for start <= len(path) {
		if len(steps) >= MaximumPathSteps {
			return nil, tooComplex(start, fmt.Sprintf("path contains more than %d location steps", MaximumPathSteps))
		}
		remainder := path[start:]
		width := strings.IndexByte(remainder, '.')
		end := len(path)
		if width >= 0 {
			end = start + width
		}
		if end == start {
			return nil, invalid(start, "path contains an empty location step")
		}
		step, err := parseStep(path[start:end], start)
		if err != nil {
			return nil, err
		}
		if step.HasIndex {
			arraySelectors++
			if arraySelectors > MaximumArraySelectors {
				return nil, tooComplex(
					start,
					fmt.Sprintf("path contains more than %d fixed array selectors", MaximumArraySelectors),
				)
			}
		}
		steps = append(steps, step)
		if width < 0 {
			break
		}
		start = end + 1
		if start == len(path) {
			return nil, invalid(start, "path contains an empty trailing location step")
		}
	}
	return steps, nil
}

func parseStep(source string, offset int) (Step, error) {
	for index, value := range source {
		if value < 0x20 || value == 0x7f {
			return Step{}, invalid(offset+index, "location step contains a control character")
		}
		if value == '\\' {
			return Step{}, unsupported(offset+index, "escaped JSON path keys are not supported")
		}
		if value == '*' {
			return Step{}, unsupported(offset+index, "wildcard JSON path keys are not supported")
		}
	}

	open := strings.IndexByte(source, '{')
	closeIndex := strings.IndexByte(source, '}')
	if open < 0 && closeIndex < 0 {
		if len(source) > MaximumKeyBytes {
			return Step{}, tooComplex(offset, fmt.Sprintf("location-step key exceeds %d UTF-8 bytes", MaximumKeyBytes))
		}
		return Step{Key: source}, nil
	}
	if open <= 0 || closeIndex != len(source)-1 || strings.Count(source, "{") != 1 || strings.Count(source, "}") != 1 {
		return Step{}, invalid(offset+max(open, closeIndex, 0), "array index must be one final {...} suffix")
	}

	key := source[:open]
	if len(key) > MaximumKeyBytes {
		return Step{}, tooComplex(offset, fmt.Sprintf("location-step key exceeds %d UTF-8 bytes", MaximumKeyBytes))
	}
	indexText := source[open+1 : closeIndex]
	if indexText == "" || indexText == "*" {
		return Step{}, unsupported(offset+open, "array wildcard extraction is not supported")
	}
	if strings.HasPrefix(indexText, "@") {
		return Step{}, unsupported(offset+open, "XML attribute extraction is not supported")
	}
	if strings.HasPrefix(indexText, "-") {
		return Step{}, unsupported(offset+open, "negative array indexes are not supported")
	}
	if len(indexText) > 1 && indexText[0] == '0' {
		return Step{}, invalid(offset+open+1, "array index must not contain leading zeros")
	}
	for index := range len(indexText) {
		if indexText[index] < '0' || indexText[index] > '9' {
			return Step{}, invalid(offset+open+1+index, "array index must be an unsigned decimal integer")
		}
	}
	value, err := strconv.ParseUint(indexText, 10, 64)
	if err != nil || value > MaximumArrayIndex {
		return Step{}, tooComplex(offset+open+1, fmt.Sprintf("array index exceeds %d", MaximumArrayIndex))
	}
	return Step{Key: key, HasIndex: true, Index: value}, nil
}

func firstInvalidUTF8(value string) int {
	for offset := 0; offset < len(value); {
		_, width := utf8.DecodeRuneInString(value[offset:])
		if width == 1 && value[offset] >= utf8.RuneSelf {
			return offset
		}
		offset += width
	}
	return len(value)
}

func invalid(offset int, message string) *Error {
	return &Error{Kind: ErrorKindInvalid, Offset: offset, Message: message}
}

func unsupported(offset int, message string) *Error {
	return &Error{Kind: ErrorKindUnsupported, Offset: offset, Message: message}
}

func tooComplex(offset int, message string) *Error {
	return &Error{Kind: ErrorKindTooComplex, Offset: offset, Message: message}
}
