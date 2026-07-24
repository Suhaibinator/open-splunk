package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumExtractionPatternBytes bounds RE2 compilation work independently
	// of the surrounding SPL source limit.
	MaximumExtractionPatternBytes = 4 << 10
	// MaximumExtractionCaptureGroups bounds the Array(String) produced for one
	// row. Unnamed groups count because ClickHouse returns them too.
	MaximumExtractionCaptureGroups = 16
)

var (
	ErrInvalidExtractionPattern  = errors.New("invalid extraction regular expression")
	ErrNoNamedCapture            = errors.New("extraction regular expression has no named capture")
	ErrDuplicateNamedCapture     = errors.New("extraction regular expression repeats a named capture")
	ErrExtractionPatternTooLarge = errors.New("extraction regular expression is too large")
	ErrTooManyExtractionCaptures = errors.New("extraction regular expression has too many capture groups")
)

// IsExtractionComplexityError reports whether validation failed because a
// bounded parser/compiler resource limit was exceeded rather than because the
// regular-expression dialect was unsupported.
func IsExtractionComplexityError(err error) bool {
	return errors.Is(err, ErrExtractionPatternTooLarge) ||
		errors.Is(err, ErrTooManyExtractionCaptures)
}

// NamedCapture maps a validated field name to its one-based RE2 capture-group
// index. Unnamed groups remain in the index sequence.
type NamedCapture struct {
	Name  string
	Group int
}

// ExtractionPattern is a bounded RE2 program and its ordered named outputs.
// Pattern explicitly disables dot-all mode before the user's expression so
// ClickHouse's default agrees with ordinary PCRE/Splunk dot behavior. A scoped
// or later user-authored (?s) flag can still opt in.
type ExtractionPattern struct {
	Pattern    string
	Captures   []NamedCapture
	GroupCount int
}

// CompileExtractionPattern validates the deliberately supported RE2 subset of
// Splunk's PCRE rex dialect and discovers every named capture exactly once.
func CompileExtractionPattern(pattern string) (ExtractionPattern, error) {
	if len(pattern) > MaximumExtractionPatternBytes {
		return ExtractionPattern{}, fmt.Errorf(
			"%w: pattern contains %d bytes, maximum is %d",
			ErrExtractionPatternTooLarge,
			len(pattern),
			MaximumExtractionPatternBytes,
		)
	}
	if pattern == "" || !utf8.ValidString(pattern) || strings.IndexByte(pattern, 0) >= 0 {
		return ExtractionPattern{}, ErrInvalidExtractionPattern
	}

	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return ExtractionPattern{}, fmt.Errorf("%w: %v", ErrInvalidExtractionPattern, err)
	}
	normalized := parsed.String()
	if len("(?-s)")+len(normalized) > MaximumExtractionPatternBytes {
		return ExtractionPattern{}, fmt.Errorf(
			"%w: normalized pattern contains %d bytes, maximum is %d",
			ErrExtractionPatternTooLarge,
			len("(?-s)")+len(normalized),
			MaximumExtractionPatternBytes,
		)
	}
	groupCount := parsed.MaxCap()
	if groupCount > MaximumExtractionCaptureGroups {
		return ExtractionPattern{}, fmt.Errorf(
			"%w: pattern contains %d groups, maximum is %d",
			ErrTooManyExtractionCaptures,
			groupCount,
			MaximumExtractionCaptureGroups,
		)
	}

	names := parsed.CapNames()
	captures := make([]NamedCapture, 0, groupCount)
	seen := make(map[string]struct{}, groupCount)
	for group := 1; group < len(names); group++ {
		name := names[group]
		if name == "" {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return ExtractionPattern{}, fmt.Errorf("%w: %q", ErrDuplicateNamedCapture, name)
		}
		seen[name] = struct{}{}
		captures = append(captures, NamedCapture{Name: name, Group: group})
	}
	if len(captures) == 0 {
		return ExtractionPattern{}, ErrNoNamedCapture
	}
	return ExtractionPattern{
		Pattern:    "(?-s)" + normalized,
		Captures:   captures,
		GroupCount: groupCount,
	}, nil
}
