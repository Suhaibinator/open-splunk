package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumMatchPatternBytes bounds both the user-authored and normalized
	// RE2 program independently of the surrounding SPL source limit.
	MaximumMatchPatternBytes = 4 << 10
)

var (
	ErrInvalidMatchPattern  = errors.New("invalid match regular expression")
	ErrMatchPatternTooLarge = errors.New("match regular expression is too large")
)

// IsMatchComplexityError reports whether validation failed at a bounded
// parser/compiler resource limit rather than at the supported regex dialect.
func IsMatchComplexityError(err error) bool {
	return errors.Is(err, ErrMatchPatternTooLarge)
}

// CompileMatchPattern validates the RE2-compatible subset of Splunk's PCRE
// match dialect. ClickHouse enables dot-all by default, so the normalized
// expression explicitly restores ordinary PCRE/Splunk dot behavior.
func CompileMatchPattern(pattern string) (string, error) {
	if len(pattern) > MaximumMatchPatternBytes {
		return "", fmt.Errorf(
			"%w: pattern contains %d bytes, maximum is %d",
			ErrMatchPatternTooLarge,
			len(pattern),
			MaximumMatchPatternBytes,
		)
	}
	if !utf8.ValidString(pattern) || strings.IndexByte(pattern, 0) >= 0 {
		return "", ErrInvalidMatchPattern
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidMatchPattern, err)
	}
	normalized := "(?-s)" + parsed.String()
	if len(normalized) > MaximumMatchPatternBytes {
		return "", fmt.Errorf(
			"%w: normalized pattern contains %d bytes, maximum is %d",
			ErrMatchPatternTooLarge,
			len(normalized),
			MaximumMatchPatternBytes,
		)
	}
	return normalized, nil
}
