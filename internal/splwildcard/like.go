// Package splwildcard validates and normalizes bounded SPL wildcard patterns.
package splwildcard

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumLikePatternBytes bounds both user-authored and normalized LIKE
	// text before it becomes a ClickHouse query argument.
	MaximumLikePatternBytes = 4 << 10
	// MaximumLikePatternWorkUnits bounds the normalized wildcard/literal token
	// stream. It is named separately from the byte ceiling so future
	// normalization cannot silently make execution work unbounded.
	MaximumLikePatternWorkUnits = 4 << 10
	// MaximumLikeQueryPatternWorkUnits bounds all LIKE occurrences in one
	// search. Every occurrence counts even when it reuses the same pattern.
	MaximumLikeQueryPatternWorkUnits = 16 << 10
)

var (
	ErrInvalidLikePattern  = errors.New("invalid LIKE pattern")
	ErrLikePatternTooLarge = errors.New("LIKE pattern exceeds its resource limit")
)

// LikePattern is a normalized ClickHouse-compatible LIKE pattern and its
// conservative execution-work estimate.
type LikePattern struct {
	Pattern   string
	WorkUnits int
}

// CompileLikePattern validates UTF-8, NUL, and complete-escape safety,
// collapses redundant unescaped percent wildcards, and counts
// literal/wildcard work without expanding the pattern. Backslash behavior
// intentionally matches ClickHouse: it escapes %, _, and itself; before any
// other rune it is literal.
func CompileLikePattern(pattern string) (LikePattern, error) {
	if !utf8.ValidString(pattern) || strings.IndexByte(pattern, 0) >= 0 {
		return LikePattern{}, ErrInvalidLikePattern
	}
	if len(pattern) > MaximumLikePatternBytes {
		return LikePattern{}, ErrLikePatternTooLarge
	}

	var normalized strings.Builder
	normalized.Grow(len(pattern))
	workUnits := 0
	previousPercent := false
	for offset := 0; offset < len(pattern); {
		current, width := utf8.DecodeRuneInString(pattern[offset:])
		if current == '\\' && offset+width >= len(pattern) {
			return LikePattern{}, ErrInvalidLikePattern
		}
		if current == '\\' {
			next, nextWidth := utf8.DecodeRuneInString(pattern[offset+width:])
			normalized.WriteRune(current)
			normalized.WriteRune(next)
			if next == '%' || next == '_' || next == '\\' {
				workUnits++
			} else {
				workUnits += 2
			}
			offset += width + nextWidth
			previousPercent = false
			continue
		}
		if current == '%' {
			if !previousPercent {
				normalized.WriteRune(current)
				workUnits++
			}
			previousPercent = true
			offset += width
			continue
		}
		normalized.WriteRune(current)
		workUnits++
		previousPercent = false
		offset += width
	}
	if workUnits == 0 {
		workUnits = 1
	}
	if normalized.Len() > MaximumLikePatternBytes ||
		workUnits > MaximumLikePatternWorkUnits {
		return LikePattern{}, ErrLikePatternTooLarge
	}
	return LikePattern{Pattern: normalized.String(), WorkUnits: workUnits}, nil
}

// IsLikeComplexityError classifies diagnostics that must use the shared
// source-located SPL_QUERY_TOO_COMPLEX code.
func IsLikeComplexityError(err error) bool {
	return errors.Is(err, ErrLikePatternTooLarge)
}
