// Package splregex defines the regular-expression subset shared by SPL
// semantic validation and backend compiler defense-in-depth checks.
package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"slices"
)

// ErrMayMatchEmpty reports a pattern whose language includes a zero-width
// match. ClickHouse deliberately replaces such matches at most once, unlike
// SPL's global PCRE replacement semantics.
var ErrMayMatchEmpty = errors.New("regular expression may match an empty substring")

// ErrReplacePatternTooLarge reports replacement patterns that exceed the shared
// match-style byte or expanded-program limits.
var ErrReplacePatternTooLarge = errors.New("replace regular expression is too large")

// ReplacePattern retains the authored pattern and its bounded program cost.
type ReplacePattern struct {
	Pattern          string
	ProgramWorkUnits int
}

// ValidateReplacePattern accepts the RE2-compatible, always-consuming subset
// that has consistent global replacement behavior in SPL and ClickHouse.
func ValidateReplacePattern(pattern string) error {
	_, err := CompileReplacePattern(pattern)
	return err
}

// CompileReplacePattern validates replacement patterns with the shared bounded
// RE2 estimator. Keep the authored text: match's Boolean-only normalization can
// consume a final newline at $, changing replacement output and captures.
func CompileReplacePattern(pattern string) (ReplacePattern, error) {
	compiled, err := compileBoundedRE2Pattern(
		pattern,
		MaximumMatchPatternBytes,
		MaximumMatchProgramWorkUnits,
	)
	if err != nil {
		if errors.Is(err, errBoundedRE2PatternTooComplex) {
			return ReplacePattern{}, fmt.Errorf("%w: %w", ErrReplacePatternTooLarge, err)
		}
		return ReplacePattern{}, fmt.Errorf("invalid RE2 regular expression: %w", err)
	}
	if mayMatchEmptySubstring(compiled.parsed) {
		return ReplacePattern{}, ErrMayMatchEmpty
	}
	return ReplacePattern{
		Pattern:          pattern,
		ProgramWorkUnits: compiled.programWorkUnits,
	}, nil
}

func mayMatchEmptySubstring(expression *syntax.Regexp) bool {
	switch expression.Op {
	case syntax.OpNoMatch:
		return false
	case syntax.OpEmptyMatch, syntax.OpBeginLine, syntax.OpEndLine, syntax.OpBeginText,
		syntax.OpEndText, syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return true
	case syntax.OpLiteral:
		return len(expression.Rune) == 0
	case syntax.OpCharClass, syntax.OpAnyCharNotNL, syntax.OpAnyChar:
		return false
	case syntax.OpCapture:
		return mayMatchEmptySubstring(expression.Sub[0])
	case syntax.OpConcat:
		for _, child := range expression.Sub {
			if !mayMatchEmptySubstring(child) {
				return false
			}
		}
		return true
	case syntax.OpAlternate:
		return slices.ContainsFunc(expression.Sub, mayMatchEmptySubstring)
	case syntax.OpQuest, syntax.OpStar:
		return true
	case syntax.OpPlus:
		return mayMatchEmptySubstring(expression.Sub[0])
	case syntax.OpRepeat:
		return expression.Min == 0 || mayMatchEmptySubstring(expression.Sub[0])
	default:
		// Reject newly introduced zero-width syntax until its consumption
		// behavior is explicitly classified here.
		return true
	}
}
