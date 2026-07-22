// Package splregex defines the regular-expression subset shared by SPL
// semantic validation and backend compiler defense-in-depth checks.
package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
)

// ErrMayMatchEmpty reports a pattern whose language includes a zero-width
// match. ClickHouse deliberately replaces such matches at most once, unlike
// SPL's global PCRE replacement semantics.
var ErrMayMatchEmpty = errors.New("regular expression may match an empty substring")

// ValidateReplacePattern accepts the RE2-compatible, always-consuming subset
// that has consistent global replacement behavior in SPL and ClickHouse.
func ValidateReplacePattern(pattern string) error {
	expression, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return fmt.Errorf("invalid RE2 regular expression: %w", err)
	}
	if mayMatchEmptySubstring(expression) {
		return ErrMayMatchEmpty
	}
	return nil
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
		for _, child := range expression.Sub {
			if mayMatchEmptySubstring(child) {
				return true
			}
		}
		return false
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
