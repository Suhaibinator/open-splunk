package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
)

const (
	// MaximumMatchPatternBytes bounds both the user-authored and normalized
	// RE2 text independently of the surrounding SPL source limit.
	MaximumMatchPatternBytes = 4 << 10
	// MaximumMatchProgramWorkUnits bounds estimated RE2 instructions after
	// counted repetitions are expanded.
	MaximumMatchProgramWorkUnits = 4 << 10
	// MaximumMatchQueryProgramWorkUnits bounds all match occurrences in one
	// compiled query, including repeated uses of a shared expression node.
	MaximumMatchQueryProgramWorkUnits = 16 << 10
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

// MatchPattern is one validated, normalized match program.
type MatchPattern struct {
	Pattern          string
	ProgramWorkUnits int
}

// CompileMatchPattern validates the RE2-compatible subset of Splunk's PCRE
// match dialect. ClickHouse enables dot-all by default, so the normalized
// expression explicitly restores ordinary PCRE/Splunk dot behavior.
func CompileMatchPattern(pattern string) (MatchPattern, error) {
	parsedPattern, err := compileBoundedRE2Pattern(
		pattern,
		MaximumMatchPatternBytes,
		MaximumMatchProgramWorkUnits,
	)
	if err != nil {
		if errors.Is(err, errBoundedRE2PatternTooComplex) {
			return MatchPattern{}, fmt.Errorf("%w: %w", ErrMatchPatternTooLarge, err)
		}
		return MatchPattern{}, fmt.Errorf("%w: %w", ErrInvalidMatchPattern, err)
	}
	parsed := parsedPattern.parsed
	rewriteMatchDollarAnchors(parsed)
	compiled, err := finishBoundedRE2Pattern(
		parsed,
		MaximumMatchPatternBytes,
		MaximumMatchProgramWorkUnits,
	)
	if err != nil {
		if errors.Is(err, errBoundedRE2PatternTooComplex) {
			return MatchPattern{}, fmt.Errorf("%w: %w", ErrMatchPatternTooLarge, err)
		}
		return MatchPattern{}, fmt.Errorf("%w: %w", ErrInvalidMatchPattern, err)
	}
	return MatchPattern{
		Pattern:          compiled.normalized,
		ProgramWorkUnits: compiled.programWorkUnits,
	}, nil
}

// rewriteMatchDollarAnchors preserves PCRE's non-multiline $ behavior: it can
// match at strict end of text or immediately before one final newline. Go's
// syntax serializer otherwise emits \z for OpEndText and loses the WasDollar
// distinction. Consuming the optional newline is equivalent for match's
// Boolean result; rex deliberately keeps its capture-sensitive normalization.
func rewriteMatchDollarAnchors(expression *syntax.Regexp) {
	if expression == nil {
		return
	}
	for _, child := range expression.Sub {
		rewriteMatchDollarAnchors(child)
	}
	if expression.Op != syntax.OpEndText || expression.Flags&syntax.WasDollar == 0 {
		return
	}
	*expression = syntax.Regexp{
		Op: syntax.OpConcat,
		Sub: []*syntax.Regexp{
			{
				Op: syntax.OpQuest,
				Sub: []*syntax.Regexp{{
					Op:   syntax.OpLiteral,
					Rune: []rune{'\n'},
				}},
			},
			{Op: syntax.OpEndText},
		},
	}
}
