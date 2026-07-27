package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"strings"
	"unicode/utf8"
)

var (
	errInvalidBoundedRE2Pattern    = errors.New("invalid RE2 regular expression")
	errBoundedRE2PatternTooComplex = errors.New("RE2 regular expression exceeds a resource limit")
)

type boundedRE2Pattern struct {
	parsed           *syntax.Regexp
	normalized       string
	programWorkUnits int
}

// compileBoundedRE2Pattern parses and normalizes a bounded RE2 expression
// without first simplifying or compiling it. RE2 compilers expand counted
// repetitions, so estimating that work from the compact AST first prevents a
// small expression from allocating a large program merely to measure it.
func compileBoundedRE2Pattern(
	pattern string,
	maximumBytes int,
	maximumProgramWorkUnits int,
) (boundedRE2Pattern, error) {
	if len(pattern) > maximumBytes {
		return boundedRE2Pattern{}, fmt.Errorf(
			"%w: pattern contains %d bytes, maximum is %d",
			errBoundedRE2PatternTooComplex,
			len(pattern),
			maximumBytes,
		)
	}
	if !utf8.ValidString(pattern) || strings.IndexByte(pattern, 0) >= 0 {
		return boundedRE2Pattern{}, errInvalidBoundedRE2Pattern
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return boundedRE2Pattern{}, fmt.Errorf("%w: %w", errInvalidBoundedRE2Pattern, err)
	}
	return finishBoundedRE2Pattern(parsed, maximumBytes, maximumProgramWorkUnits)
}

func finishBoundedRE2Pattern(
	parsed *syntax.Regexp,
	maximumBytes int,
	maximumProgramWorkUnits int,
) (boundedRE2Pattern, error) {
	programWorkUnits, exceedsLimit := boundedRE2ProgramWorkUnits(
		parsed,
		maximumProgramWorkUnits,
	)
	if exceedsLimit {
		return boundedRE2Pattern{}, fmt.Errorf(
			"%w: estimated program requires more than %d work units",
			errBoundedRE2PatternTooComplex,
			maximumProgramWorkUnits,
		)
	}
	normalized := "(?-s)" + parsed.String()
	if len(normalized) > maximumBytes {
		return boundedRE2Pattern{}, fmt.Errorf(
			"%w: normalized pattern contains %d bytes, maximum is %d",
			errBoundedRE2PatternTooComplex,
			len(normalized),
			maximumBytes,
		)
	}
	return boundedRE2Pattern{
		parsed:           parsed,
		normalized:       normalized,
		programWorkUnits: programWorkUnits,
	}, nil
}

// boundedRE2ProgramWorkUnits conservatively models the instruction count
// produced after counted repetitions are simplified. It uses saturating
// arithmetic and stops at limit+1, so nested repetitions cannot overflow or
// make the estimator itself perform unbounded work.
func boundedRE2ProgramWorkUnits(expression *syntax.Regexp, limit int) (int, bool) {
	if expression == nil || limit < 1 {
		return 0, true
	}
	body := boundedRE2NodeWorkUnits(expression, limit)
	total := boundedAddWorkUnits(body, 2, limit) // implicit fail and match
	return total, total > limit
}

func boundedRE2NodeWorkUnits(expression *syntax.Regexp, limit int) int {
	if expression == nil {
		return limit + 1
	}
	switch expression.Op {
	case syntax.OpNoMatch:
		return 0
	case syntax.OpEmptyMatch:
		return 1
	case syntax.OpLiteral:
		if len(expression.Rune) == 0 {
			return 1
		}
		return min(len(expression.Rune), limit+1)
	case syntax.OpCharClass, syntax.OpAnyCharNotNL, syntax.OpAnyChar,
		syntax.OpBeginLine, syntax.OpEndLine, syntax.OpBeginText,
		syntax.OpEndText, syntax.OpWordBoundary, syntax.OpNoWordBoundary:
		return 1
	case syntax.OpCapture:
		return boundedAddWorkUnits(
			boundedRE2NodeWorkUnits(expression.Sub[0], limit),
			2,
			limit,
		)
	case syntax.OpStar:
		// Nullable operands use (operand+)?, which needs two alternations.
		return boundedAddWorkUnits(
			boundedRE2NodeWorkUnits(expression.Sub[0], limit),
			2,
			limit,
		)
	case syntax.OpPlus, syntax.OpQuest:
		return boundedAddWorkUnits(
			boundedRE2NodeWorkUnits(expression.Sub[0], limit),
			1,
			limit,
		)
	case syntax.OpConcat:
		total := 0
		for _, child := range expression.Sub {
			total = boundedAddWorkUnits(
				total,
				boundedRE2NodeWorkUnits(child, limit),
				limit,
			)
		}
		return total
	case syntax.OpAlternate:
		total := 0
		for index, child := range expression.Sub {
			total = boundedAddWorkUnits(
				total,
				boundedRE2NodeWorkUnits(child, limit),
				limit,
			)
			if index > 0 {
				total = boundedAddWorkUnits(total, 1, limit)
			}
		}
		return total
	case syntax.OpRepeat:
		return boundedRE2RepeatWorkUnits(expression, limit)
	default:
		// Reject new syntax operations until their expansion is classified.
		return limit + 1
	}
}

func boundedRE2RepeatWorkUnits(expression *syntax.Regexp, limit int) int {
	if expression.Min == 0 && expression.Max == 0 {
		return 1
	}
	child := boundedRE2NodeWorkUnits(expression.Sub[0], limit)
	if expression.Max == -1 {
		switch expression.Min {
		case 0:
			return boundedAddWorkUnits(child, 2, limit)
		case 1:
			return boundedAddWorkUnits(child, 1, limit)
		default:
			return boundedAddWorkUnits(
				boundedMultiplyWorkUnits(child, expression.Min, limit),
				1,
				limit,
			)
		}
	}
	total := boundedMultiplyWorkUnits(child, expression.Max, limit)
	return boundedAddWorkUnits(total, expression.Max-expression.Min, limit)
}

func boundedAddWorkUnits(left, right, limit int) int {
	if left > limit || right > limit || right > limit-left {
		return limit + 1
	}
	return left + right
}

func boundedMultiplyWorkUnits(value, count, limit int) int {
	if value < 0 || count < 0 || value > limit ||
		count != 0 && value > limit/count {
		return limit + 1
	}
	return value * count
}
