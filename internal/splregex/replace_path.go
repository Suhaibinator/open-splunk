package splregex

import (
	"errors"
	"fmt"
	"regexp/syntax"
	"strings"
)

const (
	// MaximumReplacePathPatternBytes bounds authored and lowered path patterns.
	MaximumReplacePathPatternBytes = 4 << 10
	// MaximumReplacePathProgramWorkUnits bounds one expanded path program.
	MaximumReplacePathProgramWorkUnits = 4 << 10
	// MaximumReplacePathQueryProgramWorkUnits bounds all path replacements in a query.
	MaximumReplacePathQueryProgramWorkUnits = 16 << 10
)

var (
	ErrUnsupportedReplacePCRE = errors.New("replace PCRE construct is outside the supported path-boundary subset")
	ErrReplacePathTooComplex  = errors.New("replace path pattern exceeds a resource limit")
)

// ReplacePattern describes either the existing RE2 path or a whole-segment
// program. PathSegment programs retain the author's capture numbering.
type ReplacePattern struct {
	Pattern          string
	PathSegment      bool
	ProgramWorkUnits int
}

// CompileReplacePattern supports ordinary consuming RE2 patterns and the exact
// /BODY(?=/|$) subset. BODY cannot consume a slash or newline or inspect context.
func CompileReplacePattern(pattern string) (ReplacePattern, error) {
	// Classify without building a syntax tree so new PCRE paths are byte-bounded
	// before parsing, including compiler callers with manually constructed plans.
	pcre := hasReplacePCRE(pattern)
	if pcre && len(pattern) > MaximumReplacePathPatternBytes {
		return ReplacePattern{}, ErrReplacePathTooComplex
	}
	parsed, err := syntax.Parse(pattern, syntax.Perl)
	if err == nil {
		if mayMatchEmptySubstring(parsed) {
			return ReplacePattern{}, ErrMayMatchEmpty
		}
		return ReplacePattern{Pattern: pattern}, nil
	}
	if !pcre {
		return ReplacePattern{}, fmt.Errorf("invalid RE2 regular expression: %w", err)
	}
	const suffix = "(?=/|$)"
	if !strings.HasPrefix(pattern, "/") || !strings.HasSuffix(pattern, suffix) {
		return ReplacePattern{}, ErrUnsupportedReplacePCRE
	}
	body := pattern[1 : len(pattern)-len(suffix)]
	// Reserve the strict anchors and slash (five normalized bytes and three
	// program instructions) before inspecting the bounded segment body.
	compiled, err := compileBoundedRE2Pattern(body, MaximumReplacePathPatternBytes-5, MaximumReplacePathProgramWorkUnits-3)
	if err != nil {
		if errors.Is(err, errBoundedRE2PatternTooComplex) {
			return ReplacePattern{}, ErrReplacePathTooComplex
		}
		if hasReplacePCRE(body) {
			return ReplacePattern{}, ErrUnsupportedReplacePCRE
		}
		return ReplacePattern{}, fmt.Errorf("invalid replace path body: %w", err)
	}
	if !replacePathBodySyntax(body) {
		return ReplacePattern{}, ErrUnsupportedReplacePCRE
	}
	if mayMatchEmptySubstring(compiled.parsed) || !replacePathBodySafe(compiled.parsed) {
		return ReplacePattern{}, ErrUnsupportedReplacePCRE
	}
	// No capture is introduced, and strict anchors prevent partial segments.
	lowered, err := compileBoundedRE2Pattern(`\A/(?:`+body+`)\z`, MaximumReplacePathPatternBytes, MaximumReplacePathProgramWorkUnits)
	if err != nil {
		return ReplacePattern{}, fmt.Errorf("%w: %w", ErrReplacePathTooComplex, err)
	}
	return ReplacePattern{Pattern: lowered.normalized, PathSegment: true, ProgramWorkUnits: lowered.programWorkUnits}, nil
}

// ReplacePatternDiagnostic separates compatibility debt, empty matches, invalid
// syntax, and bounded-program admission failures for parser and compiler callers.
func ReplacePatternDiagnostic(err error) (string, string) {
	switch {
	case errors.Is(err, ErrReplacePathTooComplex):
		return "SPL_QUERY_TOO_COMPLEX", "replace path regular expression exceeds the pattern or program resource limit"
	case errors.Is(err, ErrUnsupportedReplacePCRE):
		return "SPL_UNSUPPORTED_PCRE", "replace supports consuming RE2 patterns and /BODY(?=/|$) path boundaries; this PCRE construct remains unsupported"
	case errors.Is(err, ErrMayMatchEmpty):
		return "SPL_UNSUPPORTED_REGEX", "replace regular expression may match an empty substring"
	default:
		return "SPL_UNSUPPORTED_REGEX", "replace regular expression has invalid syntax"
	}
}

// Scan only syntax outside escaped literals and character classes after the
// body parser validates class syntax and repetition bounds. A top-level
// alternative would change the scope of the leading slash or trailing assertion.
func replacePathBodySyntax(body string) bool {
	depth, quoted := 0, false
	for i := 0; i < len(body); i++ {
		c := body[i]
		if quoted {
			if c == '\\' && i+1 < len(body) && body[i+1] == 'E' {
				quoted = false
				i++
			}
			continue
		}
		if c == '\\' && i+1 < len(body) {
			if body[i+1] == 'Q' {
				quoted = true
			}
			i++
			continue
		}
		switch c {
		case '[':
			i = replaceCharacterClassEnd(body, i)
		case '(':
			if i+1 < len(body) && body[i+1] == '?' &&
				!strings.HasPrefix(body[i:], "(?:") &&
				!strings.HasPrefix(body[i:], "(?P<") &&
				!strings.HasPrefix(body[i:], "(?<") {
				return false
			}
			depth++
		case ')':
			depth--
		case '|':
			if depth == 0 {
				return false
			}
		}
		if depth < 0 {
			return false
		}
	}
	return depth == 0
}

func replacePathBodySafe(expression *syntax.Regexp) bool {
	switch expression.Op {
	case syntax.OpLiteral:
		for _, r := range expression.Rune {
			if r == '/' || r == '\n' {
				return false
			}
		}
	case syntax.OpCharClass:
		for i := 0; i < len(expression.Rune); i += 2 {
			for _, r := range []rune{'/', '\n'} {
				if expression.Rune[i] <= r && r <= expression.Rune[i+1] {
					return false
				}
			}
		}
	case syntax.OpCapture, syntax.OpConcat, syntax.OpAlternate, syntax.OpQuest, syntax.OpStar, syntax.OpPlus, syntax.OpRepeat, syntax.OpEmptyMatch, syntax.OpNoMatch:
	default:
		return false
	}
	for _, child := range expression.Sub {
		if !replacePathBodySafe(child) {
			return false
		}
	}
	return true
}

func hasReplacePCRE(pattern string) bool {
	quoted, groupStart := false, false
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		previousGroupStart := groupStart
		groupStart = false
		if quoted {
			if c == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i++
			}
			continue
		}
		if c == '\\' && i+1 < len(pattern) {
			next := pattern[i+1]
			if next >= '1' && next <= '9' || next == 'g' || next == 'k' || next == 'K' {
				return true
			}
			// Braced hexadecimal escapes are literals, not counted repetitions.
			if next == 'x' && i+2 < len(pattern) && pattern[i+2] == '{' {
				if end := strings.IndexByte(pattern[i+3:], '}'); end >= 0 {
					i += end + 3
					continue
				}
				return false
			}
			if next == 'Q' {
				quoted = true
			}
			i++
			continue
		}
		if c == '[' {
			i = replaceCharacterClassEnd(pattern, i)
			continue
		}
		if i+1 < len(pattern) && pattern[i+1] == '+' &&
			(c == '*' || c == '+' || c == '?' && !previousGroupStart) {
			return true
		}
		if c == '{' && replacePossessiveCount(pattern[i+1:]) {
			return true
		}
		if c == '(' {
			groupStart = true
			for _, prefix := range []string{"(?=", "(?!", "(?<=", "(?<!", "(?>", "(?(", "(?R", "(?P=", "(*"} {
				if strings.HasPrefix(pattern[i:], prefix) {
					return true
				}
			}
		}
	}
	return false
}

// replaceCharacterClassEnd skips an RE2 character class, including its optional
// leading negation, leading literal closing bracket, escapes, and POSIX classes.
// Invalid or unterminated classes are diagnosed by the RE2 parser.
func replaceCharacterClassEnd(pattern string, start int) int {
	i := start + 1
	if i < len(pattern) && pattern[i] == '^' {
		i++
	}
	if i < len(pattern) && pattern[i] == ']' {
		i++
	}
	for ; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			i++
		case '[':
			if strings.HasPrefix(pattern[i:], "[:") {
				end := strings.Index(pattern[i+2:], ":]")
				if end < 0 {
					// An unterminated POSIX class is invalid. Stop rather than scanning
					// the same suffix again at every subsequent "[:" in malformed input.
					return len(pattern) - 1
				}
				i += end + 3
			}
		case ']':
			return i
		}
	}
	return len(pattern) - 1
}

// replacePossessiveCount recognizes the counted quantifier tail after an opening
// brace; ordinary literal braces and malformed counts are not PCRE constructs.
func replacePossessiveCount(tail string) bool {
	i := 0
	for i < len(tail) && tail[i] >= '0' && tail[i] <= '9' {
		i++
	}
	if i == 0 {
		return false
	}
	if i < len(tail) && tail[i] == ',' {
		i++
		for i < len(tail) && tail[i] >= '0' && tail[i] <= '9' {
			i++
		}
	}
	return strings.HasPrefix(tail[i:], "}+")
}
