package pgtest

import (
	"regexp"
	"strings"
)

var betweenCheck = regexp.MustCompile(`(?i)([a-z_][a-z_0-9]*(?:\([^()]*\))?)\s+BETWEEN\s+([0-9]+)\s+AND\s+([0-9]+)`)
var checkCasts = regexp.MustCompile(`::(?:text|bigint|integer|smallint|boolean|bytea)`)
var checkWhitespace = regexp.MustCompile(`\s+`)

// ContainsCheck compares simple constraint expressions across PostgreSQL's
// deparser formatting (casts, parentheses and expanded BETWEEN expressions).
// It inspects the actual server constraint definition, never a source copy.
func ContainsCheck(definitions, expression string) bool {
	normalize := func(value string) string {
		value = betweenCheck.ReplaceAllString(value, "$1 >= $2 AND $1 <= $3")
		value = checkCasts.ReplaceAllString(value, "")
		value = strings.Map(func(r rune) rune {
			if r == '(' || r == ')' || r == '"' {
				return -1
			}
			return r
		}, value)
		return checkWhitespace.ReplaceAllString(strings.ToLower(strings.TrimSpace(value)), "")
	}
	return strings.Contains(normalize(definitions), normalize(expression))
}
