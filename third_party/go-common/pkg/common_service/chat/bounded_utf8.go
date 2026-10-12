package chat

import (
	"strings"
	"unicode/utf8"
)

// boundedUTF8 normalizes malformed input before enforcing a byte ceiling without splitting a rune.
func boundedUTF8(s string, limit int) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	if len(s) <= limit {
		return s
	}
	for limit > 0 && !utf8.RuneStart(s[limit]) {
		limit--
	}
	return s[:limit]
}
