package common_util

// TruncateRunes returns s cut to at most limit runes, never splitting a multi-byte character. A
// non-positive limit returns the empty string.
func TruncateRunes(s string, limit int) string {
	if limit <= 0 {
		return ""
	}
	count := 0
	for index := range s {
		if count == limit {
			return s[:index]
		}
		count++
	}
	return s
}
