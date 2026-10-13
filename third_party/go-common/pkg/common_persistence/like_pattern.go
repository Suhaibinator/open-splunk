package common_persistence

import "strings"

// EscapeLikePattern escapes the LIKE/ILIKE metacharacters (\, %, _) in s so that
// a free-text query is matched literally rather than interpreted as a wildcard
// pattern. Callers wrap the result in their own % anchors, e.g.:
//
//	pattern := "%" + EscapeLikePattern(query) + "%"
//	db.Where("name ILIKE ?", pattern)
func EscapeLikePattern(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(s)
}
