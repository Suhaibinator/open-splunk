package spl

import "strings"

// IsFieldsFieldGlob reports whether pattern is one valid fields-command
// wildcard selector. SPL fields selectors use '*' as their only
// metacharacter, with every other byte retaining exact field-name meaning.
func IsFieldsFieldGlob(pattern string) bool {
	return IsStatsFieldGlob(pattern)
}

// MatchFieldsFieldGlob reports whether one exact field name matches a
// fields-command wildcard selector.
func MatchFieldsFieldGlob(pattern, name string) bool {
	// The fields command treats leading-underscore fields as internal. Splunk
	// requires an authored wildcard to target that namespace explicitly, so a
	// broad `*` does not remove _time/_raw while `_*` does.
	if strings.HasPrefix(name, "_") && !strings.HasPrefix(pattern, "_") {
		return false
	}
	return IsFieldsFieldGlob(pattern) && MatchStatsFieldGlob(pattern, name)
}
