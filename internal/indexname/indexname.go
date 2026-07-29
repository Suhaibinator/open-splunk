// Package indexname owns the dependency-neutral canonical grammar shared by
// control-plane index definitions and physical ClickHouse deletion scopes.
package indexname

import "strings"

// MaximumBytes is the canonical Splunk-compatible index-name bound.
const MaximumBytes = 255

// ValidCanonical reports whether value is already a canonical user index
// name. It does not trim or lowercase input.
func ValidCanonical(value string) bool {
	return ValidSyntax(value) && !ContainsReservedWord(value)
}

// ValidSyntax reports whether value satisfies the canonical byte and
// character grammar without applying the reserved-word rule.
func ValidSyntax(value string) bool {
	if len(value) < 1 || len(value) > MaximumBytes {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if index == 0 && !asciiLetterOrNumber(character) {
			return false
		}
		if !asciiLetterOrNumber(character) &&
			character != '_' &&
			character != '-' {
			return false
		}
	}
	return true
}

// ContainsReservedWord reports whether value contains Splunk's reserved
// kvstore substring.
func ContainsReservedWord(value string) bool {
	return strings.Contains(value, "kvstore")
}

func asciiLetterOrNumber(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= '0' && character <= '9'
}
