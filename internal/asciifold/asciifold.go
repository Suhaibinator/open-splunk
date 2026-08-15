// Package asciifold provides a substring matcher whose ASCII letters compare
// case-insensitively while every other byte, including every non-ASCII UTF-8
// continuation byte, compares exactly. That predicate matches SQLite lower()
// semantics without allocating a lower-cased copy of the scanned value.
package asciifold

// Matcher is a Knuth-Morris-Pratt substring matcher over ASCII-folded bytes.
// The pattern is stored pre-folded so scanning only folds the scanned value.
// The zero Matcher has an empty pattern and matches every value.
type Matcher struct {
	pattern []byte
	prefix  []int
}

// New builds a Matcher for pattern. The pattern length is unbounded; the
// failure table grows with it.
func New(pattern string) Matcher {
	matcher := Matcher{
		pattern: []byte(pattern),
		prefix:  make([]int, len(pattern)),
	}
	for index := range matcher.pattern {
		matcher.pattern[index] = Fold(matcher.pattern[index])
	}
	for index, matched := 1, 0; index < len(matcher.pattern); {
		if matcher.pattern[index] == matcher.pattern[matched] {
			matched++
			matcher.prefix[index] = matched
			index++
			continue
		}
		if matched > 0 {
			matched = matcher.prefix[matched-1]
			continue
		}
		index++
	}
	return matcher
}

// Contains reports whether value contains the pattern. A nil matcher and an
// empty pattern both match every value.
func (matcher *Matcher) Contains(value string) bool {
	if matcher == nil || len(matcher.pattern) == 0 {
		return true
	}
	if len(matcher.pattern) > len(value) {
		return false
	}
	matched := 0
	for index := 0; index < len(value); index++ {
		character := Fold(value[index])
		for matched > 0 && character != matcher.pattern[matched] {
			matched = matcher.prefix[matched-1]
		}
		if character != matcher.pattern[matched] {
			continue
		}
		matched++
		if matched == len(matcher.pattern) {
			return true
		}
	}
	return false
}

// ContainsFunc scans value like Contains while calling check every everyNBytes
// scanned bytes, and once more before reporting a non-match, so that long
// values stay cancelable. everyNBytes must be a power of two. The first error
// returned by check aborts the scan.
func (matcher *Matcher) ContainsFunc(value string, everyNBytes int, check func() error) (bool, error) {
	if matcher == nil || len(matcher.pattern) == 0 {
		return true, nil
	}
	matched := 0
	for index := 0; index < len(value); index++ {
		if index&(everyNBytes-1) == 0 {
			if err := check(); err != nil {
				return false, err
			}
		}
		character := Fold(value[index])
		for matched > 0 && character != matcher.pattern[matched] {
			matched = matcher.prefix[matched-1]
		}
		if character == matcher.pattern[matched] {
			matched++
			if matched == len(matcher.pattern) {
				return true, nil
			}
		}
	}
	if err := check(); err != nil {
		return false, err
	}
	return false, nil
}

// Fold lower-cases ASCII letters and leaves every other byte untouched.
func Fold(value byte) byte {
	if value >= 'A' && value <= 'Z' {
		return value + ('a' - 'A')
	}
	return value
}
