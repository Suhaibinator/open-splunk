package parserconfig

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

type patternPart struct {
	name      string
	delimiter literal
}

type literal struct {
	text       string
	whitespace bool
	normalized string
	failure    []int
}

func compilePattern(pattern string) ([]patternPart, bool, error) {
	if len(pattern) == 0 || len(pattern) > maxPatternBytes || !utf8.ValidString(pattern) {
		return nil, false, invalidPattern("empty, oversized, or invalid UTF-8")
	}
	for _, r := range pattern {
		if unicode.IsControl(r) && r != '\t' {
			return nil, false, invalidPattern("header literals must not contain control characters")
		}
	}
	parts := []patternPart{{}}
	seen := make(map[string]bool)
	var pending strings.Builder
	hasTimestamp := false
	for i := 0; i < len(pattern); {
		if pattern[i] != '%' {
			pending.WriteByte(pattern[i])
			i++
			continue
		}
		if i+1 < len(pattern) && pattern[i+1] == '%' {
			pending.WriteByte('%')
			i += 2
			continue
		}
		if i+1 >= len(pattern) || pattern[i+1] != '{' {
			return nil, false, invalidPattern("invalid percent escape")
		}
		end := strings.IndexByte(pattern[i+2:], '}')
		if end < 0 {
			return nil, false, invalidPattern("unterminated capture")
		}
		end += i + 2
		name := pattern[i+2 : end]
		if !validName(name) {
			return nil, false, invalidPattern("invalid capture name")
		}
		if eventfields.IsCollectorReservedRoot(name) && !semantic(name) {
			return nil, false, invalidPattern("reserved capture name")
		}
		if seen[name] {
			return nil, false, invalidPattern("duplicate capture")
		}
		seen[name] = true
		if len(seen) > maxCaptures {
			return nil, false, invalidPattern("too many captures")
		}
		if len(parts) > 1 && pending.Len() == 0 {
			return nil, false, invalidPattern("adjacent captures")
		}
		parts[len(parts)-1].delimiter = compileLiteral(pending.String())
		pending.Reset()
		parts = append(parts, patternPart{name: name})
		hasTimestamp = hasTimestamp || name == "timestamp"
		i = end + 1
	}
	if len(parts) < 2 || parts[len(parts)-1].name != "message" || pending.Len() != 0 {
		return nil, false, invalidPattern("terminal message capture is required")
	}
	return parts, hasTimestamp, nil
}

func compileLiteral(text string) literal {
	l := literal{text: text}
	if !strings.ContainsAny(text, " \t") {
		return l
	}
	if strings.Trim(text, " \t") == "" {
		l.whitespace = true
		return l
	}
	// KMP matches a virtual byte stream where each horizontal-whitespace run
	// becomes one space. Failure links prevent repeated literal prefixes from
	// causing quadratic work; no record-specific matcher scratch is needed.
	var normalized strings.Builder
	for i := 0; i < len(text); i++ {
		if horizontal(text[i]) {
			normalized.WriteByte(' ')
			for i+1 < len(text) && horizontal(text[i+1]) {
				i++
			}
		} else {
			normalized.WriteByte(text[i])
		}
	}
	l.normalized = normalized.String()
	l.failure = make([]int, len(l.normalized))
	for i, matched := 1, 0; i < len(l.normalized); i++ {
		for matched > 0 && l.normalized[i] != l.normalized[matched] {
			matched = l.failure[matched-1]
		}
		if l.normalized[i] == l.normalized[matched] {
			matched++
		}
		l.failure[i] = matched
	}
	return l
}

func horizontal(b byte) bool { return b == ' ' || b == '\t' }

func (l literal) find(value string) (int, int) {
	if l.whitespace {
		start := strings.IndexAny(value, " \t")
		if start < 0 {
			return -1, -1
		}
		end := start + 1
		for end < len(value) && horizontal(value[end]) {
			end++
		}
		return start, end
	}
	if l.normalized != "" {
		matched := 0
		for i := 0; i < len(value); i++ {
			next := value[i]
			if horizontal(next) {
				next = ' '
				for i+1 < len(value) && horizontal(value[i+1]) {
					i++
				}
			}
			for matched > 0 && next != l.normalized[matched] {
				matched = l.failure[matched-1]
			}
			if next == l.normalized[matched] {
				matched++
			}
			if matched == len(l.normalized) {
				end := i + 1
				start := end
				// Recover original byte coordinates once, after the first full match.
				for token := len(l.normalized) - 1; token >= 0; token-- {
					start--
					if l.normalized[token] == ' ' {
						for start > 0 && horizontal(value[start-1]) {
							start--
						}
					}
				}
				return start, end
			}
		}
		return -1, -1
	}
	start := strings.Index(value, l.text)
	if start < 0 {
		return -1, -1
	}
	return start, start + len(l.text)
}

// Capture applies an anchored, left-to-right delimiter program. Delimiter
// collisions choose the first complete delimiter; later captures never cause
// an earlier delimiter choice to be revisited.
func (c *Compiled) Capture(raw []byte) ([]Capture, error) {
	return c.CaptureInto(raw, nil)
}

// CaptureInto has Capture's semantics and reuses destination capacity when it
// fits. The result replaces destination's contents. Captured strings own their
// bytes independently of raw; no destination storage is retained by Compiled.
func (c *Compiled) CaptureInto(raw []byte, destination []Capture) ([]Capture, error) {
	if len(c.parts) == 0 {
		return nil, errors.New("parser has no capture pattern")
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("record is not valid UTF-8")
	}
	if len(raw) > maxEventBytes {
		return nil, errors.New("record exceeds parser byte limit")
	}
	text := string(raw)
	start, end := c.parts[0].delimiter.find(text)
	if start != 0 {
		return nil, errors.New("record does not match pattern prefix")
	}
	text = text[end:]
	captures := destination[:0]
	if cap(captures) < len(c.parts)-1 {
		captures = make([]Capture, 0, len(c.parts)-1)
	}
	for _, part := range c.parts[1:] {
		if part.name == "message" {
			captures = append(captures, Capture{Name: part.name, Value: text})
			return captures, nil
		}
		start, end = part.delimiter.find(text)
		if start < 0 {
			return nil, errors.New("record is missing pattern delimiter")
		}
		if strings.ContainsAny(text[:start], "\r\n") {
			return nil, errors.New("header capture contains a line break")
		}
		captures = append(captures, Capture{Name: part.name, Value: text[:start]})
		text = text[end:]
	}
	return nil, errors.New("parser is missing terminal message capture")
}
