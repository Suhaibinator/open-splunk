// Package knowledge defines protobuf-independent normalization and selector
// primitives for search-time knowledge objects.
package knowledge

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

const (
	// MaximumObjectNameBytes is the hard UTF-8 byte ceiling after ASCII trim.
	MaximumObjectNameBytes = 255
	// MaximumFieldDestinationBytes follows the authoritative dynamic-field path
	// ceiling. The destination is still independently parsed and root-checked by
	// eventfields.
	MaximumFieldDestinationBytes = 255
)

var (
	// ErrInvalidText identifies malformed, empty, non-UTF-8, or control-bearing
	// knowledge text.
	ErrInvalidText = errors.New("invalid knowledge text")
	// ErrInvalidFieldDestination identifies a noncanonical or reserved field
	// destination.
	ErrInvalidFieldDestination = errors.New("invalid knowledge field destination")
	// ErrResourceLimit identifies a definition that exceeds a hard compile-time
	// bound.
	ErrResourceLimit = errors.New("knowledge resource limit exceeded")
)

// Name is a normalized, binary-case-sensitive knowledge-object name. Its
// representation is immutable and comparable.
type Name struct {
	value string
}

// NormalizeName trims the pinned ASCII whitespace set, validates UTF-8, and
// rejects the pinned C0/C1 control set. It deliberately performs no Unicode
// normalization or case folding: names remain byte-for-byte case-sensitive.
func NormalizeName(source string) (Name, error) {
	value, err := normalizeBoundedText(source, MaximumObjectNameBytes, "object name")
	if err != nil {
		return Name{}, err
	}
	return Name{value: value}, nil
}

// String returns the canonical name.
func (name Name) String() string {
	return name.value
}

// FieldDestination is an immutable canonical dynamic-field path.
type FieldDestination struct {
	path string
}

// NormalizeFieldDestination normalizes outer ASCII whitespace, then delegates
// path grammar and segment validation to eventfields. Server-owned, canonical,
// and compiler-private roots are rejected through the same eventfields
// authority used by ingestion and storage.
func NormalizeFieldDestination(source string) (FieldDestination, error) {
	value, err := normalizeBoundedText(source, MaximumFieldDestinationBytes, "field destination")
	if err != nil {
		return FieldDestination{}, fmt.Errorf("%w: %w", ErrInvalidFieldDestination, err)
	}
	segments, err := eventfields.ParseNormalizedSearchFieldPath(value)
	if err != nil {
		return FieldDestination{}, fmt.Errorf("%w: %w", ErrInvalidFieldDestination, err)
	}
	if eventfields.IsReservedDynamicRoot(segments[0]) {
		return FieldDestination{}, fmt.Errorf("%w: reserved root", ErrInvalidFieldDestination)
	}
	return FieldDestination{path: value}, nil
}

// String returns the canonical normalized dynamic path.
func (destination FieldDestination) String() string {
	return destination.path
}

func normalizeBoundedText(source string, maximumBytes int, label string) (string, error) {
	value := TrimASCIIWhitespace(source)
	if value == "" {
		return "", fmt.Errorf("%w: %s is empty", ErrInvalidText, label)
	}
	if len(value) > maximumBytes {
		return "", fmt.Errorf("%w: %s exceeds %d bytes", ErrResourceLimit, label, maximumBytes)
	}
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("%w: %s is not UTF-8", ErrInvalidText, label)
	}
	for _, character := range value {
		if IsPinnedControl(character) {
			return "", fmt.Errorf("%w: %s contains a pinned C0/C1 control", ErrInvalidText, label)
		}
	}
	return value, nil
}

// TrimASCIIWhitespace is intentionally independent of Go's evolving Unicode
// tables. Its set is fixed to HT, LF, VT, FF, CR, and space.
func TrimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}

// IsPinnedControl reports whether value is a pinned C0 or C1 control.
func IsPinnedControl(value rune) bool {
	return value <= 0x1f || value >= 0x7f && value <= 0x9f
}

// ValidIdentity reports whether value is a bounded, UTF-8, untrimmed-free
// identity with no NUL and no pinned C0/C1 control.
func ValidIdentity(value string, maximumBytes int) bool {
	if value == "" || len(value) > maximumBytes || !utf8.ValidString(value) ||
		TrimASCIIWhitespace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if IsPinnedControl(character) {
			return false
		}
	}
	return true
}
