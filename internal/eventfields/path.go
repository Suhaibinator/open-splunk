package eventfields

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// NormalizeDynamicPath joins logical field-name segments into the canonical
// dotted metadata spelling. Literal backslashes and dots remain injective.
func NormalizeDynamicPath(segments []string) string {
	totalBytes := 0
	for _, segment := range segments {
		totalBytes = NormalizedDynamicPathBytes(totalBytes, segment)
	}
	var normalized strings.Builder
	normalized.Grow(totalBytes)
	for index, segment := range segments {
		if index != 0 {
			normalized.WriteByte('.')
		}
		appendNormalizedDynamicPathSegment(&normalized, segment)
	}
	return normalized.String()
}

// NormalizedDynamicPathBytes returns the encoded byte length after appending
// one logical segment to a canonical prefix of prefixBytes. It shares the
// encoder's escaping rules so ingestion can enforce aggregate metadata limits
// without materializing every flattened name.
func NormalizedDynamicPathBytes(prefixBytes int, segment string) int {
	if prefixBytes != 0 {
		prefixBytes++
	}
	return prefixBytes + appendNormalizedDynamicPathSegment(nil, segment)
}

func appendNormalizedDynamicPathSegment(destination *strings.Builder, segment string) int {
	length := 0
	for index := 0; index < len(segment); index++ {
		value := segment[index]
		if value == '\\' || value == '.' {
			if destination != nil {
				destination.WriteByte('\\')
			}
			length++
		}
		if destination != nil {
			destination.WriteByte(value)
		}
		length++
	}
	return length
}

// ParseNormalizedDynamicPath reverses NormalizeDynamicPath and rejects
// noncanonical or empty metadata paths.
func ParseNormalizedDynamicPath(path string) ([]string, error) {
	return parseNormalizedPath(path, MaximumDynamicPathSegments)
}

// ParseNormalizedSearchFieldPath parses the canonical logical field spelling
// accepted by SPL. Ingestion permits a leaf below sixteen nested objects, so a
// search field can contain one root plus sixteen child segments. This leaves
// the storage-oriented ParseNormalizedDynamicPath ceiling unchanged.
func ParseNormalizedSearchFieldPath(path string) ([]string, error) {
	return parseNormalizedPath(path, MaximumDynamicPathSegments+1)
}

func parseNormalizedPath(path string, maximumSegments int) ([]string, error) {
	if path == "" {
		return nil, errors.New("dynamic field path is empty")
	}
	if len(path) > MaximumNormalizedFieldNameBytes || !utf8.ValidString(path) {
		return nil, errors.New("dynamic field path has invalid encoding or length")
	}
	segments := make([]string, 0, maximumSegments)
	var segment strings.Builder
	escaped := false
	for _, character := range path {
		if escaped {
			if character != '\\' && character != '.' {
				return nil, fmt.Errorf("dynamic field path has invalid escape %q", character)
			}
			segment.WriteRune(character)
			escaped = false
			continue
		}
		switch character {
		case '\\':
			escaped = true
		case '.':
			if segment.Len() == 0 {
				return nil, errors.New("dynamic field path contains an empty segment")
			}
			if len(segments) >= maximumSegments {
				return nil, errors.New("dynamic field path is too deep")
			}
			if err := validateDynamicPathSegment(segment.String()); err != nil {
				return nil, err
			}
			segments = append(segments, segment.String())
			segment.Reset()
		default:
			segment.WriteRune(character)
		}
	}
	if escaped {
		return nil, errors.New("dynamic field path ends with an escape")
	}
	if segment.Len() == 0 {
		return nil, errors.New("dynamic field path contains an empty segment")
	}
	if len(segments) >= maximumSegments {
		return nil, errors.New("dynamic field path is too deep")
	}
	if err := validateDynamicPathSegment(segment.String()); err != nil {
		return nil, err
	}
	segments = append(segments, segment.String())
	if NormalizeDynamicPath(segments) != path {
		return nil, errors.New("dynamic field path is not canonical")
	}
	return segments, nil
}

func validateDynamicPathSegment(segment string) error {
	if len(segment) > MaximumDynamicPathSegmentBytes || !utf8.ValidString(segment) {
		return errors.New("dynamic field path segment has invalid encoding or length")
	}
	for _, character := range segment {
		if unicode.IsControl(character) {
			return errors.New("dynamic field path segment contains a control character")
		}
	}
	return nil
}

// EncodePhysicalPathSegment maps one logical JSON key to ClickHouse's
// json_type_escape_dots_in_keys representation.
func EncodePhysicalPathSegment(segment string) string {
	segment = strings.ReplaceAll(segment, "%", "%25")
	return strings.ReplaceAll(segment, ".", "%2E")
}

// DecodePhysicalPathSegment reverses EncodePhysicalPathSegment. Only the two
// escapes emitted by the encoder are accepted.
func DecodePhysicalPathSegment(segment string) (string, error) {
	if segment == "" {
		return "", errors.New("physical JSON path segment is empty")
	}
	var decoded strings.Builder
	for index := 0; index < len(segment); {
		if segment[index] != '%' {
			decoded.WriteByte(segment[index])
			index++
			continue
		}
		if index+2 >= len(segment) {
			return "", errors.New("physical JSON path has a truncated escape")
		}
		switch segment[index+1 : index+3] {
		case "25":
			decoded.WriteByte('%')
		case "2E":
			decoded.WriteByte('.')
		default:
			return "", errors.New("physical JSON path has an unknown escape")
		}
		index += 3
	}
	if decoded.Len() == 0 {
		return "", errors.New("physical JSON path decodes to an empty segment")
	}
	return decoded.String(), nil
}

// NormalizePhysicalDynamicPath converts a ClickHouse JSON ValuesByPath key
// into the canonical field_names spelling.
func NormalizePhysicalDynamicPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("physical JSON path is empty")
	}
	physical := strings.Split(path, ".")
	logical := make([]string, len(physical))
	for index, segment := range physical {
		decoded, err := DecodePhysicalPathSegment(segment)
		if err != nil {
			return "", fmt.Errorf("physical JSON path segment %d: %w", index, err)
		}
		logical[index] = decoded
	}
	normalized := NormalizeDynamicPath(logical)
	if _, err := ParseNormalizedDynamicPath(normalized); err != nil {
		return "", err
	}
	return normalized, nil
}

// ParseStoredFieldNames validates one event's complete field_names array and
// returns its decoded logical paths. The array is the authoritative per-row
// presence set for ClickHouse JSON results.
func ParseStoredFieldNames(names []string) ([][]string, error) {
	return parseStoredFieldNames(names, false)
}

// parseStoredFieldNames applies the durable flattened-path bounds shared by
// absolute event metadata and relative container metadata. Reserved roots are
// rejected only for absolute event fields: a nested object may legitimately
// contain keys such as index, tenant_id, or __os_private.
func parseStoredFieldNames(names []string, allowReservedRoots bool) ([][]string, error) {
	if len(names) > MaximumStoredFieldsPerEvent {
		return nil, errors.New("stored field names exceed the field-count limit")
	}
	paths := make([][]string, len(names))
	root := &storedPathNode{}
	totalBytes := 0
	previous := ""
	for index, name := range names {
		if len(name) > MaximumStoredFieldNamesBytes-totalBytes ||
			index > 0 && previous >= name {
			return nil, errors.New("stored field names are not bounded and strictly sorted")
		}
		totalBytes += len(name)
		segments, err := ParseNormalizedDynamicPath(name)
		if err != nil || !allowReservedRoots && IsReservedDynamicRoot(segments[0]) {
			return nil, errors.New("stored field name is invalid")
		}
		if err := root.insert(segments); err != nil {
			return nil, err
		}
		paths[index] = segments
		previous = name
	}
	return paths, nil
}

type storedPathNode struct {
	leaf     bool
	children map[string]*storedPathNode
}

func (node *storedPathNode) insert(segments []string) error {
	current := node
	for index, segment := range segments {
		if current.leaf {
			return errors.New("stored field name collides with an ancestor")
		}
		if current.children == nil {
			current.children = make(map[string]*storedPathNode)
		}
		next := current.children[segment]
		if next == nil {
			next = &storedPathNode{}
			current.children[segment] = next
		}
		current = next
		if index == len(segments)-1 {
			if current.leaf || len(current.children) != 0 {
				return errors.New("stored field name is duplicated or collides with a descendant")
			}
			current.leaf = true
		}
	}
	return nil
}
