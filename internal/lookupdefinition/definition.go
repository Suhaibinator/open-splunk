// Package lookupdefinition validates and canonicalizes the logical metadata
// that binds one immutable CSV asset version to SPL event fields.
package lookupdefinition

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	MaximumNameBytes        = 255
	MaximumDescriptionBytes = 16 << 10
	MaximumKeyMappings      = spl.MaximumLookupKeys
	MaximumOutputMappings   = spl.MaximumLookupOutputs
)

var ErrInvalid = errors.New("invalid lookup definition")

type mappingKind uint8

const (
	mappingKindKey mappingKind = iota + 1
	mappingKindOutput
)

// Normalized is detached publication authority. Definition and Selector do
// not alias the submitted protobuf.
type Normalized struct {
	Definition *opensplunkv1.LookupDefinition
	Selector   *knowledge.Selector
}

// Normalize validates definition against the exact immutable asset header and
// returns one canonical detached representation.
func Normalize(input *opensplunkv1.LookupDefinition, columns []string) (Normalized, error) {
	if input == nil {
		return Normalized{}, invalid("definition", "is required")
	}
	if !ValidIdentity(input.GetAppId(), 128) {
		return Normalized{}, invalid("app_id", "must be a nonempty bounded canonical identifier")
	}
	if !IsValidLookupName(input.GetName()) {
		return Normalized{}, invalid("name", "must be an exact unquoted lookup name")
	}
	if input.Description != nil {
		if !utf8.ValidString(input.GetDescription()) || len(input.GetDescription()) > MaximumDescriptionBytes || hasPinnedControl(input.GetDescription()) {
			return Normalized{}, invalid("description", fmt.Sprintf("must be control-free UTF-8 within %d bytes", MaximumDescriptionBytes))
		}
	}
	if !validSharingScope(input.GetSharingScope()) {
		return Normalized{}, invalid("sharing_scope", "is unspecified or unknown")
	}
	if input.GetOverwriteBehavior() != opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING &&
		input.GetOverwriteBehavior() != opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING {
		return Normalized{}, invalid("overwrite_behavior", "is unspecified or unknown")
	}
	columnSet, err := exactColumns(columns)
	if err != nil {
		return Normalized{}, err
	}
	keys, err := normalizeMappings(mappingKindKey, "key_mappings", input.GetKeyMappings(), columnSet, 1, MaximumKeyMappings)
	if err != nil {
		return Normalized{}, err
	}
	outputs, err := normalizeMappings(mappingKindOutput, "output_mappings", input.GetOutputMappings(), columnSet, 1, MaximumOutputMappings)
	if err != nil {
		return Normalized{}, err
	}
	if err := validateCanonicallyAuthorable(input.GetName(), keys, outputs, input.GetOverwriteBehavior()); err != nil {
		return Normalized{}, err
	}
	selector, canonicalSelector, err := normalizeSelector(input.GetSelector())
	if err != nil {
		return Normalized{}, err
	}
	description := input.Description
	if description != nil {
		value := strings.Clone(*description)
		description = &value
	}
	return Normalized{
		Definition: &opensplunkv1.LookupDefinition{
			AppId:             strings.Clone(input.GetAppId()),
			Name:              strings.Clone(input.GetName()),
			Description:       description,
			SharingScope:      input.GetSharingScope(),
			Selector:          canonicalSelector,
			Automatic:         input.GetAutomatic(),
			KeyMappings:       keys,
			OutputMappings:    outputs,
			OverwriteBehavior: input.GetOverwriteBehavior(),
		},
		Selector: selector,
	}, nil
}

// KeyColumns projects the ordered lookup-side key field names.
func KeyColumns(definition *opensplunkv1.LookupDefinition) []string {
	columns := make([]string, len(definition.GetKeyMappings()))
	for index, mapping := range definition.GetKeyMappings() {
		columns[index] = mapping.GetLookupField()
	}
	return columns
}

func normalizeMappings(kind mappingKind, path string, input []*opensplunkv1.LookupFieldMapping, columns map[string]struct{}, minimum, maximum int) ([]*opensplunkv1.LookupFieldMapping, error) {
	if kind != mappingKindKey && kind != mappingKindOutput {
		return nil, invalid(path, "has an unknown mapping kind")
	}
	if len(input) < minimum || len(input) > maximum {
		return nil, invalid(path, fmt.Sprintf("must contain between %d and %d mappings", minimum, maximum))
	}
	lookupFields := make(map[string]struct{}, len(input))
	eventFields := make(map[string]struct{}, len(input))
	result := make([]*opensplunkv1.LookupFieldMapping, 0, len(input))
	keyMapping := kind == mappingKindKey
	for index, mapping := range input {
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		if mapping == nil {
			return nil, invalid(itemPath, "is required")
		}
		if !spl.IsExactUnquotedFieldName(mapping.GetLookupField()) ||
			strings.HasPrefix(strings.ToLower(mapping.GetLookupField()), "__os_") ||
			hasPinnedControl(mapping.GetLookupField()) {
			return nil, invalid(itemPath+".lookup_field", "must be an exact unquoted public lookup column")
		}
		if keyMapping && isLookupOutputMarker(mapping.GetLookupField()) {
			return nil, invalid(itemPath+".lookup_field", "collides with the authored OUTPUT or OUTPUTNEW marker")
		}
		if _, exists := columns[mapping.GetLookupField()]; !exists {
			return nil, invalid(itemPath+".lookup_field", "does not name an exact asset column")
		}
		if _, duplicate := lookupFields[mapping.GetLookupField()]; duplicate {
			return nil, invalid(itemPath+".lookup_field", "duplicates a prior lookup column")
		}
		if !validEventField(mapping.GetEventField()) {
			return nil, invalid(itemPath+".event_field", "must be an exact unquoted public event field")
		}
		if keyMapping && isLookupOutputMarker(mapping.GetEventField()) {
			return nil, invalid(itemPath+".event_field", "collides with the authored OUTPUT or OUTPUTNEW marker")
		}
		if _, duplicate := eventFields[mapping.GetEventField()]; duplicate {
			return nil, invalid(itemPath+".event_field", "duplicates a prior event field")
		}
		lookupFields[mapping.GetLookupField()] = struct{}{}
		eventFields[mapping.GetEventField()] = struct{}{}
		result = append(result, &opensplunkv1.LookupFieldMapping{
			LookupField: strings.Clone(mapping.GetLookupField()),
			EventField:  strings.Clone(mapping.GetEventField()),
		})
	}
	return result, nil
}

// canonicalAuthoredLookupSource uses AS for every output, including identity
// mappings. That closed spelling stays unambiguous even when a lookup column
// itself is named AS, while OUTPUT and OUTPUTNEW remain valid output columns
// after the explicit mode marker.
func canonicalAuthoredLookupSource(
	name string,
	keys, outputs []*opensplunkv1.LookupFieldMapping,
	overwrite opensplunkv1.KnowledgeOverwriteBehavior,
) string {
	var source strings.Builder
	source.WriteString("* | lookup ")
	source.WriteString(name)
	for _, mapping := range keys {
		source.WriteByte(' ')
		source.WriteString(mapping.GetLookupField())
		source.WriteString(" AS ")
		source.WriteString(mapping.GetEventField())
	}
	if overwrite == opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING {
		source.WriteString(" OUTPUTNEW")
	} else {
		source.WriteString(" OUTPUT")
	}
	for _, mapping := range outputs {
		source.WriteByte(' ')
		source.WriteString(mapping.GetLookupField())
		source.WriteString(" AS ")
		source.WriteString(mapping.GetEventField())
	}
	return source.String()
}

func validateCanonicallyAuthorable(
	name string,
	keys, outputs []*opensplunkv1.LookupFieldMapping,
	overwrite opensplunkv1.KnowledgeOverwriteBehavior,
) error {
	source := canonicalAuthoredLookupSource(name, keys, outputs, overwrite)
	if len(source) > spl.MaximumSearchSourceBytes {
		return invalid("mappings", fmt.Sprintf(
			"canonical authored lookup exceeds %d UTF-8 bytes",
			spl.MaximumSearchSourceBytes,
		))
	}
	if _, err := spl.Parse(source); err != nil {
		return invalid("mappings", "cannot be represented by the authored lookup grammar")
	}
	return nil
}

func exactColumns(columns []string) (map[string]struct{}, error) {
	if len(columns) == 0 || len(columns) > 64 {
		return nil, invalid("columns", "asset header is empty or exceeds 64 columns")
	}
	result := make(map[string]struct{}, len(columns))
	for index, column := range columns {
		if column == "" || !utf8.ValidString(column) {
			return nil, invalid(fmt.Sprintf("columns[%d]", index), "is not a nonempty UTF-8 header")
		}
		if _, duplicate := result[column]; duplicate {
			return nil, invalid(fmt.Sprintf("columns[%d]", index), "duplicates a prior header")
		}
		result[column] = struct{}{}
	}
	return result, nil
}

// IsValidLookupName reports whether value is the exact public name accepted by
// both lookup publication and authored SPL resolution. Keeping this exported
// predicate at the logical-definition boundary prevents the resolver from
// silently narrowing the parser's name language.
func IsValidLookupName(value string) bool {
	return len(value) <= MaximumNameBytes && spl.IsExactUnquotedFieldName(value) &&
		!strings.EqualFold(value, "fields") &&
		!strings.HasPrefix(strings.ToLower(value), "__os_") &&
		!hasPinnedControl(value)
}

func validEventField(value string) bool {
	if !spl.IsExactUnquotedFieldName(value) || strings.EqualFold(value, "fields") ||
		strings.HasPrefix(strings.ToLower(value), "__os_") || hasPinnedControl(value) {
		return false
	}
	_, err := eventfields.ParseNormalizedSearchFieldPath(value)
	return err == nil
}

func isLookupOutputMarker(value string) bool {
	return strings.EqualFold(value, "OUTPUT") || strings.EqualFold(value, "OUTPUTNEW")
}

// ValidIdentity reports whether value is a bounded dotted lookup-family
// identifier: UTF-8, nonempty, at most maximum bytes, alphanumeric first byte,
// and thereafter alphanumeric or one of . _ : -
func ValidIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		alphaNumeric := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
		if !alphaNumeric && (index == 0 || character != '.' && character != '_' && character != ':' && character != '-') {
			return false
		}
	}
	return true
}

func validSharingScope(value opensplunkv1.SharingScope) bool {
	return value == opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE || value == opensplunkv1.SharingScope_SHARING_SCOPE_APP || value == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
}

func hasPinnedControl(value string) bool {
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f || unicode.Is(unicode.Cf, character) {
			return true
		}
	}
	return false
}

func normalizeSelector(input *opensplunkv1.KnowledgeSelector) (*knowledge.Selector, *opensplunkv1.KnowledgeSelector, error) {
	dimensions := []struct {
		dimension knowledge.Dimension
		patterns  []*opensplunkv1.KnowledgeSelectorPattern
	}{
		{knowledge.DimensionIndex, nil},
		{knowledge.DimensionHost, nil},
		{knowledge.DimensionSource, nil},
		{knowledge.DimensionSourcetype, nil},
	}
	if input != nil {
		dimensions[0].patterns = input.GetIndexPatterns()
		dimensions[1].patterns = input.GetHostPatterns()
		dimensions[2].patterns = input.GetSourcePatterns()
		dimensions[3].patterns = input.GetSourcetypePatterns()
	}
	spec := knowledge.SelectorSpec{}
	for _, dimension := range dimensions {
		if len(dimension.patterns) == 0 {
			continue
		}
		values := make([]string, 0, len(dimension.patterns))
		for patternIndex, pattern := range dimension.patterns {
			if pattern == nil {
				return nil, nil, invalid("selector", "contains a nil pattern")
			}
			normalized, normalizeErr := knowledge.NormalizePattern(pattern.GetValue())
			if normalizeErr != nil {
				return nil, nil, invalid(
					fmt.Sprintf("selector.patterns[%d].value", patternIndex),
					"is invalid",
				)
			}
			expectedKind := opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
			if normalized.IsLiteral() {
				expectedKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
			}
			if pattern.GetMatchKind() != opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED &&
				pattern.GetMatchKind() != expectedKind {
				return nil, nil, invalid(
					fmt.Sprintf("selector.patterns[%d].match_kind", patternIndex),
					"disagrees with normalized value",
				)
			}
			values = append(values, pattern.GetValue())
		}
		spec.Dimensions = append(spec.Dimensions, knowledge.DimensionSpec{Dimension: dimension.dimension, Patterns: values})
	}
	compiled, err := knowledge.CompileSelector(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: selector: %w", ErrInvalid, err)
	}
	canonical := &opensplunkv1.KnowledgeSelector{}
	for _, dimension := range dimensions {
		values := compiled.Patterns(dimension.dimension)
		patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, 0, len(values))
		for _, value := range values {
			normalized, normalizeErr := knowledge.NormalizePattern(value)
			if normalizeErr != nil {
				return nil, nil, fmt.Errorf("%w: selector canonicalization", ErrInvalid)
			}
			kind := opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
			if normalized.IsLiteral() {
				kind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
			}
			patterns = append(patterns, &opensplunkv1.KnowledgeSelectorPattern{MatchKind: kind, Value: value})
		}
		switch dimension.dimension {
		case knowledge.DimensionIndex:
			canonical.IndexPatterns = patterns
		case knowledge.DimensionHost:
			canonical.HostPatterns = patterns
		case knowledge.DimensionSource:
			canonical.SourcePatterns = patterns
		case knowledge.DimensionSourcetype:
			canonical.SourcetypePatterns = patterns
		}
	}
	if compiled.Stats().Patterns == 0 {
		canonical = nil
	}
	return compiled, canonical, nil
}

func invalid(path, message string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalid, path, message)
}
