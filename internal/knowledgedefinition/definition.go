// Package knowledgedefinition defines the protobuf-aware canonical storage
// boundary for knowledge-object definitions. Executable publication performs
// additional regex, JSON-path, expression, dependency, and aggregate-budget
// validation after this package has normalized the stored message.
package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// MaximumCanonicalBytes is the hard stored-definition ceiling shared with
	// the SQLite definition-blob schema and immutable snapshot contract.
	MaximumCanonicalBytes = 4 << 20
	// MaximumDescriptionBytes is the normalized optional-description ceiling.
	MaximumDescriptionBytes = 16 << 10
	// MaximumAppIDBytes matches the tenant-local binary app identity ceiling.
	MaximumAppIDBytes = 255
	// MaximumFieldExtractionOutputs is the per-regex named-output ceiling. The
	// publication compiler later proves that these names exactly match captures.
	MaximumFieldExtractionOutputs = 16
	// MaximumExpressionBytes is the per-calculated-field authored expression
	// ceiling. Publication additionally applies parser, nesting, and SQL bounds.
	MaximumExpressionBytes = 16 << 10
)

var (
	// ErrInvalidDefinition identifies a structurally invalid or non-normalizable
	// recognized definition.
	ErrInvalidDefinition = errors.New("invalid knowledge definition")
	// ErrUnknownFields identifies protobuf fields the current server cannot
	// safely interpret. Mutation paths reject these recursively.
	ErrUnknownFields = errors.New("knowledge definition contains unknown fields")
	// ErrDefinitionTooLarge identifies a hard definition storage bound.
	ErrDefinitionTooLarge = errors.New("knowledge definition exceeds a resource limit")
	// ErrDigestMismatch identifies stored bytes that do not match their immutable
	// SHA-256 authority.
	ErrDigestMismatch = errors.New("knowledge definition digest mismatch")
	// ErrNonCanonical identifies stored protobuf bytes that decode but are not
	// exactly the one deterministic normalized representation.
	ErrNonCanonical = errors.New("knowledge definition is not canonical")
	// ErrUnknownFutureBody identifies a stored definition whose top-level
	// unknown-field set does not contain exactly one canonical future body in
	// the compatibility-reserved field range. Only inactive administrative
	// reads may accept a successful future-body decode.
	ErrUnknownFutureBody = errors.New("knowledge definition has an unknown future body")
)

// Normalized is a detached canonical definition and its derived immutable
// storage/indexing authorities. Definition, Bytes, and Description do not
// alias caller-owned memory. Selector is immutable and race-safe.
type Normalized struct {
	Definition   *opensplunkv1.KnowledgeObjectDefinition
	Bytes        []byte
	Digest       [sha256.Size]byte
	ObjectType   opensplunkv1.KnowledgeObjectType
	AppID        string
	Name         string
	SharingScope opensplunkv1.SharingScope
	Description  *string
	Selector     *knowledge.Selector
}

// ForwardCompatible is a detached canonical definition whose body is a future
// oneof alternative unknown to this binary. Definition retains the exact
// top-level future body and metadata bytes so a Go response can preserve them.
// Bytes remains the immutable storage authority. This type deliberately has no
// ObjectType: the current binary cannot infer semantics from an unreadable body.
type ForwardCompatible struct {
	Definition   *opensplunkv1.KnowledgeObjectDefinition
	Bytes        []byte
	Digest       [sha256.Size]byte
	AppID        string
	Name         string
	SharingScope opensplunkv1.SharingScope
	Description  *string
	Selector     *knowledge.Selector
}

// Normalize rejects unknown protobuf data recursively, clones the input, and
// produces its one deterministic canonical storage representation. It does not
// decide whether a draft is executable; active publication must additionally
// compile and budget the body and dependency graph.
func Normalize(input *opensplunkv1.KnowledgeObjectDefinition) (Normalized, error) {
	if input == nil {
		return Normalized{}, invalid("definition", errors.New("is required"))
	}
	if err := preflightInput(input); err != nil {
		return Normalized{}, err
	}
	if err := rejectUnknownFields(input.ProtoReflect(), "definition"); err != nil {
		return Normalized{}, err
	}

	definition, ok := proto.Clone(input).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || definition == nil {
		return Normalized{}, invalid("definition", errors.New("could not be cloned"))
	}

	description, selector, err := normalizeMetadata(definition)
	if err != nil {
		return Normalized{}, err
	}

	objectType, err := normalizeBody(definition)
	if err != nil {
		return Normalized{}, err
	}

	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		return Normalized{}, fmt.Errorf("%w: deterministic marshal: %v", ErrInvalidDefinition, err)
	}
	if len(encoded) == 0 || len(encoded) > MaximumCanonicalBytes {
		return Normalized{}, fmt.Errorf(
			"%w: canonical bytes must be between 1 and %d",
			ErrDefinitionTooLarge,
			MaximumCanonicalBytes,
		)
	}
	digest := sha256.Sum256(encoded)

	return Normalized{
		Definition:   definition,
		Bytes:        bytes.Clone(encoded),
		Digest:       digest,
		ObjectType:   objectType,
		AppID:        definition.GetAppId(),
		Name:         definition.GetName(),
		SharingScope: definition.GetSharingScope(),
		Description:  cloneOptionalString(description),
		Selector:     selector,
	}, nil
}

// preflightInput rejects shapes that could otherwise amplify memory while
// cloning or recursively walking a caller-constructed message. The transport
// applies its own request bound, but this package also defends direct callers.
func preflightInput(input *opensplunkv1.KnowledgeObjectDefinition) error {
	if selector := input.GetSelector(); selector != nil {
		counts := [...]struct {
			path  string
			count int
		}{
			{path: "selector.index_patterns", count: len(selector.GetIndexPatterns())},
			{path: "selector.host_patterns", count: len(selector.GetHostPatterns())},
			{path: "selector.source_patterns", count: len(selector.GetSourcePatterns())},
			{path: "selector.sourcetype_patterns", count: len(selector.GetSourcetypePatterns())},
		}
		for _, candidate := range counts {
			if candidate.count > knowledge.MaximumSelectorPatternsPerDimension {
				return fmt.Errorf(
					"%w: %s exceeds %d entries",
					ErrDefinitionTooLarge,
					candidate.path,
					knowledge.MaximumSelectorPatternsPerDimension,
				)
			}
		}
	}
	if extraction := input.GetFieldExtraction(); extraction != nil && extraction.GetRegex() != nil &&
		len(extraction.GetRegex().GetOutputFields()) > MaximumFieldExtractionOutputs {
		return fmt.Errorf(
			"%w: field_extraction.regex.output_fields exceeds %d entries",
			ErrDefinitionTooLarge,
			MaximumFieldExtractionOutputs,
		)
	}
	if size := proto.Size(input); size > MaximumCanonicalBytes {
		return fmt.Errorf(
			"%w: submitted protobuf contains %d bytes, maximum is %d",
			ErrDefinitionTooLarge,
			size,
			MaximumCanonicalBytes,
		)
	}
	return nil
}

// DecodeCanonical verifies the exact digest before parsing, rejects malformed
// or recursively unknown protobuf data, re-normalizes it, and requires the raw
// bytes to already equal the deterministic normalized representation.
func DecodeCanonical(data, expectedDigest []byte) (Normalized, error) {
	if _, err := verifyStoredBytes(data, expectedDigest); err != nil {
		return Normalized{}, err
	}

	definition := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false, RecursionLimit: 32}).Unmarshal(data, definition); err != nil {
		return Normalized{}, fmt.Errorf("%w: malformed protobuf", ErrNonCanonical)
	}
	normalized, err := Normalize(definition)
	if err != nil {
		return Normalized{}, fmt.Errorf("%w: %v", ErrNonCanonical, err)
	}
	if !bytes.Equal(data, normalized.Bytes) {
		return Normalized{}, ErrNonCanonical
	}
	return normalized, nil
}

// DecodeCanonicalInactiveFutureBody verifies a stored digest and exact
// deterministic encoding, then decodes the known metadata around one
// unreadable future body. The immutable version's trusted state is mandatory:
// only draft, disabled, and deleted versions may take this path. Active,
// quarantined, unspecified, and unknown states fail closed before bytes are
// inspected. Active publication and resolution always use DecodeCanonical.
//
// The decoder rejects known bodies, nested unknown fields, a missing or
// ambiguous body, noncanonical outer wire data, and noncanonical known
// metadata. Canonical top-level future metadata fields are retained.
func DecodeCanonicalInactiveFutureBody(
	data, expectedDigest []byte,
	state opensplunkv1.KnowledgeObjectState,
) (ForwardCompatible, error) {
	switch state {
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED:
	default:
		return ForwardCompatible{}, fmt.Errorf(
			"%w: lifecycle state %d cannot expose an unknown body",
			ErrUnknownFutureBody,
			state,
		)
	}
	return decodeCanonicalFutureBody(data, expectedDigest)
}

func decodeCanonicalFutureBody(data, expectedDigest []byte) (ForwardCompatible, error) {
	digest, err := verifyStoredBytes(data, expectedDigest)
	if err != nil {
		return ForwardCompatible{}, err
	}

	definition := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false, RecursionLimit: 32}).Unmarshal(data, definition); err != nil {
		return ForwardCompatible{}, fmt.Errorf("%w: malformed protobuf", ErrNonCanonical)
	}
	if err := preflightInput(definition); err != nil {
		return ForwardCompatible{}, fmt.Errorf("%w: %v", ErrNonCanonical, err)
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil || !bytes.Equal(encoded, data) {
		return ForwardCompatible{}, ErrNonCanonical
	}
	if definition.GetBody() != nil {
		return ForwardCompatible{}, fmt.Errorf("%w: recognized body is present", ErrUnknownFutureBody)
	}
	if err := validateFutureBodyUnknown(definition.ProtoReflect().GetUnknown()); err != nil {
		return ForwardCompatible{}, err
	}
	if err := rejectNestedUnknownFields(definition.ProtoReflect(), "definition"); err != nil {
		return ForwardCompatible{}, fmt.Errorf("%w: %v", ErrNonCanonical, err)
	}

	known, ok := cloneWithoutTopLevelUnknown(definition)
	if !ok || known == nil {
		return ForwardCompatible{}, fmt.Errorf("%w: definition could not be cloned", ErrNonCanonical)
	}
	canonical, ok := proto.Clone(known).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || canonical == nil {
		return ForwardCompatible{}, fmt.Errorf("%w: definition metadata could not be cloned", ErrNonCanonical)
	}
	description, selector, err := normalizeMetadata(canonical)
	if err != nil || !proto.Equal(known, canonical) {
		return ForwardCompatible{}, fmt.Errorf("%w: known metadata is not canonical", ErrNonCanonical)
	}

	return ForwardCompatible{
		Definition:   definition,
		Bytes:        bytes.Clone(data),
		Digest:       digest,
		AppID:        canonical.GetAppId(),
		Name:         canonical.GetName(),
		SharingScope: canonical.GetSharingScope(),
		Description:  cloneOptionalString(description),
		Selector:     selector,
	}, nil
}

func cloneWithoutTopLevelUnknown(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (*opensplunkv1.KnowledgeObjectDefinition, bool) {
	cloned, ok := proto.Clone(definition).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || cloned == nil {
		return nil, false
	}
	cloned.ProtoReflect().SetUnknown(nil)
	return cloned, true
}

func verifyStoredBytes(data, expectedDigest []byte) ([sha256.Size]byte, error) {
	if len(data) == 0 || len(data) > MaximumCanonicalBytes {
		return [sha256.Size]byte{}, fmt.Errorf(
			"%w: stored bytes must be between 1 and %d",
			ErrDefinitionTooLarge,
			MaximumCanonicalBytes,
		)
	}
	if len(expectedDigest) != sha256.Size {
		return [sha256.Size]byte{}, fmt.Errorf("%w: digest must contain %d bytes", ErrDigestMismatch, sha256.Size)
	}
	digest := sha256.Sum256(data)
	if subtle.ConstantTimeCompare(digest[:], expectedDigest) != 1 {
		return [sha256.Size]byte{}, ErrDigestMismatch
	}
	return digest, nil
}

func normalizeMetadata(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) (*string, *knowledge.Selector, error) {
	appID, err := normalizeRequiredText(definition.GetAppId(), MaximumAppIDBytes)
	if err != nil {
		return nil, nil, invalid("app_id", err)
	}
	definition.AppId = appID

	name, err := knowledge.NormalizeName(definition.GetName())
	if err != nil {
		return nil, nil, invalid("name", err)
	}
	definition.Name = name.String()

	description, err := normalizeDescription(definition.Description)
	if err != nil {
		return nil, nil, invalid("description", err)
	}
	definition.Description = cloneOptionalString(description)

	if !validSharingScope(definition.GetSharingScope()) {
		return nil, nil, invalid("sharing_scope", errors.New("is unspecified or unknown"))
	}

	selector, canonicalSelector, err := normalizeSelector(definition.GetSelector())
	if err != nil {
		return nil, nil, err
	}
	definition.Selector = canonicalSelector
	return description, selector, nil
}

func normalizeSelector(input *opensplunkv1.KnowledgeSelector) (*knowledge.Selector, *opensplunkv1.KnowledgeSelector, error) {
	dimensions := []struct {
		path      string
		dimension knowledge.Dimension
		patterns  []*opensplunkv1.KnowledgeSelectorPattern
	}{
		{path: "selector.index_patterns", dimension: knowledge.DimensionIndex},
		{path: "selector.host_patterns", dimension: knowledge.DimensionHost},
		{path: "selector.source_patterns", dimension: knowledge.DimensionSource},
		{path: "selector.sourcetype_patterns", dimension: knowledge.DimensionSourcetype},
	}
	if input != nil {
		dimensions[0].patterns = input.GetIndexPatterns()
		dimensions[1].patterns = input.GetHostPatterns()
		dimensions[2].patterns = input.GetSourcePatterns()
		dimensions[3].patterns = input.GetSourcetypePatterns()
	}

	spec := knowledge.SelectorSpec{Dimensions: make([]knowledge.DimensionSpec, 0, len(dimensions))}
	for _, dimension := range dimensions {
		if len(dimension.patterns) == 0 {
			continue
		}
		values := make([]string, 0, len(dimension.patterns))
		for index, submitted := range dimension.patterns {
			path := fmt.Sprintf("%s[%d]", dimension.path, index)
			if submitted == nil {
				return nil, nil, invalid(path, errors.New("is required"))
			}
			pattern, err := knowledge.NormalizePattern(submitted.GetValue())
			if err != nil {
				return nil, nil, invalid(path+".value", err)
			}
			expected := opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
			if pattern.IsLiteral() {
				expected = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
			}
			if submitted.GetMatchKind() != opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_UNSPECIFIED &&
				submitted.GetMatchKind() != expected {
				return nil, nil, invalid(path+".match_kind", errors.New("disagrees with normalized value"))
			}
			values = append(values, submitted.GetValue())
		}
		spec.Dimensions = append(spec.Dimensions, knowledge.DimensionSpec{
			Dimension: dimension.dimension,
			Patterns:  values,
		})
	}

	compiled, err := knowledge.CompileSelector(spec)
	if err != nil {
		return nil, nil, invalid("selector", err)
	}
	canonical := &opensplunkv1.KnowledgeSelector{}
	canonical.IndexPatterns, err = canonicalPatterns(compiled, knowledge.DimensionIndex)
	if err != nil {
		return nil, nil, err
	}
	canonical.HostPatterns, err = canonicalPatterns(compiled, knowledge.DimensionHost)
	if err != nil {
		return nil, nil, err
	}
	canonical.SourcePatterns, err = canonicalPatterns(compiled, knowledge.DimensionSource)
	if err != nil {
		return nil, nil, err
	}
	canonical.SourcetypePatterns, err = canonicalPatterns(compiled, knowledge.DimensionSourcetype)
	if err != nil {
		return nil, nil, err
	}
	if compiled.Stats().Patterns == 0 {
		canonical = nil
	}
	return compiled, canonical, nil
}

func canonicalPatterns(selector *knowledge.Selector, dimension knowledge.Dimension) ([]*opensplunkv1.KnowledgeSelectorPattern, error) {
	values := selector.Patterns(dimension)
	if len(values) == 0 {
		return nil, nil
	}
	patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, 0, len(values))
	for _, value := range values {
		pattern, err := knowledge.NormalizePattern(value)
		if err != nil {
			return nil, fmt.Errorf("%w: selector canonicalization failed", ErrInvalidDefinition)
		}
		matchKind := opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
		if pattern.IsLiteral() {
			matchKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
		}
		patterns = append(patterns, &opensplunkv1.KnowledgeSelectorPattern{
			MatchKind: matchKind,
			Value:     value,
		})
	}
	return patterns, nil
}

func normalizeBody(definition *opensplunkv1.KnowledgeObjectDefinition) (opensplunkv1.KnowledgeObjectType, error) {
	switch body := definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		if body == nil || body.FieldExtraction == nil {
			return 0, invalid("field_extraction", errors.New("is required"))
		}
		if err := normalizeFieldExtraction(body.FieldExtraction); err != nil {
			return 0, err
		}
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION, nil
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		if body == nil || body.FieldAlias == nil {
			return 0, invalid("field_alias", errors.New("is required"))
		}
		if err := normalizeFieldAlias(body.FieldAlias); err != nil {
			return 0, err
		}
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS, nil
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		if body == nil || body.CalculatedField == nil {
			return 0, invalid("calculated_field", errors.New("is required"))
		}
		if err := normalizeCalculatedField(body.CalculatedField); err != nil {
			return 0, err
		}
		return opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD, nil
	default:
		return 0, invalid("body", errors.New("is missing or unknown"))
	}
}

func normalizeFieldExtraction(body *opensplunkv1.FieldExtractionDefinition) error {
	input := body.GetInputField()
	if input == "" {
		input = "_raw"
	}
	normalizedInput, err := normalizeSourceField(input)
	if err != nil {
		return invalid("field_extraction.input_field", err)
	}
	if normalizedInput != "_raw" {
		return invalid("field_extraction.input_field", errors.New("must be _raw in compatibility version 0.1"))
	}
	body.InputField = normalizedInput

	overwrite, err := normalizeOverwrite(body.GetOverwriteBehavior())
	if err != nil {
		return invalid("field_extraction.overwrite_behavior", err)
	}
	body.OverwriteBehavior = overwrite

	switch extraction := body.GetExtraction().(type) {
	case *opensplunkv1.FieldExtractionDefinition_Regex:
		if extraction == nil || extraction.Regex == nil {
			return invalid("field_extraction.regex", errors.New("is required"))
		}
		pattern, err := normalizeOpaqueBodyText(extraction.Regex.GetPattern(), splregex.MaximumExtractionPatternBytes, false)
		if err != nil {
			return invalid("field_extraction.regex.pattern", err)
		}
		extraction.Regex.Pattern = pattern
		if len(extraction.Regex.GetOutputFields()) == 0 || len(extraction.Regex.GetOutputFields()) > MaximumFieldExtractionOutputs {
			return invalid("field_extraction.regex.output_fields", fmt.Errorf("must contain between 1 and %d fields", MaximumFieldExtractionOutputs))
		}
		seen := make(map[string]struct{}, len(extraction.Regex.GetOutputFields()))
		for index, output := range extraction.Regex.GetOutputFields() {
			destination, err := knowledge.NormalizeFieldDestination(output)
			if err != nil {
				return invalid(fmt.Sprintf("field_extraction.regex.output_fields[%d]", index), err)
			}
			if _, duplicate := seen[destination.String()]; duplicate {
				return invalid(fmt.Sprintf("field_extraction.regex.output_fields[%d]", index), errors.New("duplicates a prior output"))
			}
			seen[destination.String()] = struct{}{}
			extraction.Regex.OutputFields[index] = destination.String()
		}
	case *opensplunkv1.FieldExtractionDefinition_Json:
		if extraction == nil || extraction.Json == nil {
			return invalid("field_extraction.json", errors.New("is required"))
		}
		path, err := normalizeOpaqueBodyText(extraction.Json.GetPath(), splpath.MaximumPathBytes, false)
		if err != nil {
			return invalid("field_extraction.json.path", err)
		}
		extraction.Json.Path = path
		destination, err := knowledge.NormalizeFieldDestination(extraction.Json.GetOutputField())
		if err != nil {
			return invalid("field_extraction.json.output_field", err)
		}
		extraction.Json.OutputField = destination.String()
	default:
		return invalid("field_extraction.extraction", errors.New("is missing or unknown"))
	}
	return nil
}

func normalizeFieldAlias(body *opensplunkv1.FieldAliasDefinition) error {
	source, err := normalizeSourceField(body.GetSourceField())
	if err != nil {
		return invalid("field_alias.source_field", err)
	}
	destination, err := knowledge.NormalizeFieldDestination(body.GetDestinationField())
	if err != nil {
		return invalid("field_alias.destination_field", err)
	}
	if source == destination.String() {
		return invalid("field_alias.destination_field", errors.New("must differ from source_field"))
	}
	overwrite, err := normalizeOverwrite(body.GetOverwriteBehavior())
	if err != nil {
		return invalid("field_alias.overwrite_behavior", err)
	}
	body.SourceField = source
	body.DestinationField = destination.String()
	body.OverwriteBehavior = overwrite
	return nil
}

func normalizeCalculatedField(body *opensplunkv1.CalculatedFieldDefinition) error {
	destination, err := knowledge.NormalizeFieldDestination(body.GetDestinationField())
	if err != nil {
		return invalid("calculated_field.destination_field", err)
	}
	expression, err := normalizeOpaqueBodyText(body.GetExpression(), MaximumExpressionBytes, true)
	if err != nil {
		return invalid("calculated_field.expression", err)
	}
	overwrite, err := normalizeOverwrite(body.GetOverwriteBehavior())
	if err != nil {
		return invalid("calculated_field.overwrite_behavior", err)
	}
	body.DestinationField = destination.String()
	body.Expression = expression
	body.OverwriteBehavior = overwrite
	return nil
}

func normalizeOverwrite(value opensplunkv1.KnowledgeOverwriteBehavior) (opensplunkv1.KnowledgeOverwriteBehavior, error) {
	switch value {
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED:
		return opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING, nil
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING:
		return value, nil
	default:
		return 0, errors.New("is unknown")
	}
}

func normalizeSourceField(source string) (string, error) {
	value, err := normalizeRequiredText(source, knowledge.MaximumFieldDestinationBytes)
	if err != nil {
		return "", err
	}
	if _, err := eventfields.ParseNormalizedSearchFieldPath(value); err != nil {
		return "", err
	}
	return value, nil
}

func normalizeDescription(description *string) (*string, error) {
	if description == nil {
		return nil, nil
	}
	value := trimASCIIWhitespace(*description)
	if value == "" {
		return nil, nil
	}
	if err := validateText(value, MaximumDescriptionBytes); err != nil {
		return nil, err
	}
	return &value, nil
}

func normalizeRequiredText(source string, maximumBytes int) (string, error) {
	value := trimASCIIWhitespace(source)
	if value == "" {
		return "", errors.New("is empty")
	}
	if err := validateText(value, maximumBytes); err != nil {
		return "", err
	}
	return value, nil
}

func normalizeOpaqueBodyText(source string, maximumBytes int, trim bool) (string, error) {
	if len(source) > maximumBytes {
		return "", fmt.Errorf("exceeds %d UTF-8 bytes", maximumBytes)
	}
	value := source
	if trim {
		value = trimASCIIWhitespace(value)
	}
	if value == "" {
		return "", errors.New("is empty")
	}
	if !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("has invalid UTF-8 or contains NUL")
	}
	return value, nil
}

func validateText(value string, maximumBytes int) error {
	if len(value) > maximumBytes {
		return fmt.Errorf("exceeds %d UTF-8 bytes", maximumBytes)
	}
	if !utf8.ValidString(value) {
		return errors.New("is not valid UTF-8")
	}
	for _, character := range value {
		if character <= 0x1f || character >= 0x7f && character <= 0x9f {
			return errors.New("contains a pinned C0/C1 control")
		}
	}
	return nil
}

func validSharingScope(scope opensplunkv1.SharingScope) bool {
	return scope == opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE ||
		scope == opensplunkv1.SharingScope_SHARING_SCOPE_APP ||
		scope == opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL
}

func rejectUnknownFields(message protoreflect.Message, path string) error {
	if !message.IsValid() {
		return nil
	}
	if len(message.GetUnknown()) != 0 {
		return fmt.Errorf("%w: %s", ErrUnknownFields, path)
	}
	return rejectNestedUnknownFields(message, path)
}

func rejectNestedUnknownFields(message protoreflect.Message, path string) error {
	if !message.IsValid() {
		return nil
	}
	var nestedErr error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		fieldPath := path + "." + string(field.Name())
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind && field.MapValue().Kind() != protoreflect.GroupKind {
				return true
			}
			value.Map().Range(func(key protoreflect.MapKey, mapValue protoreflect.Value) bool {
				nestedErr = rejectUnknownFields(mapValue.Message(), fmt.Sprintf("%s[%v]", fieldPath, key.Interface()))
				return nestedErr == nil
			})
			return nestedErr == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind && field.Kind() != protoreflect.GroupKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				nestedErr = rejectUnknownFields(list.Get(index).Message(), fmt.Sprintf("%s[%d]", fieldPath, index))
				if nestedErr != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind || field.Kind() == protoreflect.GroupKind {
			nestedErr = rejectUnknownFields(value.Message(), fieldPath)
			return nestedErr == nil
		}
		return true
	})
	return nestedErr
}

func validateFutureBodyUnknown(raw []byte) error {
	if len(raw) == 0 {
		return fmt.Errorf("%w: body field is absent", ErrUnknownFutureBody)
	}
	bodyCount := 0
	previousNumber := protowire.Number(0)
	for len(raw) != 0 {
		number, wireType, tagBytes := protowire.ConsumeTag(raw)
		if tagBytes < 0 || number < 13 || number >= 19_000 && number <= 19_999 {
			return fmt.Errorf("%w: top-level unknown field number is not forward-compatible", ErrUnknownFutureBody)
		}
		canonicalTag := protowire.AppendTag(nil, number, wireType)
		if !bytes.Equal(raw[:tagBytes], canonicalTag) || number < previousNumber {
			return fmt.Errorf("%w: top-level unknown field order or tag is not canonical", ErrUnknownFutureBody)
		}
		previousNumber = number
		raw = raw[tagBytes:]

		if number <= 31 {
			if wireType != protowire.BytesType {
				return fmt.Errorf("%w: future body is not a message field", ErrUnknownFutureBody)
			}
			bodyCount++
			if bodyCount > 1 {
				return fmt.Errorf("%w: multiple future bodies are present", ErrUnknownFutureBody)
			}
		}

		valueBytes, canonical := canonicalUnknownValue(raw, wireType)
		if valueBytes < 0 || !canonical {
			return fmt.Errorf("%w: top-level unknown value is not canonical", ErrUnknownFutureBody)
		}
		raw = raw[valueBytes:]
	}
	if bodyCount != 1 {
		return fmt.Errorf("%w: body field is absent", ErrUnknownFutureBody)
	}
	return nil
}

func canonicalUnknownValue(raw []byte, wireType protowire.Type) (int, bool) {
	switch wireType {
	case protowire.VarintType:
		value, count := protowire.ConsumeVarint(raw)
		return count, count >= 0 && bytes.Equal(raw[:count], protowire.AppendVarint(nil, value))
	case protowire.Fixed32Type:
		_, count := protowire.ConsumeFixed32(raw)
		return count, count >= 0
	case protowire.Fixed64Type:
		_, count := protowire.ConsumeFixed64(raw)
		return count, count >= 0
	case protowire.BytesType:
		value, count := protowire.ConsumeBytes(raw)
		if count < 0 {
			return count, false
		}
		lengthBytes := count - len(value)
		return count, bytes.Equal(raw[:lengthBytes], protowire.AppendVarint(nil, uint64(len(value))))
	default:
		return -1, false
	}
}

func cloneOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func trimASCIIWhitespace(value string) string {
	return strings.TrimFunc(value, func(character rune) bool {
		return character == ' ' || character >= '\t' && character <= '\r'
	})
}

func invalid(path string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrInvalidDefinition, path, err)
}
