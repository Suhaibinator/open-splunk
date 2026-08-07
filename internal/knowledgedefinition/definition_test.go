package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestNormalizeProducesDetachedDeterministicCanonicalAuthorities(t *testing.T) {
	t.Parallel()

	emptyDescription := " \t\r\n "
	input := &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "\tapp_AAAAAAAAAAAAAAAAAAAAAA ",
		Name:         " Revenue ",
		Description:  &emptyDescription,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{
				{Value: " prod** "},
				{Value: "alpha", MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT},
				{Value: "prod*", MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD},
			},
			HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: `api\*literal`}},
		},
		Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField:      " source.value ",
				DestinationField: " derived.value ",
			},
		},
	}

	normalized, err := Normalize(input)
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if normalized.AppID != "app_AAAAAAAAAAAAAAAAAAAAAA" || normalized.Name != "Revenue" {
		t.Fatalf("normalized identity = (%q, %q)", normalized.AppID, normalized.Name)
	}
	if normalized.Description != nil || normalized.Definition.Description != nil {
		t.Fatalf("normalized empty description = (%v, %v), want absent", normalized.Description, normalized.Definition.Description)
	}
	if normalized.ObjectType != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		normalized.SharingScope != opensplunkv1.SharingScope_SHARING_SCOPE_APP {
		t.Fatalf("derived type/scope = (%v, %v)", normalized.ObjectType, normalized.SharingScope)
	}
	alias := normalized.Definition.GetFieldAlias()
	if alias.GetSourceField() != "source.value" || alias.GetDestinationField() != "derived.value" ||
		alias.GetOverwriteBehavior() != opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING {
		t.Fatalf("canonical alias = %#v", alias)
	}

	indexPatterns := normalized.Definition.GetSelector().GetIndexPatterns()
	if len(indexPatterns) != 2 ||
		indexPatterns[0].GetValue() != "alpha" ||
		indexPatterns[0].GetMatchKind() != opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT ||
		indexPatterns[1].GetValue() != "prod*" ||
		indexPatterns[1].GetMatchKind() != opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD {
		t.Fatalf("canonical index patterns = %#v", indexPatterns)
	}
	if got := normalized.Selector.Patterns(knowledge.DimensionHost); len(got) != 1 || got[0] != `api\*literal` {
		t.Fatalf("canonical host patterns = %#v", got)
	}
	if normalized.Selector.Stats().NormalizedBytes > knowledge.MaximumSelectorNormalizedBytes {
		t.Fatalf("selector charge = %d", normalized.Selector.Stats().NormalizedBytes)
	}
	if got := sha256.Sum256(normalized.Bytes); got != normalized.Digest {
		t.Fatalf("digest = %x, want %x", normalized.Digest, got)
	}

	again, err := Normalize(normalized.Definition)
	if err != nil {
		t.Fatalf("Normalize(canonical): %v", err)
	}
	if !bytes.Equal(again.Bytes, normalized.Bytes) || again.Digest != normalized.Digest {
		t.Fatal("canonical normalization is not idempotent")
	}

	// The caller's entire tree remains untouched and cannot mutate the result.
	if input.GetAppId() != "\tapp_AAAAAAAAAAAAAAAAAAAAAA " ||
		input.GetFieldAlias().GetOverwriteBehavior() != opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED ||
		len(input.GetSelector().GetIndexPatterns()) != 3 {
		t.Fatal("Normalize mutated its input")
	}
	stableBytes := bytes.Clone(normalized.Bytes)
	input.AppId = "attacker"
	input.GetFieldAlias().DestinationField = "attacker"
	input.GetSelector().IndexPatterns[0].Value = "attacker"
	if normalized.Definition.GetAppId() != "app_AAAAAAAAAAAAAAAAAAAAAA" ||
		normalized.Definition.GetFieldAlias().GetDestinationField() != "derived.value" ||
		!bytes.Equal(normalized.Bytes, stableBytes) {
		t.Fatal("normalized result aliases caller memory")
	}
}

func TestNormalizePreservesDescriptionPresenceAsDetachedCanonicalValue(t *testing.T) {
	t.Parallel()

	description := "\t Safe description \r"
	input := validAliasDefinition()
	input.Description = &description
	normalized, err := Normalize(input)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Description == nil || *normalized.Description != "Safe description" ||
		normalized.Definition.Description == nil || *normalized.Definition.Description != "Safe description" {
		t.Fatalf("description = (%v, %v)", normalized.Description, normalized.Definition.Description)
	}
	*normalized.Description = "changed"
	if *normalized.Definition.Description != "Safe description" {
		t.Fatal("derived description aliases protobuf description")
	}
}

func TestNormalizeRejectsUnknownFieldsRecursively(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*opensplunkv1.KnowledgeObjectDefinition)
	}{
		{name: "root", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "selector", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "repeated pattern", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "oneof body", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.GetFieldAlias().ProtoReflect().SetUnknown(testUnknownField())
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := validAliasDefinition()
			test.mutate(definition)
			if _, err := Normalize(definition); !errors.Is(err, ErrUnknownFields) {
				t.Fatalf("Normalize error = %v, want ErrUnknownFields", err)
			}
		})
	}

	nestedBodies := []struct {
		name       string
		definition func() *opensplunkv1.KnowledgeObjectDefinition
		message    func(*opensplunkv1.KnowledgeObjectDefinition) proto.Message
	}{
		{
			name: "field extraction",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
						Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
							Regex: &opensplunkv1.RegexFieldExtractionDefinition{
								Pattern: "(?<value>x)", OutputFields: []string{"value"},
							},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunkv1.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction()
			},
		},
		{
			name: "regex extraction leaf",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
						Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
							Regex: &opensplunkv1.RegexFieldExtractionDefinition{
								Pattern: "(?<value>x)", OutputFields: []string{"value"},
							},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunkv1.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction().GetRegex()
			},
		},
		{
			name: "JSON extraction leaf",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
						Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
							Json: &opensplunkv1.JsonFieldExtractionDefinition{Path: "value", OutputField: "value"},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunkv1.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction().GetJson()
			},
		},
		{
			name: "calculated field leaf",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
					CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
						DestinationField: "value", Expression: "1",
					},
				}
				return definition
			},
			message: func(definition *opensplunkv1.KnowledgeObjectDefinition) proto.Message {
				return definition.GetCalculatedField()
			},
		},
	}
	for _, test := range nestedBodies {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := test.definition()
			test.message(definition).ProtoReflect().SetUnknown(testUnknownField())
			if _, err := Normalize(definition); !errors.Is(err, ErrUnknownFields) {
				t.Fatalf("Normalize error = %v, want ErrUnknownFields", err)
			}
		})
	}
}

func TestNormalizeRejectsUnknownEnumsSelectorDisagreementAndBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*opensplunkv1.KnowledgeObjectDefinition)
	}{
		{name: "unspecified scope", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_UNSPECIFIED
		}},
		{name: "future scope", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.SharingScope = opensplunkv1.SharingScope(99)
		}},
		{name: "exact marked wildcard", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].Value = "prod*"
			definition.Selector.IndexPatterns[0].MatchKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
		}},
		{name: "wildcard marked exact", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].Value = "prod"
			definition.Selector.IndexPatterns[0].MatchKind = opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
		}},
		{name: "future selector kind", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].MatchKind = opensplunkv1.KnowledgeSelectorMatchKind(99)
		}},
		{name: "nil selector entry", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0] = nil
		}},
		{name: "missing body", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Body = nil
		}},
		{name: "typed nil body", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{}
		}},
		{name: "future overwrite", mutate: func(definition *opensplunkv1.KnowledgeObjectDefinition) {
			definition.GetFieldAlias().OverwriteBehavior = opensplunkv1.KnowledgeOverwriteBehavior(99)
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := validAliasDefinition()
			test.mutate(definition)
			if _, err := Normalize(definition); !errors.Is(err, ErrInvalidDefinition) {
				t.Fatalf("Normalize error = %v, want ErrInvalidDefinition", err)
			}
		})
	}
}

func TestNormalizeBodyFamiliesAndStructuralBounds(t *testing.T) {
	t.Parallel()

	regex := validBaseDefinition()
	regex.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
				Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern:      `status=(?<status>[0-9]+)`,
					OutputFields: []string{" status ", "nested.code"},
				},
			},
		},
	}
	normalizedRegex, err := Normalize(regex)
	if err != nil {
		t.Fatalf("Normalize(regex): %v", err)
	}
	if normalizedRegex.ObjectType != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		normalizedRegex.Definition.GetFieldExtraction().GetInputField() != "_raw" ||
		normalizedRegex.Definition.GetFieldExtraction().GetOverwriteBehavior() != opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING ||
		normalizedRegex.Definition.GetFieldExtraction().GetRegex().GetOutputFields()[0] != "status" {
		t.Fatalf("canonical regex extraction = %#v", normalizedRegex.Definition.GetFieldExtraction())
	}

	json := validBaseDefinition()
	json.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: " _raw ",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
				Json: &opensplunkv1.JsonFieldExtractionDefinition{Path: " items{0}.name ", OutputField: " item.name "},
			},
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
		},
	}
	normalizedJSON, err := Normalize(json)
	if err != nil {
		t.Fatalf("Normalize(json): %v", err)
	}
	if normalizedJSON.Definition.GetFieldExtraction().GetJson().GetPath() != " items{0}.name " ||
		normalizedJSON.Definition.GetFieldExtraction().GetJson().GetOutputField() != "item.name" {
		t.Fatalf("canonical JSON extraction = %#v", normalizedJSON.Definition.GetFieldExtraction().GetJson())
	}

	calculated := validBaseDefinition()
	calculated.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
		CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			DestinationField: " latency.class ",
			Expression:       "\t if(latency > 100, \"slow\", \"fast\") \r\n",
		},
	}
	normalizedCalculated, err := Normalize(calculated)
	if err != nil {
		t.Fatalf("Normalize(calculated): %v", err)
	}
	if normalizedCalculated.ObjectType != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD ||
		normalizedCalculated.Definition.GetCalculatedField().GetExpression() != `if(latency > 100, "slow", "fast")` {
		t.Fatalf("canonical calculated field = %#v", normalizedCalculated.Definition.GetCalculatedField())
	}

	badInput := proto.Clone(regex).(*opensplunkv1.KnowledgeObjectDefinition)
	badInput.GetFieldExtraction().InputField = "message"
	assertInvalidDefinition(t, badInput)

	duplicateOutput := proto.Clone(regex).(*opensplunkv1.KnowledgeObjectDefinition)
	duplicateOutput.GetFieldExtraction().GetRegex().OutputFields = []string{"status", " status "}
	assertInvalidDefinition(t, duplicateOutput)

	tooManyOutputs := proto.Clone(regex).(*opensplunkv1.KnowledgeObjectDefinition)
	tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields = make([]string, MaximumFieldExtractionOutputs+1)
	for index := range tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields {
		tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields[index] = strings.Repeat("f", index+1)
	}
	if _, err := Normalize(tooManyOutputs); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("too many regex outputs error = %v, want ErrDefinitionTooLarge", err)
	}

	missingExtraction := proto.Clone(regex).(*opensplunkv1.KnowledgeObjectDefinition)
	missingExtraction.GetFieldExtraction().Extraction = nil
	assertInvalidDefinition(t, missingExtraction)

	sameAlias := validAliasDefinition()
	sameAlias.GetFieldAlias().DestinationField = sameAlias.GetFieldAlias().SourceField
	assertInvalidDefinition(t, sameAlias)

	reservedDestination := validAliasDefinition()
	reservedDestination.GetFieldAlias().DestinationField = "INDEX.child"
	assertInvalidDefinition(t, reservedDestination)

	emptyExpression := proto.Clone(calculated).(*opensplunkv1.KnowledgeObjectDefinition)
	emptyExpression.GetCalculatedField().Expression = " \t\n "
	assertInvalidDefinition(t, emptyExpression)

	oversizedExpression := proto.Clone(calculated).(*opensplunkv1.KnowledgeObjectDefinition)
	oversizedExpression.GetCalculatedField().Expression = strings.Repeat("x", MaximumExpressionBytes+1)
	assertInvalidDefinition(t, oversizedExpression)

	oversizedPaddedExpression := proto.Clone(calculated).(*opensplunkv1.KnowledgeObjectDefinition)
	oversizedPaddedExpression.GetCalculatedField().Expression = "1" + strings.Repeat(" ", MaximumExpressionBytes)
	assertInvalidDefinition(t, oversizedPaddedExpression)

	oversizedDescription := validAliasDefinition()
	description := strings.Repeat("d", MaximumDescriptionBytes+1)
	oversizedDescription.Description = &description
	assertInvalidDefinition(t, oversizedDescription)
}

func TestDecodeCanonicalVerifiesDigestUnknownsAndExactBytes(t *testing.T) {
	t.Parallel()

	normalized, err := Normalize(validAliasDefinition())
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeCanonical(normalized.Bytes, normalized.Digest[:])
	if err != nil {
		t.Fatalf("DecodeCanonical: %v", err)
	}
	if !proto.Equal(decoded.Definition, normalized.Definition) || decoded.Digest != normalized.Digest {
		t.Fatal("decoded canonical authorities disagree")
	}

	wrongDigest := normalized.Digest
	wrongDigest[0] ^= 0xff
	if _, err := DecodeCanonical(normalized.Bytes, wrongDigest[:]); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong digest error = %v", err)
	}
	if _, err := DecodeCanonical(normalized.Bytes, normalized.Digest[:sha256.Size-1]); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("short digest error = %v", err)
	}

	nonCanonicalMessage := validAliasDefinition()
	nonCanonicalMessage.AppId = " app_AAAAAAAAAAAAAAAAAAAAAA "
	nonCanonicalBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(nonCanonicalMessage)
	if err != nil {
		t.Fatal(err)
	}
	nonCanonicalDigest := sha256.Sum256(nonCanonicalBytes)
	if _, err := DecodeCanonical(nonCanonicalBytes, nonCanonicalDigest[:]); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("noncanonical definition error = %v", err)
	}

	duplicateKnownField := protowire.AppendTag(bytes.Clone(normalized.Bytes), 2, protowire.BytesType)
	duplicateKnownField = protowire.AppendString(duplicateKnownField, normalized.Name)
	duplicateDigest := sha256.Sum256(duplicateKnownField)
	if _, err := DecodeCanonical(duplicateKnownField, duplicateDigest[:]); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("duplicate known field error = %v", err)
	}

	withUnknown := append(bytes.Clone(normalized.Bytes), testUnknownField()...)
	unknownDigest := sha256.Sum256(withUnknown)
	if _, err := DecodeCanonical(withUnknown, unknownDigest[:]); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("unknown field error = %v", err)
	}

	malformed := []byte{0x80}
	malformedDigest := sha256.Sum256(malformed)
	if _, err := DecodeCanonical(malformed, malformedDigest[:]); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("malformed protobuf error = %v", err)
	}

	oversized := make([]byte, MaximumCanonicalBytes+1)
	oversizedDigest := sha256.Sum256(oversized)
	if _, err := DecodeCanonical(oversized, oversizedDigest[:]); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("oversized definition error = %v", err)
	}
}

func TestNormalizeCollapsesEmptySelectorAndDescriptionPresence(t *testing.T) {
	t.Parallel()

	empty := ""
	definition := validAliasDefinition()
	definition.Description = &empty
	definition.Selector = &opensplunkv1.KnowledgeSelector{}
	normalized, err := Normalize(definition)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.Definition.Description != nil || normalized.Description != nil || normalized.Definition.Selector != nil {
		t.Fatalf("empty optionals were not collapsed: %#v", normalized.Definition)
	}
	if normalized.Selector == nil || normalized.Selector.Stats().Patterns != 0 {
		t.Fatalf("compiled unrestricted selector = %#v", normalized.Selector)
	}
}

func TestNormalizePreflightsUntrustedRepeatedShapesAndWireBytes(t *testing.T) {
	t.Parallel()

	tooManyPatterns := validAliasDefinition()
	tooManyPatterns.Selector.IndexPatterns = make(
		[]*opensplunkv1.KnowledgeSelectorPattern,
		knowledge.MaximumSelectorPatternsPerDimension+1,
	)
	for index := range tooManyPatterns.Selector.IndexPatterns {
		tooManyPatterns.Selector.IndexPatterns[index] = &opensplunkv1.KnowledgeSelectorPattern{Value: "x"}
	}
	if _, err := Normalize(tooManyPatterns); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("too many submitted patterns error = %v, want ErrDefinitionTooLarge", err)
	}

	tooManyWireBytes := validAliasDefinition()
	tooManyWireBytes.Name = strings.Repeat("x", MaximumCanonicalBytes+1)
	if _, err := Normalize(tooManyWireBytes); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("oversized submitted protobuf error = %v, want ErrDefinitionTooLarge", err)
	}
}

func validAliasDefinition() *opensplunkv1.KnowledgeObjectDefinition {
	definition := validBaseDefinition()
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField:       "source.value",
			DestinationField:  "derived.value",
			OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		},
	}
	return definition
}

func validBaseDefinition() *opensplunkv1.KnowledgeObjectDefinition {
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		Name:         "revenue",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
				MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     "main",
			}},
		},
	}
}

func assertInvalidDefinition(t *testing.T, definition *opensplunkv1.KnowledgeObjectDefinition) {
	t.Helper()
	if _, err := Normalize(definition); !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("Normalize error = %v, want ErrInvalidDefinition", err)
	}
}

func testUnknownField() []byte {
	return protowire.AppendVarint(
		protowire.AppendTag(nil, 2_047, protowire.VarintType),
		1,
	)
}
