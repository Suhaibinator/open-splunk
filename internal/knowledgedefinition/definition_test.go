package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestNormalizeProducesDetachedDeterministicCanonicalAuthorities(t *testing.T) {
	t.Parallel()

	emptyDescription := " \t\r\n "
	input := &opensplunk.KnowledgeObjectDefinition{
		AppId:        "\tapp_AAAAAAAAAAAAAAAAAAAAAA ",
		Name:         " Revenue ",
		Description:  &emptyDescription,
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{
				{Value: " prod** "},
				{Value: "alpha", MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT},
				{Value: "prod*", MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD},
			},
			HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: `api\*literal`}},
		},
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{
			FieldAlias: &opensplunk.FieldAliasDefinition{
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
	if normalized.ObjectType != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		normalized.SharingScope != opensplunk.SharingScope_SHARING_SCOPE_APP {
		t.Fatalf("derived type/scope = (%v, %v)", normalized.ObjectType, normalized.SharingScope)
	}
	alias := normalized.Definition.GetFieldAlias()
	if alias.GetSourceField() != "source.value" || alias.GetDestinationField() != "derived.value" ||
		alias.GetOverwriteBehavior() != opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING {
		t.Fatalf("canonical alias = %#v", alias)
	}

	indexPatterns := normalized.Definition.GetSelector().GetIndexPatterns()
	if len(indexPatterns) != 2 ||
		indexPatterns[0].GetValue() != "alpha" ||
		indexPatterns[0].GetMatchKind() != opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT ||
		indexPatterns[1].GetValue() != "prod*" ||
		indexPatterns[1].GetMatchKind() != opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD {
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
		input.GetFieldAlias().GetOverwriteBehavior() != opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED ||
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
		mutate func(*opensplunk.KnowledgeObjectDefinition)
	}{
		{name: "root", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "selector", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "repeated pattern", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].ProtoReflect().SetUnknown(testUnknownField())
		}},
		{name: "oneof body", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.GetFieldAlias().ProtoReflect().SetUnknown(testUnknownField())
		}},
	}
	for _, test := range tests {
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
		definition func() *opensplunk.KnowledgeObjectDefinition
		message    func(*opensplunk.KnowledgeObjectDefinition) proto.Message
	}{
		{
			name: "field extraction",
			definition: func() *opensplunk.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunk.FieldExtractionDefinition{
						Extraction: &opensplunk.FieldExtractionDefinition_Regex{
							Regex: &opensplunk.RegexFieldExtractionDefinition{
								Pattern: "(?<value>x)", OutputFields: []string{"value"},
							},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunk.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction()
			},
		},
		{
			name: "regex extraction leaf",
			definition: func() *opensplunk.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunk.FieldExtractionDefinition{
						Extraction: &opensplunk.FieldExtractionDefinition_Regex{
							Regex: &opensplunk.RegexFieldExtractionDefinition{
								Pattern: "(?<value>x)", OutputFields: []string{"value"},
							},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunk.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction().GetRegex()
			},
		},
		{
			name: "JSON extraction leaf",
			definition: func() *opensplunk.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
					FieldExtraction: &opensplunk.FieldExtractionDefinition{
						Extraction: &opensplunk.FieldExtractionDefinition_Json{
							Json: &opensplunk.JsonFieldExtractionDefinition{Path: "value", OutputField: "value"},
						},
					},
				}
				return definition
			},
			message: func(definition *opensplunk.KnowledgeObjectDefinition) proto.Message {
				return definition.GetFieldExtraction().GetJson()
			},
		},
		{
			name: "calculated field leaf",
			definition: func() *opensplunk.KnowledgeObjectDefinition {
				definition := validBaseDefinition()
				definition.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{
					CalculatedField: &opensplunk.CalculatedFieldDefinition{
						DestinationField: "value", Expression: "1",
					},
				}
				return definition
			},
			message: func(definition *opensplunk.KnowledgeObjectDefinition) proto.Message {
				return definition.GetCalculatedField()
			},
		},
	}
	for _, test := range nestedBodies {
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
		mutate func(*opensplunk.KnowledgeObjectDefinition)
	}{
		{name: "unspecified scope", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_UNSPECIFIED
		}},
		{name: "future scope", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.SharingScope = opensplunk.SharingScope(99)
		}},
		{name: "exact marked wildcard", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].Value = "prod*"
			definition.Selector.IndexPatterns[0].MatchKind = opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT
		}},
		{name: "wildcard marked exact", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].Value = "prod"
			definition.Selector.IndexPatterns[0].MatchKind = opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD
		}},
		{name: "future selector kind", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0].MatchKind = opensplunk.KnowledgeSelectorMatchKind(99)
		}},
		{name: "nil selector entry", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Selector.IndexPatterns[0] = nil
		}},
		{name: "missing body", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Body = nil
		}},
		{name: "typed nil body", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{}
		}},
		{name: "future overwrite", mutate: func(definition *opensplunk.KnowledgeObjectDefinition) {
			definition.GetFieldAlias().OverwriteBehavior = opensplunk.KnowledgeOverwriteBehavior(99)
		}},
	}
	for _, test := range tests {
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
	regex.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunk.FieldExtractionDefinition{
			Extraction: &opensplunk.FieldExtractionDefinition_Regex{
				Regex: &opensplunk.RegexFieldExtractionDefinition{
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
	if normalizedRegex.ObjectType != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		normalizedRegex.Definition.GetFieldExtraction().GetInputField() != "_raw" ||
		normalizedRegex.Definition.GetFieldExtraction().GetOverwriteBehavior() != opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING ||
		normalizedRegex.Definition.GetFieldExtraction().GetRegex().GetOutputFields()[0] != "status" {
		t.Fatalf("canonical regex extraction = %#v", normalizedRegex.Definition.GetFieldExtraction())
	}

	json := validBaseDefinition()
	json.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunk.FieldExtractionDefinition{
			InputField: " _raw ",
			Extraction: &opensplunk.FieldExtractionDefinition_Json{
				Json: &opensplunk.JsonFieldExtractionDefinition{Path: " items{0}.name ", OutputField: " item.name "},
			},
			OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING,
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
	calculated.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{
		CalculatedField: &opensplunk.CalculatedFieldDefinition{
			DestinationField: " latency.class ",
			Expression:       "\t if(latency > 100, \"slow\", \"fast\") \r\n",
		},
	}
	normalizedCalculated, err := Normalize(calculated)
	if err != nil {
		t.Fatalf("Normalize(calculated): %v", err)
	}
	if normalizedCalculated.ObjectType != opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD ||
		normalizedCalculated.Definition.GetCalculatedField().GetExpression() != `if(latency > 100, "slow", "fast")` {
		t.Fatalf("canonical calculated field = %#v", normalizedCalculated.Definition.GetCalculatedField())
	}

	badInput := proto.Clone(regex).(*opensplunk.KnowledgeObjectDefinition)
	badInput.GetFieldExtraction().InputField = "message"
	assertInvalidDefinition(t, badInput)

	duplicateOutput := proto.Clone(regex).(*opensplunk.KnowledgeObjectDefinition)
	duplicateOutput.GetFieldExtraction().GetRegex().OutputFields = []string{"status", " status "}
	assertInvalidDefinition(t, duplicateOutput)

	tooManyOutputs := proto.Clone(regex).(*opensplunk.KnowledgeObjectDefinition)
	tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields = make([]string, MaximumFieldExtractionOutputs+1)
	for index := range tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields {
		tooManyOutputs.GetFieldExtraction().GetRegex().OutputFields[index] = strings.Repeat("f", index+1)
	}
	if _, err := Normalize(tooManyOutputs); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("too many regex outputs error = %v, want ErrDefinitionTooLarge", err)
	}

	missingExtraction := proto.Clone(regex).(*opensplunk.KnowledgeObjectDefinition)
	missingExtraction.GetFieldExtraction().Extraction = nil
	assertInvalidDefinition(t, missingExtraction)

	sameAlias := validAliasDefinition()
	sameAlias.GetFieldAlias().DestinationField = sameAlias.GetFieldAlias().SourceField
	assertInvalidDefinition(t, sameAlias)

	reservedDestination := validAliasDefinition()
	reservedDestination.GetFieldAlias().DestinationField = "INDEX.child"
	assertInvalidDefinition(t, reservedDestination)

	emptyExpression := proto.Clone(calculated).(*opensplunk.KnowledgeObjectDefinition)
	emptyExpression.GetCalculatedField().Expression = " \t\n "
	assertInvalidDefinition(t, emptyExpression)

	oversizedExpression := proto.Clone(calculated).(*opensplunk.KnowledgeObjectDefinition)
	oversizedExpression.GetCalculatedField().Expression = strings.Repeat("x", MaximumExpressionBytes+1)
	assertInvalidDefinition(t, oversizedExpression)

	oversizedPaddedExpression := proto.Clone(calculated).(*opensplunk.KnowledgeObjectDefinition)
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

func TestDecodeCanonicalFutureBodyPreservesCanonicalInactiveDefinition(t *testing.T) {
	t.Parallel()

	futureMetadata := protowire.AppendVarint(
		protowire.AppendTag(nil, 32, protowire.VarintType),
		7,
	)
	futureMetadata = protowire.AppendBytes(
		protowire.AppendTag(futureMetadata, 33, protowire.BytesType),
		[]byte("future metadata"),
	)
	canonical := futureDefinitionBytesWithMetadata(
		t, validBaseDefinition(), futureMetadata, 13, []byte{0x08, 0x01},
	)
	digest := sha256.Sum256(canonical)
	decoded, err := DecodeCanonicalInactiveFutureBody(
		canonical,
		digest[:],
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	)
	if err != nil {
		t.Fatalf("DecodeCanonicalInactiveFutureBody: %v", err)
	}
	if decoded.Digest != digest || !bytes.Equal(decoded.Bytes, canonical) ||
		decoded.AppID != validBaseDefinition().GetAppId() || decoded.Name != "revenue" ||
		decoded.SharingScope != opensplunk.SharingScope_SHARING_SCOPE_PRIVATE ||
		decoded.Description != nil || decoded.Selector == nil || decoded.Selector.Stats().Patterns != 1 {
		t.Fatalf("decoded future authorities = %#v", decoded)
	}
	wantUnknown := append(bytes.Clone(futureMetadata), canonicalFutureBodyField(13, []byte{0x08, 0x01})...)
	if decoded.Definition.GetBody() != nil ||
		!bytes.Equal(decoded.Definition.ProtoReflect().GetUnknown(), wantUnknown) {
		t.Fatalf("future body was not retained exactly: %x", decoded.Definition.ProtoReflect().GetUnknown())
	}
	if _, err := DecodeCanonical(canonical, digest[:]); !errors.Is(err, ErrNonCanonical) {
		t.Fatalf("strict decode error = %v, want ErrNonCanonical", err)
	}

	// Every returned mutable authority is detached from both the input and a
	// later decode, including the protobuf unknown-field storage.
	original := bytes.Clone(canonical)
	canonical[0] ^= 0xff
	decoded.Bytes[0] ^= 0xff
	decoded.Definition.Name = "mutated"
	decoded.Definition.ProtoReflect().SetUnknown(nil)
	again, err := DecodeCanonicalInactiveFutureBody(
		original,
		digest[:],
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	)
	if err != nil {
		t.Fatalf("DecodeCanonicalInactiveFutureBody(second): %v", err)
	}
	if again.Name != "revenue" || again.Definition.GetName() != "revenue" ||
		len(again.Definition.ProtoReflect().GetUnknown()) == 0 || again.Bytes[0] != original[0] {
		t.Fatalf("future decode retained caller-owned storage: %#v", again)
	}
}

func TestDecodeCanonicalFutureBodyRejectsAmbiguousOrNoncanonicalShapes(t *testing.T) {
	t.Parallel()

	known := validAliasDefinition()
	knownNormalized, err := Normalize(known)
	if err != nil {
		t.Fatal(err)
	}
	metadataOnly, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validBaseDefinition())
	if err != nil {
		t.Fatal(err)
	}
	varintUnknown := protowire.AppendVarint(
		protowire.AppendTag(bytes.Clone(metadataOnly), 13, protowire.VarintType),
		1,
	)
	reservedUnknown := append(bytes.Clone(metadataOnly), canonicalFutureBodyField(9, []byte{1})...)
	twoBodies := append(bytes.Clone(metadataOnly), canonicalFutureBodyField(13, []byte{1})...)
	twoBodies = append(twoBodies, canonicalFutureBodyField(14, []byte{2})...)
	compilerReservedMetadata := protowire.AppendBytes(
		protowire.AppendTag(nil, 19_000, protowire.BytesType), []byte{1},
	)
	compilerReserved := futureDefinitionBytesWithMetadata(
		t, validBaseDefinition(), compilerReservedMetadata, 13, []byte{1},
	)
	descendingMetadataFields := protowire.AppendVarint(
		protowire.AppendTag(nil, 33, protowire.VarintType), 1,
	)
	descendingMetadataFields = protowire.AppendVarint(
		protowire.AppendTag(descendingMetadataFields, 32, protowire.VarintType), 1,
	)
	descendingMetadata := futureDefinitionBytesWithMetadata(
		t, validBaseDefinition(), descendingMetadataFields, 13, []byte{1},
	)
	metadataAfterBody := append(bytes.Clone(metadataOnly), canonicalFutureBodyField(13, []byte{1})...)
	metadataAfterBody = protowire.AppendVarint(
		protowire.AppendTag(metadataAfterBody, 32, protowire.VarintType), 1,
	)
	overlongTag := append(bytes.Clone(metadataOnly), 0xea, 0x00, 0x01, 0x01)
	overlongLength := append(bytes.Clone(metadataOnly), 0x6a, 0x81, 0x00, 0x01)
	knownAndFuture := append(bytes.Clone(knownNormalized.Bytes), canonicalFutureBodyField(13, []byte{1})...)
	nestedUnknown := validBaseDefinition()
	nestedUnknown.Selector.ProtoReflect().SetUnknown(testUnknownField())
	nestedUnknownBytes := futureDefinitionBytes(t, nestedUnknown, []byte{1})
	noncanonicalMetadata := validBaseDefinition()
	noncanonicalMetadata.Name = " revenue "
	noncanonicalBytes := futureDefinitionBytes(t, noncanonicalMetadata, []byte{1})
	tooManyPatterns := validBaseDefinition()
	for len(tooManyPatterns.Selector.IndexPatterns) <= knowledge.MaximumSelectorPatternsPerDimension {
		tooManyPatterns.Selector.IndexPatterns = append(
			tooManyPatterns.Selector.IndexPatterns,
			&opensplunk.KnowledgeSelectorPattern{
				MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     "bounded",
			},
		)
	}
	tooManyPatternsBytes := futureDefinitionBytes(t, tooManyPatterns, []byte{1})
	duplicateName := protowire.AppendString(
		protowire.AppendTag(
			futureDefinitionBytes(t, validBaseDefinition(), []byte{1}),
			2,
			protowire.BytesType,
		),
		"revenue",
	)

	tests := []struct {
		name string
		data []byte
		err  error
	}{
		{name: "recognized body", data: knownNormalized.Bytes, err: ErrUnknownFutureBody},
		{name: "missing body", data: metadataOnly, err: ErrUnknownFutureBody},
		{name: "wrong wire type", data: varintUnknown, err: ErrUnknownFutureBody},
		{name: "reserved field number", data: reservedUnknown, err: ErrUnknownFutureBody},
		{name: "two future bodies", data: twoBodies, err: ErrUnknownFutureBody},
		{name: "compiler-reserved field", data: compilerReserved, err: ErrUnknownFutureBody},
		{name: "descending metadata", data: descendingMetadata, err: ErrUnknownFutureBody},
		{name: "metadata after body", data: metadataAfterBody, err: ErrUnknownFutureBody},
		{name: "overlong tag", data: overlongTag, err: ErrNonCanonical},
		{name: "overlong length", data: overlongLength, err: ErrUnknownFutureBody},
		{name: "known and future bodies", data: knownAndFuture, err: ErrUnknownFutureBody},
		{name: "nested unknown", data: nestedUnknownBytes, err: ErrNonCanonical},
		{name: "noncanonical metadata", data: noncanonicalBytes, err: ErrNonCanonical},
		{name: "preflight repeated values", data: tooManyPatternsBytes, err: ErrNonCanonical},
		{name: "duplicate known field", data: duplicateName, err: ErrNonCanonical},
		{name: "malformed", data: []byte{0x80}, err: ErrNonCanonical},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			digest := sha256.Sum256(test.data)
			if _, err := DecodeCanonicalInactiveFutureBody(
				test.data,
				digest[:],
				opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			); !errors.Is(err, test.err) {
				t.Fatalf("error = %v, want %v", err, test.err)
			}
		})
	}

	valid := futureDefinitionBytes(t, validBaseDefinition(), []byte{1})
	digest := sha256.Sum256(valid)
	wrong := digest
	wrong[0] ^= 0xff
	if _, err := DecodeCanonicalInactiveFutureBody(
		valid,
		wrong[:],
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong digest error = %v", err)
	}
	if _, err := DecodeCanonicalInactiveFutureBody(
		valid,
		digest[:sha256.Size-1],
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("short digest error = %v", err)
	}
	oversized := make([]byte, MaximumCanonicalBytes+1)
	oversizedDigest := sha256.Sum256(oversized)
	if _, err := DecodeCanonicalInactiveFutureBody(
		oversized,
		oversizedDigest[:],
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("oversized error = %v", err)
	}
}

func TestDecodeCanonicalInactiveFutureBodyEnforcesTrustedLifecycleState(t *testing.T) {
	t.Parallel()

	data := futureDefinitionBytes(t, validBaseDefinition(), []byte{1})
	digest := sha256.Sum256(data)
	for _, state := range []opensplunk.KnowledgeObjectState{
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED,
	} {
		if _, err := DecodeCanonicalInactiveFutureBody(data, digest[:], state); err != nil {
			t.Errorf("state %v: %v", state, err)
		}
	}
	for _, state := range []opensplunk.KnowledgeObjectState{
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED,
		opensplunk.KnowledgeObjectState(99),
	} {
		// The state gate deliberately runs before size, digest, or wire parsing.
		if _, err := DecodeCanonicalInactiveFutureBody(nil, nil, state); !errors.Is(err, ErrUnknownFutureBody) {
			t.Errorf("state %v error = %v, want ErrUnknownFutureBody", state, err)
		}
	}
}

func TestKnowledgeDefinitionDescriptorPinsFutureBodyNumberNamespace(t *testing.T) {
	t.Parallel()

	descriptor := (&opensplunk.KnowledgeObjectDefinition{}).ProtoReflect().Descriptor()
	body := descriptor.Oneofs().ByName("body")
	if body == nil || body.IsSynthetic() {
		t.Fatal("KnowledgeObjectDefinition.body oneof is missing or synthetic")
	}
	fields := descriptor.Fields()
	bodyDeclarationSeen := false
	previousMetadataNumber := protoreflect.FieldNumber(0)
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		number := int(field.Number())
		inBody := field.ContainingOneof() == body
		if inBody {
			bodyDeclarationSeen = true
		} else {
			if bodyDeclarationSeen {
				t.Errorf("ordinary metadata field %s (%d) is declared after the body oneof", field.Name(), number)
			}
			if field.Number() < previousMetadataNumber {
				t.Errorf("ordinary metadata field %s (%d) is not declared in ascending field-number order", field.Name(), number)
			}
			previousMetadataNumber = field.Number()
		}
		switch {
		case number <= 5:
			if inBody {
				t.Errorf("metadata field %s (%d) unexpectedly belongs to body", field.Name(), number)
			}
		case number <= 9:
			t.Errorf("field %s occupies compatibility-reserved gap %d", field.Name(), number)
		case number <= 31:
			if !inBody {
				t.Errorf("field %s (%d) must be a body alternative", field.Name(), number)
			}
		default:
			if inBody {
				t.Errorf("body field %s (%d) exceeds reserved body range", field.Name(), number)
			}
		}
	}
}

func TestNormalizeCollapsesEmptySelectorAndDescriptionPresence(t *testing.T) {
	t.Parallel()

	empty := ""
	definition := validAliasDefinition()
	definition.Description = &empty
	definition.Selector = &opensplunk.KnowledgeSelector{}
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
		[]*opensplunk.KnowledgeSelectorPattern,
		knowledge.MaximumSelectorPatternsPerDimension+1,
	)
	for index := range tooManyPatterns.Selector.IndexPatterns {
		tooManyPatterns.Selector.IndexPatterns[index] = &opensplunk.KnowledgeSelectorPattern{Value: "x"}
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

func validAliasDefinition() *opensplunk.KnowledgeObjectDefinition {
	definition := validBaseDefinition()
	definition.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunk.FieldAliasDefinition{
			SourceField:       "source.value",
			DestinationField:  "derived.value",
			OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
		},
	}
	return definition
}

func validBaseDefinition() *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		Name:         "revenue",
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
		Selector: &opensplunk.KnowledgeSelector{
			IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{
				MatchKind: opensplunk.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     "main",
			}},
		},
	}
}

func assertInvalidDefinition(t *testing.T, definition *opensplunk.KnowledgeObjectDefinition) {
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

func futureDefinitionBytes(
	t *testing.T,
	definition *opensplunk.KnowledgeObjectDefinition,
	payload []byte,
) []byte {
	t.Helper()
	return futureDefinitionBytesWithMetadata(t, definition, nil, 13, payload)
}

func futureDefinitionBytesWithMetadata(
	t *testing.T,
	definition *opensplunk.KnowledgeObjectDefinition,
	futureMetadata []byte,
	field protowire.Number,
	payload []byte,
) []byte {
	t.Helper()
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		t.Fatal(err)
	}
	withMetadata := append(known, futureMetadata...)
	return append(withMetadata, canonicalFutureBodyField(field, payload)...)
}

func canonicalFutureBodyField(field protowire.Number, payload []byte) []byte {
	return protowire.AppendBytes(
		protowire.AppendTag(nil, field, protowire.BytesType),
		payload,
	)
}
