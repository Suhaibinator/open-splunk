package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestInactiveFutureBodyLifecycleGatePrecedesEveryByteAuthority(t *testing.T) {
	t.Parallel()

	valid := futureDefinitionBytes(t, adversarialFutureMetadata(), []byte{0x08, 0x01})
	validDigest := sha256.Sum256(valid)
	wrongDigest := validDigest
	wrongDigest[0] ^= 0xff
	oversized := make([]byte, MaximumCanonicalBytes+1)
	malformed := []byte{0x80}
	malformedDigest := sha256.Sum256(malformed)

	inputs := []struct {
		name   string
		data   []byte
		digest []byte
	}{
		{name: "valid", data: valid, digest: validDigest[:]},
		{name: "empty and absent digest"},
		{name: "wrong digest", data: valid, digest: wrongDigest[:]},
		{name: "short digest", data: valid, digest: validDigest[:sha256.Size-1]},
		{name: "oversized", data: oversized, digest: make([]byte, sha256.Size)},
		{name: "malformed", data: malformed, digest: malformedDigest[:]},
	}
	disallowed := []opensplunkv1.KnowledgeObjectState{
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_UNSPECIFIED,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED,
		opensplunkv1.KnowledgeObjectState(-1),
		opensplunkv1.KnowledgeObjectState(99),
	}
	for _, state := range disallowed {
		for _, input := range inputs {
			t.Run(state.String()+"/"+input.name, func(t *testing.T) {
				t.Parallel()
				_, err := DecodeCanonicalInactiveFutureBody(input.data, input.digest, state)
				if !errors.Is(err, ErrUnknownFutureBody) {
					t.Fatalf("error = %v, want lifecycle ErrUnknownFutureBody", err)
				}
				for _, later := range []error{ErrDefinitionTooLarge, ErrDigestMismatch, ErrNonCanonical} {
					if errors.Is(err, later) {
						t.Fatalf("lifecycle gate leaked later validation class %v through %v", later, err)
					}
				}
			})
		}
	}
}

func TestKnowledgeDefinitionDescriptorPinsExactV01Namespace(t *testing.T) {
	t.Parallel()

	descriptor := (&opensplunkv1.KnowledgeObjectDefinition{}).ProtoReflect().Descriptor()
	body := descriptor.Oneofs().ByName("body")
	if body == nil || body.IsSynthetic() {
		t.Fatal("body must be a non-synthetic oneof")
	}
	want := map[protoreflect.FieldNumber]struct {
		name   protoreflect.Name
		kind   protoreflect.Kind
		oneof  bool
		option bool
	}{
		1:  {name: "app_id", kind: protoreflect.StringKind},
		2:  {name: "name", kind: protoreflect.StringKind},
		3:  {name: "description", kind: protoreflect.StringKind, option: true},
		4:  {name: "sharing_scope", kind: protoreflect.EnumKind},
		5:  {name: "selector", kind: protoreflect.MessageKind},
		10: {name: "field_extraction", kind: protoreflect.MessageKind, oneof: true},
		11: {name: "field_alias", kind: protoreflect.MessageKind, oneof: true},
		12: {name: "calculated_field", kind: protoreflect.MessageKind, oneof: true},
	}
	fields := descriptor.Fields()
	if fields.Len() != len(want) {
		t.Fatalf("descriptor has %d fields, want exact v0.1 count %d", fields.Len(), len(want))
	}
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		expected, ok := want[field.Number()]
		if !ok {
			t.Errorf("unexpected known field %s = %d", field.Name(), field.Number())
			continue
		}
		if field.Name() != expected.name || field.Kind() != expected.kind ||
			(field.ContainingOneof() == body) != expected.oneof || field.HasOptionalKeyword() != expected.option ||
			field.Cardinality() == protoreflect.Repeated {
			t.Errorf("field %d descriptor = name:%s kind:%s body:%t optional:%t cardinality:%s",
				field.Number(), field.Name(), field.Kind(), field.ContainingOneof() == body,
				field.HasOptionalKeyword(), field.Cardinality())
		}
	}
	if body.Fields().Len() != 3 {
		t.Fatalf("body oneof has %d alternatives, want 3", body.Fields().Len())
	}
	for number := protoreflect.FieldNumber(13); number <= 31; number++ {
		if descriptor.Fields().ByNumber(number) != nil {
			t.Errorf("future-body allocation %d is already occupied", number)
		}
	}
	for number := protoreflect.FieldNumber(32); number <= 63; number++ {
		if descriptor.Fields().ByNumber(number) != nil {
			t.Errorf("future-metadata allocation %d is unexpectedly occupied", number)
		}
	}
}

func TestInactiveFutureBodyAcceptsAllocationEdgesAndAllMetadataWireKinds(t *testing.T) {
	t.Parallel()

	for _, bodyNumber := range []protowire.Number{13, 31} {
		t.Run(string(rune('a'+bodyNumber-13)), func(t *testing.T) {
			t.Parallel()
			metadata, err := (proto.MarshalOptions{Deterministic: true}).Marshal(adversarialFutureMetadata())
			if err != nil {
				t.Fatal(err)
			}
			unknown := protowire.AppendVarint(protowire.AppendTag(nil, 32, protowire.VarintType), ^uint64(0))
			unknown = protowire.AppendFixed64(protowire.AppendTag(unknown, 33, protowire.Fixed64Type), 0xfeedfacecafebeef)
			unknown = protowire.AppendBytes(protowire.AppendTag(unknown, 34, protowire.BytesType), []byte("future metadata"))
			unknown = protowire.AppendFixed32(protowire.AppendTag(unknown, 35, protowire.Fixed32Type), 0xdecafbad)
			unknown = protowire.AppendBytes(protowire.AppendTag(unknown, 18_999, protowire.BytesType), []byte{})
			unknown = protowire.AppendVarint(protowire.AppendTag(unknown, 20_000, protowire.VarintType), 0)
			unknown = protowire.AppendBytes(protowire.AppendTag(unknown, protowire.Number(1<<29-1), protowire.BytesType), []byte{1})
			unknown = protowire.AppendBytes(
				protowire.AppendTag(unknown, bodyNumber, protowire.BytesType), []byte{0x08, 0x96, 0x01},
			)
			data := append(metadata, unknown...)
			digest := sha256.Sum256(data)

			decoded, err := DecodeCanonicalInactiveFutureBody(
				data, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			)
			if err != nil {
				t.Fatalf("DecodeCanonicalInactiveFutureBody: %v", err)
			}
			if !bytes.Equal(decoded.Definition.ProtoReflect().GetUnknown(), unknown) ||
				!bytes.Equal(decoded.Bytes, data) || decoded.Digest != digest {
				t.Fatal("unknown future body/metadata or immutable authorities changed")
			}
			if decoded.AppID != "app_AAAAAAAAAAAAAAAAAAAAAA" || decoded.Name != "future_name" ||
				decoded.SharingScope != opensplunkv1.SharingScope_SHARING_SCOPE_APP ||
				decoded.Description == nil || *decoded.Description != "future description" ||
				decoded.Selector == nil || decoded.Selector.Stats().Patterns != 2 {
				t.Fatalf("known metadata projection = %#v", decoded)
			}
		})
	}
}

func TestInactiveFutureBodyPreservesRepeatedFutureMetadataField(t *testing.T) {
	t.Parallel()

	futureMetadata := protowire.AppendString(protowire.AppendTag(nil, 32, protowire.BytesType), "first")
	futureMetadata = protowire.AppendString(
		protowire.AppendTag(futureMetadata, 32, protowire.BytesType), "second",
	)
	data := futureDefinitionBytesWithMetadata(
		t, adversarialFutureMetadata(), futureMetadata, 13, []byte{0x08, 0x01},
	)
	digest := sha256.Sum256(data)

	decoded, err := DecodeCanonicalInactiveFutureBody(
		data,
		digest[:],
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	)
	if err != nil {
		t.Fatalf("DecodeCanonicalInactiveFutureBody: %v", err)
	}
	if !bytes.Equal(decoded.Bytes, data) {
		t.Fatal("repeated future metadata changed stored byte authority")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(decoded.Definition)
	if err != nil {
		t.Fatalf("marshal decoded definition: %v", err)
	}
	if !bytes.Equal(encoded, data) {
		t.Fatalf("repeated future metadata did not round-trip: got %x want %x", encoded, data)
	}
}

func TestInactiveFutureBodyRejectsEveryAmbiguousWireClass(t *testing.T) {
	t.Parallel()

	metadata, err := (proto.MarshalOptions{Deterministic: true}).Marshal(adversarialFutureMetadata())
	if err != nil {
		t.Fatal(err)
	}
	body := func(number protowire.Number, wireType protowire.Type, value []byte) []byte {
		return append(append(bytes.Clone(metadata), protowire.AppendTag(nil, number, wireType)...), value...)
	}
	validBody := protowire.AppendBytes(protowire.AppendTag(nil, 13, protowire.BytesType), []byte{1})
	metadataThenBody := func(futureMetadata []byte) []byte {
		data := append(bytes.Clone(metadata), futureMetadata...)
		return append(data, validBody...)
	}
	descendingMetadataFields := protowire.AppendVarint(
		protowire.AppendTag(nil, 33, protowire.VarintType), 1,
	)
	descendingMetadataFields = protowire.AppendVarint(
		protowire.AppendTag(descendingMetadataFields, 32, protowire.VarintType), 1,
	)
	descendingMetadata := metadataThenBody(descendingMetadataFields)
	metadataAfterBody := append(bytes.Clone(metadata), validBody...)
	metadataAfterBody = protowire.AppendVarint(
		protowire.AppendTag(metadataAfterBody, 32, protowire.VarintType), 1,
	)
	compilerReservedLow := metadataThenBody(protowire.AppendVarint(
		protowire.AppendTag(nil, 19_000, protowire.VarintType), 1,
	))
	compilerReservedHigh := metadataThenBody(protowire.AppendVarint(
		protowire.AppendTag(nil, 19_999, protowire.VarintType), 1,
	))
	overlongMetadataVarint := append(bytes.Clone(metadata), protowire.AppendTag(nil, 32, protowire.VarintType)...)
	overlongMetadataVarint = append(overlongMetadataVarint, 0x81, 0x00)
	overlongMetadataVarint = append(overlongMetadataVarint, validBody...)
	truncatedMetadataFixed32 := append(
		bytes.Clone(metadata), protowire.AppendTag(nil, 32, protowire.Fixed32Type)...,
	)
	truncatedMetadataFixed32 = append(truncatedMetadataFixed32, 1, 2, 3)
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "body below allocation", data: body(9, protowire.BytesType, []byte{0}), want: ErrUnknownFutureBody},
		{name: "body varint", data: body(13, protowire.VarintType, []byte{1}), want: ErrUnknownFutureBody},
		{name: "body fixed64", data: body(13, protowire.Fixed64Type, make([]byte, 8)), want: ErrUnknownFutureBody},
		{name: "body fixed32", data: body(13, protowire.Fixed32Type, make([]byte, 4)), want: ErrUnknownFutureBody},
		{name: "body start group", data: body(13, protowire.StartGroupType, protowire.AppendTag(nil, 13, protowire.EndGroupType)), want: ErrUnknownFutureBody},
		{name: "metadata without body", data: protowire.AppendVarint(protowire.AppendTag(bytes.Clone(metadata), 32, protowire.VarintType), 1), want: ErrUnknownFutureBody},
		{name: "second same body", data: append(append(bytes.Clone(metadata), validBody...), validBody...), want: ErrUnknownFutureBody},
		{name: "second distinct body", data: protowire.AppendBytes(append(append(bytes.Clone(metadata), validBody...), protowire.AppendTag(nil, 31, protowire.BytesType)...), []byte{2}), want: ErrUnknownFutureBody},
		{name: "compiler reserved low", data: compilerReservedLow, want: ErrUnknownFutureBody},
		{name: "compiler reserved high", data: compilerReservedHigh, want: ErrUnknownFutureBody},
		{name: "metadata descending", data: descendingMetadata, want: ErrUnknownFutureBody},
		{name: "metadata after body", data: metadataAfterBody, want: ErrUnknownFutureBody},
		{name: "overlong body tag", data: append(bytes.Clone(metadata), []byte{0xea, 0x00, 0x01, 0x00}...), want: ErrNonCanonical},
		{name: "overlong body length", data: append(bytes.Clone(metadata), []byte{0x6a, 0x81, 0x00, 0x00}...), want: ErrUnknownFutureBody},
		{name: "overlong metadata varint", data: overlongMetadataVarint, want: ErrUnknownFutureBody},
		{name: "truncated body", data: append(bytes.Clone(metadata), []byte{0x6a, 0x02, 0x01}...), want: ErrNonCanonical},
		{name: "truncated metadata fixed32", data: truncatedMetadataFixed32, want: ErrNonCanonical},
		{name: "unknown before known metadata", data: append(bytes.Clone(validBody), metadata...), want: ErrNonCanonical},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			digest := sha256.Sum256(test.data)
			_, got := DecodeCanonicalInactiveFutureBody(
				test.data, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			)
			if !errors.Is(got, test.want) {
				t.Fatalf("error = %v, want %v", got, test.want)
			}
		})
	}
}

func TestInactiveFutureBodyPreservesDetachedKnownAndUnknownMetadata(t *testing.T) {
	t.Parallel()

	futureMetadata := protowire.AppendBytes(
		protowire.AppendTag(nil, 32, protowire.BytesType), []byte("opaque-metadata"),
	)
	data := futureDefinitionBytesWithMetadata(
		t, adversarialFutureMetadata(), futureMetadata, 17, []byte("opaque-body"),
	)
	original := bytes.Clone(data)
	digest := sha256.Sum256(data)
	decoded, err := DecodeCanonicalInactiveFutureBody(
		data, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantUnknown := bytes.Clone(decoded.Definition.ProtoReflect().GetUnknown())
	wantSelector := decoded.Selector.CanonicalBytes()
	if decoded.Description == nil || decoded.Definition.Description == nil ||
		decoded.Description == decoded.Definition.Description {
		t.Fatal("derived optional description must be present and separately owned")
	}

	data[0] ^= 0xff
	decoded.Bytes[0] ^= 0xff
	*decoded.Description = "caller mutation"
	*decoded.Definition.Description = "protobuf mutation"
	decoded.Definition.Selector.HostPatterns[0].Value = "attacker-*"
	decoded.Definition.ProtoReflect().SetUnknown(nil)

	again, err := DecodeCanonicalInactiveFutureBody(
		original, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again.Bytes, original) || !bytes.Equal(again.Definition.ProtoReflect().GetUnknown(), wantUnknown) ||
		again.Description == nil || *again.Description != "future description" ||
		!bytes.Equal(again.Selector.CanonicalBytes(), wantSelector) ||
		again.Definition.GetSelector().GetHostPatterns()[0].GetValue() != "api-*" {
		t.Fatalf("later decode observed caller mutation: %#v", again)
	}
}

func TestInactiveFutureBodyFourMiBBoundaryAndRepeatedShapePreflight(t *testing.T) {
	metadata := adversarialFutureMetadata()
	metadata.Selector = nil
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	payloadBytes := MaximumCanonicalBytes - len(known) - len(protowire.AppendTag(nil, 13, protowire.BytesType))
	for {
		lengthBytes := len(protowire.AppendVarint(nil, uint64(payloadBytes)))
		next := MaximumCanonicalBytes - len(known) - len(protowire.AppendTag(nil, 13, protowire.BytesType)) - lengthBytes
		if next == payloadBytes {
			break
		}
		payloadBytes = next
	}
	exact := protowire.AppendBytes(
		protowire.AppendTag(bytes.Clone(known), 13, protowire.BytesType),
		bytes.Repeat([]byte{0xa5}, payloadBytes),
	)
	if len(exact) != MaximumCanonicalBytes {
		t.Fatalf("constructed boundary = %d bytes, want %d", len(exact), MaximumCanonicalBytes)
	}
	digest := sha256.Sum256(exact)
	if _, err := DecodeCanonicalInactiveFutureBody(
		exact, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	); err != nil {
		t.Fatalf("exact four-MiB future body rejected: %v", err)
	}

	over := append(bytes.Clone(exact), 0)
	overDigest := sha256.Sum256(over)
	if _, err := DecodeCanonicalInactiveFutureBody(
		over, overDigest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	); !errors.Is(err, ErrDefinitionTooLarge) {
		t.Fatalf("boundary+1 error = %v, want ErrDefinitionTooLarge", err)
	}

	for _, dimension := range []struct {
		name string
		set  func(*opensplunkv1.KnowledgeSelector, []*opensplunkv1.KnowledgeSelectorPattern)
	}{
		{name: "index", set: func(s *opensplunkv1.KnowledgeSelector, p []*opensplunkv1.KnowledgeSelectorPattern) {
			s.IndexPatterns = p
		}},
		{name: "host", set: func(s *opensplunkv1.KnowledgeSelector, p []*opensplunkv1.KnowledgeSelectorPattern) {
			s.HostPatterns = p
		}},
		{name: "source", set: func(s *opensplunkv1.KnowledgeSelector, p []*opensplunkv1.KnowledgeSelectorPattern) {
			s.SourcePatterns = p
		}},
		{name: "sourcetype", set: func(s *opensplunkv1.KnowledgeSelector, p []*opensplunkv1.KnowledgeSelectorPattern) {
			s.SourcetypePatterns = p
		}},
	} {
		t.Run("selector/"+dimension.name, func(t *testing.T) {
			definition := adversarialFutureMetadata()
			patterns := make([]*opensplunkv1.KnowledgeSelectorPattern, knowledge.MaximumSelectorPatternsPerDimension+1)
			for index := range patterns {
				patterns[index] = &opensplunkv1.KnowledgeSelectorPattern{
					MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
					Value:     "x",
				}
			}
			dimension.set(definition.Selector, patterns)
			data := futureDefinitionBytes(t, definition, []byte{1})
			digest := sha256.Sum256(data)
			if _, err := DecodeCanonicalInactiveFutureBody(
				data, digest[:], opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			); !errors.Is(err, ErrNonCanonical) {
				t.Fatalf("error = %v, want ErrNonCanonical resource rejection", err)
			}
		})
	}
}

func TestInactiveFutureBodyErrorTaxonomyIsClosedAndStable(t *testing.T) {
	t.Parallel()

	valid := futureDefinitionBytes(t, adversarialFutureMetadata(), []byte{1})
	digest := sha256.Sum256(valid)
	known, err := Normalize(validAliasDefinition())
	if err != nil {
		t.Fatal(err)
	}
	malformed := []byte{0x80}
	malformedDigest := sha256.Sum256(malformed)
	noncanonicalMetadata := adversarialFutureMetadata()
	noncanonicalMetadata.Name = " future_name "
	noncanonical := futureDefinitionBytes(t, noncanonicalMetadata, []byte{1})
	noncanonicalDigest := sha256.Sum256(noncanonical)

	tests := []struct {
		name   string
		data   []byte
		digest []byte
		state  opensplunkv1.KnowledgeObjectState
		want   error
	}{
		{name: "empty", state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrDefinitionTooLarge},
		{name: "short digest", data: valid, digest: digest[:31], state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrDigestMismatch},
		{name: "wrong digest", data: valid, digest: make([]byte, 32), state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrDigestMismatch},
		{name: "malformed", data: malformed, digest: malformedDigest[:], state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrNonCanonical},
		{name: "known body", data: known.Bytes, digest: known.Digest[:], state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrUnknownFutureBody},
		{name: "noncanonical metadata", data: noncanonical, digest: noncanonicalDigest[:], state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED, want: ErrNonCanonical},
		{name: "active valid future", data: valid, digest: digest[:], state: opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE, want: ErrUnknownFutureBody},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, got := DecodeCanonicalInactiveFutureBody(test.data, test.digest, test.state)
			if !errors.Is(got, test.want) {
				t.Fatalf("error = %v, want %v", got, test.want)
			}
		})
	}
}

func adversarialFutureMetadata() *opensplunkv1.KnowledgeObjectDefinition {
	description := "future description"
	return &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        "app_AAAAAAAAAAAAAAAAAAAAAA",
		Name:         "future_name",
		Description:  &description,
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		Selector: &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
				MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_EXACT,
				Value:     "main",
			}},
			HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{
				MatchKind: opensplunkv1.KnowledgeSelectorMatchKind_KNOWLEDGE_SELECTOR_MATCH_KIND_WILDCARD,
				Value:     "api-*",
			}},
		},
	}
}
