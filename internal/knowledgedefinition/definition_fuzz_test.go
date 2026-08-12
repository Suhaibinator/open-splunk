package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func FuzzDecodeCanonicalNeverPanicsAndSuccessIsStable(f *testing.F) {
	normalized, err := Normalize(validAliasDefinition())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(normalized.Bytes)
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Add([]byte{0x0a, 0x01, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaximumCanonicalBytes+1 {
			return
		}
		digest := sha256.Sum256(data)
		decoded, err := DecodeCanonical(data, digest[:])
		if err != nil {
			return
		}
		if !bytes.Equal(decoded.Bytes, data) || decoded.Digest != digest {
			t.Fatal("successful decode changed canonical bytes or digest")
		}
		again, err := Normalize(decoded.Definition)
		if err != nil {
			t.Fatalf("successful definition does not re-normalize: %v", err)
		}
		if !bytes.Equal(again.Bytes, decoded.Bytes) || again.Digest != decoded.Digest {
			t.Fatal("successful decode is not normalization-stable")
		}
	})
}

func FuzzNormalizeNeverPanicsAndSuccessIsIdempotent(f *testing.F) {
	normalized, err := Normalize(validAliasDefinition())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(normalized.Bytes)
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Add([]byte{0x0a, 0x01, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaximumCanonicalBytes {
			return
		}
		definition := &opensplunkv1.KnowledgeObjectDefinition{}
		if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, definition); err != nil {
			return
		}
		one, err := Normalize(definition)
		if err != nil {
			return
		}
		two, err := Normalize(one.Definition)
		if err != nil {
			t.Fatalf("successful normalization does not repeat: %v", err)
		}
		if !bytes.Equal(one.Bytes, two.Bytes) || one.Digest != two.Digest {
			t.Fatal("successful normalization is not idempotent")
		}
	})
}

func FuzzDecodeCanonicalFutureBodyNeverPanicsAndSuccessIsStable(f *testing.F) {
	metadata, err := (proto.MarshalOptions{Deterministic: true}).Marshal(validBaseDefinition())
	if err != nil {
		f.Fatal(err)
	}
	future := protowire.AppendBytes(
		protowire.AppendTag(bytes.Clone(metadata), 13, protowire.BytesType),
		[]byte{0x08, 0x01},
	)
	f.Add(future)
	f.Add(metadata)
	f.Add([]byte{})
	f.Add([]byte{0x80})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaximumCanonicalBytes+1 {
			return
		}
		digest := sha256.Sum256(data)
		decoded, err := DecodeCanonicalInactiveFutureBody(
			data,
			digest[:],
			opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		)
		if err != nil {
			return
		}
		if decoded.Digest != digest || !bytes.Equal(decoded.Bytes, data) ||
			decoded.Definition.GetBody() != nil || len(decoded.Definition.ProtoReflect().GetUnknown()) == 0 {
			t.Fatal("successful future decode changed canonical authorities")
		}
		again, err := DecodeCanonicalInactiveFutureBody(
			decoded.Bytes,
			decoded.Digest[:],
			opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
		)
		if err != nil {
			t.Fatalf("successful future decode is not repeatable: %v", err)
		}
		if !bytes.Equal(again.Bytes, decoded.Bytes) || again.Digest != decoded.Digest ||
			!proto.Equal(again.Definition, decoded.Definition) {
			t.Fatal("successful future decode is not stable")
		}
	})
}
