package knowledgedefinition

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func FuzzInactiveFutureBodyCanonicalWireRoundTrip(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint8(0), uint64(0), []byte{})
	f.Add(uint8(18), uint16(31), uint8(1), ^uint64(0), []byte("opaque"))
	f.Add(uint8(7), uint16(19_000), uint8(2), uint64(42), []byte{0, 1, 2, 3})
	f.Add(uint8(1), uint16(65_535), uint8(3), uint64(1<<32), []byte("metadata"))

	f.Fuzz(func(t *testing.T, bodyOffset uint8, metadataOffset uint16, wireChoice uint8, integer uint64, value []byte) {
		if len(value) > 64<<10 {
			t.Skip()
		}
		bodyNumber := protowire.Number(13 + bodyOffset%19)
		metadataNumber := protowire.Number(32 + uint32(metadataOffset))
		if metadataNumber >= 19_000 && metadataNumber <= 19_999 {
			metadataNumber += 1_000
		}
		known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(adversarialFutureMetadata())
		if err != nil {
			t.Fatal(err)
		}
		var unknown []byte
		switch wireChoice % 4 {
		case 0:
			unknown = protowire.AppendVarint(
				protowire.AppendTag(unknown, metadataNumber, protowire.VarintType), integer,
			)
		case 1:
			unknown = protowire.AppendFixed64(
				protowire.AppendTag(unknown, metadataNumber, protowire.Fixed64Type), integer,
			)
		case 2:
			unknown = protowire.AppendBytes(
				protowire.AppendTag(unknown, metadataNumber, protowire.BytesType), value,
			)
		case 3:
			unknown = protowire.AppendFixed32(
				protowire.AppendTag(unknown, metadataNumber, protowire.Fixed32Type), uint32(integer),
			)
		}
		unknown = protowire.AppendBytes(
			protowire.AppendTag(unknown, bodyNumber, protowire.BytesType), value,
		)
		data := append(known, unknown...)
		if len(data) > MaximumCanonicalBytes {
			t.Skip()
		}
		digest := sha256.Sum256(data)

		for _, state := range []opensplunk.KnowledgeObjectState{
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED,
		} {
			decoded, decodeErr := DecodeCanonicalInactiveFutureBody(data, digest[:], state)
			if decodeErr != nil {
				t.Fatalf("canonical future wire rejected for state %v: %v", state, decodeErr)
			}
			if decoded.Digest != digest || !bytes.Equal(decoded.Bytes, data) ||
				!bytes.Equal(decoded.Definition.ProtoReflect().GetUnknown(), unknown) {
				t.Fatalf("successful future decode changed immutable wire authorities")
			}
		}
		if _, decodeErr := DecodeCanonicalInactiveFutureBody(
			data, digest[:], opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		); !errors.Is(decodeErr, ErrUnknownFutureBody) {
			t.Fatalf("active future body error = %v, want ErrUnknownFutureBody", decodeErr)
		}
		if _, decodeErr := DecodeCanonical(data, digest[:]); !errors.Is(decodeErr, ErrNonCanonical) {
			t.Fatalf("strict decoder accepted future body: %v", decodeErr)
		}
	})
}

func FuzzInactiveFutureBodyDisallowedStateAlwaysWins(f *testing.F) {
	f.Add(int32(opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE), []byte{}, []byte{})
	f.Add(int32(opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED), []byte{0x80}, make([]byte, 32))
	f.Add(int32(99), []byte("arbitrary"), []byte("wrong digest"))

	f.Fuzz(func(t *testing.T, rawState int32, data, digest []byte) {
		if len(data) > 64<<10 || len(digest) > 1<<10 {
			t.Skip()
		}
		state := opensplunk.KnowledgeObjectState(rawState)
		switch state {
		case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED:
			t.Skip()
		}
		_, err := DecodeCanonicalInactiveFutureBody(data, digest, state)
		if !errors.Is(err, ErrUnknownFutureBody) {
			t.Fatalf("state %v error = %v, want lifecycle ErrUnknownFutureBody", state, err)
		}
	})
}
