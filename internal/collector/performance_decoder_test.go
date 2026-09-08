package collector

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func TestPerformanceDecoderHistoricalEventID(t *testing.T) {
	t.Parallel()
	for _, inputID := range []string{"", "collector", "输入\x00id"} {
		for _, position := range []SourcePosition{{}, {FileIdentity: "generation", StartOffset: 12, EndOffset: 99}, {FileIdentity: "世代\x00", StartOffset: math.MaxUint64 - 1, EndOffset: math.MaxUint64}} {
			for _, raw := range [][]byte{nil, {}, []byte("hello"), {0, 255, '\n'}, []byte("é世界")} {
				// Assemble the historical wire preimage independently of the
				// streaming implementation, including byte-length prefixes.
				var preimage []byte
				for _, value := range []string{inputID, position.FileIdentity} {
					preimage = binary.BigEndian.AppendUint64(preimage, uint64(len(value)))
					preimage = append(preimage, value...)
				}
				preimage = binary.BigEndian.AppendUint64(preimage, position.StartOffset)
				preimage = binary.BigEndian.AppendUint64(preimage, position.EndOffset)
				preimage = binary.BigEndian.AppendUint64(preimage, uint64(len(raw)))
				preimage = append(preimage, raw...)
				digest := sha256.Sum256(preimage)
				want := hex.EncodeToString(digest[:])
				if got := stableEventID(inputID, position, raw); got != want {
					t.Fatalf("historical event ID = %q, want %q", got, want)
				}
				position.SourcePath = "changed"
				position.LineNumber = 100
				position.NextLineNumber = 101
				position.FileFingerprintLength = 42
				position.CheckpointGuardFingerprint = "guard"
				position.CheckpointGuardLength = 5
				if got := stableEventID(inputID, position, raw); got != want {
					t.Fatal("diagnostic and checkpoint fields changed event ID")
				}
			}
		}
	}
}

func TestPerformanceDecoderOwnership(t *testing.T) {
	t.Parallel()
	for _, constants := range []*opensplunk.TypedObject{nil, {Fields: []*opensplunk.TypedObjectField{
		{Name: "nested", Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ObjectValue{ObjectValue: &opensplunk.TypedObject{Fields: []*opensplunk.TypedObjectField{{Name: "value", Value: stringValue("trusted")}}}}}},
		{Name: "appended", Value: stringValue("last")},
	}}} {
		decoder := newTestDecoder(t, DecodeConfig{Format: InputFormatNDJSON, ConstantFields: constants})
		raw := []byte(`{"first":"one","nested":{"value":"source"},"last":true}`)
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		decode := func() *opensplunk.LogEvent {
			t.Helper()
			event, err := decoder.Decode(raw, SourcePosition{FileIdentity: "file", EndOffset: uint64(len(raw))}, now)
			if err != nil {
				t.Fatal(err)
			}
			return event
		}
		first, second := decode(), decode()
		want := proto.Clone(second).(*opensplunk.LogEvent)
		if constants != nil {
			if first.Fields.Fields[1].Name != "nested" || first.Fields.Fields[3].Name != "appended" {
				t.Fatal("constants changed dynamic order or append order")
			}
			assertStringValue(t, first.Fields.Fields[1].Value.GetObjectValue().Fields[0].Value, "trusted")
			constants.Fields[0].Value.GetObjectValue().Fields[0].Value = stringValue("mutated config")
		}
		first.Fields.Fields[1].Value.GetObjectValue().Fields[0].Value = stringValue("mutated event")
		first.Fields.Fields[0] = &opensplunk.TypedObjectField{Name: "changed", Value: stringValue("changed")}
		first.EventTime.Seconds++
		if first.CollectedAt.Seconds != want.CollectedAt.Seconds {
			t.Fatal("fallback timestamp aliases collection timestamp")
		}
		first.CollectedAt.Seconds++
		first.Raw[0] = '!'
		*first.Origin.EndOffset = 0
		if !proto.Equal(second, want) || !proto.Equal(decode(), want) {
			t.Fatal("event or configuration mutation escaped its owner")
		}
		raw[0] = '?'
		if !proto.Equal(second, want) {
			t.Fatal("event retained caller raw bytes")
		}
	}
}

func TestPerformanceDecoderEmptyFieldsShape(t *testing.T) {
	t.Parallel()
	for _, format := range []InputFormat{InputFormatNDJSON, InputFormatRaw} {
		decoder := newTestDecoder(t, DecodeConfig{Format: format})
		event, err := decoder.Decode([]byte("{}"), SourcePosition{}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if event.Fields == nil || event.Fields.Fields == nil || len(event.Fields.Fields) != 0 {
			t.Fatal("empty fields must retain their nonnil representation")
		}
	}
}
