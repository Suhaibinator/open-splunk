package hec

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeAcknowledgmentRequestPreservesUniqueSignedIDs(t *testing.T) {
	t.Parallel()
	request, err := DecodeAcknowledgmentRequest(
		strings.NewReader(" \n {\"acks\" : [1, 42, 9223372036854775807]} \r\n"),
		DefaultLimits(),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 42, 9223372036854775807}
	if !reflect.DeepEqual(request.IDs, want) {
		t.Fatalf("IDs = %v, want %v", request.IDs, want)
	}
}

func TestDecodeAcknowledgmentRequestRejectsEveryShapeExtension(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body []byte
		kind ErrorKind
	}{
		{name: "empty", kind: ErrorNoData},
		{name: "whitespace", body: []byte(" \n\t"), kind: ErrorNoData},
		{name: "top array", body: []byte(`[]`), kind: ErrorInvalidDataFormat},
		{name: "empty object", body: []byte(`{}`), kind: ErrorInvalidDataFormat},
		{name: "unknown", body: []byte(`{"ack":[1]}`), kind: ErrorInvalidDataFormat},
		{name: "duplicate decoded member", body: []byte(`{"acks":[1],"\u0061cks":[2]}`), kind: ErrorInvalidDataFormat},
		{name: "wrong member type", body: []byte(`{"acks":"1"}`), kind: ErrorInvalidDataFormat},
		{name: "empty IDs", body: []byte(`{"acks":[]}`), kind: ErrorInvalidDataFormat},
		{name: "string", body: []byte(`{"acks":["1"]}`), kind: ErrorInvalidDataFormat},
		{name: "zero", body: []byte(`{"acks":[0]}`), kind: ErrorInvalidDataFormat},
		{name: "negative", body: []byte(`{"acks":[-1]}`), kind: ErrorInvalidDataFormat},
		{name: "fraction", body: []byte(`{"acks":[1.0]}`), kind: ErrorInvalidDataFormat},
		{name: "exponent", body: []byte(`{"acks":[1e0]}`), kind: ErrorInvalidDataFormat},
		{name: "overflow", body: []byte(`{"acks":[9223372036854775808]}`), kind: ErrorInvalidDataFormat},
		{name: "duplicate ID", body: []byte(`{"acks":[1,1]}`), kind: ErrorInvalidDataFormat},
		{name: "second object", body: []byte(`{"acks":[1]}{"acks":[2]}`), kind: ErrorInvalidDataFormat},
		{name: "trailing garbage", body: []byte(`{"acks":[1]}x`), kind: ErrorInvalidDataFormat},
		{name: "truncated", body: []byte(`{"acks":[1`), kind: ErrorInvalidDataFormat},
		{name: "invalid UTF-8", body: append([]byte(`{"acks":[1]} `), 0xff), kind: ErrorInvalidUTF8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request, err := DecodeAcknowledgmentRequest(bytes.NewReader(test.body), DefaultLimits())
			if request.IDs != nil || !IsErrorKind(err, test.kind) {
				t.Fatalf("DecodeAcknowledgmentRequest() = %#v, %v, want kind %v", request, err, test.kind)
			}
		})
	}
}

func TestDecodeAcknowledgmentRequestEnforcesCountAndBodyLimits(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	limits.MaximumAcknowledgmentIDs = 2
	if _, err := DecodeAcknowledgmentRequest(strings.NewReader(`{"acks":[1,2,3]}`), limits); !IsErrorKind(err, ErrorInvalidDataFormat) {
		t.Fatalf("count limit error = %v", err)
	}
	body := `{"acks":[1]}`
	limits = DefaultLimits()
	limits.MaximumAcknowledgmentBodyBytes = int64(len(body))
	if request, err := DecodeAcknowledgmentRequest(strings.NewReader(body), limits); err != nil || !reflect.DeepEqual(request.IDs, []int64{1}) {
		t.Fatalf("exact body limit = %#v, %v", request, err)
	}
	limits.MaximumAcknowledgmentBodyBytes--
	if _, err := DecodeAcknowledgmentRequest(strings.NewReader(body), limits); !IsErrorKind(err, ErrorDecompressedBodyTooLarge) {
		t.Fatalf("body limit error = %v", err)
	}
}

func FuzzDecodeAcknowledgmentRequest(f *testing.F) {
	f.Add([]byte(`{"acks":[1,2,3]}`))
	f.Add([]byte(`{"acks":[]}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<15 {
			return
		}
		request, err := DecodeAcknowledgmentRequest(bytes.NewReader(body), DefaultLimits())
		if err != nil {
			if request.IDs != nil {
				t.Fatalf("failed request retained IDs: %v", request.IDs)
			}
			if _, ok := ErrorKindOf(err); !ok {
				t.Fatalf("non-protocol error: %T %v", err, err)
			}
			return
		}
		if len(request.IDs) == 0 || len(request.IDs) > HardMaximumAcknowledgmentIDs {
			t.Fatalf("accepted invalid count %d", len(request.IDs))
		}
		seen := make(map[int64]struct{}, len(request.IDs))
		for _, id := range request.IDs {
			if id <= 0 {
				t.Fatalf("accepted ID %d", id)
			}
			if _, duplicate := seen[id]; duplicate {
				t.Fatalf("accepted duplicate ID %d", id)
			}
			seen[id] = struct{}{}
		}
	})
}
