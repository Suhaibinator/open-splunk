package hec

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEnvelopeDecoderStreamsConcatenatedObjectsAndPreservesExactValues(t *testing.T) {
	t.Parallel()
	source := " \n" +
		`{"host":"api-01","time":1786323296.123456789,"event":{"second":2,"first":9007199254740993,"exponent":1e+03},"fields":{"ok":true}}` +
		"\t" + `{"event":"line\n😀"}` + " \r\n"
	decoder, err := NewEnvelopeDecoder(strings.NewReader(source), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	first, err := decoder.Next()
	if err != nil {
		t.Fatal(err)
	}
	if first.Number != 0 || !first.Time.Present || first.Time.Value.Kind != JSONNumber ||
		first.Time.Value.NumberValue.String() != "1786323296.123456789" {
		t.Fatalf("first envelope = %#v", first)
	}
	raw, err := first.RawEvent()
	if err != nil {
		t.Fatal(err)
	}
	wantRaw := `{"second":2,"first":9007199254740993,"exponent":1e+03}`
	if string(raw) != wantRaw {
		t.Fatalf("first RawEvent() = %s, want %s", raw, wantRaw)
	}
	second, err := decoder.Next()
	if err != nil {
		t.Fatal(err)
	}
	raw, err = second.RawEvent()
	if err != nil || string(raw) != "line\n😀" || second.Number != 1 {
		t.Fatalf("second RawEvent() = %q, %v; envelope %#v", raw, err, second)
	}
	if _, err := decoder.Next(); !errors.Is(err, io.EOF) || decoder.Count() != 2 {
		t.Fatalf("final Next() = %v, count %d", err, decoder.Count())
	}
}

func TestEnvelopeDecoderEventDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		body     string
		wantRaw  string
		wantKind ErrorKind
	}{
		{name: "missing", body: `{}`, wantKind: ErrorEventRequired},
		{name: "null", body: `{"event":null}`, wantKind: ErrorEventBlank},
		{name: "empty string", body: `{"event":""}`, wantKind: ErrorEventBlank},
		{name: "empty object", body: `{"event":{}}`, wantRaw: `{}`},
		{name: "empty array", body: `{"event":[]}`, wantRaw: `[]`},
		{name: "boolean true", body: `{"event":true}`, wantRaw: `true`},
		{name: "boolean false", body: `{"event":false}`, wantRaw: `false`},
		{name: "number", body: `{"event":-0.000e+9}`, wantRaw: `-0.000e+9`},
		{name: "string", body: `{"event":"<exact>&"}`, wantRaw: `<exact>&`},
		{name: "string NUL", body: `{"event":"nul\u0000value"}`, wantKind: ErrorInvalidDataFormat},
		{name: "fields object", body: `{"fields":{},"event":"ok"}`, wantRaw: `ok`},
		{name: "fields array", body: `{"fields":[],"event":"ok"}`, wantKind: ErrorIndexedFields},
		{name: "fields null", body: `{"fields":null,"event":"ok"}`, wantKind: ErrorIndexedFields},
		{name: "missing event precedes duplicate field", body: `{"fields":{"key":1,"key":2}}`, wantKind: ErrorEventRequired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			decoder, err := NewEnvelopeDecoder(strings.NewReader(test.body), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := decoder.Next()
			if test.wantKind != 0 {
				assertEventFailure(t, err, test.wantKind, 0)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			raw, err := envelope.RawEvent()
			if err != nil || string(raw) != test.wantRaw {
				t.Fatalf("RawEvent() = %q, %v, want %q", raw, err, test.wantRaw)
			}
		})
	}
}

func TestEnvelopeDecoderRejectsUnknownDuplicateAndNonObjectFraming(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body []byte
	}{
		{name: "top-level array", body: []byte(`[{"event":"a"}]`)},
		{name: "top-level scalar", body: []byte(`true`)},
		{name: "unknown", body: []byte(`{"Event":"a","event":"b"}`)},
		{name: "duplicate event", body: []byte(`{"event":"first","event":"second"}`)},
		{name: "duplicate metadata", body: []byte(`{"host":"first","host":"second","event":"ok"}`)},
		{name: "duplicate event object member", body: []byte(`{"event":{"key":1,"key":2}}`)},
		{name: "duplicate fields member", body: []byte(`{"event":"ok","fields":{"key":1,"key":2}}`)},
		{name: "truncated envelope", body: []byte(`{"event":"a"`)},
		{name: "truncated value", body: []byte(`{"event":{"a":`)},
		{name: "invalid escape", body: []byte(`{"event":"\x00"}`)},
		{name: "invalid UTF-8", body: append([]byte(`{"event":"`), []byte{0xff, '"', '}'}...)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			decoder, err := NewEnvelopeDecoder(bytes.NewReader(test.body), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			_, err = decoder.Next()
			switch test.name {
			case "invalid UTF-8":
				assertRequestFailure(t, err, ErrorInvalidUTF8)
			case "duplicate fields member":
				assertEventFailure(t, err, ErrorIndexedFields, 0)
			default:
				assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
			}
			if _, secondErr := decoder.Next(); !errors.Is(secondErr, err) {
				t.Fatalf("decoder failure was not sticky: first %p second %p (%v)", err, secondErr, secondErr)
			}
		})
	}
}

func TestEnvelopeDecoderReportsFirstDeterministicZeroBasedEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		wantKind   ErrorKind
		wantNumber int
	}{
		{name: "second missing event", body: `{"event":"ok"}{"host":"h"}`, wantKind: ErrorEventRequired, wantNumber: 1},
		{name: "third duplicate", body: `{"event":1}{"event":2}{"event":3,"event":4}`, wantKind: ErrorInvalidDataFormat, wantNumber: 2},
		{name: "trailing garbage", body: `{"event":"ok"} garbage`, wantKind: ErrorInvalidDataFormat, wantNumber: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			decoder, err := NewEnvelopeDecoder(strings.NewReader(test.body), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			for number := 0; number < test.wantNumber; number++ {
				if _, err := decoder.Next(); err != nil {
					t.Fatalf("event %d: %v", number, err)
				}
			}
			_, err = decoder.Next()
			assertEventFailure(t, err, test.wantKind, test.wantNumber)
		})
	}
}

func TestEnvelopeDecoderKeepsLateGzipFailureRequestScoped(t *testing.T) {
	t.Parallel()
	compressed := gzipBytes(t, []byte(`{"event":"ok"}`))
	compressed[len(compressed)-8] ^= 0xff // corrupt the CRC after valid JSON bytes
	body, err := NewBodyReader(bytes.NewReader(compressed), "gzip", DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	decoder, err := NewEnvelopeDecoder(body, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if envelope, nextErr := decoder.Next(); nextErr != nil || envelope.Number != 0 {
		t.Fatalf("first envelope = %#v, %v", envelope, nextErr)
	}
	_, err = decoder.Next()
	assertRequestFailure(t, err, ErrorInvalidCompressedBody)
}

func TestEnvelopeDecoderResourceLimits(t *testing.T) {
	t.Parallel()
	t.Run("events", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEvents = 1
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":1}{"event":2}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decoder.Next(); err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 1)
	})
	t.Run("depth", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumJSONDepth = 1
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":{"nested":{}}}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
	})
	t.Run("values", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumJSONValues = 1
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":[1]}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
	})
	t.Run("members", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumObjectMembers = 1
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":{"a":1}}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
	})
	t.Run("event bytes", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEventBytes = 2
		limits.MaximumNormalizedBytes = 2
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":"abc"}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorEventTooLarge, 0)
	})
	t.Run("normalized bytes", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEventBytes = 8
		limits.MaximumNormalizedBytes = 8
		decoder, err := NewEnvelopeDecoder(strings.NewReader(`{"event":"a"}`), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		if !IsErrorKind(err, ErrorNormalizedBodyTooLarge) {
			t.Fatalf("normalized limit error = %v", err)
		}
		var failure *ProtocolError
		if !errors.As(err, &failure) || failure.InvalidEventNumber != nil {
			t.Fatalf("normalized request limit unexpectedly has event number: %#v", failure)
		}
	})
}

func TestEnvelopeDecoderEnforcesNumberLexicalBoundsEverywhere(t *testing.T) {
	t.Parallel()
	maximumInteger := strings.Repeat("9", MaximumJSONNumberBytes)
	for _, body := range []string{
		`{"event":{"number":` + maximumInteger + `}}`,
		`{"event":{"number":1e1024}}`,
		`{"event":{"number":0e-1024}}`,
	} {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decoder.Next(); err != nil {
			t.Fatalf("bounded number body %q: %v", body, err)
		}
	}
	for _, body := range []string{
		`{"event":{"number":` + maximumInteger + `0}}`,
		`{"event":{"number":1e1025}}`,
		`{"event":{"number":0e-1025}}`,
	} {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
	}

	decoder, err := NewEnvelopeDecoder(
		strings.NewReader(`{"event":"ok","fields":{"number":0e1025}}`),
		DefaultLimits(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = decoder.Next()
	assertEventFailure(t, err, ErrorIndexedFields, 0)
}

func TestEnvelopeDecoderEmptyAndWhitespaceOnlyBodyIsEOF(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"", " \n\r\t"} {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decoder.Next(); !errors.Is(err, io.EOF) || decoder.Count() != 0 {
			t.Fatalf("body %q Next() = %v count %d", body, err, decoder.Count())
		}
	}
}

func TestCompactJSONRejectsUninitializedValue(t *testing.T) {
	t.Parallel()
	if encoded, err := (JSONValue{}).CompactJSON(); err == nil || encoded != nil {
		t.Fatalf("CompactJSON() = %q, %v", encoded, err)
	}
}

func FuzzEnvelopeDecoder(f *testing.F) {
	f.Add([]byte(`{"event":"ok"}`))
	f.Add([]byte(`{"event":{"large":9007199254740993}} {"event":[]}`))
	f.Add([]byte(`{"event":null}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<16 {
			return
		}
		limits := DefaultLimits()
		limits.MaximumDecompressedBodyBytes = 1 << 16
		limits.MaximumNormalizedBytes = 1 << 16
		limits.MaximumEventBytes = 1 << 16
		decoder, err := NewEnvelopeDecoder(bytes.NewReader(body), limits)
		if err != nil {
			t.Fatal(err)
		}
		for count := 0; count <= limits.MaximumEvents; count++ {
			envelope, nextErr := decoder.Next()
			if nextErr != nil {
				if !errors.Is(nextErr, io.EOF) {
					if _, ok := ErrorKindOf(nextErr); !ok {
						t.Fatalf("non-protocol error: %T %v", nextErr, nextErr)
					}
				}
				return
			}
			raw, rawErr := envelope.RawEvent()
			if rawErr != nil || !utf8.Valid(raw) {
				t.Fatalf("accepted envelope raw = %q, %v", raw, rawErr)
			}
			if envelope.Event.Value.Kind != JSONString && !json.Valid(raw) {
				t.Fatalf("accepted structured raw is invalid JSON: %q", raw)
			}
		}
		t.Fatal("decoder exceeded its event limit without EOF or failure")
	})
}

func assertEventFailure(t testing.TB, err error, kind ErrorKind, eventNumber int) {
	t.Helper()
	var failure *ProtocolError
	if !errors.As(err, &failure) || failure.Kind != kind ||
		failure.InvalidEventNumber == nil || *failure.InvalidEventNumber != eventNumber {
		t.Fatalf("failure = %#v (%v), want kind %v event %d", failure, err, kind, eventNumber)
	}
}

func assertRequestFailure(t testing.TB, err error, kind ErrorKind) {
	t.Helper()
	var failure *ProtocolError
	if !errors.As(err, &failure) || failure.Kind != kind || failure.InvalidEventNumber != nil {
		t.Fatalf("failure = %#v (%v), want request kind %v", failure, err, kind)
	}
}
