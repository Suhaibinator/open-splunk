package hec

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRawDecoderImplementsFixedLFCRLFBreaker(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want []string
	}{
		{name: "empty"},
		{name: "only empty LF", body: "\n\n"},
		{name: "only empty CRLF", body: "\r\n\r\n"},
		{name: "single final", body: "one", want: []string{"one"}},
		{name: "single LF", body: "one\n", want: []string{"one"}},
		{name: "single CRLF", body: "one\r\n", want: []string{"one"}},
		{name: "mixed and skipped", body: "\nfirst\r\n\r\nsecond\nthird", want: []string{"first", "second", "third"}},
		{name: "strip one CR only", body: "value\r\r\n", want: []string{"value\r"}},
		{name: "unterminated CR preserved", body: "value\r", want: []string{"value\r"}},
		{name: "unicode", body: "😀\né\r\n", want: []string{"😀", "é"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := decodeRawAll(strings.NewReader(test.body), DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("events = %q, want %q", got, test.want)
			}
			for index := range got {
				if string(got[index]) != test.want[index] {
					t.Fatalf("events[%d] = %q, want %q", index, got[index], test.want[index])
				}
			}
		})
	}
}

func TestRawDecoderLimitsUTF8AndStickyFailure(t *testing.T) {
	t.Parallel()
	t.Run("invalid UTF-8 second event", func(t *testing.T) {
		body := append([]byte("ok\n"), 0xff, '\n')
		decoder, err := NewRawDecoder(bytes.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if got, err := decoder.Next(); err != nil || string(got) != "ok" {
			t.Fatalf("first = %q, %v", got, err)
		}
		_, firstErr := decoder.Next()
		assertRequestFailure(t, firstErr, ErrorInvalidUTF8)
		_, secondErr := decoder.Next()
		if firstErr != secondErr {
			t.Fatalf("failure not sticky: %p != %p", firstErr, secondErr)
		}
	})
	t.Run("NUL second event", func(t *testing.T) {
		decoder, err := NewRawDecoder(bytes.NewReader([]byte{'o', 'k', '\n', 'x', 0, 'y'}), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		if got, err := decoder.Next(); err != nil || string(got) != "ok" {
			t.Fatalf("first = %q, %v", got, err)
		}
		_, err = decoder.Next()
		assertRequestFailure(t, err, ErrorInvalidDataFormat)
	})
	t.Run("event bytes exact with CRLF", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEventBytes = 3
		limits.MaximumNormalizedBytes = 3
		decoder, err := NewRawDecoder(strings.NewReader("abc\r\n"), limits)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decoder.Next()
		if err != nil || string(got) != "abc" {
			t.Fatalf("exact event = %q, %v", got, err)
		}
	})
	t.Run("event bytes over", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEventBytes = 3
		limits.MaximumNormalizedBytes = 3
		decoder, err := NewRawDecoder(strings.NewReader("abcd\n"), limits)
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorEventTooLarge, 0)
		if failure := err.(*ProtocolError); failure.HTTPStatus() != 413 {
			t.Fatalf("raw event limit HTTP status = %d, want 413", failure.HTTPStatus())
		}
	})
	t.Run("event count ignores empty", func(t *testing.T) {
		limits := DefaultLimits()
		limits.MaximumEvents = 2
		decoder, err := NewRawDecoder(strings.NewReader("a\n\n\nb\n\nc"), limits)
		if err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"a", "b"} {
			got, err := decoder.Next()
			if err != nil || string(got) != want {
				t.Fatalf("Next() = %q, %v, want %q", got, err, want)
			}
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 2)
	})
}

func TestRawDecoderHandlesFragmentsBeyondBufferWithoutJoiningRequests(t *testing.T) {
	t.Parallel()
	large := strings.Repeat("x", 40<<10)
	decoder, err := NewRawDecoder(&oneByteReader{source: strings.NewReader(large + "\nnext")}, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	first, err := decoder.Next()
	if err != nil || string(first) != large {
		t.Fatalf("first fragmented event = %d bytes, %v", len(first), err)
	}
	second, err := decoder.Next()
	if err != nil || string(second) != "next" {
		t.Fatalf("second fragmented event = %q, %v", second, err)
	}
	if _, err := decoder.Next(); !errors.Is(err, io.EOF) || decoder.Count() != 2 {
		t.Fatalf("EOF = %v, count %d", err, decoder.Count())
	}
	// A new request never inherits an unterminated prefix from the first.
	separate, err := NewRawDecoder(strings.NewReader("tail"), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	got, err := separate.Next()
	if err != nil || string(got) != "tail" {
		t.Fatalf("separate request = %q, %v", got, err)
	}
}

func FuzzRawBreaker(f *testing.F) {
	f.Add([]byte("a\r\n\nb\nc"))
	f.Add([]byte(""))
	f.Add([]byte{0xff, '\n'})
	f.Fuzz(func(t *testing.T, body []byte) {
		if len(body) > 1<<16 {
			return
		}
		limits := DefaultLimits()
		limits.MaximumDecompressedBodyBytes = 1 << 16
		limits.MaximumEventBytes = 1 << 16
		limits.MaximumNormalizedBytes = 1 << 16
		got, err := decodeRawAll(bytes.NewReader(body), limits)
		if err != nil {
			if _, ok := ErrorKindOf(err); !ok {
				t.Fatalf("non-protocol error: %T %v", err, err)
			}
			return
		}
		want := referenceRawBreak(body)
		if len(got) != len(want) {
			t.Fatalf("event count %d, want %d", len(got), len(want))
		}
		for index := range got {
			if !bytes.Equal(got[index], want[index]) || !utf8.Valid(got[index]) {
				t.Fatalf("event %d = %x, want %x", index, got[index], want[index])
			}
		}
	})
}

func decodeRawAll(source io.Reader, limits Limits) ([][]byte, error) {
	decoder, err := NewRawDecoder(source, limits)
	if err != nil {
		return nil, err
	}
	var events [][]byte
	for {
		event, err := decoder.Next()
		if errors.Is(err, io.EOF) {
			return events, nil
		}
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
}

func referenceRawBreak(body []byte) [][]byte {
	segments := bytes.Split(body, []byte{'\n'})
	if len(body) != 0 && body[len(body)-1] == '\n' {
		segments = segments[:len(segments)-1]
	}
	result := make([][]byte, 0, len(segments))
	for index, segment := range segments {
		terminated := index < len(segments)-1 || len(body) != 0 && body[len(body)-1] == '\n'
		if terminated && len(segment) != 0 && segment[len(segment)-1] == '\r' {
			segment = segment[:len(segment)-1]
		}
		if len(segment) == 0 || !utf8.Valid(segment) {
			if !utf8.Valid(segment) {
				return nil
			}
			continue
		}
		result = append(result, bytes.Clone(segment))
	}
	return result
}
