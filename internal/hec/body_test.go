package hec

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"testing"
)

func TestIdentityBodyEnforcesExactDecompressedLimit(t *testing.T) {
	t.Parallel()
	limits := bodyTestLimits(5, 5)
	reader, err := NewBodyReader(bytes.NewReader([]byte("12345")), "", limits)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || string(got) != "12345" {
		t.Fatalf("exact body = %q, %v", got, err)
	}

	reader, err = NewBodyReader(bytes.NewReader([]byte("123456")), "", limits)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(reader)
	if string(got) != "12345" || !IsErrorKind(err, ErrorDecompressedBodyTooLarge) {
		t.Fatalf("oversized body = %q, %v", got, err)
	}
}

func TestSingleMemberGzipRoundTripsAndRejectsEveryTrailingForm(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"event":{"z":9007199254740993}}`)
	compressed := gzipBytes(t, payload)
	for _, encoding := range []string{"gzip", "GZIP", "GZip"} {
		reader, err := NewBodyReader(bytes.NewReader(compressed), encoding, DefaultLimits())
		if err != nil {
			t.Fatalf("NewBodyReader(%q): %v", encoding, err)
		}
		got, readErr := io.ReadAll(reader)
		if closeErr := reader.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if readErr != nil || !bytes.Equal(got, payload) {
			t.Fatalf("gzip %q = %q, %v", encoding, got, readErr)
		}
	}

	trailingCases := map[string][]byte{
		"arbitrary byte": append(bytes.Clone(compressed), '!'),
		"second member":  append(bytes.Clone(compressed), gzipBytes(t, []byte("second"))...),
	}
	for name, body := range trailingCases {
		t.Run(name, func(t *testing.T) {
			reader, err := NewBodyReader(bytes.NewReader(body), "gzip", DefaultLimits())
			if err != nil {
				if !IsErrorKind(err, ErrorInvalidCompressedBody) {
					t.Fatalf("constructor error = %v", err)
				}
				return
			}
			got, readErr := io.ReadAll(reader)
			if !bytes.Equal(got, payload) || !IsErrorKind(readErr, ErrorInvalidCompressedBody) {
				t.Fatalf("trailing gzip = %q, %v", got, readErr)
			}
		})
	}
}

func TestGzipRejectsInvalidTruncatedAndCorruptBodies(t *testing.T) {
	t.Parallel()
	valid := gzipBytes(t, []byte("payload"))
	corrupt := bytes.Clone(valid)
	corrupt[len(corrupt)-1] ^= 0xff
	tests := map[string][]byte{
		"empty":     {},
		"not gzip":  []byte("not gzip"),
		"truncated": valid[:len(valid)-3],
		"checksum":  corrupt,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			reader, err := NewBodyReader(bytes.NewReader(body), "gzip", DefaultLimits())
			if err != nil {
				if !IsErrorKind(err, ErrorInvalidCompressedBody) {
					t.Fatalf("NewBodyReader() error = %v", err)
				}
				return
			}
			_, readErr := io.ReadAll(reader)
			if !IsErrorKind(readErr, ErrorInvalidCompressedBody) {
				t.Fatalf("ReadAll() error = %v", readErr)
			}
		})
	}
}

func TestBodyReaderEnforcesCompressedAndDecompressedLimitsIndependently(t *testing.T) {
	t.Parallel()
	payload := bytes.Repeat([]byte("a"), 4096)
	compressed := gzipBytes(t, payload)

	compressedLimits := DefaultLimits()
	compressedLimits.MaximumCompressedBodyBytes = int64(len(compressed) - 1)
	reader, err := NewBodyReader(bytes.NewReader(compressed), "gzip", compressedLimits)
	if err == nil {
		_, err = io.ReadAll(reader)
	}
	if !IsErrorKind(err, ErrorCompressedBodyTooLarge) {
		t.Fatalf("compressed limit error = %v", err)
	}

	decompressedLimits := bodyTestLimits(int64(len(compressed)), 64)
	reader, err = NewBodyReader(bytes.NewReader(compressed), "gzip", decompressedLimits)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if len(got) != 64 || !IsErrorKind(err, ErrorDecompressedBodyTooLarge) {
		t.Fatalf("decompression limit = %d bytes, %v", len(got), err)
	}
}

func TestBodyReaderRejectsUnsupportedEncodingAndInvalidConfiguration(t *testing.T) {
	t.Parallel()
	for _, encoding := range []string{"identity", "br", " gzip", "gzip ", "gzip, gzip"} {
		reader, err := NewBodyReader(bytes.NewReader(nil), encoding, DefaultLimits())
		if reader != nil || !IsErrorKind(err, ErrorUnsupportedContentEncoding) {
			t.Errorf("encoding %q = %#v, %v", encoding, reader, err)
		}
	}
	if reader, err := NewBodyReader(nil, "", DefaultLimits()); reader != nil || !IsErrorKind(err, ErrorInternal) {
		t.Fatalf("nil source = %#v, %v", reader, err)
	}
	limits := DefaultLimits()
	limits.MaximumDecompressedBodyBytes = 0
	if reader, err := NewBodyReader(bytes.NewReader(nil), "", limits); reader != nil || !IsErrorKind(err, ErrorInternal) {
		t.Fatalf("invalid limits = %#v, %v", reader, err)
	}
}

func TestUTF8ValidatingReaderHandlesSplitSequencesAndRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	valid := []byte("a😀éz")
	reader := newUTF8ValidatingReader(&oneByteReader{source: bytes.NewReader(valid)})
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, valid) {
		t.Fatalf("split valid UTF-8 = %q, %v", got, err)
	}
	invalidCases := [][]byte{
		{0xff},
		{0xc0, 0x80},
		{0xe2, 0x82},
		{0xed, 0xa0, 0x80},
		{0xf4, 0x90, 0x80, 0x80},
	}
	for _, value := range invalidCases {
		_, err := io.ReadAll(newUTF8ValidatingReader(&oneByteReader{source: bytes.NewReader(value)}))
		if !IsErrorKind(err, ErrorInvalidUTF8) {
			t.Errorf("invalid UTF-8 %x error = %v", value, err)
		}
	}
}

func FuzzGzipLimitEnforcement(f *testing.F) {
	f.Add([]byte("payload"), int64(16))
	f.Add(bytes.Repeat([]byte("a"), 100), int64(10))
	f.Fuzz(func(t *testing.T, payload []byte, rawLimit int64) {
		if len(payload) > 1<<16 {
			return
		}
		limit := int64(uint64(rawLimit)%4096) + 1
		compressed := gzipBytes(t, payload)
		limits := bodyTestLimits(int64(len(compressed)), limit)
		reader, err := NewBodyReader(bytes.NewReader(compressed), "gzip", limits)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		if int64(len(got)) > limit {
			t.Fatalf("read %d bytes above %d", len(got), limit)
		}
		if int64(len(payload)) <= limit {
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatalf("bounded payload = %d bytes, err %v", len(got), err)
			}
		} else if !IsErrorKind(err, ErrorDecompressedBodyTooLarge) {
			t.Fatalf("oversized payload error = %v", err)
		}
	})
}

func gzipBytes(t testing.TB, payload []byte) []byte {
	t.Helper()
	var destination bytes.Buffer
	writer := gzip.NewWriter(&destination)
	if _, err := writer.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return destination.Bytes()
}

func bodyTestLimits(compressed, decompressed int64) Limits {
	limits := DefaultLimits()
	limits.MaximumCompressedBodyBytes = compressed
	limits.MaximumDecompressedBodyBytes = decompressed
	limits.MaximumEventBytes = min(decompressed, HardMaximumEventBytes)
	return limits
}

type oneByteReader struct{ source io.Reader }

func (reader *oneByteReader) Read(destination []byte) (int, error) {
	if len(destination) > 1 {
		destination = destination[:1]
	}
	return reader.source.Read(destination)
}

var _ = errors.Is
