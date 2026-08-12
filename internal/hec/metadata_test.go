package hec

import (
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/jsonnumber"
)

func TestDecodeAndResolveMetadataPreservesPresenceAndPrecedence(t *testing.T) {
	t.Parallel()
	envelope := decodeOneEnvelope(t, `{"event":"ok","host":"event-host","source":"event-source","sourcetype":"event:type"}`)
	event, err := DecodeEnvelopeMetadata(envelope)
	if err != nil {
		t.Fatal(err)
	}
	token := MetadataValues{
		Host:       optional("token-host"),
		Source:     optional("token-source"),
		Sourcetype: optional("token:type"),
		Index:      optional("token-index"),
	}
	index := MetadataValues{
		Host: optional("forbidden-index-host"), Source: optional("forbidden-index-source"),
		Sourcetype: optional("index:type"), Index: optional("forbidden-index-name"),
	}
	fallback := DefaultMetadataFallbacks()
	// Presence, including an explicit empty value supplied by a validated
	// higher-level source, wins without fallback or trimming.
	event.Host = optional("")
	got := ResolveMetadata(event, token, index, fallback)
	if !got.Host.Present || got.Host.Value != "" || got.Source.Value != "event-source" ||
		got.Sourcetype.Value != "event:type" || got.Index.Value != "token-index" {
		t.Fatalf("ResolveMetadata() = %#v", got)
	}
	if absent := ResolveMetadata(MetadataValues{}, MetadataValues{}, MetadataValues{}, MetadataValues{}); absent != (MetadataValues{}) {
		t.Fatalf("absent ResolveMetadata() = %#v", absent)
	}
	withoutEventOrToken := ResolveMetadata(MetadataValues{}, MetadataValues{}, index, fallback)
	if withoutEventOrToken.Host.Value != "hec" || withoutEventOrToken.Source.Value != "http:hec" ||
		withoutEventOrToken.Sourcetype.Value != "index:type" || withoutEventOrToken.Index.Present {
		t.Fatalf("field-specific ResolveMetadata() = %#v", withoutEventOrToken)
	}
}

func TestDecodeEnvelopeMetadataRejectsWrongTypeNULAndOversize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		body string
		kind ErrorKind
	}{
		{`{"event":"ok","host":null}`, ErrorInvalidDataFormat},
		{`{"event":"ok","source":1}`, ErrorInvalidDataFormat},
		{`{"event":"ok","sourcetype":[]}`, ErrorInvalidDataFormat},
		{`{"event":"ok","index":{}}`, ErrorIncorrectIndex},
		{`{"event":"ok","index":"Not Canonical"}`, ErrorIncorrectIndex},
		{`{"event":"ok","host":""}`, ErrorInvalidDataFormat},
		{`{"event":"ok","host":" edge"}`, ErrorInvalidDataFormat},
		{`{"event":"ok","source":"edge "}`, ErrorInvalidDataFormat},
		{`{"event":"ok","host":"nul\u0000value"}`, ErrorInvalidDataFormat},
		{`{"event":"ok","source":"line\nvalue"}`, ErrorInvalidDataFormat},
		{`{"event":"ok","source":"` + strings.Repeat("x", MaximumMetadataValueBytes+1) + `"}`, ErrorInvalidDataFormat},
	}
	for _, test := range tests {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(test.body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, test.kind, 0)
	}
}

func TestParseEpochNanosecondsIsExactAndBounded(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		want int64
	}{
		{"0", 0},
		{"-0", 0},
		{"1", int64(time.Second)},
		{"1.000000001", int64(time.Second) + 1},
		{"1e-9", 1},
		{"100e-9", 100},
		{"-0.000000001", -1},
		{"1786323296.123456789", 1786323296123456789},
		{"9223372036.854775807", math.MaxInt64},
		{"-9223372036.854775808", math.MinInt64},
	}
	for _, test := range tests {
		got, err := ParseEpochNanoseconds(test.text)
		if err != nil || got != test.want {
			t.Errorf("ParseEpochNanoseconds(%q) = %d, %v, want %d", test.text, got, err, test.want)
		}
	}
	invalid := []string{
		"", "+1", " 1", "1 ", "01", ".1", "1.", "NaN", "Infinity",
		"0.0000000001", "1e-10", "9223372036.854775808", "-9223372036.854775809",
		"1e01", "1e+01", "1e-01", "1e1025", "1e-1025", "0e1025", "0e-1025",
		"1e10000", "1e-10000", strings.Repeat("1", MaximumEpochTextBytes+1),
	}
	for _, text := range invalid {
		if got, err := ParseEpochNanoseconds(text); err == nil {
			t.Errorf("ParseEpochNanoseconds(%q) = %d, want error", text, got)
		}
	}
}

func TestParseEnvelopeTimeUsesOneCapturedBoundaryOrExplicitExactTime(t *testing.T) {
	t.Parallel()
	received := time.Date(2026, time.August, 10, 12, 34, 56, 987654321, time.FixedZone("offset", -7*60*60))
	absent := decodeOneEnvelope(t, `{"event":"ok"}`)
	got, explicit, err := ParseEnvelopeTime(absent, received)
	if err != nil || explicit || !got.Equal(received) || got.Location() != time.UTC {
		t.Fatalf("absent ParseEnvelopeTime() = %s explicit=%t err=%v", got, explicit, err)
	}
	explicitEnvelope := decodeOneEnvelope(t, `{"event":"ok","time":"1786323296.123456789"}`)
	got, explicit, err = ParseEnvelopeTime(explicitEnvelope, time.Time{})
	if err != nil || !explicit || got.Unix() != 1786323296 || got.Nanosecond() != 123456789 {
		t.Fatalf("explicit ParseEnvelopeTime() = %s explicit=%t err=%v", got, explicit, err)
	}
	for _, body := range []string{
		`{"event":"ok","time":null}`,
		`{"event":"ok","time":true}`,
		`{"event":"ok","time":"1e-10"}`,
	} {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorInvalidDataFormat, 0)
	}
}

func FuzzParseEpochNanoseconds(f *testing.F) {
	f.Add("0")
	f.Add("1786323296.123456789")
	f.Add("1e-9")
	f.Add("9223372036.854775808")
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > MaximumEpochTextBytes+1 {
			return
		}
		got, err := ParseEpochNanoseconds(text)
		if err != nil {
			return
		}
		rat, parseErr := jsonnumber.ParseDecimalRat(text)
		if parseErr != nil {
			t.Fatalf("accepted unparsable decimal %q", text)
		}
		want := new(big.Rat).Mul(rat, big.NewRat(int64(time.Second), 1))
		if !want.IsInt() || !want.Num().IsInt64() || want.Num().Int64() != got {
			t.Fatalf("ParseEpochNanoseconds(%q) = %d, exact = %s", text, got, want.RatString())
		}
	})
}

func decodeOneEnvelope(t testing.TB, body string) Envelope {
	t.Helper()
	decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := decoder.Next()
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func optional(value string) OptionalString { return OptionalString{Present: true, Value: value} }
