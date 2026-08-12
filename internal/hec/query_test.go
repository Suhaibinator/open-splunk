package hec

import (
	"testing"
)

func TestParseRawQueryDecodesExactMetadataAndChannel(t *testing.T) {
	t.Parallel()
	raw := "time=1786323296.123456789&host=raw-host&source=%2Fvar%2Flog%2Fraw+file.log&" +
		"sourcetype=raw%3Atest&index=main&channel=FE0ECFAD-13D5-401B-847D-77833BD77131"
	got, err := ParseRawQuery(raw, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !got.Time.Present || got.Time.Value != "1786323296.123456789" ||
		got.Metadata.Host.Value != "raw-host" || got.Metadata.Source.Value != "/var/log/raw file.log" ||
		got.Metadata.Sourcetype.Value != "raw:test" || got.Metadata.Index.Value != "main" ||
		!got.ChannelPresent || string(got.Channel) != "FE0ECFAD-13D5-401B-847D-77833BD77131" {
		t.Fatalf("ParseRawQuery() = %#v", got)
	}
	values := got.ChannelValues()
	if len(values) != 1 || values[0] != string(got.Channel) {
		t.Fatalf("ChannelValues() = %q", values)
	}
	empty, err := ParseRawQuery("", DefaultLimits())
	if err != nil || empty != (RawQuery{}) || empty.ChannelValues() != nil {
		t.Fatalf("empty ParseRawQuery() = %#v, %v", empty, err)
	}
}

func TestParseRawQueryRejectsClosedSyntaxAndUsesStableCategories(t *testing.T) {
	t.Parallel()
	guid := "FE0ECFAD-13D5-401B-847D-77833BD77131"
	tests := []struct {
		name string
		raw  string
		kind ErrorKind
	}{
		{name: "token", raw: "token=secret", kind: ErrorQueryAuthorizationDisabled},
		{name: "blank token", raw: "token=", kind: ErrorQueryAuthorizationDisabled},
		{name: "unsupported", raw: "fields=status", kind: ErrorInvalidDataFormat},
		{name: "empty name", raw: "=value", kind: ErrorInvalidDataFormat},
		{name: "malformed escape", raw: "host=%zz", kind: ErrorInvalidDataFormat},
		{name: "empty host", raw: "host=", kind: ErrorInvalidDataFormat},
		{name: "edge host whitespace", raw: "host=+value", kind: ErrorInvalidDataFormat},
		{name: "bad time", raw: "time=1e-10", kind: ErrorInvalidDataFormat},
		{name: "bad index", raw: "index=Not+Canonical", kind: ErrorIncorrectIndex},
		{name: "empty index", raw: "index=", kind: ErrorIncorrectIndex},
		{name: "bad channel", raw: "channel=not-a-guid", kind: ErrorChannelInvalid},
		{name: "empty channel", raw: "channel=", kind: ErrorChannelInvalid},
		{name: "duplicate channel", raw: "channel=" + guid + "&channel=" + guid, kind: ErrorChannelInvalid},
		{name: "duplicate metadata", raw: "host=one&host=two", kind: ErrorInvalidDataFormat},
		{name: "leading empty segment", raw: "&host=one", kind: ErrorInvalidDataFormat},
		{name: "middle empty segment", raw: "host=one&&source=two", kind: ErrorInvalidDataFormat},
		{name: "trailing empty segment", raw: "host=one&", kind: ErrorInvalidDataFormat},
		{name: "channel precedence", raw: "time=1e-10&channel=bad", kind: ErrorChannelInvalid},
		{name: "semicolon separator", raw: "host=one;source=two", kind: ErrorInvalidDataFormat},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseRawQuery(test.raw, DefaultLimits())
			if got != (RawQuery{}) || !IsErrorKind(err, test.kind) {
				t.Fatalf("ParseRawQuery(%q) = %#v, %v, want kind %v", test.raw, got, err, test.kind)
			}
		})
	}
	limits := DefaultLimits()
	limits.MaximumRequestTargetBytes = 8
	if _, err := ParseRawQuery("host=value", limits); !IsErrorKind(err, ErrorInvalidDataFormat) {
		t.Fatalf("target limit error = %v", err)
	}
}

func FuzzParseRawQuery(f *testing.F) {
	f.Add("host=node&index=main")
	f.Add("channel=FE0ECFAD-13D5-401B-847D-77833BD77131")
	f.Add("token=secret")
	f.Add("host=%zz")
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 1<<12 {
			return
		}
		query, err := ParseRawQuery(raw, DefaultLimits())
		if err != nil {
			if _, ok := ErrorKindOf(err); !ok {
				t.Fatalf("non-protocol error: %T %v", err, err)
			}
			return
		}
		if query.ChannelPresent {
			if _, parseErr := ParseChannel(string(query.Channel), HardMaximumChannelBytes); parseErr != nil {
				t.Fatalf("accepted invalid channel %q", query.Channel)
			}
		}
		for _, value := range []OptionalString{query.Metadata.Host, query.Metadata.Source, query.Metadata.Sourcetype} {
			if value.Present && !ValidTextMetadata(value.Value) {
				t.Fatalf("accepted invalid metadata %q", value.Value)
			}
		}
	})
}
