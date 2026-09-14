package collector

import (
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func TestLogfmtPerformancePathPreservesNumericGrammar(t *testing.T) {
	t.Parallel()
	decoder := nativeTextDecoder(t, "logfmt", nil)

	for _, test := range []struct {
		name    string
		lexeme  string
		numeric bool
	}{
		{"zero", "0", true},
		{"negative zero", "-0", true},
		{"exponent", "1e+2", true},
		{"leading zero", "01", false},
		{"negative leading zero", "-01", false},
		{"missing fraction", "1.", false},
		{"missing exponent", "1e", false},
		{"missing signed exponent", "1e+", false},
		{"leading plus", "+1", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := nativeTextDecode(t, decoder, "value="+test.lexeme)
			value := fieldValue(event, "value")
			_, stringValue := value.GetKind().(*opensplunk.TypedValue_StringValue)
			if stringValue == test.numeric {
				t.Fatalf("value %q numeric = %v, want %v", test.lexeme, !stringValue, test.numeric)
			}
		})
	}
}

func TestLogfmtPlainQuotedValueOwnsDecodedText(t *testing.T) {
	t.Parallel()
	decoder := nativeTextDecoder(t, "logfmt", nil)
	raw := []byte(`message="request complete"`)
	event, err := decoder.Decode(raw, SourcePosition{}, nativeTextNow)
	if err != nil {
		t.Fatal(err)
	}
	raw[9] = 'X'
	if got := event.GetMessage(); got != "request complete" {
		t.Fatalf("message changed with caller buffer: %q", got)
	}
}

func TestLogfmtQuotedFastPathPreservesJSONEscapesAndValidation(t *testing.T) {
	t.Parallel()
	decoder := nativeTextDecoder(t, "logfmt", nil)
	event := nativeTextDecode(t, decoder, `message="line\nquote:\" slash:\\ snowman:\u2603 lock:\ud83d\udd10"`)
	if got, want := event.GetMessage(), "line\nquote:\" slash:\\ snowman:☃ lock:🔐"; got != want {
		t.Fatalf("decoded message = %q, want %q", got, want)
	}

	for _, raw := range [][]byte{
		[]byte("message=\"raw\ncontrol\""),
		[]byte(`message="bad\x20escape"`),
		[]byte(`message="bad\ud800"`),
		[]byte(`message="bad\udfff"`),
	} {
		if _, err := decoder.Decode(raw, SourcePosition{}, nativeTextNow); err == nil {
			t.Fatalf("accepted invalid quoted value %q", raw)
		}
	}
}

func TestLogfmtQuotedFastPathPreservesErrorDisposition(t *testing.T) {
	decoder, err := NewDecoder(DecodeConfig{Format: "logfmt", InputID: "error-bench", IndexName: "main", Source: "input.log", Sourcetype: "logfmt", Host: "host"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ raw, want string }{
		{"message=\"a\nb\"", "invalid logfmt string escape"},
		{"message=\"a\nb", "invalid quoted logfmt value"},
		{"message=\"a\nb\"tail", "invalid quoted logfmt value"},
	} {
		_, err := decoder.Decode([]byte(tc.raw), SourcePosition{}, time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC))
		if err == nil || err.Error() != tc.want {
			t.Errorf("error=%v, want %q", err, tc.want)
		}
	}
}
