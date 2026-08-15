package hec

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestClassifyFieldNumberUsesExactHECTypedDomain(t *testing.T) {
	t.Parallel()
	tests := []struct {
		text string
		want FieldNumber
	}{
		{"0", FieldNumber{Kind: FieldNumberUint64, Uint64: 0}},
		{strconv.FormatUint(math.MaxUint64, 10), FieldNumber{Kind: FieldNumberUint64, Uint64: math.MaxUint64}},
		{"-0", FieldNumber{Kind: FieldNumberSint64, Sint64: 0}},
		{strconv.FormatInt(math.MinInt64, 10), FieldNumber{Kind: FieldNumberSint64, Sint64: math.MinInt64}},
		{"1.25", FieldNumber{Kind: FieldNumberDecimal, Decimal: "1.25"}},
		{"1E+001", FieldNumber{Kind: FieldNumberDecimal, Decimal: "1e1"}},
		{"-0.0e-001", FieldNumber{Kind: FieldNumberDecimal, Decimal: "-0.0e-1"}},
		{"1e1024", FieldNumber{Kind: FieldNumberDecimal, Decimal: "1e1024"}},
		{"1e-1024", FieldNumber{Kind: FieldNumberDecimal, Decimal: "1e-1024"}},
	}
	for _, test := range tests {
		got, err := ClassifyFieldNumber(json.Number(test.text))
		if err != nil || got != test.want {
			t.Errorf("ClassifyFieldNumber(%q) = %#v, %v, want %#v", test.text, got, err, test.want)
		}
	}
	invalid := []string{
		"", "-9223372036854775809", "18446744073709551616", "1e1025", "1e-1025",
		strings.Repeat("1", MaximumJSONNumberBytes+1), "not-a-number", "01", "+1", "1e+",
	}
	for _, text := range invalid {
		if got, err := ClassifyFieldNumber(json.Number(text)); err == nil || got != (FieldNumber{}) {
			t.Errorf("ClassifyFieldNumber(%q) = %#v, %v, want error", text, got, err)
		}
	}
}

func TestEnvelopeFieldsAcceptFlatTypedValuesAndRejectUnsupportedShapes(t *testing.T) {
	t.Parallel()
	valid := `{"event":"ok","fields":{"text":"value"," spaced ":1,"signed":-1,"unsigned":18446744073709551615,"decimal":1.25e+2,"bool":true,"null":null,"mixed":["x",1,-2,1.5,false,null]}}`
	if envelope := decodeOneEnvelope(t, valid); !envelope.Fields.Present {
		t.Fatal("valid fields were not retained")
	}
	tests := []string{
		`{"event":"ok","fields":{"nested":{}}}`,
		`{"event":"ok","fields":{"nested":[[]]}}`,
		`{"event":"ok","fields":{"overflow":18446744073709551616}}`,
		`{"event":"ok","fields":{"overflow":-9223372036854775809}}`,
		`{"event":"ok","fields":{"exponent":1e1025}}`,
		`{"event":"ok","fields":{"_time":1}}`,
		`{"event":"ok","fields":{"control\u0001":1}}`,
	}
	for _, body := range tests {
		decoder, err := NewEnvelopeDecoder(strings.NewReader(body), DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		_, err = decoder.Next()
		assertEventFailure(t, err, ErrorIndexedFields, 0)
	}
}

func TestEnvelopeFieldsEnforceAggregateArrayScalarBound(t *testing.T) {
	t.Parallel()
	items := strings.Repeat("0,", MaximumFieldArrayElements)
	items = strings.TrimSuffix(items, ",")
	valid := `{"event":"ok","fields":{"items":[` + items + `]}}`
	decodeOneEnvelope(t, valid)
	invalid := `{"event":"ok","fields":{"items":[` + items + `,0]}}`
	decoder, err := NewEnvelopeDecoder(strings.NewReader(invalid), DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	_, err = decoder.Next()
	assertEventFailure(t, err, ErrorIndexedFields, 0)
}

func FuzzClassifyFieldNumber(f *testing.F) {
	f.Add("0")
	f.Add("-1")
	f.Add("1.25e+2")
	f.Add("18446744073709551616")
	f.Fuzz(func(t *testing.T, text string) {
		if len(text) > MaximumJSONNumberBytes+1 {
			return
		}
		got, err := ClassifyFieldNumber(json.Number(text))
		if err != nil {
			return
		}
		switch got.Kind {
		case FieldNumberSint64:
			if !strings.HasPrefix(text, "-") || strings.ContainsAny(text, ".eE") {
				t.Fatalf("classified %q as sint64", text)
			}
		case FieldNumberUint64:
			if strings.HasPrefix(text, "-") || strings.ContainsAny(text, ".eE") {
				t.Fatalf("classified %q as uint64", text)
			}
		case FieldNumberDecimal:
			if !strings.ContainsAny(text, ".eE") {
				t.Fatalf("classified %q as decimal", text)
			}
		default:
			t.Fatalf("unknown kind %#v", got)
		}
	})
}
