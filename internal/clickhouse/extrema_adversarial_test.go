package clickhouse

import (
	"regexp"
	"strings"
	"testing"
)

func TestDynamicBytesPayloadValidityRequiresCanonicalRawStd(t *testing.T) {
	t.Parallel()

	pattern := strings.Trim(dynamicBytesPayloadPattern, "'")
	validRawStd := regexp.MustCompile(pattern)
	for _, test := range []struct {
		payload string
		valid   bool
	}{
		{payload: "", valid: true},
		{payload: "AA", valid: true},
		{payload: "AAA", valid: true},
		{payload: "AP8", valid: true},
		{payload: "0JA", valid: true},
		{payload: "A", valid: false},
		{payload: "AB", valid: false},
		{payload: "AAB", valid: false},
		{payload: "AA=", valid: false},
		{payload: "AA-", valid: false},
	} {
		test := test
		t.Run(test.payload, func(t *testing.T) {
			t.Parallel()
			if got := validRawStd.MatchString(test.payload); got != test.valid {
				t.Fatalf("canonical RawStd validity for %q = %t, want %t", test.payload, got, test.valid)
			}
		})
	}

	columnValidity := newDynamicEnvelopePayloadValiditySQL("payload_column").bytesValid
	if strings.ContainsAny(columnValidity, "?{}") {
		t.Fatalf("bytes-validity SQL contains driver parameter syntax: %s", columnValidity)
	}
	placeholderValidity := newDynamicEnvelopePayloadValiditySQL("?").bytesValid
	if got := strings.Count(placeholderValidity, "?"); got != 1 {
		t.Fatalf("bytes-validity SQL placeholder count = %d, want only the payload placeholder: %s", got, placeholderValidity)
	}
}

func TestStatsExtremaLexicalTypeTieBreakOrdersStringBeforeBytes(t *testing.T) {
	t.Parallel()

	if statsExtremaPublicationLexical >= statsExtremaPublicationEncodedBytes {
		t.Fatalf(
			"lexical publication kind %d must sort before bytes kind %d",
			statsExtremaPublicationLexical,
			statsExtremaPublicationEncodedBytes,
		)
	}

	orderingKey := statsExtremaOrderingKeySQL("raw_value", "exact_key", "publication_kind")
	if !strings.HasSuffix(orderingKey, ", raw_value), toUInt8(publication_kind))") {
		t.Fatalf("publication kind is not the final lexical ordering component: %s", orderingKey)
	}
}

func TestCompileStatsExtremaBindsAggregateWinnersWithinTightBounds(t *testing.T) {
	t.Parallel()

	compiled := compileSPL(
		t,
		`index=gradethis | stats min(metric) AS low max(metric) AS high`,
	)
	minAggregates := strings.Count(compiled.SQL, "argMinArray(")
	maxAggregates := strings.Count(compiled.SQL, "argMaxArray(")
	if minAggregates != 1 {
		t.Fatalf("argMinArray expression count = %d, want one winner\nSQL: %s", minAggregates, compiled.SQL)
	}
	if maxAggregates != 1 {
		t.Fatalf("argMaxArray expression count = %d, want one winner\nSQL: %s", maxAggregates, compiled.SQL)
	}
}
