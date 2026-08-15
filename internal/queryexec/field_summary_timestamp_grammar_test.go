package queryexec

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// TestDecodeFieldSummaryTimestampWireGrammarEdges extends the existing
// timestamp rejection table with the grammar boundaries the shared RFC 3339
// scanner owns, plus the canonicalization that is deliberately not the identity
// (offset spellings and trailing fractional zeros both collapse to Zulu).
func TestDecodeFieldSummaryTimestampWireGrammarEdges(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		encoded       string
		wantCanonical string
	}{
		{encoded: "2026-01-01T00:00:00Z", wantCanonical: "2026-01-01T00:00:00Z"},
		{encoded: "2026-01-01T00:00:00+00:00", wantCanonical: "2026-01-01T00:00:00Z"},
		{encoded: "2026-01-01T00:00:00-00:00", wantCanonical: "2026-01-01T00:00:00Z"},
		{encoded: "2026-01-01T00:00:00.000000000Z", wantCanonical: "2026-01-01T00:00:00Z"},
		{encoded: "2026-01-01T00:00:00.100000000Z", wantCanonical: "2026-01-01T00:00:00.1Z"},
		{encoded: "2026-01-01T05:30:00+05:30", wantCanonical: "2026-01-01T00:00:00Z"},
		{encoded: "9999-12-31T23:59:59.999999999Z", wantCanonical: "9999-12-31T23:59:59.999999999Z"},
		{encoded: "0001-01-01T00:00:00Z", wantCanonical: "0001-01-01T00:00:00Z"},
		{encoded: "2016-12-31T23:59:60Z"},                // leap second
		{encoded: "0000-12-31T23:59:59Z"},                // year zero
		{encoded: "0001-01-01T00:00:00+00:01"},           // UTC year zero after shift
		{encoded: "9999-12-31T23:59:59.999999999-00:01"}, // UTC year 10000 after shift
		{encoded: "2026-01-01T00:00:00.1234567890Z"},     // ten fractional digits
		{encoded: "2026-01-01T00:00:00.Z"},               // period with no digits
		{encoded: "2026-01-01T00:00:00.123456789"},       // fraction with no zone
		{encoded: "2026-01-01t00:00:00z"},                // lowercase designators
		{encoded: "2026-01-01T00:00:00Z "},               // trailing space
		{encoded: "2026-01-01T00:00:00+0000"},            // basic-format offset
	} {
		t.Run(test.encoded, func(t *testing.T) {
			t.Parallel()
			got, err := decodeFieldSummaryValue(
				eventfields.StoredValueTypeTimestamp, test.encoded,
			)
			if test.wantCanonical == "" {
				if err == nil {
					t.Fatalf("decodeFieldSummaryValue(%q) = %#v, want an error", test.encoded, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("decodeFieldSummaryValue(%q) error = %v", test.encoded, err)
			}
			if got.kind != searchjobs.ValueKindTime {
				t.Fatalf("kind = %v, want ValueKindTime", got.kind)
			}
			if got.canonical != test.wantCanonical {
				t.Fatalf(
					"canonical = %q, want %q", got.canonical, test.wantCanonical,
				)
			}
		})
	}
}
