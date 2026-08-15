package searchtimebounds_test

import (
	"math/rand/v2"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

// grammarPattern is an independent statement of the wire grammar the hand
// rolled scanner implements, used as a differential oracle.
var grammarPattern = regexp.MustCompile(
	`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$`,
)

func referenceValid(value string) bool {
	if !grammarPattern.MatchString(value) {
		return false
	}
	if strings.HasSuffix(value, "Z") {
		return true
	}
	offset := value[len(value)-5:]
	return offset[:2] <= "23" && offset[3:] <= "59"
}

func TestValidRFC3339NanoWireGrammarEdges(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		value string
		want  bool
	}{
		{"1970-01-01T00:00:00Z", true},
		{"1970-01-01T00:00:00+00:00", true},
		{"1970-01-01T00:00:00-00:00", true},
		{"0000-01-01T00:00:00Z", true},           // grammar allows year zero
		{"9999-12-31T23:59:59.999999999Z", true}, // maximum fraction width
		{"2016-12-31T23:59:60Z", true},           // leap-second spelling is grammatical
		{"2026-13-45T99:99:99Z", true},           // field ranges belong to time.Parse
		{"1970-01-01T00:00:00.1Z", true},
		{"1970-01-01T00:00:00.000000000+23:59", true},
		{"1970-01-01T00:00:00.1234567890Z", false}, // ten fraction digits
		{"1970-01-01T00:00:00.Z", false},           // period with no digits
		{"1970-01-01T00:00:00.", false},
		{"1970-01-01T00:00:00.123456789", false}, // fraction but no zone
		{"1970-01-01T00:00:00,1Z", false},        // comma fraction
		{"1970-01-01 00:00:00Z", false},          // missing T
		{"1970-01-01t00:00:00Z", false},          // lowercase designator
		{"1970-01-01T00:00:00z", false},          // lowercase zulu
		{"1970-01-01T00:00:00+24:00", false},
		{"1970-01-01T00:00:00+23:60", false},
		{"1970-01-01T00:00:00+0000", false}, // basic-format offset
		{"1970-01-01T00:00:00+00:0", false},
		{"1970-01-01T00:00:00Z ", false},
		{" 1970-01-01T00:00:00Z", false},
		{"1970-01-01T00:00:00Z\x00", false},
		{"1970-01-01T00:00:0Z", false}, // one byte short of the minimum
		{"", false},
		{"-970-01-01T00:00:00Z", false},
		{"+1970-01-01T00:00:0Z", false},
		{"١٩٧٠-01-01T00:00:00Z", false}, // non-ASCII digits
	} {
		t.Run(test.value, func(t *testing.T) {
			t.Parallel()
			if got := searchtimebounds.ValidRFC3339Nano(test.value); got != test.want {
				t.Fatalf("ValidRFC3339Nano(%q) = %t, want %t", test.value, got, test.want)
			}
			if got := referenceValid(test.value); got != test.want {
				t.Fatalf("reference grammar disagrees on %q: %t", test.value, got)
			}
		})
	}
}

// TestValidRFC3339NanoMatchesReferenceGrammarUnderMutation hunts for scanner
// index bugs by mutating a canonical timestamp in every position and length.
func TestValidRFC3339NanoMatchesReferenceGrammarUnderMutation(t *testing.T) {
	t.Parallel()
	alphabet := []byte("0129:-+.TZzt ,\x00")
	seeds := []string{
		"1970-01-01T00:00:00Z",
		"1970-01-01T00:00:00.123456789+05:30",
		"1970-01-01T00:00:00.1-14:00",
	}
	source := rand.New(rand.NewPCG(0x5eed, 0xf00d))
	for _, seed := range seeds {
		for index := range len(seed) {
			for _, replacement := range alphabet {
				mutated := seed[:index] + string(replacement) + seed[index+1:]
				if searchtimebounds.ValidRFC3339Nano(mutated) != referenceValid(mutated) {
					t.Fatalf("substitution mismatch on %q", mutated)
				}
				truncated := seed[:index] + string(replacement)
				if searchtimebounds.ValidRFC3339Nano(truncated) != referenceValid(truncated) {
					t.Fatalf("truncation mismatch on %q", truncated)
				}
				inserted := seed[:index] + string(replacement) + seed[index:]
				if searchtimebounds.ValidRFC3339Nano(inserted) != referenceValid(inserted) {
					t.Fatalf("insertion mismatch on %q", inserted)
				}
			}
		}
	}
	for range 20_000 {
		length := source.IntN(40)
		random := make([]byte, length)
		for index := range random {
			random[index] = alphabet[source.IntN(len(alphabet))]
		}
		candidate := string(random)
		if searchtimebounds.ValidRFC3339Nano(candidate) != referenceValid(candidate) {
			t.Fatalf("random mismatch on %q", candidate)
		}
	}
}

// TestSearchTimeIntentAgreesWithWireGrammar proves the searchtime consumer
// still gates on the same grammar and then defers real field ranges to
// time.Parse, so grammatical-but-impossible stamps are rejected downstream.
func TestSearchTimeIntentAgreesWithWireGrammar(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for _, test := range []struct {
		earliest    string
		wantIntent  bool
		wantResolve bool
	}{
		{earliest: "2026-01-01T00:00:00Z", wantIntent: true, wantResolve: true},
		{earliest: "2026-01-01T00:00:00+00:00", wantIntent: true, wantResolve: true},
		{earliest: "2026-01-01T00:00:00.999999999-05:00", wantIntent: true, wantResolve: true},
		{earliest: "2016-12-31T23:59:60Z", wantIntent: false}, // leap second
		{earliest: "2026-02-29T00:00:00Z", wantIntent: false}, // impossible date
		{earliest: "2026-01-01T00:00:00,5Z", wantIntent: false},
		{earliest: "2026-01-01 00:00:00Z", wantIntent: false},
		{earliest: "2026-01-01T00:00:00.1234567890Z", wantIntent: false},
		{earliest: "0000-01-01T00:00:00Z", wantIntent: true}, // parses, but out of domain
		{earliest: "9999-12-31T23:59:59Z", wantIntent: true}, // parses, but out of domain
	} {
		t.Run(test.earliest, func(t *testing.T) {
			t.Parallel()
			intent := searchtime.Intent{
				Earliest: test.earliest, Latest: "now", Timezone: "UTC",
			}
			if err := searchtime.ValidateIntent(intent); (err == nil) != test.wantIntent {
				t.Fatalf("ValidateIntent(%q) error = %v, want accepted = %t",
					test.earliest, err, test.wantIntent)
			}
			_, err := searchtime.Resolve(test.earliest, "now", nil, anchor)
			if (err == nil) != test.wantResolve {
				t.Fatalf("Resolve(%q) error = %v, want resolved = %t",
					test.earliest, err, test.wantResolve)
			}
		})
	}
}
