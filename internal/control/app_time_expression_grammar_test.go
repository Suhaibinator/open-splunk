package control

import (
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchtimebounds"
)

// TestValidAppTimeExpressionMirrorsTheRFC3339WireGrammar keeps control's
// dependency-cycle-avoiding copy of the time-expression validator byte-for-byte
// consistent with the shared grammar plus time.Parse, in both endpoint roles.
func TestValidAppTimeExpressionMirrorsTheRFC3339WireGrammar(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00+00:00",
		"2026-01-01T00:00:00-00:00",
		"2026-01-01T00:00:00.999999999+23:59",
		"2026-01-01T00:00:00.1234567890Z",
		"2016-12-31T23:59:60Z",
		"2026-02-29T00:00:00Z",
		"2026-13-45T99:99:99Z",
		"0000-01-01T00:00:00Z",
		"9999-12-31T23:59:59Z",
		"2026-01-01 00:00:00Z",
		"2026-01-01t00:00:00z",
		"2026-01-01T00:00:00,5Z",
		"2026-01-01T00:00:00+24:00",
		"2026-01-01T00:00:00",
		"2026-01-01T00:00:0Z",
		"",
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()
			_, parseErr := time.Parse(time.RFC3339Nano, value)
			want := searchtimebounds.ValidRFC3339Nano(value) && parseErr == nil
			for _, earliest := range []bool{true, false} {
				if got := validAppTimeExpression(value, earliest); got != want {
					t.Fatalf(
						"validAppTimeExpression(%q, earliest=%t) = %t, want %t",
						value, earliest, got, want,
					)
				}
			}
		})
	}
}

// TestNormalizeAppTimeRangeGatesAbsoluteBoundariesOnTheSharedGrammar drives the
// same grammar through the app-definition normalizer, including the endpoint
// asymmetry for the earliest-data sentinel and whitespace-only trimming.
func TestNormalizeAppTimeRangeGatesAbsoluteBoundariesOnTheSharedGrammar(t *testing.T) {
	t.Parallel()
	text := func(value string) *string { return &value }
	for name, test := range map[string]struct {
		earliest *string
		latest   *string
		wantErr  bool
	}{
		"zulu and offset spellings both accepted": {
			earliest: text("2026-01-01T00:00:00Z"),
			latest:   text("2026-01-02T00:00:00+00:00"),
		},
		"maximum fractional width accepted": {
			earliest: text("2026-01-01T00:00:00.123456789-05:00"),
			latest:   text("now"),
		},
		"leap second rejected": {
			earliest: text("2016-12-31T23:59:60Z"), latest: text("now"), wantErr: true,
		},
		"ten fractional digits rejected": {
			earliest: text("2026-01-01T00:00:00.1234567890Z"), latest: text("now"), wantErr: true,
		},
		"missing designator rejected": {
			earliest: text("2026-01-01 00:00:00Z"), latest: text("now"), wantErr: true,
		},
		"out-of-range offset rejected": {
			earliest: text("2026-01-01T00:00:00+24:00"), latest: text("now"), wantErr: true,
		},
		"surrounding whitespace is trimmed, not rejected": {
			earliest: text("  2026-01-01T00:00:00Z  "), latest: text("now"),
		},
		"earliest-data sentinel is earliest-only": {
			earliest: text("2026-01-01T00:00:00Z"), latest: text("0"), wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := normalizeAppTimeRange(&AppTimeRange{
				Earliest: test.earliest, Latest: test.latest,
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("normalizeAppTimeRange() error = %v, want error = %t", err, test.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("normalizeAppTimeRange() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}
