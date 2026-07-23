package searchtime

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestResolveUsesOneAnchorAndPreservesNormalizedIntent(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, time.July, 22, 12, 34, 56, 987654321, time.FixedZone("west", -7*60*60))
	timezone := " America/Los_Angeles "
	resolved, err := Resolve(" -24h ", " now ", &timezone, anchor)
	if err != nil {
		t.Fatal(err)
	}
	wantLatest := anchor.Round(0).UTC()
	if !resolved.Earliest().Equal(wantLatest.Add(-24*time.Hour)) || !resolved.Latest().Equal(wantLatest) {
		t.Fatalf("resolved range = [%s, %s), want [%s, %s)", resolved.Earliest(), resolved.Latest(), wantLatest.Add(-24*time.Hour), wantLatest)
	}
	intent := resolved.Intent()
	if intent.Earliest != "-24h" || intent.Latest != "now" ||
		intent.Timezone != "America/Los_Angeles" || !intent.TimezoneSpecified {
		t.Fatalf("normalized intent = %+v", intent)
	}

	withoutTimezone, err := Resolve("-2h", "-1h", nil, anchor)
	if err != nil {
		t.Fatal(err)
	}
	defaultIntent := withoutTimezone.Intent()
	if !withoutTimezone.Earliest().Equal(wantLatest.Add(-2*time.Hour)) ||
		!withoutTimezone.Latest().Equal(wantLatest.Add(-time.Hour)) ||
		defaultIntent.Timezone != "UTC" || defaultIntent.TimezoneSpecified {
		t.Fatalf("default-zone range = %+v", withoutTimezone)
	}
	withLeadingZeros, err := Resolve("-024h", "now", nil, anchor)
	if err != nil || !withLeadingZeros.Earliest().Equal(wantLatest.Add(-24*time.Hour)) {
		t.Fatalf("zero-padded relative range = (%+v, %v)", withLeadingZeros, err)
	}
}

func TestResolveAcceptsStrictRFC3339NanoAndStorageBounds(t *testing.T) {
	t.Parallel()

	timezone := "UTC"
	resolved, err := Resolve(
		"2026-07-22T10:00:00.123456789-02:00",
		"2026-07-22T13:00:00Z",
		&timezone,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantEarliest := time.Date(2026, 7, 22, 12, 0, 0, 123456789, time.UTC)
	if !resolved.Earliest().Equal(wantEarliest) || !resolved.Latest().Equal(time.Date(2026, 7, 22, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("resolved absolute range = [%s, %s)", resolved.Earliest(), resolved.Latest())
	}
	minimum := clickhouse.MinimumSearchTime()
	maximum := clickhouse.MaximumSearchTime()
	if _, err := Resolve(minimum.Format(time.RFC3339Nano), maximum.Format(time.RFC3339Nano), nil, time.Time{}); err != nil {
		t.Fatalf("exact storage bounds rejected: %v", err)
	}
	oneNanosecond, err := Resolve(maximum.Add(-time.Nanosecond).Format(time.RFC3339Nano), maximum.Format(time.RFC3339Nano), nil, time.Time{})
	if err != nil || oneNanosecond.Latest().Sub(oneNanosecond.Earliest()) != time.Nanosecond {
		t.Fatalf("one-nanosecond range = %+v, error = %v", oneNanosecond, err)
	}
}

func TestNewAbsoluteRangeProducesImmutableCanonicalIntent(t *testing.T) {
	t.Parallel()

	earliest := time.Date(2026, 7, 22, 10, 0, 0, 123456789, time.FixedZone("west", -7*60*60))
	latest := earliest.Add(time.Hour)
	resolved, err := NewAbsoluteRange(earliest, latest)
	if err != nil {
		t.Fatal(err)
	}
	intent := resolved.Intent()
	if !resolved.Valid() || !resolved.Earliest().Equal(earliest) || !resolved.Latest().Equal(latest) ||
		resolved.Earliest().Location() != time.UTC || resolved.Latest().Location() != time.UTC ||
		intent.Earliest != earliest.UTC().Format(time.RFC3339Nano) ||
		intent.Latest != latest.UTC().Format(time.RFC3339Nano) ||
		intent.Timezone != "UTC" || intent.TimezoneSpecified {
		t.Fatalf("absolute range = intent %+v [%s, %s), valid %t", intent, resolved.Earliest(), resolved.Latest(), resolved.Valid())
	}
	if (Range{}).Valid() {
		t.Fatal("zero range is valid")
	}
	if _, err := NewAbsoluteRange(latest, earliest); err == nil {
		t.Fatal("inverted absolute range unexpectedly succeeded")
	}
}

func TestResolveDistinguishesCalendarDaysFromElapsedHoursAcrossDST(t *testing.T) {
	t.Parallel()

	zone := "America/Los_Angeles"
	location, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		anchor time.Time
		want   time.Duration
	}{
		{name: "spring forward", anchor: time.Date(2026, time.March, 8, 12, 0, 0, 123, location), want: 23 * time.Hour},
		{name: "fall back", anchor: time.Date(2026, time.November, 1, 12, 0, 0, 456, location), want: 25 * time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			calendar, err := Resolve("-1d", "now", &zone, test.anchor)
			if err != nil {
				t.Fatal(err)
			}
			elapsed, err := Resolve("-24h", "now", &zone, test.anchor)
			if err != nil {
				t.Fatal(err)
			}
			if got := calendar.Latest().Sub(calendar.Earliest()); got != test.want {
				t.Fatalf("calendar-day duration = %s, want %s", got, test.want)
			}
			if got := elapsed.Latest().Sub(elapsed.Earliest()); got != 24*time.Hour {
				t.Fatalf("elapsed-hour duration = %s, want 24h", got)
			}
			wantWallClock := test.anchor.In(location).AddDate(0, 0, -1)
			if !calendar.Earliest().Equal(wantWallClock) || calendar.Earliest().Equal(elapsed.Earliest()) {
				t.Fatalf("resolved earliest: calendar=%s elapsed=%s want calendar=%s", calendar.Earliest(), elapsed.Earliest(), wantWallClock)
			}
		})
	}
}

func TestLoadLocationCachesOnlySuccessfulZonesConcurrently(t *testing.T) {
	t.Parallel()

	const (
		workers = 32
		zone    = "Pacific/Chatham"
	)
	start := make(chan struct{})
	locations := make(chan *time.Location, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			location, err := loadLocation(zone)
			if err != nil {
				errs <- err
				return
			}
			locations <- location
		}()
	}
	close(start)
	group.Wait()
	close(locations)
	close(errs)

	for err := range errs {
		t.Errorf("loadLocation(%q) error = %v", zone, err)
	}
	var cached *time.Location
	for location := range locations {
		if cached == nil {
			cached = location
			continue
		}
		if location != cached {
			t.Fatalf("concurrent cache returned distinct locations: %p and %p", cached, location)
		}
	}
	if cached == nil {
		t.Fatal("concurrent cache returned no locations")
	}
	if stored, ok := locationCache.Load(zone); !ok || stored != cached {
		t.Fatalf("cached location = %v, want %p", stored, cached)
	}

	invalid := "OpenSplunk/Invalid"
	if _, err := loadLocation(invalid); err == nil {
		t.Fatalf("loadLocation(%q) unexpectedly succeeded", invalid)
	}
	if _, ok := locationCache.Load(invalid); ok {
		t.Fatalf("failed location %q was cached", invalid)
	}
	if _, err := loadLocation("Local"); !errors.Is(err, errLocalTimezone) {
		t.Fatalf("loadLocation(Local) error = %v, want %v", err, errLocalTimezone)
	}
	if _, ok := locationCache.Load("Local"); ok {
		t.Fatal("server-local location was cached")
	}
}

func TestResolveRejectsUnsupportedOrUnsafeExpressions(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	huge := "-" + strings.Repeat("9", MaximumExpressionBytes) + "h"
	oversizedPaddedExpression := "-1h" + strings.Repeat(" ", MaximumExpressionBytes)
	oversizedPaddedTimezone := "UTC" + strings.Repeat(" ", MaximumTimezoneBytes)
	tests := []struct {
		name     string
		earliest string
		latest   string
		zone     *string
		anchor   time.Time
	}{
		{name: "blank earliest", earliest: " ", latest: "now", anchor: anchor},
		{name: "blank latest", earliest: "-1h", latest: " ", anchor: anchor},
		{name: "zero relative", earliest: "-0h", latest: "now", anchor: anchor},
		{name: "positive relative", earliest: "+1h", latest: "now", anchor: anchor},
		{name: "unsigned relative", earliest: "1h", latest: "now", anchor: anchor},
		{name: "compound relative", earliest: "now-1h", latest: "now", anchor: anchor},
		{name: "unsupported week", earliest: "-1w", latest: "now", anchor: anchor},
		{name: "fractional relative", earliest: "-1.5h", latest: "now", anchor: anchor},
		{name: "uppercase now", earliest: "-1h", latest: "NOW", anchor: anchor},
		{name: "duration overflow", earliest: huge, latest: "now", anchor: anchor},
		{name: "raw expression bound before trim", earliest: oversizedPaddedExpression, latest: "now", anchor: anchor},
		{name: "numeric overflow", earliest: "-18446744073709551616s", latest: "now", anchor: anchor},
		{name: "calendar overflow", earliest: "-2147483648d", latest: "now", anchor: anchor},
		{name: "comma fraction", earliest: "2026-07-22T10:00:00,1Z", latest: "2026-07-22T11:00:00Z"},
		{name: "excess fraction", earliest: "2026-07-22T10:00:00.1234567890Z", latest: "2026-07-22T11:00:00Z"},
		{name: "lowercase zone", earliest: "2026-07-22T10:00:00z", latest: "2026-07-22T11:00:00Z"},
		{name: "missing zone", earliest: "2026-07-22T10:00:00", latest: "2026-07-22T11:00:00Z"},
		{name: "invalid date", earliest: "2026-02-30T10:00:00Z", latest: "2026-07-22T11:00:00Z"},
		{name: "equal", earliest: "now", latest: "now", anchor: anchor},
		{name: "inverted", earliest: "-1h", latest: "-2h", anchor: anchor},
		{name: "relative outside storage", earliest: "-1d", latest: "now", anchor: clickhouse.MinimumSearchTime()},
		{name: "absolute outside storage", earliest: "1899-12-31T23:59:59Z", latest: "1900-01-01T00:00:01Z"},
	}
	emptyZone := " "
	invalidZone := "Mars/Olympus_Mons"
	localZone := "Local"
	tests = append(tests,
		struct {
			name     string
			earliest string
			latest   string
			zone     *string
			anchor   time.Time
		}{name: "blank timezone", earliest: "-1h", latest: "now", zone: &emptyZone, anchor: anchor},
		struct {
			name     string
			earliest string
			latest   string
			zone     *string
			anchor   time.Time
		}{name: "invalid timezone", earliest: "-1h", latest: "now", zone: &invalidZone, anchor: anchor},
		struct {
			name     string
			earliest string
			latest   string
			zone     *string
			anchor   time.Time
		}{name: "server-local timezone", earliest: "-1d", latest: "now", zone: &localZone, anchor: anchor},
		struct {
			name     string
			earliest string
			latest   string
			zone     *string
			anchor   time.Time
		}{name: "raw timezone bound before trim", earliest: "-1h", latest: "now", zone: &oversizedPaddedTimezone, anchor: anchor},
	)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Resolve(test.earliest, test.latest, test.zone, test.anchor); err == nil {
				t.Fatalf("Resolve(%q, %q) unexpectedly succeeded", test.earliest, test.latest)
			}
		})
	}
}

func FuzzResolveMaintainsRangeInvariants(f *testing.F) {
	anchor := time.Date(2026, time.March, 8, 19, 0, 0, 123456789, time.UTC)
	for _, seed := range []struct {
		earliest, latest, timezone string
		specified                  bool
	}{
		{earliest: "-1d", latest: "now", timezone: "America/Los_Angeles", specified: true},
		{earliest: "-24h", latest: "now"},
		{earliest: "2026-03-08T10:00:00Z", latest: "2026-03-08T11:00:00.000000001Z", timezone: "UTC", specified: true},
		{earliest: "-18446744073709551616s", latest: "NOW", timezone: "Mars/Olympus_Mons", specified: true},
	} {
		f.Add(seed.earliest, seed.latest, seed.timezone, seed.specified)
	}
	f.Fuzz(func(t *testing.T, earliest, latest, timezone string, specified bool) {
		var timezonePointer *string
		if specified {
			timezonePointer = &timezone
		}
		resolved, err := Resolve(earliest, latest, timezonePointer, anchor)
		if err != nil {
			return
		}
		intent := resolved.Intent()
		if err := ValidateIntent(intent); err != nil {
			t.Fatalf("successful Resolve returned invalid intent %+v: %v", intent, err)
		}
		if !resolved.Earliest().Before(resolved.Latest()) ||
			!clickhouse.SupportsSearchTimeRange(resolved.Earliest(), resolved.Latest()) ||
			resolved.Earliest().Location() != time.UTC || resolved.Latest().Location() != time.UTC {
			t.Fatalf("successful Resolve violated range invariants: %+v", resolved)
		}
	})
}

func TestValidateIntentRequiresCanonicalExecutableMetadata(t *testing.T) {
	t.Parallel()

	valid := []Intent{
		{Earliest: "-024h", Latest: "now", Timezone: "UTC"},
		{Earliest: "2026-07-22T10:00:00.123456789Z", Latest: "2026-07-22T11:00:00+01:00", Timezone: "America/Los_Angeles", TimezoneSpecified: true},
	}
	for _, intent := range valid {
		if err := ValidateIntent(intent); err != nil {
			t.Fatalf("ValidateIntent(%+v) error = %v", intent, err)
		}
	}

	invalid := []Intent{
		{Earliest: " -1h", Latest: "now", Timezone: "UTC"},
		{Earliest: "-1h", Latest: "now ", Timezone: "UTC"},
		{Earliest: "-1w", Latest: "now", Timezone: "UTC"},
		{Earliest: "-1h", Latest: "now", Timezone: "America/Los_Angeles"},
		{Earliest: "-1h", Latest: "now", Timezone: " UTC", TimezoneSpecified: true},
	}
	for _, intent := range invalid {
		if err := ValidateIntent(intent); err == nil {
			t.Fatalf("ValidateIntent(%+v) unexpectedly succeeded", intent)
		}
	}
}
