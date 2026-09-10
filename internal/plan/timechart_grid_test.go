package plan

import (
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestExactTimechartGrid(t *testing.T) {
	tests := []struct {
		name, query, earliest, latest, timezone, first, end string
		count                                               int
	}{
		{"subsecond negative", "span=250ms", "1969-12-31T23:59:59.9Z", "1970-01-01T00:00:00.6Z", "UTC", "1969-12-31T23:59:59.75Z", "1970-01-01T00:00:00.75Z", 4},
		{"long elapsed", "span=49h", "2026-01-01T00:00:00Z", "2026-01-04T00:00:00Z", "UTC", "2025-12-31T10:00:00Z", "2026-01-04T12:00:00Z", 2},
		{"two civil days DST", "span=2d", "2026-03-07T08:00:00Z", "2026-03-11T07:00:00Z", "America/Los_Angeles", "2026-03-06T08:00:00Z", "2026-03-12T07:00:00Z", 3},
		{"quarter magnitude", "span=2q", "2026-02-03T00:00:00Z", "2026-10-01T00:00:00Z", "UTC", "2026-01-01T00:00:00Z", "2027-01-01T00:00:00Z", 2},
		{"year magnitude", "span=2y", "2025-02-03T00:00:00Z", "2027-01-01T00:00:00Z", "UTC", "2024-01-01T00:00:00Z", "2028-01-01T00:00:00Z", 2},
		{"epoch alignment fractional", "span=250ms aligntime=0.1", "1970-01-01T00:00:00.15Z", "1970-01-01T00:00:00.6Z", "UTC", "1970-01-01T00:00:00.1Z", "1970-01-01T00:00:00.6Z", 2},
		{"earliest alignment", "span=1h aligntime=earliest", "2026-01-01T00:17:00Z", "2026-01-01T02:00:00Z", "UTC", "2026-01-01T00:17:00Z", "2026-01-01T02:17:00Z", 2},
		{"calendar ignores alignment", "span=1month aligntime=earliest", "2026-01-17T01:00:00Z", "2026-03-01T00:00:00Z", "UTC", "2026-01-01T00:00:00Z", "2026-03-01T00:00:00Z", 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parse := func(text string) time.Time {
				value, err := time.Parse(time.RFC3339Nano, text)
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			query, err := spl.Parse("index=main | timechart " + test.query + " count")
			if err != nil {
				t.Fatal(err)
			}
			logical, err := Build(query, Scope{VisibilityCutoff: new(uint64), TenantID: "tenant", AuthorizedIndexes: []string{"main"}, Earliest: parse(test.earliest), Latest: parse(test.latest), SearchStart: parse("2026-01-01T12:37:45Z"), SearchTimezone: test.timezone, IndexTimeCutoff: parse("2028-01-01T00:00:00Z")})
			if err != nil {
				t.Fatal(err)
			}
			op := logical.Operators[len(logical.Operators)-1].(*Timechart)
			if len(op.GridBoundaries) != test.count+1 || !op.FirstBucket.Equal(parse(test.first)) || !op.GridBoundaries[len(op.GridBoundaries)-1].Equal(parse(test.end)) {
				t.Fatalf("grid=%v", op.GridBoundaries)
			}
		})
	}
}

func TestTimechartRuntimeExtentDefersSearchGrid(t *testing.T) {
	query, err := spl.Parse("index=main | timechart span=1ms fixedrange=false cont=false partial=false count")
	if err != nil {
		t.Fatal(err)
	}
	earliest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	logical, err := Build(query, Scope{VisibilityCutoff: new(uint64), TenantID: "tenant", AuthorizedIndexes: []string{"main"}, Earliest: earliest, Latest: earliest.Add(24 * time.Hour), SearchStart: earliest, SearchTimezone: "UTC", IndexTimeCutoff: earliest})
	if err != nil {
		t.Fatal(err)
	}
	op := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if op.FixedRange || op.Continuous || op.IncludePartial || op.BucketCount != 0 {
		t.Fatalf("controls=%+v", op)
	}
	if err := ResolveTimechartGrid(op, earliest.Add(500*time.Millisecond), earliest.Add(501*time.Millisecond+time.Nanosecond), "UTC"); err != nil {
		t.Fatal(err)
	}
	if op.BucketCount != 2 || !op.SearchEarliest.Equal(earliest) {
		t.Fatalf("resolved=%+v", op)
	}
}

func TestTimechartRelativeAlignmentUsesSearchStart(t *testing.T) {
	earliest := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	query, err := spl.Parse(`index=main | timechart span=1h aligntime="@d+17m" count`)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := Build(query, Scope{VisibilityCutoff: new(uint64), TenantID: "tenant", AuthorizedIndexes: []string{"main"}, Earliest: earliest, Latest: earliest.Add(2 * time.Hour), SearchStart: earliest.Add(37 * time.Hour), SearchTimezone: "UTC", IndexTimeCutoff: earliest})
	if err != nil {
		t.Fatal(err)
	}
	op := logical.Operators[len(logical.Operators)-1].(*Timechart)
	if want := earliest.Add(24*time.Hour + 17*time.Minute); !op.Alignment.Equal(want) {
		t.Fatalf("alignment=%v want=%v", op.Alignment, want)
	}
}

func TestTimechartGridRejectsUnrepresentableAlignmentAndExtent(t *testing.T) {
	location := time.UTC
	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sourceRange := spl.Range{Start: spl.Position{Line: 1, Column: 1}, End: spl.Position{Line: 1, Column: 10}}
	for _, source := range []string{"+3000000h", "+200000000m", "+2000000000s"} {
		_, err := resolveTimechartAlignment(spl.TimechartAxisOptions{AlignTime: source, AlignTimeSpecified: true, AlignTimeRange: sourceRange}, anchor, anchor.Add(time.Hour), anchor, location)
		if source != "+2000000000s" && err == nil {
			t.Errorf("overflow alignment %s accepted", source)
		}
	}
	if _, err := timechartBoundarySequence(time.Date(1600, 1, 1, 0, 0, 0, 0, time.UTC), anchor, time.Second, CalendarNone, 0, time.Time{}, location); err == nil {
		t.Fatal("out-of-domain earliest accepted")
	}
	if _, err := timechartBoundarySequence(anchor, anchor.Add(time.Hour), time.Second, CalendarNone, 0, time.Date(2400, 1, 1, 0, 0, 0, 0, time.UTC), location); err == nil {
		t.Fatal("out-of-domain alignment accepted")
	}
}
