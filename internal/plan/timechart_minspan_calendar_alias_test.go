package plan

import (
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestTimechartMinspanCalendarAliasesSelectEquivalentGrids(t *testing.T) {
	earliest := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	scope := Scope{TenantID: "tenant", AuthorizedIndexes: []string{"main"}, Earliest: earliest, Latest: earliest.AddDate(5, 0, 0), SearchStart: earliest, SearchTimezone: "UTC", IndexTimeCutoff: earliest, VisibilityCutoff: new(uint64)}
	build := func(t *testing.T, minspan string) *Timechart {
		t.Helper()
		parsed, err := spl.Parse("index=main | timechart minspan=" + minspan + " count")
		if err != nil {
			t.Fatal(err)
		}
		logical, err := Build(parsed, scope)
		if err != nil {
			t.Fatal(err)
		}
		return logical.Operators[len(logical.Operators)-1].(*Timechart)
	}
	for _, test := range []struct {
		alias, months string
		magnitude     uint64
	}{
		{"1y", "12mon", 12}, {"2y", "24mon", 24}, {"1q", "3mon", 3}, {"2q", "6mon", 6}, {"4q", "12mon", 12}, {"8q", "24mon", 24},
	} {
		t.Run(test.alias, func(t *testing.T) {
			alias, months := build(t, test.alias), build(t, test.months)
			if alias.Calendar != CalendarMonth || alias.CalendarMagnitude != test.magnitude || alias.Span != months.Span || alias.Calendar != months.Calendar || alias.CalendarMagnitude != months.CalendarMagnitude || !slices.Equal(alias.GridBoundaries, months.GridBoundaries) {
				t.Fatalf("minspan=%s selected %d months, minspan=%s selected %d months; want equivalent %d-month grids", test.alias, alias.CalendarMagnitude, test.months, months.CalendarMagnitude, test.magnitude)
			}
		})
	}
}
