package plan

import (
	"strconv"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/ianatimezone"
)

func TestTimechartCivilTransitionBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, zone string
		unit       CalendarUnit
		magnitude  uint64
		bounds     []string
	}{
		{"Sao Paulo day", "America/Sao_Paulo", CalendarDay, 1, []string{"2018-11-03T00:00:00-03:00", "2018-11-04T01:00:00-02:00", "2018-11-05T00:00:00-02:00", "2018-11-06T00:00:00-02:00"}},
		{"Sao Paulo week", "America/Sao_Paulo", CalendarWeek, 1, []string{"2018-10-28T00:00:00-03:00", "2018-11-04T01:00:00-02:00", "2018-11-11T00:00:00-02:00"}},
		{"Cairo month", "Africa/Cairo", CalendarMonth, 1, []string{"2014-07-01T00:00:00+02:00", "2014-08-01T01:00:00+03:00", "2014-09-01T00:00:00+03:00"}},
		{"Apia skipped day", "Pacific/Apia", CalendarDay, 1, []string{"2011-12-29T00:00:00-10:00", "2011-12-31T00:00:00+14:00", "2012-01-01T00:00:00+14:00"}},
		{"Asuncion quarter", "America/Asuncion", CalendarMonth, 3, []string{"2017-07-01T00:00:00-04:00", "2017-10-01T01:00:00-03:00", "2018-01-01T00:00:00-03:00"}},
		{"Lima year", "America/Lima", CalendarMonth, 12, []string{"1985-01-01T00:00:00-05:00", "1986-01-01T01:00:00-04:00", "1987-01-01T01:00:00-04:00", "1988-01-01T00:00:00-05:00"}},
		{"Havana fold", "America/Havana", CalendarDay, 1, []string{"2018-11-03T00:00:00-04:00", "2018-11-04T00:00:00-04:00", "2018-11-05T00:00:00-05:00"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			location, err := ianatimezone.Load(test.zone)
			if err != nil {
				t.Fatal(err)
			}
			want := make([]time.Time, len(test.bounds))
			for i, text := range test.bounds {
				want[i], err = time.Parse(time.RFC3339, text)
				if err != nil {
					t.Fatal(err)
				}
			}
			// Start both before the transition and inside its first interval.
			// The latter prevents normalization of the first boundary from
			// silently changing the origin for subsequent boundaries.
			for start := range 2 {
				got, err := timechartBoundarySequence(want[start].Add(30*time.Minute), want[len(want)-1], 0, test.unit, test.magnitude, time.Time{}, location, uint64(len(want)-start-1))
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want)-start {
					t.Fatalf("boundaries=%v want=%v", got, want[start:])
				}
				for i, boundary := range got {
					if !boundary.Equal(want[start+i]) {
						t.Fatalf("boundary[%d]=%v want=%v", i, boundary.In(location), want[start+i])
					}
				}
			}
		})
	}
}

func TestTimechartCivilFoldAndAlignedClock(t *testing.T) {
	location, err := ianatimezone.Load("America/Havana")
	if err != nil {
		t.Fatal(err)
	}
	secondFold, err := time.Parse(time.RFC3339, "2018-11-04T00:30:00-05:00")
	if err != nil {
		t.Fatal(err)
	}
	got, err := timechartBoundarySequence(secondFold, secondFold.Add(time.Hour), 0, CalendarDay, 1, time.Time{}, location, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := secondFold.Add(-90 * time.Minute); !got[0].Equal(want) {
		t.Fatalf("fold origin=%v want=%v", got[0], want)
	}
	got, err = timechartBoundarySequence(secondFold, secondFold.Add(8*24*time.Hour), 0, CalendarWeek, 1, secondFold, location, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !got[0].Equal(secondFold) || got[1].In(location).Hour() != 0 || got[1].In(location).Minute() != 30 {
		t.Fatalf("aligned boundaries=%v", got)
	}
}

func BenchmarkTimechartCivilGrid(b *testing.B) {
	location, err := ianatimezone.Load("America/Sao_Paulo")
	if err != nil {
		b.Fatal(err)
	}
	earliest := time.Date(2000, 1, 1, 3, 0, 0, 0, time.UTC)
	for _, count := range []int{100, 10000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			latest := earliest.AddDate(0, 0, count-1)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := timechartBoundarySequence(earliest, latest, 0, CalendarDay, 1, time.Time{}, location, 10000); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
