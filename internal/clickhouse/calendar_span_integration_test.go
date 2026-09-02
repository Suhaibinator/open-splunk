package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCalendarSpansAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)

	const index = "calendar-span"
	indexTime := time.Date(2027, time.January, 12, 0, 0, 0, 0, time.UTC)
	newEvent := func(id, source string, at time.Time) *ingest.StoredEvent {
		t.Helper()
		event := testStoredEvent(id, index, indexTime)
		event.BatchID = "calendar-span-batch"
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(at)
		event.Event.CollectedAt = timestamppb.New(at)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("spring-1", "calendar-spring", time.Date(2026, time.March, 8, 12, 0, 0, 0, time.UTC)),
		newEvent("spring-2", "calendar-spring", time.Date(2026, time.March, 9, 12, 0, 0, 0, time.UTC)),
		newEvent("fall-1", "calendar-fall", time.Date(2026, time.November, 1, 12, 0, 0, 0, time.UTC)),
		newEvent("fall-2", "calendar-fall", time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC)),
		newEvent("week-1", "calendar-week", time.Date(2026, time.December, 31, 12, 0, 0, 0, time.UTC)),
		newEvent("week-2", "calendar-week", time.Date(2027, time.January, 3, 12, 0, 0, 0, time.UTC)),
	}
	if result, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        "collector",
		BatchID:            "calendar-span-batch",
		BatchSequence:      901,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("calendar-span-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil || result.Accepted != uint32(len(events)) {
		t.Fatalf("store calendar fixtures: result=%+v err=%v", result, err)
	}
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture calendar fixture visibility cutoff: %v", err)
	}

	compile := func(
		t *testing.T,
		source string,
		earliest time.Time,
		latest time.Time,
		timezone string,
	) CompiledQuery {
		t.Helper()
		parsed, parseErr := spl.Parse(source)
		if parseErr != nil {
			t.Fatalf("parse calendar SPL %q: %v", source, parseErr)
		}
		logical, buildErr := plan.Build(parsed, plan.Scope{
			TenantID:          "tenant",
			AuthorizedIndexes: []string{index},
			Earliest:          earliest,
			Latest:            latest,
			SearchStart:       indexTime.Add(2 * time.Minute),
			SearchTimezone:    timezone,
			IndexTimeCutoff:   indexTime.Add(time.Minute),
			VisibilityCutoff:  new(visibilityCutoff),
		})
		if buildErr != nil {
			t.Fatalf("build calendar SPL %q: %v", source, buildErr)
		}
		compiled, compileErr := (Compiler{}).Compile(logical)
		if compileErr != nil {
			t.Fatalf("compile calendar SPL %q: %v", source, compileErr)
		}
		if got, want := strings.Count(compiled.SQL, "?"), len(compiled.Args); got != want {
			t.Fatalf("placeholder count = %d, args = %d for %q", got, want, source)
		}
		return compiled
	}

	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(clickhousedriver.Settings{
			"short_circuit_function_evaluation": "enable",
			"use_variant_as_common_type":        uint8(0),
		}),
	)

	t.Run("bin follows the spring civil day", func(t *testing.T) {
		earliest := time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC)
		latest := time.Date(2026, time.March, 10, 4, 0, 0, 0, time.UTC)
		compiled := compile(
			t,
			`index=calendar-span source="calendar-spring" | bin _time span=1d | stats count BY _time | sort 0 +_time`,
			earliest,
			latest,
			"America/New_York",
		)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute calendar bin: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
		}
		defer rows.Close()
		var boundaries []time.Time
		var counts []uint64
		for rows.Next() {
			var boundary time.Time
			var count uint64
			if scanErr := rows.Scan(&boundary, &count); scanErr != nil {
				t.Fatalf("scan calendar bin: %v", scanErr)
			}
			boundaries = append(boundaries, boundary)
			counts = append(counts, count)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate calendar bin: %v", rowsErr)
		}
		wantBoundaries := []time.Time{
			time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
			time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
		}
		if !reflect.DeepEqual(boundaries, wantBoundaries) || !reflect.DeepEqual(counts, []uint64{1, 1}) {
			t.Fatalf("calendar bin = boundaries %v counts %v, want %v/[1 1]", boundaries, counts, wantBoundaries)
		}
	})

	type timechartCase struct {
		name       string
		source     string
		earliest   time.Time
		latest     time.Time
		timezone   string
		boundaries []time.Time
		counts     []uint64
	}
	for _, test := range []timechartCase{
		{
			name:     "spring day has a 23-hour UTC interval",
			source:   `index=calendar-span source="calendar-spring" | timechart span=1d count`,
			earliest: time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
			latest:   time.Date(2026, time.March, 10, 4, 0, 0, 0, time.UTC),
			timezone: "America/New_York",
			boundaries: []time.Time{
				time.Date(2026, time.March, 8, 5, 0, 0, 0, time.UTC),
				time.Date(2026, time.March, 9, 4, 0, 0, 0, time.UTC),
			},
			counts: []uint64{1, 1},
		},
		{
			name:     "fall day has a 25-hour UTC interval",
			source:   `index=calendar-span source="calendar-fall" | timechart span=1d count`,
			earliest: time.Date(2026, time.November, 1, 4, 0, 0, 0, time.UTC),
			latest:   time.Date(2026, time.November, 3, 5, 0, 0, 0, time.UTC),
			timezone: "America/New_York",
			boundaries: []time.Time{
				time.Date(2026, time.November, 1, 4, 0, 0, 0, time.UTC),
				time.Date(2026, time.November, 2, 5, 0, 0, 0, time.UTC),
			},
			counts: []uint64{1, 1},
		},
		{
			name:     "week starts Sunday across the year boundary",
			source:   `index=calendar-span source="calendar-week" | timechart span=1w count`,
			earliest: time.Date(2026, time.December, 31, 12, 0, 0, 0, time.UTC),
			latest:   time.Date(2027, time.January, 11, 0, 0, 0, 0, time.UTC),
			timezone: "UTC",
			boundaries: []time.Time{
				time.Date(2026, time.December, 27, 0, 0, 0, 0, time.UTC),
				time.Date(2027, time.January, 3, 0, 0, 0, 0, time.UTC),
				time.Date(2027, time.January, 10, 0, 0, 0, 0, time.UTC),
			},
			counts: []uint64{1, 1, 0},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(t, test.source, test.earliest, test.latest, test.timezone)
			if compiled.Timechart == nil || !compiled.Timechart.Calendar ||
				compiled.Timechart.Span != 0 ||
				compiled.Timechart.BucketCount != uint64(len(test.boundaries)) {
				t.Fatalf("calendar timechart metadata = %#v", compiled.Timechart)
			}
			rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
			if queryErr != nil {
				t.Fatalf("execute calendar timechart: %v\nSQL: %s\nargs: %#v", queryErr, compiled.SQL, compiled.Args)
			}
			defer rows.Close()
			if types := rows.ColumnTypes(); len(types) != 3 ||
				types[0].DatabaseTypeName() != "UInt64" ||
				types[1].DatabaseTypeName() != "DateTime64(9, 'UTC')" ||
				types[2].DatabaseTypeName() != "UInt64" {
				t.Fatalf("calendar timechart column types = %#v", types)
			}
			var boundaries []time.Time
			var counts []uint64
			ordinal := uint64(0)
			for rows.Next() {
				var gotOrdinal uint64
				var boundary time.Time
				var count uint64
				if scanErr := rows.Scan(&gotOrdinal, &boundary, &count); scanErr != nil {
					t.Fatalf("scan calendar timechart: %v", scanErr)
				}
				if gotOrdinal != ordinal {
					t.Fatalf("calendar timechart ordinal = %d, want %d", gotOrdinal, ordinal)
				}
				ordinal++
				boundaries = append(boundaries, boundary)
				counts = append(counts, count)
			}
			if rowsErr := rows.Err(); rowsErr != nil {
				t.Fatalf("iterate calendar timechart: %v", rowsErr)
			}
			if !reflect.DeepEqual(boundaries, test.boundaries) || !reflect.DeepEqual(counts, test.counts) {
				t.Fatalf(
					"calendar timechart = boundaries %v counts %v, want %v/%v",
					boundaries,
					counts,
					test.boundaries,
					test.counts,
				)
			}
		})
	}
}
