package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func testCalendarSourceClipping(t *testing.T, ctx context.Context, connection clickhousedriver.Conn) {
	t.Helper()
	for _, first := range []time.Time{MinimumSearchTime(), time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} {
		t.Run(first.Format("2006-01-02"), func(t *testing.T) {
			end := first.Add(24 * time.Hour)
			spec := timechartGridSpec{exact: true, calendar: plan.CalendarDay, firstBucket: first, bucketCount: 1, boundaries: []time.Time{first, end}}
			input := []int64{first.UnixNano() - 1, first.UnixNano(), end.UnixNano() - 1, end.UnixNano()}
			relation, args, clock := spec.assignCalendarSource(newScanRelation(`SELECT fromUnixTimestamp64Nano(arrayJoin(?), 'UTC') AS "_time"`, spl.Range{}), []any{input}, `"_time"`)
			rows, err := connection.Query(ctx, "SELECT toUnixTimestamp64Nano("+clock+") FROM ("+relation.sql+") ORDER BY \"_time\"", args...)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			want := []int64{first.UnixNano() - 1, first.UnixNano(), first.UnixNano(), end.UnixNano()}
			i := 0
			for rows.Next() {
				var key int64
				if err := rows.Scan(&key); err != nil {
					t.Fatal(err)
				}
				if i >= len(want) || key != want[i] {
					t.Fatalf("key[%d]=%d want=%v", i, key, want)
				}
				i++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if i != len(want) {
				t.Fatalf("rows=%d want=%d", i, len(want))
			}
		})
	}

	t.Run("nullable input is validated before sparse lookup", func(t *testing.T) {
		first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		spec := timechartGridSpec{exact: true, calendar: plan.CalendarDay, firstBucket: first, bucketCount: 1, boundaries: []time.Time{first, first.Add(24 * time.Hour)}}
		timestamp := `addNanoseconds(assumeNotNull("_time"), toInt64(throwIf(toUInt8(isNull("_time")), 'calendar-null-input')))`
		relation, args, clock := spec.assignCalendarSource(newScanRelation(`SELECT CAST(NULL, 'Nullable(DateTime64(9, \'UTC\'))') AS "_time"`, spl.Range{}), nil, timestamp)
		rows, err := connection.Query(ctx, "SELECT "+clock+" FROM ("+relation.sql+")", args...)
		if err == nil {
			defer rows.Close()
			for rows.Next() {
				var value time.Time
				if scanErr := rows.Scan(&value); scanErr != nil {
					err = scanErr
					break
				}
			}
			if err == nil {
				err = rows.Err()
			}
		}
		if err == nil || !strings.Contains(err.Error(), "calendar-null-input") {
			t.Fatalf("nullable lookup error=%v", err)
		}
	})
}
