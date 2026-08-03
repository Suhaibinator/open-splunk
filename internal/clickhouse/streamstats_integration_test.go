package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const streamStatsIntegrationCollectorID = "streamstats-integration-collector"

// testStreamStatsAgainstClickHouse pins the bounded running-count contract to
// the production ClickHouse image. The 10,000-row fixture is deliberately
// shared with eventstats, whose phase runs immediately before this helper, so
// the full store integration does not ingest a duplicate boundary corpus.
func testStreamStatsAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	newEvent := func(
		id, source string,
		ordinal int,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"streamstats-host",
			"streamstats fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "streamstats-batch"
		event.CollectorID = streamStatsIntegrationCollectorID
		event.Event.Source = source
		event.Event.EventTime = timestamppb.New(
			indexTime.Add(time.Duration(ordinal) * time.Second),
		)
		return event
	}

	events := []*ingest.StoredEvent{
		newEvent(
			"streamstats-01",
			"streamstats-order",
			1,
			typedField("streamstats_group", typedString("500")),
			typedField("streamstats_existing", typedString("shadowed")),
		),
		newEvent(
			"streamstats-02",
			"streamstats-order",
			2,
			typedField("streamstats_group", typedString("other")),
		),
		newEvent(
			"streamstats-03",
			"streamstats-order",
			3,
			typedField("streamstats_group", typedSint(500)),
		),
		newEvent("streamstats-04", "streamstats-order", 4),
		newEvent(
			"streamstats-05",
			"streamstats-order",
			5,
			typedField("streamstats_group", typedNull()),
		),
		newEvent(
			"streamstats-06",
			"streamstats-order",
			6,
			typedField("streamstats_group", typedString("other")),
		),
		newEvent(
			"streamstats-07",
			"streamstats-order",
			7,
			typedField("streamstats_group", typedUint(500)),
		),
		newEvent(
			"streamstats-poison-scalar",
			"streamstats-poison",
			8,
			typedField("streamstats_group", typedString("safe")),
		),
		newEvent(
			"streamstats-poison-container",
			"streamstats-poison",
			9,
			typedField(
				"streamstats_group",
				typedList(typedString("not"), typedString("scalar")),
			),
		),
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "tenant",
		CollectorID:        streamStatsIntegrationCollectorID,
		BatchID:            "streamstats-batch",
		BatchSequence:      90,
		OriginalEventCount: uint32(len(events)),
		SourceBatchSHA256:  testSourceBatchDigest("streamstats-batch"),
		ReceivedAt:         indexTime,
		Events:             events,
	}); err != nil {
		t.Fatalf("store streamstats fixtures: %v", err)
	}

	// A newer foreign-tenant row carries a poison group value. Successful
	// grouped execution below therefore proves both count and validation scope.
	foreign := newEvent(
		"streamstats-foreign-poison",
		"streamstats-order",
		10,
		typedField(
			"streamstats_group",
			typedObject(typedField("child", typedString("foreign"))),
		),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "streamstats-foreign-batch"
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:           "other-tenant",
		CollectorID:        streamStatsIntegrationCollectorID,
		BatchID:            "streamstats-foreign-batch",
		BatchSequence:      91,
		OriginalEventCount: 1,
		SourceBatchSHA256:  testSourceBatchDigest("streamstats-foreign-batch"),
		ReceivedAt:         indexTime,
		Events:             []*ingest.StoredEvent{foreign},
	}); err != nil {
		t.Fatalf("store foreign streamstats poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture streamstats visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(20*time.Second),
			visibilityCutoff,
		)
	}
	compileBoundary := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(20*time.Second),
			visibilityCutoff,
			"eventstats-boundary",
		)
	}
	base := `index=compiler source="streamstats-order"`

	type countRow struct {
		id    string
		count uint64
	}
	collectCounts := func(name string, query CompiledQuery) []countRow {
		t.Helper()
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute %s: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		var got []countRow
		for rows.Next() {
			var row countRow
			if scanErr := rows.Scan(&row.id, &row.count); scanErr != nil {
				t.Fatalf("scan %s: %v", name, scanErr)
			}
			got = append(got, row)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close %s: %v", name, closeErr)
		}
		return got
	}
	assertCounts := func(
		name, source string,
		wantIDs []string,
		wantCounts []uint64,
	) {
		t.Helper()
		got := collectCounts(name, compile(source))
		want := make([]countRow, len(wantIDs))
		for index := range wantIDs {
			want[index] = countRow{id: wantIDs[index], count: wantCounts[index]}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s rows = %#v, want %#v", name, got, want)
		}
	}

	ascendingIDs := []string{
		"streamstats-01",
		"streamstats-02",
		"streamstats-03",
		"streamstats-04",
		"streamstats-05",
		"streamstats-06",
		"streamstats-07",
	}
	descendingIDs := slices.Clone(ascendingIDs)
	slices.Reverse(descendingIDs)
	ascendingCounts := []uint64{1, 2, 3, 4, 5, 6, 7}
	assertCounts(
		"default descending event order",
		base+` | streamstats count AS running | table event_id running`,
		descendingIDs,
		ascendingCounts,
	)
	assertCounts(
		"explicit ascending order",
		base+` | sort 0 +event_id | streamstats count AS running | table event_id running`,
		ascendingIDs,
		ascendingCounts,
	)
	assertCounts(
		"explicit descending order",
		base+` | sort 0 -event_id | streamstats count AS running | table event_id running`,
		descendingIDs,
		ascendingCounts,
	)

	for _, test := range []struct {
		name       string
		options    string
		wantCounts []uint64
	}{
		{
			name:       "current false starts at zero",
			options:    "current=false",
			wantCounts: []uint64{0, 1, 2, 3, 4, 5, 6},
		},
		{
			name:       "one-row current window",
			options:    "window=1",
			wantCounts: []uint64{1, 1, 1, 1, 1, 1, 1},
		},
		{
			name:       "one-row prior window",
			options:    "current=false window=1",
			wantCounts: []uint64{0, 1, 1, 1, 1, 1, 1},
		},
		{
			name:       "two-row current window",
			options:    "window=2",
			wantCounts: []uint64{1, 2, 2, 2, 2, 2, 2},
		},
		{
			name:       "two-row prior window",
			options:    "current=false window=2",
			wantCounts: []uint64{0, 1, 2, 2, 2, 2, 2},
		},
	} {
		assertCounts(
			test.name,
			base+` | sort 0 +event_id | streamstats `+test.options+
				` count AS running | table event_id running`,
			ascendingIDs,
			test.wantCounts,
		)
	}

	grouped := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats window=2 global=false count AS peers BY streamstats_group` +
			` | table event_id peers`,
	)
	groupedRows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if err != nil {
		t.Fatalf(
			"execute grouped streamstats: %v\nSQL: %s\nargs: %#v",
			err,
			grouped.SQL,
			grouped.Args,
		)
	}
	types := groupedRows.ColumnTypes()
	if len(types) != 2 || types[1].DatabaseTypeName() != "Nullable(UInt64)" {
		_ = groupedRows.Close()
		t.Fatalf("grouped streamstats column types = %#v", types)
	}
	type groupedRow struct {
		id    string
		peers *uint64
	}
	var groupedGot []groupedRow
	for groupedRows.Next() {
		var row groupedRow
		if scanErr := groupedRows.Scan(&row.id, &row.peers); scanErr != nil {
			_ = groupedRows.Close()
			t.Fatalf("scan grouped streamstats: %v", scanErr)
		}
		groupedGot = append(groupedGot, row)
	}
	if rowsErr := groupedRows.Err(); rowsErr != nil {
		_ = groupedRows.Close()
		t.Fatalf("iterate grouped streamstats: %v", rowsErr)
	}
	if closeErr := groupedRows.Close(); closeErr != nil {
		t.Fatalf("close grouped streamstats: %v", closeErr)
	}
	one := uint64(1)
	two := uint64(2)
	groupedWant := []groupedRow{
		{id: "streamstats-01", peers: &one},
		{id: "streamstats-02", peers: &one},
		{id: "streamstats-03", peers: &two},
		{id: "streamstats-04"},
		{id: "streamstats-05"},
		{id: "streamstats-06", peers: &two},
		{id: "streamstats-07", peers: &two},
	}
	if !reflect.DeepEqual(groupedGot, groupedWant) {
		t.Fatalf(
			"grouped streamstats rows = %#v, want %#v",
			groupedGot,
			groupedWant,
		)
	}

	// Replacing a sparse Dynamic field publishes one non-null UInt64 cell on
	// every row, even where the previous value was absent.
	assertCounts(
		"alias replacement",
		base+` | sort 0 +event_id`+
			` | streamstats count AS streamstats_existing`+
			` | table event_id streamstats_existing`,
		ascendingIDs,
		ascendingCounts,
	)

	stacked := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats count AS current_count` +
			` | streamstats current=false count AS prior_count` +
			` | table event_id current_count prior_count`,
	)
	stackedRows, err := connection.Query(ctx, stacked.SQL, stacked.Args...)
	if err != nil {
		t.Fatalf("execute stacked streamstats: %v\nSQL: %s", err, stacked.SQL)
	}
	stackedIndex := 0
	for stackedRows.Next() {
		var id string
		var current, prior uint64
		if scanErr := stackedRows.Scan(&id, &current, &prior); scanErr != nil {
			_ = stackedRows.Close()
			t.Fatalf("scan stacked streamstats: %v", scanErr)
		}
		if stackedIndex >= len(ascendingIDs) {
			_ = stackedRows.Close()
			t.Fatalf("stacked streamstats emitted an extra row %q", id)
		}
		if id != ascendingIDs[stackedIndex] ||
			current != uint64(stackedIndex+1) ||
			prior != uint64(stackedIndex) {
			_ = stackedRows.Close()
			t.Fatalf(
				"stacked streamstats row %d = %q/%d/%d",
				stackedIndex,
				id,
				current,
				prior,
			)
		}
		stackedIndex++
	}
	if rowsErr := stackedRows.Err(); rowsErr != nil {
		_ = stackedRows.Close()
		t.Fatalf("iterate stacked streamstats: %v", rowsErr)
	}
	if closeErr := stackedRows.Close(); closeErr != nil {
		t.Fatalf("close stacked streamstats: %v", closeErr)
	}
	if stackedIndex != len(ascendingIDs) {
		t.Fatalf("stacked streamstats rows = %d, want %d", stackedIndex, len(ascendingIDs))
	}

	transformed := compile(
		base + ` | stats count AS events BY streamstats_group` +
			` | streamstats count AS ordinal` +
			` | sort 0 +ordinal | table events ordinal`,
	)
	transformedRows, err := connection.Query(ctx, transformed.SQL, transformed.Args...)
	if err != nil {
		t.Fatalf(
			"execute streamstats after stats: %v\nSQL: %s",
			err,
			transformed.SQL,
		)
	}
	var transformedCounts, transformedOrdinals []uint64
	for transformedRows.Next() {
		var count, ordinal uint64
		if scanErr := transformedRows.Scan(&count, &ordinal); scanErr != nil {
			_ = transformedRows.Close()
			t.Fatalf("scan streamstats after stats: %v", scanErr)
		}
		transformedCounts = append(transformedCounts, count)
		transformedOrdinals = append(transformedOrdinals, ordinal)
	}
	if rowsErr := transformedRows.Err(); rowsErr != nil {
		_ = transformedRows.Close()
		t.Fatalf("iterate streamstats after stats: %v", rowsErr)
	}
	if closeErr := transformedRows.Close(); closeErr != nil {
		t.Fatalf("close streamstats after stats: %v", closeErr)
	}
	slices.Sort(transformedCounts)
	if !slices.Equal(transformedCounts, []uint64{2, 3}) ||
		!slices.Equal(transformedOrdinals, []uint64{1, 2}) {
		t.Fatalf(
			"streamstats after stats counts/ordinals = %v/%v",
			transformedCounts,
			transformedOrdinals,
		)
	}

	// The default streamstats alias deliberately replaces stats' default count
	// alias. Its incoming order must already be private, or ClickHouse can bind
	// the final order to the new running value (or expose a duplicate column).
	aggregateReplacement := compile(
		base + ` | stats count | streamstats count | table count`,
	)
	var replacedCount uint64
	if err := connection.QueryRow(
		ctx,
		aggregateReplacement.SQL,
		aggregateReplacement.Args...,
	).Scan(&replacedCount); err != nil {
		t.Fatalf(
			"execute streamstats aggregate alias replacement: %v\nSQL: %s",
			err,
			aggregateReplacement.SQL,
		)
	}
	if replacedCount != 1 {
		t.Fatalf("streamstats aggregate alias replacement = %d, want 1", replacedCount)
	}

	// The foreign poison is newer than every authorized event. It must neither
	// contribute to this count nor trigger the grouped validator above.
	tenantScoped := compile(
		base + ` | streamstats count AS total` +
			` | sort 0 -total | head 1 | table total`,
	)
	var tenantTotal uint64
	if err := connection.QueryRow(
		ctx,
		tenantScoped.SQL,
		tenantScoped.Args...,
	).Scan(&tenantTotal); err != nil {
		t.Fatalf("execute tenant-scoped streamstats: %v", err)
	}
	if tenantTotal != uint64(len(ascendingIDs)) {
		t.Fatalf("tenant-scoped streamstats total = %d, want %d", tenantTotal, len(ascendingIDs))
	}

	hiddenPoison := compile(
		`index=compiler source="streamstats-poison"` +
			` | streamstats count AS peers BY streamstats_group` +
			` | fields - peers | search event_id="not-present"`,
	)
	poisonErr := executeCompiledExpectingNoRows(ctx, connection, hiddenPoison)
	var poisonException *clickhousedriver.Exception
	if !errors.As(poisonErr, &poisonException) || poisonException.Code != 395 ||
		!strings.Contains(poisonException.Message, UnsupportedStatsByValueMarker) {
		t.Fatalf(
			"downstream-hidden streamstats poison error = %v, want guarded scalar-group failure",
			poisonErr,
		)
	}

	exactBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 count AS ordinal` +
			` | sort 0 -ordinal | head 1 | table ordinal`,
	)
	var exactMaximum uint64
	if err := connection.QueryRow(
		ctx,
		exactBoundary.SQL,
		exactBoundary.Args...,
	).Scan(&exactMaximum); err != nil {
		t.Fatalf(
			"execute exact streamstats boundary: %v\nSQL: %s",
			err,
			exactBoundary.SQL,
		)
	}
	if exactMaximum != MaximumStreamStatsInputRows {
		t.Fatalf(
			"exact streamstats boundary maximum = %d, want %d",
			exactMaximum,
			MaximumStreamStatsInputRows,
		)
	}

	hiddenOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats count AS ordinal` +
			` | fields - ordinal | search event_id="not-present"`,
	)
	overflowErr := executeCompiledExpectingNoRows(ctx, connection, hiddenOverflow)
	if overflowErr == nil ||
		!strings.Contains(overflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats overflow error = %v, want %q",
			overflowErr,
			StreamStatsInputLimitMarker,
		)
	}
}
