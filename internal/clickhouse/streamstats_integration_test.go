package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"math"
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

// testStreamStatsAgainstClickHouse pins the bounded running aggregate contract
// to the production ClickHouse image. The 10,000-row fixture is deliberately
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
			fmt.Sprintf(`{"value":%d}`, ordinal),
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
			typedField("streamstats_avg_overflow", typedDouble(math.MaxFloat64)),
			typedField("streamstats_existing", typedString("shadowed")),
			typedField("streamstats_sum_value", typedSint(2)),
			typedField("streamstats_value", typedString("present")),
		),
		newEvent(
			"streamstats-02",
			"streamstats-order",
			2,
			typedField("streamstats_group", typedString("other")),
			typedField("streamstats_avg_overflow", typedDouble(math.MaxFloat64)),
			typedField(
				"streamstats_sum_value",
				typedList(
					typedUint(3),
					typedString("4.5"),
					typedString("not-a-number"),
					typedNull(),
					typedBool(true),
					typedList(typedSint(99)),
					typedObject(typedField("child", typedSint(99))),
				),
			),
			typedField(
				"streamstats_value",
				typedList(
					typedString("duplicate"),
					typedNull(),
					typedString("duplicate"),
					typedList(typedString("nested")),
					typedObject(typedField("child", typedString("nested"))),
				),
			),
		),
		newEvent(
			"streamstats-03",
			"streamstats-order",
			3,
			typedField("streamstats_group", typedSint(500)),
			typedField("streamstats_sum_value", typedString("-1.5")),
			typedField("streamstats_value", typedList()),
		),
		newEvent("streamstats-04", "streamstats-order", 4),
		newEvent(
			"streamstats-05",
			"streamstats-order",
			5,
			typedField("streamstats_group", typedNull()),
			typedField("streamstats_decimal", typedDecimal("3.25")),
			typedField("streamstats_sum_value", typedNull()),
			typedField("streamstats_value", typedNull()),
		),
		newEvent(
			"streamstats-06",
			"streamstats-order",
			6,
			typedField("streamstats_group", typedString("other")),
			typedField("streamstats_sum_value", typedString("not-a-number")),
			typedField("streamstats_value", typedString("")),
		),
		newEvent(
			"streamstats-07",
			"streamstats-order",
			7,
			typedField("streamstats_group", typedUint(500)),
			typedField("streamstats_sum_value", typedDouble(0)),
			typedField(
				"streamstats_value",
				typedObject(typedField("child", typedString("flattened"))),
			),
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
		typedField(
			"streamstats_sum_value",
			typedSint(1_000),
		),
		typedField(
			"streamstats_value",
			typedList(
				typedString("foreign-1"),
				typedString("foreign-2"),
			),
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
	type groupedRow struct {
		id    string
		peers *uint64
	}
	one := uint64(1)
	two := uint64(2)
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

	for _, test := range []struct {
		name       string
		options    string
		wantCounts []uint64
	}{
		{
			name:       "field occurrence complete prefix",
			wantCounts: []uint64{1, 5, 5, 5, 5, 6, 7},
		},
		{
			name:       "field occurrence complete prior prefix",
			options:    "current=false",
			wantCounts: []uint64{0, 1, 5, 5, 5, 5, 6},
		},
		{
			name:       "field occurrence two-row current window",
			options:    "window=2",
			wantCounts: []uint64{1, 5, 4, 0, 0, 1, 2},
		},
		{
			name:       "field occurrence two-row prior window",
			options:    "current=false window=2",
			wantCounts: []uint64{0, 1, 5, 4, 0, 0, 1},
		},
	} {
		assertCounts(
			test.name,
			base+` | sort 0 +event_id | streamstats `+test.options+
				` count(streamstats_value) AS populated | table event_id populated`,
			ascendingIDs,
			test.wantCounts,
		)
	}

	groupedField := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats window=2 global=false count(streamstats_value) AS populated BY streamstats_group` +
			` | table event_id populated`,
	)
	groupedFieldRows, err := connection.Query(
		ctx,
		groupedField.SQL,
		groupedField.Args...,
	)
	if err != nil {
		t.Fatalf(
			"execute grouped streamstats count(field): %v\nSQL: %s\nargs: %#v",
			err,
			groupedField.SQL,
			groupedField.Args,
		)
	}
	var groupedFieldGot []groupedRow
	for groupedFieldRows.Next() {
		var row groupedRow
		if scanErr := groupedFieldRows.Scan(&row.id, &row.peers); scanErr != nil {
			_ = groupedFieldRows.Close()
			t.Fatalf("scan grouped streamstats count(field): %v", scanErr)
		}
		groupedFieldGot = append(groupedFieldGot, row)
	}
	if rowsErr := groupedFieldRows.Err(); rowsErr != nil {
		_ = groupedFieldRows.Close()
		t.Fatalf("iterate grouped streamstats count(field): %v", rowsErr)
	}
	if closeErr := groupedFieldRows.Close(); closeErr != nil {
		t.Fatalf("close grouped streamstats count(field): %v", closeErr)
	}
	four := uint64(4)
	five := uint64(5)
	groupedFieldWant := []groupedRow{
		{id: "streamstats-01", peers: &one},
		{id: "streamstats-02", peers: &four},
		{id: "streamstats-03", peers: &one},
		{id: "streamstats-04"},
		{id: "streamstats-05"},
		{id: "streamstats-06", peers: &five},
		{id: "streamstats-07", peers: &one},
	}
	if !reflect.DeepEqual(groupedFieldGot, groupedFieldWant) {
		t.Fatalf(
			"grouped streamstats count(field) rows = %#v, want %#v",
			groupedFieldGot,
			groupedFieldWant,
		)
	}

	assertCounts(
		"field occurrence alias replacement",
		base+` | sort 0 +event_id`+
			` | streamstats count(streamstats_value) AS streamstats_value`+
			` | table event_id streamstats_value`,
		ascendingIDs,
		[]uint64{1, 5, 5, 5, 5, 6, 7},
	)
	assertCounts(
		"projected-away field occurrence input",
		base+` | sort 0 +event_id | fields event_id`+
			` | streamstats count(streamstats_value) AS populated`+
			` | table event_id populated`,
		ascendingIDs,
		[]uint64{0, 0, 0, 0, 0, 0, 0},
	)

	for _, test := range []struct {
		name       string
		options    string
		predicate  string
		wantCounts []uint64
	}{
		{
			name:       "conditional complete prefix",
			predicate:  `event_id="streamstats-01" OR event_id="streamstats-02" OR event_id="streamstats-05" OR event_id="streamstats-07"`,
			wantCounts: []uint64{1, 2, 2, 2, 3, 3, 4},
		},
		{
			name:       "conditional complete prior prefix",
			options:    "current=false",
			predicate:  `event_id="streamstats-01" OR event_id="streamstats-02" OR event_id="streamstats-05" OR event_id="streamstats-07"`,
			wantCounts: []uint64{0, 1, 2, 2, 2, 3, 3},
		},
		{
			name:       "conditional two-row current window",
			options:    "window=2",
			predicate:  `event_id="streamstats-01" OR event_id="streamstats-02" OR event_id="streamstats-05" OR event_id="streamstats-07"`,
			wantCounts: []uint64{1, 2, 1, 0, 1, 1, 1},
		},
		{
			name:       "conditional two-row prior window",
			options:    "current=false window=2",
			predicate:  `event_id="streamstats-01" OR event_id="streamstats-02" OR event_id="streamstats-05" OR event_id="streamstats-07"`,
			wantCounts: []uint64{0, 1, 2, 1, 0, 1, 1},
		},
		{
			name:       "conditional nullable branches",
			predicate:  `if(event_id="streamstats-01", true, if(event_id="streamstats-02", null, if(event_id="streamstats-03", true, false)))=true`,
			wantCounts: []uint64{1, 1, 2, 2, 2, 2, 2},
		},
		{
			name:       "conditional missing comparison",
			predicate:  `streamstats_missing=1`,
			wantCounts: []uint64{0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:       "conditional exact numeric and string calls",
			predicate:  `(streamstats_group=500 AND match(event_id, "-(01|03|07)$")) OR like(event_id, "%06")`,
			wantCounts: []uint64{0, 0, 1, 1, 1, 2, 3},
		},
	} {
		assertCounts(
			test.name,
			base+` | sort 0 +event_id | streamstats `+test.options+
				` count(eval(`+test.predicate+`)) AS matched | table event_id matched`,
			ascendingIDs,
			test.wantCounts,
		)
	}

	assertCounts(
		"conditional alias replacement reads incoming field",
		base+` | sort 0 +event_id`+
			` | streamstats count(eval(streamstats_existing="shadowed")) AS streamstats_existing`+
			` | table event_id streamstats_existing`,
		ascendingIDs,
		[]uint64{1, 1, 1, 1, 1, 1, 1},
	)
	assertCounts(
		"projected-away conditional input",
		base+` | sort 0 +event_id | fields event_id`+
			` | streamstats count(eval(isnotnull(streamstats_group))) AS matched`+
			` | table event_id matched`,
		ascendingIDs,
		[]uint64{0, 0, 0, 0, 0, 0, 0},
	)

	calculatedConditional := compile(
		base + ` | sort 0 +event_id` +
			` | eval selected=if(event_id="streamstats-01" OR event_id="streamstats-05", "wanted", "other")` +
			` | streamstats count(eval(selected="wanted")) AS matched` +
			` | where selected="wanted" | table event_id matched`,
	)
	if got := strings.Count(calculatedConditional.SQL, "ARRAY JOIN"); got != 0 {
		t.Fatalf(
			"fixed calculated streamstats count(eval) singleton fences = %d, want none:\n%s",
			got,
			calculatedConditional.SQL,
		)
	}
	calculatedConditionalGot := collectCounts(
		"calculated streamstats count(eval)",
		calculatedConditional,
	)
	calculatedConditionalWant := []countRow{
		{id: "streamstats-01", count: 1},
		{id: "streamstats-05", count: 2},
	}
	if !reflect.DeepEqual(calculatedConditionalGot, calculatedConditionalWant) {
		t.Fatalf(
			"calculated streamstats count(eval) rows = %#v, want %#v",
			calculatedConditionalGot,
			calculatedConditionalWant,
		)
	}

	calculatedNumericConditional := compile(
		base + ` | sort 0 +event_id` +
			` | spath input=_raw output=selected path=value` +
			` | streamstats count(eval(selected>1 AND selected<5)) AS matched` +
			` | table event_id matched`,
	)
	if got := strings.Count(calculatedNumericConditional.SQL, "ARRAY JOIN"); got != 1 {
		t.Fatalf(
			"calculated numeric streamstats count(eval) singleton fences = %d, want one:\n%s",
			got,
			calculatedNumericConditional.SQL,
		)
	}
	for _, definition := range []string{
		` AS "__os_streamstats_exact_key_`,
		` AS "__os_streamstats_exact_numeric_`,
	} {
		if got := strings.Count(calculatedNumericConditional.SQL, definition); got != 1 {
			t.Fatalf(
				"calculated numeric streamstats count(eval) definition %q count = %d, want one:\n%s",
				definition,
				got,
				calculatedNumericConditional.SQL,
			)
		}
	}
	calculatedNumericConditionalGot := collectCounts(
		"calculated numeric streamstats count(eval)",
		calculatedNumericConditional,
	)
	calculatedNumericConditionalWant := []countRow{
		{id: "streamstats-01", count: 0},
		{id: "streamstats-02", count: 1},
		{id: "streamstats-03", count: 2},
		{id: "streamstats-04", count: 3},
		{id: "streamstats-05", count: 3},
		{id: "streamstats-06", count: 3},
		{id: "streamstats-07", count: 3},
	}
	if !reflect.DeepEqual(
		calculatedNumericConditionalGot,
		calculatedNumericConditionalWant,
	) {
		t.Fatalf(
			"calculated numeric streamstats count(eval) rows = %#v, want %#v",
			calculatedNumericConditionalGot,
			calculatedNumericConditionalWant,
		)
	}

	groupedConditional := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats window=2 global=false count(eval(event_id!="streamstats-03")) AS matched BY streamstats_group` +
			` | table event_id matched`,
	)
	groupedConditionalRows, err := connection.Query(
		ctx,
		groupedConditional.SQL,
		groupedConditional.Args...,
	)
	if err != nil {
		t.Fatalf(
			"execute grouped streamstats count(eval): %v\nSQL: %s\nargs: %#v",
			err,
			groupedConditional.SQL,
			groupedConditional.Args,
		)
	}
	var groupedConditionalGot []groupedRow
	for groupedConditionalRows.Next() {
		var row groupedRow
		if scanErr := groupedConditionalRows.Scan(&row.id, &row.peers); scanErr != nil {
			_ = groupedConditionalRows.Close()
			t.Fatalf("scan grouped streamstats count(eval): %v", scanErr)
		}
		groupedConditionalGot = append(groupedConditionalGot, row)
	}
	if rowsErr := groupedConditionalRows.Err(); rowsErr != nil {
		_ = groupedConditionalRows.Close()
		t.Fatalf("iterate grouped streamstats count(eval): %v", rowsErr)
	}
	if closeErr := groupedConditionalRows.Close(); closeErr != nil {
		t.Fatalf("close grouped streamstats count(eval): %v", closeErr)
	}
	groupedConditionalWant := []groupedRow{
		{id: "streamstats-01", peers: &one},
		{id: "streamstats-02", peers: &one},
		{id: "streamstats-03", peers: &one},
		{id: "streamstats-04"},
		{id: "streamstats-05"},
		{id: "streamstats-06", peers: &two},
		{id: "streamstats-07", peers: &one},
	}
	if !reflect.DeepEqual(groupedConditionalGot, groupedConditionalWant) {
		t.Fatalf(
			"grouped streamstats count(eval) rows = %#v, want %#v",
			groupedConditionalGot,
			groupedConditionalWant,
		)
	}

	type sumRow struct {
		id    string
		total *float64
	}
	floatPointer := func(value float64) *float64 { return &value }
	collectSums := func(name string, query CompiledQuery) []sumRow {
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
		types := rows.ColumnTypes()
		if len(types) != 2 || types[1].DatabaseTypeName() != "Nullable(Float64)" {
			_ = rows.Close()
			t.Fatalf("%s column types = %#v, want Nullable(Float64)", name, types)
		}
		var got []sumRow
		for rows.Next() {
			var row sumRow
			if scanErr := rows.Scan(&row.id, &row.total); scanErr != nil {
				_ = rows.Close()
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
	assertSums := func(name, source string, wantTotals []*float64) {
		t.Helper()
		got := collectSums(name, compile(source))
		want := make([]sumRow, len(ascendingIDs))
		for index, id := range ascendingIDs {
			want[index] = sumRow{id: id, total: wantTotals[index]}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s rows = %#v, want %#v", name, got, want)
		}
	}
	for _, test := range []struct {
		name       string
		options    string
		wantTotals []*float64
	}{
		{
			name: "numeric complete prefix",
			wantTotals: []*float64{
				floatPointer(2), floatPointer(9.5), floatPointer(8),
				floatPointer(8), floatPointer(8), floatPointer(8),
				floatPointer(8),
			},
		},
		{
			name:    "numeric complete prior prefix",
			options: "current=false",
			wantTotals: []*float64{
				nil, floatPointer(2), floatPointer(9.5), floatPointer(8),
				floatPointer(8), floatPointer(8), floatPointer(8),
			},
		},
		{
			name:    "numeric two-row current window",
			options: "window=2",
			wantTotals: []*float64{
				floatPointer(2), floatPointer(9.5), floatPointer(6),
				floatPointer(-1.5), nil, nil, floatPointer(0),
			},
		},
		{
			name:    "numeric two-row prior window",
			options: "current=false window=2",
			wantTotals: []*float64{
				nil, floatPointer(2), floatPointer(9.5), floatPointer(6),
				floatPointer(-1.5), nil, nil,
			},
		},
	} {
		assertSums(
			test.name,
			base+` | sort 0 +event_id | streamstats `+test.options+
				` sum(streamstats_sum_value) AS running_total | table event_id running_total`,
			test.wantTotals,
		)
	}
	assertAverages := func(name, source string, wantMeans []*float64) {
		t.Helper()
		got := collectSums(name, compile(source))
		want := make([]sumRow, len(ascendingIDs))
		for index, id := range ascendingIDs {
			want[index] = sumRow{id: id, total: wantMeans[index]}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s rows = %#v, want %#v", name, got, want)
		}
	}
	for _, test := range []struct {
		name      string
		options   string
		wantMeans []*float64
	}{
		{
			name: "numeric average complete prefix",
			wantMeans: []*float64{
				floatPointer(2), floatPointer(19.0 / 6.0), floatPointer(2),
				floatPointer(2), floatPointer(2), floatPointer(2),
				floatPointer(8.0 / 5.0),
			},
		},
		{
			name:    "numeric average complete prior prefix",
			options: "current=false",
			wantMeans: []*float64{
				nil, floatPointer(2), floatPointer(19.0 / 6.0), floatPointer(2),
				floatPointer(2), floatPointer(2), floatPointer(2),
			},
		},
		{
			name:    "numeric average two-row current window",
			options: "window=2",
			wantMeans: []*float64{
				floatPointer(2), floatPointer(19.0 / 6.0), floatPointer(2),
				floatPointer(-1.5), nil, nil, floatPointer(0),
			},
		},
		{
			name:    "numeric average two-row prior window",
			options: "current=false window=2",
			wantMeans: []*float64{
				nil, floatPointer(2), floatPointer(19.0 / 6.0), floatPointer(2),
				floatPointer(-1.5), nil, nil,
			},
		},
	} {
		assertAverages(
			test.name,
			base+` | sort 0 +event_id | streamstats `+test.options+
				` avg(streamstats_sum_value) AS running_mean | table event_id running_mean`,
			test.wantMeans,
		)
	}

	canonicalTimeAverage := compile(
		base + ` | sort 0 +event_id | streamstats avg(_time) AS mean_time` +
			` | tail 1 | table mean_time`,
	)
	var canonicalTimeMean *float64
	if err := connection.QueryRow(
		ctx,
		canonicalTimeAverage.SQL,
		canonicalTimeAverage.Args...,
	).Scan(&canonicalTimeMean); err != nil {
		t.Fatalf(
			"execute canonical-time streamstats avg(field): %v\nSQL: %s",
			err,
			canonicalTimeAverage.SQL,
		)
	}
	wantCanonicalTime := float64(indexTime.Add(4*time.Second).UnixNano()) / 1e9
	if canonicalTimeMean == nil ||
		math.Abs(*canonicalTimeMean-wantCanonicalTime) > 1e-6 {
		t.Fatalf(
			"canonical-time streamstats avg(field) = %v, want %g",
			canonicalTimeMean,
			wantCanonicalTime,
		)
	}

	decimalAverage := compile(
		base + ` | sort 0 +event_id | streamstats avg(streamstats_decimal) AS mean_decimal` +
			` | tail 1 | table mean_decimal`,
	)
	var decimalMean *float64
	if err := connection.QueryRow(
		ctx,
		decimalAverage.SQL,
		decimalAverage.Args...,
	).Scan(&decimalMean); err != nil {
		t.Fatalf("execute decimal streamstats avg(field): %v", err)
	}
	if decimalMean == nil || *decimalMean != 3.25 {
		t.Fatalf("decimal streamstats avg(field) = %v, want 3.25", decimalMean)
	}

	computedNonFiniteAverage := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats avg(streamstats_avg_overflow) AS mean_overflow` +
			` | tail 1 | table mean_overflow`,
	)
	var computedNonFiniteMean *float64
	if err := connection.QueryRow(
		ctx,
		computedNonFiniteAverage.SQL,
		computedNonFiniteAverage.Args...,
	).Scan(&computedNonFiniteMean); err != nil {
		t.Fatalf("execute computed non-finite streamstats avg(field): %v", err)
	}
	if computedNonFiniteMean == nil || !math.IsInf(*computedNonFiniteMean, 1) {
		t.Fatalf(
			"computed non-finite streamstats avg(field) = %v, want +Inf",
			computedNonFiniteMean,
		)
	}

	groupedSum := collectSums(
		"grouped streamstats sum(field)",
		compile(
			base+` | sort 0 +event_id`+
				` | streamstats window=2 global=false sum(streamstats_sum_value) AS running_total BY streamstats_group`+
				` | table event_id running_total`,
		),
	)
	groupedSumWant := []sumRow{
		{id: "streamstats-01", total: floatPointer(2)},
		{id: "streamstats-02", total: floatPointer(7.5)},
		{id: "streamstats-03", total: floatPointer(0.5)},
		{id: "streamstats-04"},
		{id: "streamstats-05"},
		{id: "streamstats-06", total: floatPointer(7.5)},
		{id: "streamstats-07", total: floatPointer(-1.5)},
	}
	if !reflect.DeepEqual(groupedSum, groupedSumWant) {
		t.Fatalf(
			"grouped streamstats sum(field) rows = %#v, want %#v",
			groupedSum,
			groupedSumWant,
		)
	}
	groupedAverage := collectSums(
		"grouped streamstats avg(field)",
		compile(
			base+` | sort 0 +event_id`+
				` | streamstats window=2 global=false avg(streamstats_sum_value) AS running_mean BY streamstats_group`+
				` | table event_id running_mean`,
		),
	)
	groupedAverageWant := []sumRow{
		{id: "streamstats-01", total: floatPointer(2)},
		{id: "streamstats-02", total: floatPointer(15.0 / 4.0)},
		{id: "streamstats-03", total: floatPointer(1.0 / 4.0)},
		{id: "streamstats-04"},
		{id: "streamstats-05"},
		{id: "streamstats-06", total: floatPointer(15.0 / 4.0)},
		{id: "streamstats-07", total: floatPointer(-3.0 / 4.0)},
	}
	if !reflect.DeepEqual(groupedAverage, groupedAverageWant) {
		t.Fatalf(
			"grouped streamstats avg(field) rows = %#v, want %#v",
			groupedAverage,
			groupedAverageWant,
		)
	}

	assertSums(
		"numeric alias replacement",
		base+` | sort 0 +event_id`+
			` | streamstats sum(streamstats_sum_value) AS streamstats_sum_value`+
			` | table event_id streamstats_sum_value`,
		[]*float64{
			floatPointer(2), floatPointer(9.5), floatPointer(8),
			floatPointer(8), floatPointer(8), floatPointer(8),
			floatPointer(8),
		},
	)
	assertSums(
		"projected-away numeric input",
		base+` | sort 0 +event_id | fields event_id`+
			` | streamstats sum(streamstats_sum_value) AS running_total`+
			` | table event_id running_total`,
		[]*float64{nil, nil, nil, nil, nil, nil, nil},
	)
	assertAverages(
		"numeric average alias replacement",
		base+` | sort 0 +event_id`+
			` | streamstats avg(streamstats_sum_value) AS streamstats_sum_value`+
			` | table event_id streamstats_sum_value`,
		[]*float64{
			floatPointer(2), floatPointer(19.0 / 6.0), floatPointer(2),
			floatPointer(2), floatPointer(2), floatPointer(2),
			floatPointer(8.0 / 5.0),
		},
	)
	assertAverages(
		"projected-away numeric average input",
		base+` | sort 0 +event_id | fields event_id`+
			` | streamstats avg(streamstats_sum_value) AS running_mean`+
			` | table event_id running_mean`,
		[]*float64{nil, nil, nil, nil, nil, nil, nil},
	)

	fixedMultivalue := compile(
		base + ` | stats values(streamstats_group) AS groups` +
			` | streamstats count(groups) AS populated | table populated`,
	)
	var fixedMultivalueCount uint64
	if err := connection.QueryRow(
		ctx,
		fixedMultivalue.SQL,
		fixedMultivalue.Args...,
	).Scan(&fixedMultivalueCount); err != nil {
		t.Fatalf("execute fixed multivalue streamstats count(field): %v", err)
	}
	if fixedMultivalueCount != 2 {
		t.Fatalf("fixed multivalue streamstats count(field) = %d, want 2", fixedMultivalueCount)
	}

	fixedMultivalueSum := compile(
		base + ` | stats values(streamstats_group) AS groups` +
			` | streamstats sum(groups) AS total | table total`,
	)
	if got := strings.Count(fixedMultivalueSum.SQL, "groupUniqArray"); got != 1 {
		t.Fatalf(
			"fixed multivalue streamstats sum collectors = %d, want only the upstream values collector:\n%s",
			got,
			fixedMultivalueSum.SQL,
		)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray("} {
		if strings.Contains(fixedMultivalueSum.SQL, forbidden) {
			t.Fatalf(
				"fixed multivalue streamstats sum introduced %q:\n%s",
				forbidden,
				fixedMultivalueSum.SQL,
			)
		}
	}
	var fixedMultivalueTotal *float64
	if err := connection.QueryRow(
		ctx,
		fixedMultivalueSum.SQL,
		fixedMultivalueSum.Args...,
	).Scan(&fixedMultivalueTotal); err != nil {
		t.Fatalf("execute fixed multivalue streamstats sum(field): %v", err)
	}
	if fixedMultivalueTotal == nil || *fixedMultivalueTotal != 500 {
		t.Fatalf(
			"fixed multivalue streamstats sum(field) = %v, want 500",
			fixedMultivalueTotal,
		)
	}
	fixedMultivalueAverage := compile(
		base + ` | stats values(streamstats_group) AS groups` +
			` | streamstats avg(groups) AS mean | table mean`,
	)
	if got := strings.Count(fixedMultivalueAverage.SQL, "groupUniqArray"); got != 1 {
		t.Fatalf(
			"fixed multivalue streamstats average collectors = %d, want only the upstream values collector:\n%s",
			got,
			fixedMultivalueAverage.SQL,
		)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin(", "groupArray(", "arrayAvg("} {
		if strings.Contains(fixedMultivalueAverage.SQL, forbidden) {
			t.Fatalf(
				"fixed multivalue streamstats average introduced %q:\n%s",
				forbidden,
				fixedMultivalueAverage.SQL,
			)
		}
	}
	var fixedMultivalueMean *float64
	if err := connection.QueryRow(
		ctx,
		fixedMultivalueAverage.SQL,
		fixedMultivalueAverage.Args...,
	).Scan(&fixedMultivalueMean); err != nil {
		t.Fatalf("execute fixed multivalue streamstats avg(field): %v", err)
	}
	if fixedMultivalueMean == nil || *fixedMultivalueMean != 500 {
		t.Fatalf(
			"fixed multivalue streamstats avg(field) = %v, want 500",
			fixedMultivalueMean,
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
		base + ` | streamstats count(streamstats_value) AS total` +
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
	if tenantTotal != 7 {
		t.Fatalf("tenant-scoped streamstats field total = %d, want 7", tenantTotal)
	}

	tenantScopedSum := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats sum(streamstats_sum_value) AS total` +
			` | tail 1 | table total`,
	)
	var tenantSum *float64
	if err := connection.QueryRow(
		ctx,
		tenantScopedSum.SQL,
		tenantScopedSum.Args...,
	).Scan(&tenantSum); err != nil {
		t.Fatalf("execute tenant-scoped streamstats sum: %v", err)
	}
	if tenantSum == nil || *tenantSum != 8 {
		t.Fatalf("tenant-scoped streamstats sum total = %v, want 8", tenantSum)
	}

	tenantScopedAverage := compile(
		base + ` | sort 0 +event_id` +
			` | streamstats avg(streamstats_sum_value) AS mean` +
			` | tail 1 | table mean`,
	)
	var tenantMean *float64
	if err := connection.QueryRow(
		ctx,
		tenantScopedAverage.SQL,
		tenantScopedAverage.Args...,
	).Scan(&tenantMean); err != nil {
		t.Fatalf("execute tenant-scoped streamstats average: %v", err)
	}
	if tenantMean == nil || *tenantMean != 8.0/5.0 {
		t.Fatalf("tenant-scoped streamstats average = %v, want 1.6", tenantMean)
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
			` | streamstats window=10000 count(event_id) AS ordinal` +
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

	exactConditionalBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 count(eval(host="in")) AS matched` +
			` | sort 0 -matched | head 1 | table matched`,
	)
	var exactConditionalMaximum uint64
	if err := connection.QueryRow(
		ctx,
		exactConditionalBoundary.SQL,
		exactConditionalBoundary.Args...,
	).Scan(&exactConditionalMaximum); err != nil {
		t.Fatalf(
			"execute exact streamstats count(eval) boundary: %v\nSQL: %s",
			err,
			exactConditionalBoundary.SQL,
		)
	}
	if exactConditionalMaximum != MaximumStreamStatsInputRows {
		t.Fatalf(
			"exact streamstats count(eval) boundary maximum = %d, want %d",
			exactConditionalMaximum,
			MaximumStreamStatsInputRows,
		)
	}

	exactSumBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 sum(eventstats_missing) AS total` +
			` | head 1 | table total`,
	)
	var exactEmptySum *float64
	if err := connection.QueryRow(
		ctx,
		exactSumBoundary.SQL,
		exactSumBoundary.Args...,
	).Scan(&exactEmptySum); err != nil {
		t.Fatalf(
			"execute exact streamstats sum boundary: %v\nSQL: %s",
			err,
			exactSumBoundary.SQL,
		)
	}
	if exactEmptySum != nil {
		t.Fatalf("exact streamstats sum boundary = %v, want null", exactEmptySum)
	}

	exactAverageBoundary := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary" host="in"` +
			` | streamstats window=10000 avg(eventstats_missing) AS mean` +
			` | head 1 | table mean`,
	)
	var exactEmptyMean *float64
	if err := connection.QueryRow(
		ctx,
		exactAverageBoundary.SQL,
		exactAverageBoundary.Args...,
	).Scan(&exactEmptyMean); err != nil {
		t.Fatalf(
			"execute exact streamstats average boundary: %v\nSQL: %s",
			err,
			exactAverageBoundary.SQL,
		)
	}
	if exactEmptyMean != nil {
		t.Fatalf("exact streamstats average boundary = %v, want null", exactEmptyMean)
	}

	hiddenOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats count(projected_away) AS ordinal` +
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

	hiddenConditionalOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats count(eval(event_id="not-present")) AS matched` +
			` | fields - matched | search event_id="not-present"`,
	)
	conditionalOverflowErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		hiddenConditionalOverflow,
	)
	if conditionalOverflowErr == nil ||
		!strings.Contains(conditionalOverflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats count(eval) overflow error = %v, want %q",
			conditionalOverflowErr,
			StreamStatsInputLimitMarker,
		)
	}

	hiddenSumOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats sum(projected_away) AS total` +
			` | fields - total | search event_id="not-present"`,
	)
	sumOverflowErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		hiddenSumOverflow,
	)
	if sumOverflowErr == nil ||
		!strings.Contains(sumOverflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats sum overflow error = %v, want %q",
			sumOverflowErr,
			StreamStatsInputLimitMarker,
		)
	}

	hiddenAverageOverflow := compileBoundary(
		`index=eventstats-boundary source="eventstats-boundary"` +
			` | streamstats avg(projected_away) AS mean` +
			` | fields - mean | search event_id="not-present"`,
	)
	averageOverflowErr := executeCompiledExpectingNoRows(
		ctx,
		connection,
		hiddenAverageOverflow,
	)
	if averageOverflowErr == nil ||
		!strings.Contains(averageOverflowErr.Error(), StreamStatsInputLimitMarker) {
		t.Fatalf(
			"downstream-hidden streamstats average overflow error = %v, want %q",
			averageOverflowErr,
			StreamStatsInputLimitMarker,
		)
	}
}
