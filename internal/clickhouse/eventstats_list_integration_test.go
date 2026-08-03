package clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

// testEventStatsListAgainstClickHouse deliberately reuses the stats list,
// eventstats values, and eventstats input-fence fixtures. That keeps the Store
// corpus bounded while pinning list's row-preserving ordering and publication
// behavior to the production ClickHouse image.
func testEventStatsListAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats list visibility cutoff: %v", err)
	}
	prepareRepeatedElementFixtures(
		t,
		ctx,
		connection,
		indexTime,
		visibilityCutoff,
	)
	compile := func(source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	compileForIndex := func(source, index string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			index,
		)
	}
	oneList := func(name string, query CompiledQuery) []string {
		t.Helper()
		return collectEventStatsStringArray(t, ctx, connection, name, query)
	}
	expectError := func(name string, query CompiledQuery, marker string) {
		t.Helper()
		expectCompiledErrorMarker(t, ctx, connection, name, query, marker)
	}

	const orderBase = `index=compiler source="stats-list-order"`
	wantOrdered := []string{
		"first", "duplicate", "", "second", "fourth", "duplicate", "7", "true",
	}
	explicit := oneList(
		"explicit-order eventstats list",
		compile(
			orderBase+` | sort 0 +list_sequence`+
				` | eventstats list(list_value) AS ordered | head 1 | table ordered`,
		),
	)
	if !reflect.DeepEqual(explicit, wantOrdered) {
		t.Fatalf("explicit-order eventstats list = %#v, want %#v", explicit, wantOrdered)
	}

	// All fixture times tie, so the default immutable event ordering is visible
	// directly. Missing and null rows contribute nothing while immediate
	// multivalue members retain their stored order and duplicates.
	defaultOrdered := oneList(
		"default-order eventstats list",
		compile(
			orderBase+
				` | eventstats list(list_value) AS ordered | head 1 | table ordered`,
		),
	)
	wantDefault := []string{
		"second", "fourth", "first", "duplicate", "", "duplicate", "7", "true",
	}
	if !reflect.DeepEqual(defaultOrdered, wantDefault) {
		t.Fatalf("default-order eventstats list = %#v, want %#v", defaultOrdered, wantDefault)
	}

	for _, test := range []struct {
		name    string
		grouped bool
	}{
		{name: "global complete empty"},
		{name: "grouped complete empty", grouped: true},
	} {
		test := test
		base := orderBase + ` event_id="list-order-null"` +
			` | eventstats list(list_value) AS ordered`
		if test.grouped {
			base += ` BY list_group`
		}
		empty := oneList(test.name, compile(base+` | table ordered`))
		if empty == nil || len(empty) != 0 {
			t.Fatalf("%s eventstats list = %#v, want non-nil []", test.name, empty)
		}
		var present uint64
		presence := compile(base + ` | search ordered=* | stats count`)
		if queryErr := connection.QueryRow(
			ctx,
			presence.SQL,
			presence.Args...,
		).Scan(&present); queryErr != nil || present != 0 {
			t.Fatalf(
				"%s eventstats list presence = %d err=%v, want 0",
				test.name,
				present,
				queryErr,
			)
		}
	}

	groupedQuery := compile(
		`index=compiler source="stats-list-incomplete-by"` +
			` | eventstats list(list_value) AS ordered BY list_group` +
			` | sort event_id | table event_id ordered`,
	)
	grouped := collectEventStatsStringArrayRows(
		t,
		ctx,
		connection,
		"grouped eventstats list",
		groupedQuery,
	)
	wantGrouped := []eventStatsStringArrayRow{
		{id: "list-by-complete", values: []string{"visible"}},
		{id: "list-by-incomplete-poison", values: []string{}},
	}
	if !reflect.DeepEqual(grouped, wantGrouped) {
		t.Fatalf("grouped eventstats list = %#v, want %#v", grouped, wantGrouped)
	}
	groupedPresence := compile(
		`index=compiler source="stats-list-incomplete-by"` +
			` | eventstats list(list_value) AS ordered BY list_group` +
			` | search ordered=* | stats count`,
	)
	var count uint64
	if queryErr := connection.QueryRow(
		ctx,
		groupedPresence.SQL,
		groupedPresence.Args...,
	).Scan(&count); queryErr != nil || count != 1 {
		t.Fatalf("grouped eventstats list presence = %d err=%v, want 1", count, queryErr)
	}

	// The first eventstats stage publishes a fixed Array(String) on both rows.
	// The earlier row has an incomplete BY tuple whose normalized key collides
	// with the later row's present empty String. It must not consume that
	// complete group's first-100 window prefix.
	fixedGroupedQuery := compile(
		`index=compiler source="eventstats-list-fixed-incomplete-by"` +
			` | sort 0 +list_sequence` +
			` | eventstats values(list_fixed_seed) AS fixed_values` +
			` | eventstats list(fixed_values) AS ordered BY list_group` +
			` | sort event_id | table event_id ordered`,
	)
	fixedGrouped := collectEventStatsStringArrayRows(
		t,
		ctx,
		connection,
		"fixed grouped eventstats list",
		fixedGroupedQuery,
	)
	wantFixedValues := make([]string, MaximumStatsListValuesPerGroup)
	for index := range wantFixedValues {
		wantFixedValues[index] = fmt.Sprintf("fixed-%03d", index)
	}
	wantFixedGrouped := []eventStatsStringArrayRow{
		{id: "list-fixed-by-complete", values: wantFixedValues},
		{id: "list-fixed-by-incomplete", values: []string{}},
	}
	if !reflect.DeepEqual(fixedGrouped, wantFixedGrouped) {
		t.Fatalf(
			"fixed grouped eventstats list = %#v, want %#v",
			fixedGrouped,
			wantFixedGrouped,
		)
	}

	projected := oneList(
		"projected-away eventstats list",
		compile(
			orderBase+` | fields event_id`+
				` | eventstats list(list_value) AS ordered | head 1 | table ordered`,
		),
	)
	if projected == nil || len(projected) != 0 {
		t.Fatalf("projected-away eventstats list = %#v, want non-nil []", projected)
	}
	projectedPresence := compile(
		orderBase + ` | fields event_id` +
			` | eventstats list(list_value) AS ordered` +
			` | search ordered=* | stats count`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		projectedPresence.SQL,
		projectedPresence.Args...,
	).Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("projected eventstats list presence = %d err=%v, want 0", count, queryErr)
	}

	replaced := compile(
		orderBase + ` | sort 0 +list_sequence` +
			` | eventstats list(list_value) AS list_value` +
			` | stats count(list_value) AS occurrences` +
			` values(list_value) AS distinct_values list(list_value) AS repeated`,
	)
	var occurrences uint64
	var distinct, repeated []string
	if queryErr := connection.QueryRow(
		ctx,
		replaced.SQL,
		replaced.Args...,
	).Scan(&occurrences, &distinct, &repeated); queryErr != nil {
		t.Fatalf("execute replacement/downstream eventstats list: %v\nSQL: %s", queryErr, replaced.SQL)
	}
	wantDistinct := []string{"", "7", "duplicate", "first", "fourth", "second", "true"}
	wantRepeated := make([]string, 0, len(wantOrdered)*6)
	for range 6 {
		wantRepeated = append(wantRepeated, wantOrdered...)
	}
	if occurrences != uint64(len(wantRepeated)) ||
		!reflect.DeepEqual(distinct, wantDistinct) ||
		!reflect.DeepEqual(repeated, wantRepeated) {
		t.Fatalf(
			"replacement/downstream eventstats list = %d/%#v/%#v, want %d/%#v/%#v",
			occurrences,
			distinct,
			repeated,
			len(wantRepeated),
			wantDistinct,
			wantRepeated,
		)
	}

	listThenCount := compile(
		orderBase + ` | sort 0 +list_sequence` +
			` | eventstats list(list_value) AS ordered BY host` +
			` | eventstats count(ordered) AS occurrences BY host` +
			` | head 1 | table occurrences`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		listThenCount.SQL,
		listThenCount.Args...,
	).Scan(&occurrences); queryErr != nil || occurrences != uint64(len(wantRepeated)) {
		t.Fatalf(
			"stacked list then count = %d err=%v, want %d",
			occurrences,
			queryErr,
			len(wantRepeated),
		)
	}
	countThenList := oneList(
		"stacked count then list",
		compile(
			orderBase+` | sort 0 +list_sequence`+
				` | eventstats count(list_value) AS occurrences BY host`+
				` | eventstats list(occurrences) AS occurrence_values BY host`+
				` | head 1 | table occurrence_values`,
		),
	)
	wantCounts := make([]string, 6)
	for index := range wantCounts {
		wantCounts[index] = strconv.Itoa(len(wantOrdered))
	}
	if !reflect.DeepEqual(countThenList, wantCounts) {
		t.Fatalf("stacked count then list = %#v, want %#v", countThenList, wantCounts)
	}

	for _, test := range []struct {
		name  string
		field string
	}{
		{name: "object", field: "dc_object"},
		{name: "empty object", field: "dc_empty_object"},
		{name: "nested array", field: "dc_nested"},
	} {
		hiddenPoison := compile(
			`index=compiler source="stats-sum-avg" | eventstats list(` +
				test.field + `) AS discarded` +
				` | head 1 | table event_id | search event_id="definitely-absent"`,
		)
		expectError(
			"hidden eventstats list poison "+test.name,
			hiddenPoison,
			UnsupportedStatsMeasureValueMarker,
		)
	}
	expectError(
		"eventstats list poison after occurrence 100",
		compile(
			`index=compiler source="stats-list-poison" | sort 0 +list_sequence`+
				` | eventstats list(list_value) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		UnsupportedStatsMeasureValueMarker,
	)

	exactHundred := oneList(
		"exact eventstats list element boundary",
		compile(
			`index=compiler source="stats-list-truncate" | where list_sequence<100`+
				` | sort 0 +list_sequence | eventstats list(list_value) AS ordered`+
				` | head 1 | table ordered`,
		),
	)
	if len(exactHundred) != int(MaximumStatsListValuesPerGroup) ||
		exactHundred[0] != "value-000" || exactHundred[len(exactHundred)-1] != "value-099" {
		t.Fatalf(
			"exact eventstats list boundary = len:%d first:%q last:%q",
			len(exactHundred),
			exactHundred[0],
			exactHundred[len(exactHundred)-1],
		)
	}
	truncated := oneList(
		"truncated eventstats list",
		compile(
			`index=compiler source="stats-list-truncate" | sort 0 +list_sequence`+
				` | eventstats list(list_value) AS ordered | head 1 | table ordered`,
		),
	)
	if !reflect.DeepEqual(truncated, exactHundred) {
		t.Fatalf("101-member eventstats list = %#v, want capped %#v", truncated, exactHundred)
	}

	exactBytes := oneList(
		"exact eventstats list cell byte boundary",
		compile(
			`index=compiler source="stats-list-bytes-exact"`+
				` | eventstats list(list_value) AS ordered | head 1 | table ordered`,
		),
	)
	if len(exactBytes) != 1 || len(exactBytes[0]) != int(MaximumStatsListBytesPerGroup) {
		t.Fatalf("exact eventstats list bytes = %d/%d", len(exactBytes), len(exactBytes[0]))
	}
	expectError(
		"eventstats list cell byte overflow",
		compile(
			`index=compiler source="stats-list-bytes-over"`+
				` | eventstats list(list_value) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		EventStatsListBytesLimitMarker,
	)

	// The shared eventstats-values fixture has one 512 KiB value repeated over
	// exactly 16 rows, then over 17 rows. This pins the 8 MiB whole-result
	// boundary without another large integration fixture.
	resultBytesExact := oneList(
		"exact repeated eventstats list byte boundary",
		compile(
			`index=compiler source="eventstats-values-total-bytes-exact"`+
				` | eventstats list(eventstats_values_boundary) AS ordered`+
				` | head 1 | table ordered`,
		),
	)
	if len(resultBytesExact) != 1 ||
		len(resultBytesExact[0]) != int(MaximumStatsListBytesPerGroup) {
		t.Fatalf(
			"exact repeated eventstats list bytes = %d/%d",
			len(resultBytesExact),
			len(resultBytesExact[0]),
		)
	}
	expectError(
		"eventstats list repeated byte overflow",
		compile(
			`index=compiler source="eventstats-values-total-bytes-over"`+
				` | eventstats list(eventstats_values_boundary) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		EventStatsListBytesLimitMarker,
	)

	repeatedElementsExact := oneList(
		"exact repeated eventstats list element boundary",
		compile(
			`index=compiler source="eventstats-list-elements-exact"`+
				` | eventstats list(eventstats_list_boundary) AS ordered`+
				` | head 1 | table ordered`,
		),
	)
	if len(repeatedElementsExact) != int(MaximumStatsListValuesPerGroup) {
		t.Fatalf(
			"exact repeated eventstats list elements = %d, want %d",
			len(repeatedElementsExact),
			MaximumStatsListValuesPerGroup,
		)
	}
	expectError(
		"eventstats list repeated element overflow",
		compile(
			`index=compiler source="eventstats-list-elements-over"`+
				` | eventstats list(eventstats_list_boundary) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		EventStatsListLimitMarker,
	)

	invalidFixed := string([]byte{0xff, 0, 'b', 'y', 't', 'e', 's'})
	invalidRaw := oneList(
		"invalid UTF-8 fixed eventstats list",
		compile(
			`index=compiler source="eventstats-values-invalid-fixed"`+
				` | eventstats list(host) AS ordered | table ordered`,
		),
	)
	if want := []string{invalidFixed}; !reflect.DeepEqual(invalidRaw, want) {
		t.Fatalf("invalid UTF-8 eventstats list = %q, want byte-exact %q", invalidRaw, want)
	}

	exactRows := oneList(
		"exact eventstats list row boundary",
		compileForIndex(
			`index=eventstats-boundary host="in"`+
				` | eventstats list(host) AS hosts BY event_id`+
				` | head 1 | table hosts`,
			"eventstats-boundary",
		),
	)
	if want := []string{"in"}; !reflect.DeepEqual(exactRows, want) {
		t.Fatalf("exact-row eventstats list = %#v, want %#v", exactRows, want)
	}
	expectError(
		"eventstats list row overflow",
		compileForIndex(
			`index=eventstats-boundary | eventstats list(host) AS hosts BY event_id`+
				` | head 1 | table hosts`,
			"eventstats-boundary",
		),
		EventStatsInputLimitMarker,
	)

	physical := compile(
		orderBase + ` | sort 0 +list_sequence` +
			` | eventstats list(list_value) AS ordered BY list_group` +
			` | head 1 | table ordered`,
	)
	actions := explainCompiledQuery(
		t,
		ctx,
		connection,
		"EXPLAIN actions=1 ",
		physical,
	)
	// ClickHouse repeats a materialized CTE's action description for its
	// readers. Compiler tests therefore pin the single SQL aggregate definition;
	// the live plan proves that definition remains bounded and non-expanding.
	if !strings.Contains(actions, "Function: groupArraySortedArray(") {
		t.Fatalf("eventstats list physical plan lost its bounded ordered state:\n%s", actions)
	}
	if strings.Contains(actions, "ArrayJoin") || strings.Contains(actions, "Function: groupArray(") {
		t.Fatalf("eventstats list physical plan expands or unboundedly buffers rows:\n%s", actions)
	}
	if got := strings.Count(physical.SQL, `FROM "open_splunk"."events"`); got != 1 {
		t.Fatalf("eventstats list physical scans = %d, want 1:\n%s", got, physical.SQL)
	}
	for _, forbidden := range []string{"ARRAY JOIN", "arrayJoin("} {
		if strings.Contains(physical.SQL, forbidden) {
			t.Fatalf("eventstats list SQL contains %q:\n%s", forbidden, physical.SQL)
		}
	}
}

func prepareRepeatedElementFixtures(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	indexTime time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()

	batch, err := connection.PrepareBatch(ctx, `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			host, source, sourcetype, severity, raw, raw_encoding,
			fields, field_names, field_types, field_metadata_version,
			collector_id, batch_id, batch_sequence, expires_at,
			visibility_seq
		)`)
	if err != nil {
		t.Fatalf("prepare repeated eventstats list elements: %v", err)
	}
	members := make([]clickhousedriver.Dynamic, MaximumStatsListValuesPerGroup)
	for index := range members {
		members[index] = clickhousedriver.NewDynamicWithType(
			fmt.Sprintf("value-%03d", index),
			"String",
		)
	}
	appendScope := func(source string, rows int) {
		t.Helper()
		for index := 0; index < rows; index++ {
			document := clickhousedriver.NewJSON()
			var names []string
			var types []uint8
			if index == 0 {
				document.SetValueAtPath(
					"eventstats_list_boundary",
					clickhousedriver.NewDynamicWithType(members, "Array(Dynamic)"),
				)
				names = []string{"eventstats_list_boundary"}
				types = []uint8{uint8(eventfields.StoredValueTypeList)}
			}
			id := fmt.Sprintf("%s-%04d", source, index)
			appendErr := batch.Append(
				id,
				"tenant",
				"compiler",
				indexTime.Add(-time.Duration(index)*time.Nanosecond),
				indexTime,
				"eventstats-list-elements-host",
				source,
				"eventstats-list-elements",
				uint8(0),
				"eventstats list repeated elements",
				uint8(1),
				document,
				names,
				types,
				eventfields.CurrentFieldMetadataVersion,
				"eventstats-list-elements",
				source,
				uint64(index+1),
				MaximumSearchTime(),
				visibilityCutoff,
			)
			if appendErr != nil {
				_ = batch.Abort()
				t.Fatalf("append repeated eventstats list fixture %q: %v", id, appendErr)
			}
		}
	}
	appendScope("eventstats-list-elements-exact", 1_000)
	appendScope("eventstats-list-elements-over", 1_001)
	if err := batch.Send(); err != nil {
		t.Fatalf("send repeated eventstats list fixtures: %v", err)
	}
}
