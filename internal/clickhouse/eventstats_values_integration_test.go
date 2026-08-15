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

// testEventStatsValuesAgainstClickHouse reuses the transforming-stats corpus
// and eventstats row fence. Its small additional corpus pins values-specific
// cell and repeated-annotation publication limits without another large scan.
func testEventStatsValuesAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture eventstats values visibility cutoff: %v", err)
	}
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

	batch, prepareErr := connection.PrepareBatch(ctx, `
		INSERT INTO open_splunk.events
		(
			event_id, tenant_id, index_name, event_time, index_time,
			host, source, sourcetype, severity, raw, raw_encoding,
			fields, field_names, field_types, field_metadata_version,
			collector_id, batch_id, batch_sequence, expires_at,
			visibility_seq
		)`)
	if prepareErr != nil {
		t.Fatalf("prepare eventstats values boundaries: %v", prepareErr)
	}
	insertedIDs := make([]string, 0, 64)
	appendFixtureWithHost := func(
		id string,
		source string,
		host string,
		value any,
		present bool,
	) {
		t.Helper()
		document := clickhousedriver.NewJSON()
		var names []string
		var types []uint8
		if present {
			document.SetValueAtPath("eventstats_values_boundary", value)
			names = []string{"eventstats_values_boundary"}
			types = []uint8{uint8(eventfields.StoredValueTypeString)}
			if dynamic, ok := value.(clickhousedriver.Dynamic); ok &&
				dynamic.Type() == "Array(Dynamic)" {
				types[0] = uint8(eventfields.StoredValueTypeList)
			}
		}
		sequence := uint64(len(insertedIDs) + 1)
		appendErr := batch.Append(
			id,
			"tenant",
			"compiler",
			indexTime,
			indexTime,
			host,
			source,
			"eventstats-values-boundary",
			uint8(0),
			"eventstats values boundary",
			uint8(1),
			document,
			names,
			types,
			eventfields.CurrentFieldMetadataVersion,
			"eventstats-values-boundaries",
			id,
			sequence,
			MaximumSearchTime(),
			visibilityCutoff,
		)
		if appendErr != nil {
			_ = batch.Abort()
			t.Fatalf("append eventstats values boundary %q: %v", id, appendErr)
		}
		insertedIDs = append(insertedIDs, id)
	}
	appendFixture := func(id, source string, value any, present bool) {
		t.Helper()
		appendFixtureWithHost(
			id,
			source,
			"eventstats-values-boundary-host",
			value,
			present,
		)
	}

	members := make([]clickhousedriver.Dynamic, MaximumStatsValuesPerGroup+1)
	members[0] = clickhousedriver.NewDynamicWithType(true, "Bool")
	for index := 1; index < len(members); index++ {
		members[index] = clickhousedriver.NewDynamicWithType(
			strconv.Itoa(index-1),
			"String",
		)
	}
	appendFixture(
		"eventstats-values-cell-exact",
		"eventstats-values-cell-exact",
		clickhousedriver.NewDynamicWithType(
			members[:MaximumStatsValuesPerGroup],
			"Array(Dynamic)",
		),
		true,
	)
	appendFixture(
		"eventstats-values-cell-over",
		"eventstats-values-cell-over",
		clickhousedriver.NewDynamicWithType(members, "Array(Dynamic)"),
		true,
	)

	// Ten rows annotated with the exact 10,000-member cell publish exactly
	// 100,000 recursive result elements. Eleven rows annotated with 9,091
	// members publish exactly 100,001.
	for index := range 10 {
		value := any(clickhousedriver.Dynamic{})
		present := index == 0
		if present {
			value = clickhousedriver.NewDynamicWithType(
				members[:MaximumStatsValuesPerGroup],
				"Array(Dynamic)",
			)
		}
		appendFixture(
			fmt.Sprintf("eventstats-values-total-elements-exact-%02d", index),
			"eventstats-values-total-elements-exact",
			value,
			present,
		)
	}
	const valuesPerOverResultRow = 9_091
	for index := range 11 {
		value := any(clickhousedriver.Dynamic{})
		present := index == 0
		if present {
			value = clickhousedriver.NewDynamicWithType(
				members[:valuesPerOverResultRow],
				"Array(Dynamic)",
			)
		}
		appendFixture(
			fmt.Sprintf("eventstats-values-total-elements-over-%02d", index),
			"eventstats-values-total-elements-over",
			value,
			present,
		)
	}

	exactBytes := strings.Repeat("x", int(MaximumStatsValuesBytesPerGroup))
	for index := range 16 {
		appendFixture(
			fmt.Sprintf("eventstats-values-total-bytes-exact-%02d", index),
			"eventstats-values-total-bytes-exact",
			clickhousedriver.NewDynamicWithType(exactBytes, "String"),
			index == 0,
		)
	}
	for index := range 17 {
		appendFixture(
			fmt.Sprintf("eventstats-values-total-bytes-over-%02d", index),
			"eventstats-values-total-bytes-over",
			clickhousedriver.NewDynamicWithType(exactBytes, "String"),
			index == 0,
		)
	}
	appendFixture(
		"eventstats-values-cell-bytes-over",
		"eventstats-values-cell-bytes-over",
		clickhousedriver.NewDynamicWithType(exactBytes+"x", "String"),
		true,
	)
	invalidFixed := string([]byte{0xff, 0, 'b', 'y', 't', 'e', 's'})
	appendFixtureWithHost(
		"eventstats-values-invalid-fixed",
		"eventstats-values-invalid-fixed",
		invalidFixed,
		nil,
		false,
	)
	if sendErr := batch.Send(); sendErr != nil {
		t.Fatalf("send eventstats values boundaries: %v", sendErr)
	}
	var inserted uint64
	if countErr := connection.QueryRow(
		ctx,
		`SELECT count() FROM open_splunk.events WHERE collector_id = ?`,
		"eventstats-values-boundaries",
	).Scan(&inserted); countErr != nil || inserted != uint64(len(insertedIDs)) {
		t.Fatalf(
			"verify eventstats values boundaries: count=%d want=%d err=%v",
			inserted,
			len(insertedIDs),
			countErr,
		)
	}

	oneValues := func(name string, query CompiledQuery) []string {
		t.Helper()
		return collectEventStatsStringArray(t, ctx, connection, name, query)
	}
	expectError := func(name string, query CompiledQuery, marker string) {
		t.Helper()
		expectCompiledErrorMarker(t, ctx, connection, name, query, marker)
	}

	const base = `index=compiler source="stats-sum-avg"`
	global := oneValues(
		"global eventstats values",
		compile(
			base+` | eventstats values(distinct_value) AS distinct_values`+
				` | head 1 | table distinct_values`,
		),
	)
	if want := []string{"", "01", "1", "1.0", "Case", "case", "repeat"}; !reflect.DeepEqual(global, want) {
		t.Fatalf("global eventstats values = %#v, want %#v", global, want)
	}
	ordered := oneValues(
		"raw-byte ordered eventstats values",
		compile(
			base+` | eventstats values(ordering_value) AS ordered_values`+
				` | head 1 | table ordered_values`,
		),
	)
	if want := []string{"10", "100", "70", "9", "A", "a", "e\u0301", "é"}; !reflect.DeepEqual(ordered, want) {
		t.Fatalf("ordered eventstats values = %#v, want raw-byte lexical %#v", ordered, want)
	}

	grouped := collectEventStatsStringArrayRows(
		t,
		ctx,
		connection,
		"grouped eventstats values",
		compile(
			base+` | eventstats values(distinct_value) AS distinct_values BY aggregate_group`+
				` | sort event_id | table event_id distinct_values`,
		),
	)
	wantA := []string{"", "01", "1", "1.0", "Case", "case"}
	wantB := []string{"repeat"}
	if want := []eventStatsStringArrayRow{
		{id: "sum-avg-a-array", values: wantA},
		{id: "sum-avg-a-int", values: wantA},
		{id: "sum-avg-a-missing", values: wantA},
		{id: "sum-avg-a-string", values: wantA},
		{id: "sum-avg-b-bad", values: wantB},
		{id: "sum-avg-b-null", values: wantB},
	}; !reflect.DeepEqual(grouped, want) {
		t.Fatalf("grouped eventstats values = %#v, want %#v", grouped, want)
	}

	// The sole object is outside the complete official_group scopes. Every
	// retained row has a physical [] boundary, and empty complete scopes are
	// logically absent just like incomplete-BY rows.
	incomplete := collectEventStatsStringArrayRows(
		t,
		ctx,
		connection,
		"incomplete-BY eventstats values",
		compile(
			base+` | eventstats values(dc_object) AS object_kinds BY official_group`+
				` | sort event_id | table event_id object_kinds`,
		),
	)
	if len(incomplete) != 6 {
		t.Fatalf("incomplete-BY eventstats values rows = %d, want 6", len(incomplete))
	}
	for _, row := range incomplete {
		if row.values == nil || len(row.values) != 0 {
			t.Fatalf("incomplete-BY row %q physical values = %#v, want non-nil []", row.id, row.values)
		}
	}
	logicalAbsence := compile(
		base + ` | eventstats values(dc_object) AS object_kinds BY official_group` +
			` | search object_kinds=* | stats count`,
	)
	var count uint64
	if queryErr := connection.QueryRow(
		ctx,
		logicalAbsence.SQL,
		logicalAbsence.Args...,
	).Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("empty/incomplete eventstats values presence = %d err=%v, want 0", count, queryErr)
	}

	projected := oneValues(
		"projected-away eventstats values",
		compile(
			base+` | fields event_id | eventstats values(distinct_value) AS distinct_values`+
				` | head 1 | table distinct_values`,
		),
	)
	if projected == nil || len(projected) != 0 {
		t.Fatalf("projected-away eventstats values = %#v, want non-nil []", projected)
	}
	projectedPresence := compile(
		base + ` | fields event_id | eventstats values(distinct_value) AS distinct_values` +
			` | search distinct_values=* | stats count`,
	)
	if queryErr := connection.QueryRow(
		ctx,
		projectedPresence.SQL,
		projectedPresence.Args...,
	).Scan(&count); queryErr != nil || count != 0 {
		t.Fatalf("projected eventstats values presence = %d err=%v, want 0", count, queryErr)
	}

	replaced := compile(
		base + ` | eventstats values(distinct_value) AS distinct_value` +
			` | stats count(distinct_value) AS occurrences values(distinct_value) AS repeated`,
	)
	var occurrences uint64
	var repeated []string
	if queryErr := connection.QueryRow(
		ctx,
		replaced.SQL,
		replaced.Args...,
	).Scan(&occurrences, &repeated); queryErr != nil {
		t.Fatalf("execute replacement/downstream eventstats values: %v\nSQL: %s", queryErr, replaced.SQL)
	}
	if occurrences != 42 || !reflect.DeepEqual(repeated, global) {
		t.Fatalf("replacement/downstream eventstats values = %d/%#v, want 42/%#v", occurrences, repeated, global)
	}

	for _, field := range []string{"dc_object", "dc_empty_object", "dc_nested"} {
		hiddenPoison := compile(
			base + ` | eventstats values(` + field + `) AS discarded` +
				` | head 1 | table event_id | search event_id="definitely-absent"`,
		)
		expectError(
			"hidden eventstats values poison "+field,
			hiddenPoison,
			UnsupportedStatsMeasureValueMarker,
		)
	}

	cellExact := oneValues(
		"exact eventstats values cell element boundary",
		compile(
			`index=compiler source="eventstats-values-cell-exact"`+
				` | eventstats values(eventstats_values_boundary) AS values`+
				` | head 1 | table values`,
		),
	)
	if len(cellExact) != int(MaximumStatsValuesPerGroup) {
		t.Fatalf("exact eventstats values cell = %d values, want %d", len(cellExact), MaximumStatsValuesPerGroup)
	}
	expectError(
		"eventstats values cell element overflow",
		compile(
			`index=compiler source="eventstats-values-cell-over"`+
				` | eventstats values(eventstats_values_boundary) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		EventStatsValuesLimitMarker,
	)

	resultElementsExact := oneValues(
		"exact eventstats values repeated element boundary",
		compile(
			`index=compiler source="eventstats-values-total-elements-exact"`+
				` | eventstats values(eventstats_values_boundary) AS values`+
				` | head 1 | table values`,
		),
	)
	if len(resultElementsExact) != int(MaximumStatsValuesPerGroup) {
		t.Fatalf(
			"exact repeated eventstats values result = %d values, want %d",
			len(resultElementsExact),
			MaximumStatsValuesPerGroup,
		)
	}
	valuesThenCount := compile(
		`index=compiler source="eventstats-values-total-elements-exact"` +
			` | eventstats values(eventstats_values_boundary) AS distinct_values BY host` +
			` | eventstats count(distinct_values) AS occurrences BY host` +
			` | head 1 | table occurrences`,
	)
	var stackedOccurrences uint64
	if queryErr := connection.QueryRow(
		ctx,
		valuesThenCount.SQL,
		valuesThenCount.Args...,
	).Scan(&stackedOccurrences); queryErr != nil ||
		stackedOccurrences != MaximumStatsValuesPerResult {
		t.Fatalf(
			"stacked values then count = %d err=%v, want %d",
			stackedOccurrences,
			queryErr,
			MaximumStatsValuesPerResult,
		)
	}
	countThenValues := oneValues(
		"stacked count then values",
		compile(
			`index=compiler source="eventstats-values-total-elements-exact"`+
				` | eventstats count(eventstats_values_boundary) AS occurrences BY host`+
				` | eventstats values(occurrences) AS occurrence_values BY host`+
				` | head 1 | table occurrence_values`,
		),
	)
	if want := []string{strconv.FormatUint(MaximumStatsValuesPerGroup, 10)}; !reflect.DeepEqual(countThenValues, want) {
		t.Fatalf("stacked count then values = %#v, want %#v", countThenValues, want)
	}
	expectError(
		"eventstats values repeated element overflow",
		compile(
			`index=compiler source="eventstats-values-total-elements-over"`+
				` | eventstats values(eventstats_values_boundary) AS discarded`+
				` | head 1 | table event_id | search event_id="absent"`,
		),
		EventStatsValuesLimitMarker,
	)

	resultBytesExact := oneValues(
		"exact eventstats values repeated byte boundary",
		compile(
			`index=compiler source="eventstats-values-total-bytes-exact"`+
				` | eventstats values(eventstats_values_boundary) AS values`+
				` | head 1 | table values`,
		),
	)
	if len(resultBytesExact) != 1 || len(resultBytesExact[0]) != int(MaximumStatsValuesBytesPerGroup) {
		t.Fatalf("exact repeated eventstats values bytes = %d/%d", len(resultBytesExact), len(resultBytesExact[0]))
	}
	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "cell bytes",
			source: `index=compiler source="eventstats-values-cell-bytes-over"`,
		},
		{
			name:   "repeated bytes",
			source: `index=compiler source="eventstats-values-total-bytes-over"`,
		},
	} {
		expectError(
			"eventstats values "+test.name+" overflow",
			compile(
				test.source+` | eventstats values(eventstats_values_boundary) AS discarded`+
					` | head 1 | table event_id | search event_id="absent"`,
			),
			EventStatsValuesBytesLimitMarker,
		)
	}

	// ClickHouse String is byte-preserving. The Array(String) result carries the
	// exact invalid UTF-8 member; the ordinary typed-result boundary already
	// classifies every invalid Array(String) member as Bytes.
	invalidRaw := oneValues(
		"invalid UTF-8 fixed eventstats values",
		compile(
			`index=compiler source="eventstats-values-invalid-fixed"`+
				` | eventstats values(host) AS values | table values`,
		),
	)
	if want := []string{invalidFixed}; !reflect.DeepEqual(invalidRaw, want) {
		t.Fatalf("invalid UTF-8 eventstats values = %q, want byte-exact %q", invalidRaw, want)
	}

	exactRows := oneValues(
		"exact eventstats values row boundary",
		compileForIndex(
			`index=eventstats-boundary host="in"`+
				` | eventstats values(host) AS hosts | head 1 | table hosts`,
			"eventstats-boundary",
		),
	)
	if want := []string{"in"}; !reflect.DeepEqual(exactRows, want) {
		t.Fatalf("exact-row-boundary eventstats values = %#v, want %#v", exactRows, want)
	}
	expectError(
		"eventstats values row overflow",
		compileForIndex(
			`index=eventstats-boundary | eventstats values(host) AS hosts`+
				` | head 1 | table hosts`,
			"eventstats-boundary",
		),
		EventStatsInputLimitMarker,
	)

	actions := explainCompiledQuery(t, ctx, connection, explainActionsPrefix, compile(
		base+` | eventstats values(distinct_value) AS first | head 1 | table first`,
	))
	if countPhysicalAggregates(actions, "groupUniqArrayArray(", "groupUniqArrayArray(") == 0 {
		t.Fatalf("eventstats values physical plan lost its exact state:\n%s", actions)
	}
	if strings.Contains(actions, "ArrayJoin") || strings.Contains(actions, "groupArray(") {
		t.Fatalf("eventstats values physical plan expands or buffers rows:\n%s", actions)
	}
}
