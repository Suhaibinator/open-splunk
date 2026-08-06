package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testStatsExtremaAgainstClickHouse pins the runtime min/max contract against
// the same ClickHouse version production uses. It deliberately lives outside
// the already-large aggregate fixture so failures remain attributable.
func testStatsExtremaAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	newEvent := func(id, group string, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		fields = append([]*opensplunkv1.TypedObjectField{
			typedField("extrema_group", typedString(group)),
		}, fields...)
		event := compilerIntegrationEvent(id, "stats-extrema-host", "stats extrema fixture", indexTime, fields...)
		event.BatchID = "stats-extrema-batch"
		event.Event.Source = "stats-extrema"
		return event
	}
	withService := func(event *ingest.StoredEvent, service string) *ingest.StoredEvent {
		event.Event.Service = stringPointer(service)
		return event
	}
	overlongValue := strings.Repeat("9", MaximumExactNumericBinTextBytes+1)
	events := []*ingest.StoredEvent{
		withService(newEvent("extrema-numeric-a", "numeric", typedField("extrema_value", typedString("10"))), "10"),
		withService(newEvent("extrema-numeric-b", "numeric", typedField("extrema_value", typedList(
			typedString("2"), typedString("01"), typedString("1.0"), typedNull(),
		))), "2"),
		withService(newEvent("extrema-lexical-a", "lexical", typedField("extrema_value", typedString(""))), ""),
		withService(newEvent("extrema-lexical-b", "lexical", typedField("extrema_value", typedList(
			typedString("A"), typedString("a"), typedString("é"),
		))), "é"),
		withService(newEvent("extrema-mixed-a", "mixed", typedField("extrema_value", typedList(
			typedString("20"), typedString("5"), typedNull(),
		))), "5"),
		withService(newEvent("extrema-mixed-b", "mixed", typedField("extrema_value", typedString("z"))), "z"),
		withService(newEvent("extrema-symbols", "symbols", typedField("extrema_value", typedList(
			typedString("2"), typedString("!"), typedString("~"),
		))), "2"),
		withService(newEvent("extrema-symbols-a", "symbols"), "!"),
		withService(newEvent("extrema-symbols-b", "symbols"), "~"),
		withService(newEvent("extrema-fallback", "fallback", typedField("extrema_value", typedList(
			typedString("3"), typedString("NaN"), typedString("1e9999"), typedString(" 2"),
		))), "3"),
		withService(newEvent("extrema-fallback-nan", "fallback"), "NaN"),
		withService(newEvent("extrema-fallback-overflow", "fallback"), "1e9999"),
		withService(newEvent("extrema-fallback-space", "fallback"), " 2"),
		withService(newEvent("extrema-zero", "zero", typedField("extrema_value", typedList(
			typedString("-0"), typedString("+0"), typedString("0.0"),
		))), "-0"),
		withService(newEvent("extrema-zero-plus", "zero"), "+0"),
		withService(newEvent("extrema-zero-decimal", "zero"), "0.0"),
		withService(newEvent(
			"extrema-overlong",
			"overlong",
			typedField("extrema_value", typedString(overlongValue)),
		), overlongValue),
		newEvent("extrema-empty-missing", "empty"),
		newEvent("extrema-empty-null", "empty", typedField("extrema_value", typedNull())),
		newEvent("extrema-empty-list", "empty", typedField("extrema_value", typedList(typedNull()))),
		newEvent("extrema-poison-object", "poison", typedField(
			"extrema_poison", typedObject(typedField("child", typedString("secret"))),
		)),
		newEvent("extrema-poison-nested", "poison", typedField(
			"extrema_nested", typedList(typedString("safe"), typedList(typedString("secret"))),
		)),
		withService(newEvent("extrema-incomplete-by", "ignored", typedField(
			"extrema_incomplete", typedObject(typedField("child", typedString("secret"))),
		)), "-1000"),
		withService(newEvent("extrema-complete-by", "complete",
			typedField("required_group", typedString("present")),
			typedField("extrema_incomplete", typedString("7")),
		), "7"),
	}
	for eventIndex, event := range events {
		event.Event.EventTime = timestamppb.New(
			indexTime.Add(time.Duration(eventIndex) * time.Nanosecond),
		)
	}
	batch := ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       "collector",
		BatchID:           "stats-extrema-batch",
		BatchSequence:     11,
		SourceBatchSHA256: testSourceBatchDigest("stats-extrema-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}
	if _, err := store.Store(ctx, batch); err != nil {
		t.Fatalf("store min/max fixtures: %v", err)
	}

	foreign := newEvent(
		"extrema-foreign-poison",
		"foreign",
		typedField("extrema_scope", typedObject(typedField("secret", typedString("hidden")))),
	)
	foreign.TenantID = "other-tenant"
	foreign.BatchID = "stats-extrema-foreign-batch"
	foreign.Event.Service = stringPointer("-999999")
	foreignBatch := ingest.StoreBatch{
		TenantID:          "other-tenant",
		CollectorID:       "collector",
		BatchID:           "stats-extrema-foreign-batch",
		BatchSequence:     12,
		SourceBatchSHA256: testSourceBatchDigest("stats-extrema-foreign-batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{foreign},
	}
	if _, err := store.Store(ctx, foreignBatch); err != nil {
		t.Fatalf("store cross-tenant min/max poison: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture min/max visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(t, source, indexTime.Add(10*time.Second), visibilityCutoff)
	}
	base := `index=compiler source="stats-extrema"`

	grouped := compile(base + ` | stats min(extrema_value) AS low max(extrema_value) AS high BY extrema_group | sort extrema_group`)
	rows, err := connection.Query(ctx, grouped.SQL, grouped.Args...)
	if err != nil {
		t.Fatalf("execute grouped min/max: %v\nSQL: %s\nargs: %#v", err, grouped.SQL, grouped.Args)
	}
	type extremaRow struct {
		group string
		low   any
		high  any
	}
	var got []extremaRow
	for rows.Next() {
		var group string
		var low, high chcol.Dynamic
		if err := rows.Scan(&group, &low, &high); err != nil {
			_ = rows.Close()
			t.Fatalf("scan grouped min/max: %v", err)
		}
		got = append(got, extremaRow{group: group, low: low.Any(), high: high.Any()})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate grouped min/max: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close grouped min/max rows: %v", err)
	}
	want := []extremaRow{
		{group: "complete", low: nil, high: nil},
		{group: "empty", low: nil, high: nil},
		{group: "fallback", low: float64(3), high: "NaN"},
		{group: "ignored", low: nil, high: nil},
		{group: "lexical", low: "", high: "é"},
		{group: "mixed", low: float64(5), high: "z"},
		{group: "numeric", low: float64(1), high: float64(10)},
		{group: "overlong", low: overlongValue, high: overlongValue},
		{group: "poison", low: nil, high: nil},
		{group: "symbols", low: float64(2), high: "~"},
		{group: "zero", low: float64(0), high: float64(0)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("grouped min/max = %#v, want %#v", got, want)
	}

	scalarGrouped := compile(
		base + ` | stats min(service) AS low max(service) AS high BY extrema_group | sort extrema_group`,
	)
	scalarRows, err := connection.Query(ctx, scalarGrouped.SQL, scalarGrouped.Args...)
	if err != nil {
		t.Fatalf(
			"execute grouped scalar String min/max: %v\nSQL: %s\nargs: %#v",
			err,
			scalarGrouped.SQL,
			scalarGrouped.Args,
		)
	}
	var scalarGot []extremaRow
	for scalarRows.Next() {
		var group string
		var low, high chcol.Dynamic
		if err := scalarRows.Scan(&group, &low, &high); err != nil {
			_ = scalarRows.Close()
			t.Fatalf("scan grouped scalar String min/max: %v", err)
		}
		scalarGot = append(scalarGot, extremaRow{group: group, low: low.Any(), high: high.Any()})
	}
	if err := scalarRows.Err(); err != nil {
		_ = scalarRows.Close()
		t.Fatalf("iterate grouped scalar String min/max: %v", err)
	}
	if err := scalarRows.Close(); err != nil {
		t.Fatalf("close grouped scalar String min/max rows: %v", err)
	}
	scalarWant := []extremaRow{
		{group: "complete", low: float64(7), high: float64(7)},
		{group: "empty", low: nil, high: nil},
		{group: "fallback", low: float64(3), high: "NaN"},
		{group: "ignored", low: float64(-1000), high: float64(-1000)},
		{group: "lexical", low: "", high: "é"},
		{group: "mixed", low: float64(5), high: "z"},
		{group: "numeric", low: float64(2), high: float64(10)},
		{group: "overlong", low: overlongValue, high: overlongValue},
		{group: "poison", low: nil, high: nil},
		{group: "symbols", low: float64(2), high: "~"},
		{group: "zero", low: float64(0), high: float64(0)},
	}
	if !reflect.DeepEqual(scalarGot, scalarWant) {
		t.Fatalf("grouped scalar String min/max = %#v, want %#v", scalarGot, scalarWant)
	}
	scalar := compile(
		base + ` extrema_group=numeric | stats min(service) AS low max(service) AS high min(service) AS low_again`,
	)
	var scalarLow, scalarHigh, scalarLowAgain chcol.Dynamic
	if err := connection.QueryRow(ctx, scalar.SQL, scalar.Args...).Scan(
		&scalarLow,
		&scalarHigh,
		&scalarLowAgain,
	); err != nil {
		t.Fatalf("execute repeated scalar min/max: %v\nSQL: %s", err, scalar.SQL)
	}
	if scalarLow.Any() != float64(2) ||
		scalarHigh.Any() != float64(10) ||
		scalarLowAgain.Any() != float64(2) {
		t.Fatalf(
			"repeated scalar min/max = %#v/%#v/%#v, want 2/10/2",
			scalarLow.Any(),
			scalarHigh.Any(),
			scalarLowAgain.Any(),
		)
	}
	scalarActions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", scalar)
	const scalarMinSignature = "Function: argMinOrNullIf(" +
		"Tuple(UInt8, Float64, String), " +
		"Tuple(UInt8, UInt8, Int64, String, String, UInt8), UInt8)"
	if got := strings.Count(scalarActions, scalarMinSignature); got != 1 {
		t.Fatalf("repeated scalar min has %d physical aggregate states, want one:\n%s", got, scalarActions)
	}
	const scalarMaxSignature = "Function: argMaxOrNullIf(" +
		"Tuple(UInt8, Float64, String), " +
		"Tuple(UInt8, UInt8, Int64, String, String, UInt8), UInt8)"
	if got := strings.Count(scalarActions, scalarMaxSignature); got != 1 {
		t.Fatalf("scalar min/max has %d physical max states, want one:\n%s", got, scalarActions)
	}
	if strings.Contains(scalarActions, "Function: argMinArray(") ||
		strings.Contains(scalarActions, "Function: argMaxArray(") ||
		strings.Contains(scalarActions, "ArrayJoin") {
		t.Fatalf("scalar String min/max retained array work:\n%s", scalarActions)
	}

	scalarValues := compile(
		base + ` extrema_group=numeric | stats min(service) AS low values(service) AS all_values`,
	)
	var scalarValuesLow chcol.Dynamic
	var scalarValueList []string
	if err := connection.QueryRow(ctx, scalarValues.SQL, scalarValues.Args...).Scan(
		&scalarValuesLow,
		&scalarValueList,
	); err != nil {
		t.Fatalf("execute shared scalar min/values: %v\nSQL: %s", err, scalarValues.SQL)
	}
	if scalarValuesLow.Any() != float64(2) ||
		!reflect.DeepEqual(scalarValueList, []string{"10", "2"}) {
		t.Fatalf(
			"shared scalar min/values = %#v/%#v, want 2/[10 2]",
			scalarValuesLow.Any(),
			scalarValueList,
		)
	}

	scalarGlobalEmpty := compile(
		base + ` event_id=does-not-exist | stats min(service) AS low max(service) AS high`,
	)
	var scalarEmptyLow, scalarEmptyHigh chcol.Dynamic
	if err := connection.QueryRow(ctx, scalarGlobalEmpty.SQL, scalarGlobalEmpty.Args...).Scan(
		&scalarEmptyLow,
		&scalarEmptyHigh,
	); err != nil {
		t.Fatalf("execute scalar global empty min/max: %v\nSQL: %s", err, scalarGlobalEmpty.SQL)
	}
	if scalarEmptyLow.Any() != nil || scalarEmptyHigh.Any() != nil {
		t.Fatalf(
			"scalar global empty min/max = %#v/%#v, want null/null",
			scalarEmptyLow.Any(),
			scalarEmptyHigh.Any(),
		)
	}
	scalarGroupedEmpty := compile(
		base + ` event_id=does-not-exist | stats min(service) AS low BY extrema_group`,
	)
	var scalarGroupedCount uint64
	if err := connection.QueryRow(
		ctx,
		"SELECT count() FROM ("+scalarGroupedEmpty.SQL+")",
		scalarGroupedEmpty.Args...,
	).Scan(&scalarGroupedCount); err != nil {
		t.Fatalf("execute scalar grouped empty min: %v\nSQL: %s", err, scalarGroupedEmpty.SQL)
	}
	if scalarGroupedCount != 0 {
		t.Fatalf("scalar grouped empty min rows = %d, want 0", scalarGroupedCount)
	}

	scalarEligible := compile(
		base + ` (event_id=extrema-incomplete-by OR event_id=extrema-complete-by)` +
			` | stats min(service) AS low BY required_group`,
	)
	var scalarRequiredGroup string
	var scalarEligibleLow chcol.Dynamic
	if err := connection.QueryRow(ctx, scalarEligible.SQL, scalarEligible.Args...).Scan(
		&scalarRequiredGroup,
		&scalarEligibleLow,
	); err != nil {
		t.Fatalf("execute incomplete-BY scalar min guard: %v\nSQL: %s", err, scalarEligible.SQL)
	}
	if scalarRequiredGroup != "present" || scalarEligibleLow.Any() != float64(7) {
		t.Fatalf(
			"incomplete-BY scalar min = %q/%#v, want present/7",
			scalarRequiredGroup,
			scalarEligibleLow.Any(),
		)
	}

	scalarScoped := compile(base + ` event_id=extrema-foreign-poison | stats min(service) AS low`)
	var scalarScopedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, scalarScoped.SQL, scalarScoped.Args...).Scan(&scalarScopedLow); err != nil {
		t.Fatalf("execute cross-tenant scalar min: %v\nSQL: %s", err, scalarScoped.SQL)
	}
	if scalarScopedLow.Any() != nil {
		t.Fatalf("cross-tenant scalar min = %#v, want null", scalarScopedLow.Any())
	}

	scalarBinned := compile(
		base + ` extrema_group=numeric | stats min(service) AS low | bin low span=2 | table low`,
	)
	var scalarBinnedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, scalarBinned.SQL, scalarBinned.Args...).Scan(&scalarBinnedLow); err != nil {
		t.Fatalf("execute downstream scalar min bin: %v\nSQL: %s", err, scalarBinned.SQL)
	}
	if scalarBinnedLow.Any() != float64(2) {
		t.Fatalf("downstream binned scalar min = %#v, want 2", scalarBinnedLow.Any())
	}
	scalarReaggregated := compile(
		base + ` | stats min(service) AS low BY extrema_group | stats max(low) AS largest`,
	)
	var scalarLargest chcol.Dynamic
	if err := connection.QueryRow(ctx, scalarReaggregated.SQL, scalarReaggregated.Args...).Scan(&scalarLargest); err != nil {
		t.Fatalf("execute downstream scalar min/max reaggregation: %v\nSQL: %s", err, scalarReaggregated.SQL)
	}
	if scalarLargest.Any() != overlongValue {
		t.Fatalf("reaggregated scalar max = %#v, want overlong lexical String", scalarLargest.Any())
	}

	for name, source := range map[string]string{
		"flattened object": base + ` event_id=extrema-poison-object | stats min(extrema_poison) AS low`,
		"nested list":      base + ` event_id=extrema-poison-nested | stats max(extrema_nested) AS high`,
	} {
		query := compile(source)
		queryErr := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(new(chcol.Dynamic))
		if queryErr == nil || !strings.Contains(queryErr.Error(), UnsupportedStatsMeasureValueMarker) {
			t.Fatalf("%s error = %v, want guarded unsupported-value marker", name, queryErr)
		}
	}

	eligible := compile(base + ` (event_id=extrema-incomplete-by OR event_id=extrema-complete-by) | stats min(extrema_incomplete) AS low BY required_group`)
	var requiredGroup string
	var eligibleLow chcol.Dynamic
	if err := connection.QueryRow(ctx, eligible.SQL, eligible.Args...).Scan(&requiredGroup, &eligibleLow); err != nil {
		t.Fatalf("execute incomplete-BY min/max guard: %v\nSQL: %s", err, eligible.SQL)
	}
	if requiredGroup != "present" || eligibleLow.Any() != float64(7) {
		t.Fatalf("incomplete-BY min = %q/%#v, want present/7", requiredGroup, eligibleLow.Any())
	}

	scoped := compile(base + ` | stats min(extrema_scope) AS low`)
	var scopedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, scoped.SQL, scoped.Args...).Scan(&scopedLow); err != nil {
		t.Fatalf("cross-tenant poison affected min/max: %v\nSQL: %s", err, scoped.SQL)
	}
	if scopedLow.Any() != nil {
		t.Fatalf("cross-tenant scoped min = %#v, want null", scopedLow.Any())
	}

	projected := compile(base + ` | fields extrema_group | stats min(extrema_value) AS low`)
	var projectedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, projected.SQL, projected.Args...).Scan(&projectedLow); err != nil {
		t.Fatalf("execute projected-away min: %v\nSQL: %s", err, projected.SQL)
	}
	if projectedLow.Any() != nil {
		t.Fatalf("projected-away min = %#v, want null", projectedLow.Any())
	}

	globalEmpty := compile(base + ` event_id=does-not-exist | stats min(extrema_value) AS low`)
	var emptyLow chcol.Dynamic
	if err := connection.QueryRow(ctx, globalEmpty.SQL, globalEmpty.Args...).Scan(&emptyLow); err != nil {
		t.Fatalf("execute global empty min: %v\nSQL: %s", err, globalEmpty.SQL)
	}
	if emptyLow.Any() != nil {
		t.Fatalf("global empty min = %#v, want null", emptyLow.Any())
	}
	groupedEmpty := compile(base + ` event_id=does-not-exist | stats min(extrema_value) AS low BY extrema_group`)
	var groupedCount uint64
	if err := connection.QueryRow(ctx, "SELECT count() FROM ("+groupedEmpty.SQL+")", groupedEmpty.Args...).Scan(&groupedCount); err != nil {
		t.Fatalf("execute grouped empty min: %v\nSQL: %s", err, groupedEmpty.SQL)
	}
	if groupedCount != 0 {
		t.Fatalf("grouped empty min rows = %d, want 0", groupedCount)
	}

	binned := compile(base + ` extrema_group=mixed | stats min(extrema_value) AS low | bin low span=2 | table low`)
	var binnedLow chcol.Dynamic
	if err := connection.QueryRow(ctx, binned.SQL, binned.Args...).Scan(&binnedLow); err != nil {
		t.Fatalf("execute downstream min bin: %v\nSQL: %s", err, binned.SQL)
	}
	if binnedLow.Any() != float64(4) {
		t.Fatalf("downstream binned min = %#v, want 4", binnedLow.Any())
	}
	reaggregated := compile(base + ` | stats min(extrema_value) AS low BY extrema_group | stats max(low) AS largest`)
	var largest chcol.Dynamic
	if err := connection.QueryRow(ctx, reaggregated.SQL, reaggregated.Args...).Scan(&largest); err != nil {
		t.Fatalf("execute downstream min/max reaggregation: %v\nSQL: %s", err, reaggregated.SQL)
	}
	if largest.Any() != overlongValue {
		t.Fatalf("reaggregated max = %#v, want overlong lexical String", largest.Any())
	}

	nativeTime := compile(base + ` | stats min(_time) AS earliest max(_time) AS latest`)
	var earliest, latest time.Time
	if err := connection.QueryRow(ctx, nativeTime.SQL, nativeTime.Args...).Scan(&earliest, &latest); err != nil {
		t.Fatalf("execute native time extrema: %v\nSQL: %s", err, nativeTime.SQL)
	}
	wantEarliest := events[0].Event.EventTime.AsTime()
	wantLatest := events[len(events)-1].Event.EventTime.AsTime()
	if !earliest.Equal(wantEarliest) || !latest.Equal(wantLatest) {
		t.Fatalf(
			"native time extrema = %s/%s, want %s/%s",
			earliest,
			latest,
			wantEarliest,
			wantLatest,
		)
	}
	for _, value := range []bool{false, true} {
		literal := "false"
		if value {
			literal = "true"
		}
		nativeBool := compile(
			base + ` | eval selected=` + literal +
				` | stats min(selected) AS lowest max(selected) AS highest`,
		)
		var lowest, highest bool
		if err := connection.QueryRow(ctx, nativeBool.SQL, nativeBool.Args...).Scan(&lowest, &highest); err != nil {
			t.Fatalf("execute native Bool %s extrema: %v\nSQL: %s", literal, err, nativeBool.SQL)
		}
		if lowest != value || highest != value {
			t.Fatalf(
				"native Bool %s extrema = %t/%t, want %t/%t",
				literal,
				lowest,
				highest,
				value,
				value,
			)
		}
	}

	shared := compile(base + ` | stats min(extrema_value) AS low max(extrema_value) AS high min(extrema_value) AS low_again`)
	actions := explainCompiledQuery(t, ctx, connection, "EXPLAIN actions=1 ", shared)
	if got := strings.Count(actions, "Function: argMinArray("); got != 1 {
		t.Fatalf("repeated min has %d physical aggregate states, want one:\n%s", got, actions)
	}
	if got := strings.Count(actions, "Function: argMaxArray("); got != 1 {
		t.Fatalf("min/max has %d physical max states, want one:\n%s", got, actions)
	}
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("min/max physical plan expands event rows:\n%s", actions)
	}
	if strings.Count(shared.SQL, `AS "__os_measure_extrema_0"`) != 1 ||
		strings.Contains(shared.SQL, `__os_measure_extrema_1`) {
		t.Fatalf("min/max normalization is not shared:\n%s", shared.SQL)
	}
}
