package clickhouse

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// testTrustedFiniteFloatOrderingKeyAgainstClickHouse differentially proves the
// publication-only parser against the generic attacker-facing parser. Boundary
// bit patterns pin signed zero, subnormal/normal transitions, and the largest
// finite magnitudes; deterministic hashed bits densely sample binary exponent
// classes without relying on generated decimal input.
func testTrustedFiniteFloatOrderingKeyAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()

	boundarySource := `SELECT arrayJoin([
		reinterpretAsFloat64(toUInt64(0)),
		reinterpretAsFloat64(toUInt64(9223372036854775808)),
		reinterpretAsFloat64(toUInt64(1)),
		reinterpretAsFloat64(toUInt64(9223372036854775809)),
		reinterpretAsFloat64(toUInt64(4503599627370495)),
		reinterpretAsFloat64(toUInt64(4503599627370496)),
		reinterpretAsFloat64(toUInt64(4607182418800017408)),
		reinterpretAsFloat64(toUInt64(9218868437227405311)),
		reinterpretAsFloat64(toUInt64(18442240474082181119)),
		toFloat64('0.001'),
		toFloat64('1000'),
		toFloat64('1e20'),
		toFloat64('-1e20'),
		toFloat64('1.2345678901234567')
	]) AS finite_value`
	randomSource := `SELECT
		reinterpretAsFloat64(cityHash64(number)) AS finite_value
		FROM numbers(1000000)
		WHERE isFinite(finite_value)`
	for _, corpus := range []struct {
		name   string
		source string
	}{
		{name: "boundary bit patterns", source: boundarySource},
		{name: "deterministic hashed bit patterns", source: randomSource},
	} {
		generic := exactNumericOrderingKeySQL("toString(finite_value)")
		trusted := trustedFiniteFloatOrderingKeySQL("finite_value")
		query := "SELECT count(), countIf(generic_key != trusted_key) FROM (" +
			"SELECT " + generic + " AS generic_key, " + trusted +
			" AS trusted_key FROM (" + corpus.source + "))"
		var total, mismatches uint64
		if err := connection.QueryRow(ctx, query).Scan(&total, &mismatches); err != nil {
			t.Fatalf(
				"compare trusted finite Float64 keys for %s: %v\nSQL: %s",
				corpus.name,
				err,
				query,
			)
		}
		if total == 0 || mismatches != 0 {
			t.Fatalf(
				"trusted finite Float64 key %s corpus = %d values/%d mismatches",
				corpus.name,
				total,
				mismatches,
			)
		}
	}
}

// testExactNumericOrderingAgainstClickHouse pins the mixed integer/decimal
// boundary that Float64 cannot distinguish. The same values exercise
// field-to-field comparison, numeric-looking String search, auto-sort, and
// both extrema lowerings on the production ClickHouse version.
func testExactNumericOrderingAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()
	testTrustedFiniteFloatOrderingKeyAgainstClickHouse(t, ctx, connection)

	const (
		negativeLower = "-9007199254740992.75"
		negativeUpper = "-9007199254740992"
		positiveLower = "9007199254740992.75"
		positiveUpper = "9007199254740993"
	)
	raw4096 := "." + strings.Repeat(
		"1",
		MaximumExactNumericOrderingInputTextBytes-1,
	)
	raw4097 := "." + strings.Repeat(
		"2",
		MaximumExactNumericOrderingInputTextBytes,
	)
	longLexicalA := strings.Repeat(
		"a",
		MaximumExactNumericOrderingInputTextBytes+1,
	)
	longLexicalZ := strings.Repeat(
		"z",
		MaximumExactNumericOrderingInputTextBytes+1,
	)
	newEvent := func(
		id string,
		service string,
		fields ...*opensplunkv1.TypedObjectField,
	) *ingest.StoredEvent {
		event := compilerIntegrationEvent(
			id,
			"exact-numeric-host",
			"exact numeric ordering fixture",
			indexTime,
			fields...,
		)
		event.BatchID = "exact-numeric-ordering-batch"
		event.Event.Source = "exact-numeric-ordering"
		event.Event.Service = new(service)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent(
			"exact-numeric-lower",
			positiveLower,
			typedField("exact_sort", typedDecimal(positiveLower)),
			typedField("exact_ratio", typedString(positiveLower)),
			typedField("exact_case", typedString("positive")),
			typedField("exact_values", typedList(
				typedDecimal(positiveLower),
				typedUint(9_007_199_254_740_993),
			)),
		),
		newEvent(
			"exact-numeric-upper",
			positiveUpper,
			typedField("exact_sort", typedUint(9_007_199_254_740_993)),
			typedField("exact_integer", typedUint(9_007_199_254_740_993)),
			typedField("exact_decimal", typedDecimal(positiveLower)),
			typedField("exact_case", typedString("positive")),
		),
		newEvent(
			"exact-numeric-negative-lower",
			negativeLower,
			typedField("exact_sort", typedDecimal(negativeLower)),
			typedField("exact_case", typedString("negative")),
			typedField("exact_values", typedList(
				typedDecimal(negativeLower),
				typedSint(-9_007_199_254_740_992),
			)),
		),
		newEvent(
			"exact-numeric-negative-upper",
			negativeUpper,
			typedField("exact_sort", typedSint(-9_007_199_254_740_992)),
			typedField("exact_integer", typedSint(-9_007_199_254_740_992)),
			typedField("exact_decimal", typedDecimal(negativeLower)),
			typedField("exact_case", typedString("negative")),
		),
	}
	newBoundaryEvent := func(
		id string,
		group string,
		value *opensplunkv1.TypedValue,
	) *ingest.StoredEvent {
		event := newEvent(
			id,
			id,
			typedField("exact_boundary_group", typedString(group)),
			typedField("exact_boundary_value", value),
		)
		event.Event.Source = "exact-numeric-boundaries"
		return event
	}
	events = append(events,
		newBoundaryEvent("exact-zero-plain", "zero", typedDecimal("0")),
		newBoundaryEvent("exact-zero-fraction", "zero", typedDecimal("0.0")),
		newBoundaryEvent("exact-zero-negative", "zero", typedDecimal("-0")),
		newBoundaryEvent("exact-zero-oversized-exponent", "zero", typedDecimal("0e10001")),
		newBoundaryEvent("exact-prefix-lower", "negative-prefix", typedDecimal("-1.001")),
		newBoundaryEvent("exact-prefix-upper", "negative-prefix", typedDecimal("-1")),
		newBoundaryEvent("exact-equivalent-plain", "equivalent", typedDecimal("1")),
		newBoundaryEvent("exact-equivalent-fraction", "equivalent", typedDecimal("1.0")),
		newBoundaryEvent("exact-equivalent-exponent", "equivalent", typedDecimal("10e-1")),
		newBoundaryEvent("exact-exponent-negative-large", "exponent", typedDecimal("-1e10000")),
		newBoundaryEvent("exact-exponent-negative-small", "exponent", typedDecimal("-1e-10000")),
		newBoundaryEvent("exact-exponent-positive-small", "exponent", typedDecimal("1e-10000")),
		newBoundaryEvent("exact-exponent-positive-large", "exponent", typedDecimal("1e10000")),
		newBoundaryEvent("exact-exponent-rejected-negative-large", "exponent", typedDecimal("-1e10001")),
		newBoundaryEvent("exact-exponent-rejected-negative-small", "exponent", typedDecimal("-1e-10001")),
		newBoundaryEvent("exact-exponent-rejected-positive-small", "exponent", typedDecimal("1e-10001")),
		newBoundaryEvent("exact-exponent-rejected-positive-large", "exponent", typedDecimal("1e10001")),
		newBoundaryEvent("exact-length-4096", "length", typedString(raw4096)),
		newBoundaryEvent("exact-length-4097", "length", typedString(raw4097)),
		newBoundaryEvent("exact-length-small", "length-small", typedString("1")),
		// Insert lexical z before a so a collapsed over-limit sort key cannot
		// accidentally pass via the stable event-order tie-breaker.
		newBoundaryEvent("exact-long-lexical-z", "long-lexical", typedString(longLexicalZ)),
		newBoundaryEvent("exact-long-lexical-a", "long-lexical", typedString(longLexicalA)),
	)
	for eventIndex, event := range events {
		event.Event.EventTime = timestamppb.New(
			indexTime.Add(time.Duration(eventIndex) * time.Nanosecond),
		)
	}
	if _, err := store.Store(ctx, ingest.StoreBatch{
		TenantID:          "tenant",
		CollectorID:       "collector",
		BatchID:           "exact-numeric-ordering-batch",
		BatchSequence:     13,
		SourceBatchSHA256: testSourceBatchDigest("exact-numeric-ordering-batch"),
		ReceivedAt:        indexTime,
		Events:            events,
	}); err != nil {
		t.Fatalf("store exact numeric ordering fixtures: %v", err)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture exact numeric visibility cutoff: %v", err)
	}
	compile := func(source string) CompiledQuery {
		return compileIntegrationSPL(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
		)
	}
	base := `index=compiler source="exact-numeric-ordering"`

	for _, test := range []struct {
		name             string
		source           string
		want             string
		wantMaterialized bool
	}{
		{
			name:   "positive field pair",
			source: base + ` exact_case=positive | where exact_integer>exact_decimal | table event_id`,
			want:   "exact-numeric-upper",
		},
		{
			name:   "negative field pair",
			source: base + ` exact_case=negative | where exact_integer>exact_decimal | table event_id`,
			want:   "exact-numeric-negative-upper",
		},
		{
			name: "repeated positive field pair",
			source: base + ` exact_case=positive | where ` +
				`exact_integer>exact_decimal AND exact_integer>exact_decimal | table event_id`,
			want:             "exact-numeric-upper",
			wantMaterialized: true,
		},
		{
			name: "positive calculated integer domain",
			source: base + ` exact_case=positive | eval rounded=round(exact_integer)` +
				` | where rounded>exact_decimal | table event_id`,
			want: "exact-numeric-upper",
		},
		{
			name: "negative calculated integer domain",
			source: base + ` exact_case=negative | eval rounded=round(exact_integer)` +
				` | where rounded>exact_decimal | table event_id`,
			want: "exact-numeric-negative-upper",
		},
		{
			name:   "base String",
			source: base + ` exact_ratio<9007199254740993 | table event_id`,
			want:   "exact-numeric-lower",
		},
		{
			name:   "base decimal literal",
			source: base + ` exact_integer>9007199254740992.75 | table event_id`,
			want:   "exact-numeric-upper",
		},
		{
			name:   "base decimal equality",
			source: base + ` exact_decimal=9007199254740992.75 | table event_id`,
			want:   "exact-numeric-upper",
		},
		{
			name:   "where decimal literal",
			source: base + ` exact_case=positive | where exact_integer>9007199254740992.75 | table event_id`,
			want:   "exact-numeric-upper",
		},
		{
			name:   "where reversed decimal literal",
			source: base + ` exact_case=positive | where 9007199254740992.75<exact_integer | table event_id`,
			want:   "exact-numeric-upper",
		},
	} {
		query := compile(test.source)
		if test.wantMaterialized {
			for _, alias := range []string{
				"__os_filter_exact_key_",
				"__os_filter_exact_numeric_",
			} {
				if !strings.Contains(query.SQL, alias) {
					t.Fatalf(
						"exact numeric %s SQL did not materialize %q:\n%s",
						test.name,
						alias,
						query.SQL,
					)
				}
			}
		}
		var eventID string
		if err := connection.QueryRow(ctx, query.SQL, query.Args...).Scan(&eventID); err != nil {
			t.Fatalf(
				"execute exact numeric %s comparison: %v\nSQL: %s\nargs: %#v",
				test.name,
				err,
				query.SQL,
				query.Args,
			)
		}
		if eventID != test.want {
			t.Fatalf(
				"exact numeric %s comparison matched %q, want %q",
				test.name,
				eventID,
				test.want,
			)
		}
	}

	sorted := compile(base + ` | sort exact_sort | table event_id`)
	rows, err := connection.Query(ctx, sorted.SQL, sorted.Args...)
	if err != nil {
		t.Fatalf("execute exact numeric sort: %v\nSQL: %s\nargs: %#v", err, sorted.SQL, sorted.Args)
	}
	var sortedIDs []string
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			_ = rows.Close()
			t.Fatalf("scan exact numeric sort: %v", err)
		}
		sortedIDs = append(sortedIDs, eventID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("iterate exact numeric sort: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close exact numeric sort rows: %v", err)
	}
	if want := []string{
		"exact-numeric-negative-lower",
		"exact-numeric-negative-upper",
		"exact-numeric-lower",
		"exact-numeric-upper",
	}; !reflect.DeepEqual(sortedIDs, want) {
		t.Fatalf("exact numeric sort = %v, want %v", sortedIDs, want)
	}

	for _, test := range []struct {
		name   string
		source string
	}{
		{
			name:   "dynamic list",
			source: base + ` | stats min(exact_values) AS low max(exact_values) AS high`,
		},
		{
			name:   "scalar String",
			source: base + ` | stats min(service) AS low max(service) AS high`,
		},
	} {
		query := compile(test.source)
		wrapped := `SELECT
			dynamicType(low),
			if(dynamicType(low) = 'Map(String, String)',
				dynamicElement(low, 'Map(String, String)')[concat(char(0), 'open_splunk_type')], ''),
			if(dynamicType(low) = 'Map(String, String)',
				dynamicElement(low, 'Map(String, String)')[concat(char(0), 'open_splunk_value')], toString(low)),
			dynamicType(high),
			if(dynamicType(high) = 'Map(String, String)',
				dynamicElement(high, 'Map(String, String)')[concat(char(0), 'open_splunk_type')], ''),
			if(dynamicType(high) = 'Map(String, String)',
				dynamicElement(high, 'Map(String, String)')[concat(char(0), 'open_splunk_value')], toString(high))
			FROM (` + query.SQL + `)`
		var lowPhysical, lowTag, lowValue, highPhysical, highTag, highValue string
		if err := connection.QueryRow(ctx, wrapped, query.Args...).Scan(
			&lowPhysical,
			&lowTag,
			&lowValue,
			&highPhysical,
			&highTag,
			&highValue,
		); err != nil {
			t.Fatalf(
				"execute exact numeric %s extrema: %v\nSQL: %s\nargs: %#v",
				test.name,
				err,
				query.SQL,
				query.Args,
			)
		}
		if lowPhysical != "Map(String, String)" || lowTag != "decimal/v1" || lowValue != negativeLower ||
			highPhysical != "Map(String, String)" || highTag != "decimal/v1" || highValue != positiveUpper {
			t.Fatalf(
				"exact numeric %s extrema = %q/%q/%q and %q/%q/%q, want decimal %s/%s",
				test.name,
				lowPhysical,
				lowTag,
				lowValue,
				highPhysical,
				highTag,
				highValue,
				negativeLower,
				positiveUpper,
			)
		}
	}

	binnedExtremum := compile(
		base + ` | stats min(service) AS low | bin low span=1 | table low`,
	)
	var binnedPhysical, binnedValue string
	if err := connection.QueryRow(
		ctx,
		"SELECT dynamicType(low), toString(low) FROM ("+binnedExtremum.SQL+")",
		binnedExtremum.Args...,
	).Scan(&binnedPhysical, &binnedValue); err != nil {
		t.Fatalf(
			"execute exact numeric Decimal extrema bin: %v\nSQL: %s\nargs: %#v",
			err,
			binnedExtremum.SQL,
			binnedExtremum.Args,
		)
	}
	if binnedPhysical != "Int256" || binnedValue != "-9007199254740993" {
		t.Fatalf(
			"binned exact Decimal extremum = %q/%q, want Int256/-9007199254740993",
			binnedPhysical,
			binnedValue,
		)
	}

	querySingleStringColumn := func(name, source string) []string {
		t.Helper()

		query := compile(source)
		rows, queryErr := connection.Query(ctx, query.SQL, query.Args...)
		if queryErr != nil {
			t.Fatalf(
				"execute exact numeric %s: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		var values []string
		for rows.Next() {
			var value string
			if scanErr := rows.Scan(&value); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan exact numeric %s: %v", name, scanErr)
			}
			values = append(values, value)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			t.Fatalf("iterate exact numeric %s: %v", name, rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			t.Fatalf("close exact numeric %s rows: %v", name, closeErr)
		}
		return values
	}
	assertCount := func(name, source string, want uint64) {
		t.Helper()

		query := compile(source)
		var got uint64
		if queryErr := connection.QueryRow(
			ctx,
			"SELECT count() FROM ("+query.SQL+")",
			query.Args...,
		).Scan(&got); queryErr != nil {
			t.Fatalf(
				"execute exact numeric %s count: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		if got != want {
			t.Fatalf("exact numeric %s count = %d, want %d", name, got, want)
		}
	}

	downstreamExtrema := base +
		` | stats min(exact_sort) AS extremum BY exact_case`
	if got, want := querySingleStringColumn(
		"downstream extrema sort",
		downstreamExtrema+` | sort extremum | table exact_case`,
	), []string{"negative", "positive"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("downstream exact extrema sort = %v, want %v", got, want)
	}
	if got, want := querySingleStringColumn(
		"downstream extrema comparison",
		downstreamExtrema+` | where extremum>0 | table exact_case`,
	), []string{"positive"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("downstream exact extrema comparison = %v, want %v", got, want)
	}

	boundaryBase := `index=compiler source="exact-numeric-boundaries"`
	assertCount(
		"zero spelling equivalence",
		boundaryBase+` exact_boundary_group=zero | where exact_boundary_value=0`,
		4,
	)
	assertCount(
		"exponent spelling equivalence",
		boundaryBase+` exact_boundary_group=equivalent | where exact_boundary_value=1`,
		3,
	)
	if got, want := querySingleStringColumn(
		"negative coefficient-prefix ordering",
		boundaryBase+
			` exact_boundary_group="negative-prefix" | sort exact_boundary_value | table event_id`,
	), []string{"exact-prefix-lower", "exact-prefix-upper"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("negative coefficient-prefix ordering = %v, want %v", got, want)
	}
	if got, want := querySingleStringColumn(
		"negative exponent boundary ordering",
		boundaryBase+
			` exact_boundary_group=exponent | where exact_boundary_value<0 | sort exact_boundary_value | table event_id`,
	), []string{
		"exact-exponent-negative-large",
		"exact-exponent-negative-small",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("negative exponent boundary ordering = %v, want %v", got, want)
	}
	if got, want := querySingleStringColumn(
		"positive exponent boundary ordering",
		boundaryBase+
			` exact_boundary_group=exponent | where exact_boundary_value>0 | sort exact_boundary_value | table event_id`,
	), []string{
		"exact-exponent-positive-small",
		"exact-exponent-positive-large",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("positive exponent boundary ordering = %v, want %v", got, want)
	}
	if got, want := querySingleStringColumn(
		"over-limit lexical ordering",
		boundaryBase+
			` exact_boundary_group="long-lexical" | sort exact_boundary_value | table event_id`,
	), []string{
		"exact-long-lexical-a",
		"exact-long-lexical-z",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("over-limit lexical ordering = %v, want %v", got, want)
	}

	assertCount(
		"raw text byte boundary",
		boundaryBase+` exact_boundary_group=length exact_boundary_value>0`,
		1,
	)
	lengthBoundary := compile(
		boundaryBase +
			` exact_boundary_group=length exact_boundary_value>0` +
			` | stats min(exact_boundary_value) AS extremum` +
			` | where extremum>0 | table extremum`,
	)
	assertPublishedLengthBoundary := func(name string, query CompiledQuery) {
		t.Helper()

		control := `SELECT
			dynamicType(extremum),
			if(dynamicType(extremum) = 'Map(String, String)',
				dynamicElement(extremum, 'Map(String, String)')[concat(char(0), 'open_splunk_type')], ''),
			if(dynamicType(extremum) = 'Map(String, String)',
				dynamicElement(extremum, 'Map(String, String)')[concat(char(0), 'open_splunk_value')], '')
			FROM (` + query.SQL + `)`
		var physical, tag, value string
		if queryErr := connection.QueryRow(
			ctx,
			control,
			query.Args...,
		).Scan(&physical, &tag, &value); queryErr != nil {
			t.Fatalf(
				"execute exact numeric %s: %v\nSQL: %s\nargs: %#v",
				name,
				queryErr,
				query.SQL,
				query.Args,
			)
		}
		if want := "0" + raw4096; physical != "Map(String, String)" ||
			tag != "decimal/v1" ||
			value != want ||
			len(value) != MaximumExactNumericOrderingTextBytes {
			t.Fatalf(
				"%s = %q/%q/%d bytes, want decimal/v1/%d bytes",
				name,
				physical,
				tag,
				len(value),
				MaximumExactNumericOrderingTextBytes,
			)
		}
	}
	assertPublishedLengthBoundary("published-length boundary", lengthBoundary)

	repeatedLengthBoundary := compile(
		boundaryBase +
			` exact_boundary_group=length exact_boundary_value>0` +
			` | stats min(exact_boundary_value) AS first` +
			` | stats min(first) AS extremum`,
	)
	assertPublishedLengthBoundary(
		"published-length downstream extrema",
		repeatedLengthBoundary,
	)

	if got, want := querySingleStringColumn(
		"published-length downstream sort",
		boundaryBase+
			` (exact_boundary_group=length OR exact_boundary_group="length-small")`+
			` exact_boundary_value>0`+
			` | stats min(exact_boundary_value) AS extremum BY exact_boundary_group`+
			` | sort extremum | table exact_boundary_group`,
	), []string{"length", "length-small"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("published-length downstream sort = %v, want %v", got, want)
	}
}
