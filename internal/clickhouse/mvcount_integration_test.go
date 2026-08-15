package clickhouse

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
)

func testMVCountAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("mvcount-scalars", "mvcount", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("single_string", typedString("alpha")),
		typedField("single_number", typedUint(math.MaxUint64)),
		typedField("single_bool", typedBool(false)),
		typedField(
			"multi",
			typedList(
				typedString("first"),
				typedSint(2),
				typedNull(),
				typedBool(true),
			),
		),
		typedField("letters", typedList(
			typedString("MÜNCHEN"),
			typedString("Straße"),
			typedString("東京"),
		)),
		typedField("empty", typedList()),
		typedField("null_only", typedList(typedNull(), typedNull())),
		typedField(
			"nested_members",
			typedList(
				typedObject(typedField("child", typedString("value"))),
				typedList(typedString("nested")),
				typedNull(),
			),
		),
		typedField("nothing", typedNull()),
		typedField(
			"object_parent",
			typedObject(typedField("child", typedString("value"))),
		),
		typedField("decimal_value", typedDecimal("9007199254740993")),
		typedField("bytes_value", typedBytes([]byte{0, 0xff, 0x10})),
		typedField(
			"timestamp_value",
			typedTimestamp(indexTime.Add(-time.Second)),
		),
		typedField("duration_value", typedDuration(1500*time.Millisecond)),
	)
	second := testStoredEvent("mvcount-second", "mvcount", indexTime)
	second.Event.Fields = typedObjectValue(
		typedField("single_string", typedString("beta")),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"mvcount",
		"mvcount-batch",
		96,
		event,
		second,
	)

	literals := compile(
		`index=mvcount event_id="mvcount-scalars"` +
			` | eval text_count=mvcount("abc"), number_count=mvcount(42), bool_count=mvcount(true), predicate_count=mvcount(isnull(absent)), null_count=mvcount(null)` +
			` | table text_count,number_count,bool_count,predicate_count,null_count`,
	)
	var textCount, numberCount, boolCount, predicateCount uint64
	var literalNullCount *uint64
	if err := connection.QueryRow(
		queryContext,
		literals.SQL,
		literals.Args...,
	).Scan(
		&textCount,
		&numberCount,
		&boolCount,
		&predicateCount,
		&literalNullCount,
	); err != nil {
		t.Fatalf(
			"execute literal mvcount matrix: %v\nSQL: %s\nargs: %#v",
			err,
			literals.SQL,
			literals.Args,
		)
	}
	if textCount != 1 || numberCount != 1 || boolCount != 1 ||
		predicateCount != 1 || literalNullCount != nil {
		t.Fatalf(
			"literal mvcount = %d/%d/%d/%d/%v, want 1/1/1/1/null",
			textCount,
			numberCount,
			boolCount,
			predicateCount,
			literalNullCount,
		)
	}

	dynamic := compile(
		`index=mvcount event_id="mvcount-scalars"` +
			` | eval string_count=mvcount(single_string), number_count=mvcount(single_number), bool_count=mvcount(single_bool), multi_count=mvcount(multi), letters_count=mvcount(lower(letters)), empty_count=mvcount(empty), null_only_count=mvcount(null_only), nested_count=mvcount(nested_members), null_count=mvcount(nothing), missing_count=mvcount(absent), object_count=mvcount(object_parent), decimal_count=mvcount(decimal_value), bytes_count=mvcount(bytes_value), timestamp_count=mvcount(timestamp_value), duration_count=mvcount(duration_value)` +
			` | table string_count,number_count,bool_count,multi_count,letters_count,empty_count,null_only_count,nested_count,null_count,missing_count,object_count,decimal_count,bytes_count,timestamp_count,duration_count`,
	)
	var (
		stringDynamicCount, numberDynamicCount, boolDynamicCount *uint64
		multiCount, lettersCount, nestedCount                    *uint64
		emptyCount, nullOnlyCount, nullCount                     *uint64
		missingCount, objectCount                                *uint64
		decimalCount, bytesCount, timestampCount, durationCount  *uint64
	)
	if err := connection.QueryRow(
		queryContext,
		dynamic.SQL,
		dynamic.Args...,
	).Scan(
		&stringDynamicCount,
		&numberDynamicCount,
		&boolDynamicCount,
		&multiCount,
		&lettersCount,
		&emptyCount,
		&nullOnlyCount,
		&nestedCount,
		&nullCount,
		&missingCount,
		&objectCount,
		&decimalCount,
		&bytesCount,
		&timestampCount,
		&durationCount,
	); err != nil {
		t.Fatalf(
			"execute Dynamic mvcount matrix: %v\nSQL: %s\nargs: %#v",
			err,
			dynamic.SQL,
			dynamic.Args,
		)
	}
	for name, got := range map[string]*uint64{
		"string":    stringDynamicCount,
		"number":    numberDynamicCount,
		"bool":      boolDynamicCount,
		"decimal":   decimalCount,
		"bytes":     bytesCount,
		"timestamp": timestampCount,
		"duration":  durationCount,
	} {
		if got == nil || *got != 1 {
			t.Fatalf("Dynamic scalar %s count = %v, want 1", name, got)
		}
	}
	for name, test := range map[string]struct {
		got  *uint64
		want uint64
	}{
		"multi":   {got: multiCount, want: 3},
		"letters": {got: lettersCount, want: 3},
		"nested":  {got: nestedCount, want: 2},
	} {
		if test.got == nil || *test.got != test.want {
			t.Fatalf(
				"Dynamic multivalue %s count = %v, want %d",
				name,
				test.got,
				test.want,
			)
		}
	}
	for name, got := range map[string]*uint64{
		"empty":     emptyCount,
		"null-only": nullOnlyCount,
		"null":      nullCount,
		"missing":   missingCount,
		"object":    objectCount,
	} {
		if got != nil {
			t.Fatalf("Dynamic absent %s count = %d, want null", name, *got)
		}
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture malformed mvcount visibility cutoff: %v", err)
	}
	typeKey := "\x00open_splunk_type"
	valueKey := "\x00open_splunk_value"
	malformedFixtures := []binEdgeRawDecimalEnvelope{
		{
			eventID:   "mvcount-malformed-bytes",
			fieldType: eventfields.StoredValueTypeBytes,
			envelope: map[string]string{
				typeKey: "bytes/v1", valueKey: "A",
			},
		},
		{
			eventID:   "mvcount-malformed-timestamp",
			fieldType: eventfields.StoredValueTypeTimestamp,
			envelope: map[string]string{
				typeKey: "timestamp/v1", valueKey: "not-a-timestamp",
			},
		},
		{
			eventID:   "mvcount-malformed-duration",
			fieldType: eventfields.StoredValueTypeDuration,
			envelope: map[string]string{
				typeKey: "duration/v1", valueKey: "not-a-duration",
			},
		},
		{
			eventID:   "mvcount-malformed-decimal",
			fieldType: eventfields.StoredValueTypeDecimal,
			envelope: map[string]string{
				typeKey: "decimal/v1", valueKey: "malformed-secret-1e",
			},
		},
	}
	for index := range malformedFixtures {
		malformedFixtures[index].tenantID = "tenant"
		malformedFixtures[index].indexName = "mvcount"
		malformedFixtures[index].eventTime = indexTime
		malformedFixtures[index].indexTime = indexTime
		malformedFixtures[index].visibilitySeq = visibilityCutoff
		malformedFixtures[index].fieldName = "malformed"
	}
	binEdgeInsertRawDecimalEnvelopes(
		t,
		ctx,
		connection,
		"mvcount-malformed-envelopes",
		malformedFixtures,
	)
	for _, fixture := range malformedFixtures {
		malformed := compile(
			`index=mvcount event_id="` + fixture.eventID + `"` +
				` | eval count=mvcount(malformed) | table count`,
		)
		var malformedCount *uint64
		if err := connection.QueryRow(
			queryContext,
			malformed.SQL,
			malformed.Args...,
		).Scan(&malformedCount); err != nil {
			t.Fatalf(
				"execute %s mvcount: %v\nSQL: %s\nargs: %#v",
				fixture.eventID,
				err,
				malformed.SQL,
				malformed.Args,
			)
		}
		if malformedCount != nil {
			t.Fatalf("%s mvcount = %d, want null", fixture.eventID, *malformedCount)
		}
	}

	fixed := compile(
		`index=mvcount | stats values(single_string) AS values, count AS total` +
			` | eval values_count=mvcount(values), total_count=mvcount(total), missing_count=mvcount(absent)` +
			` | table values_count,total_count,missing_count`,
	)
	var fixedValuesCount, fixedTotalCount uint64
	var fixedMissingCount *uint64
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedValuesCount, &fixedTotalCount, &fixedMissingCount); err != nil {
		t.Fatalf(
			"execute fixed mvcount matrix: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if fixedValuesCount != 2 || fixedTotalCount != 1 || fixedMissingCount != nil {
		t.Fatalf(
			"fixed mvcount = %d/%d/%v, want 2/1/null",
			fixedValuesCount,
			fixedTotalCount,
			fixedMissingCount,
		)
	}

	emptyFixed := compile(
		`index=mvcount event_id="absent"` +
			` | stats values(single_string) AS values | eval count=mvcount(values) | table count`,
	)
	var emptyFixedCount *uint64
	if err := connection.QueryRow(
		queryContext,
		emptyFixed.SQL,
		emptyFixed.Args...,
	).Scan(&emptyFixedCount); err != nil {
		t.Fatalf(
			"execute empty fixed mvcount: %v\nSQL: %s\nargs: %#v",
			err,
			emptyFixed.SQL,
			emptyFixed.Args,
		)
	}
	if emptyFixedCount != nil {
		t.Fatalf("empty fixed mvcount = %d, want null", *emptyFixedCount)
	}

	predicate := compile(
		`index=mvcount event_id="mvcount-scalars"` +
			` | where mvcount(multi)=3 AND mvcount(single_string)=1 | stats count`,
	)
	var predicateRows uint64
	if err := connection.QueryRow(
		queryContext,
		predicate.SQL,
		predicate.Args...,
	).Scan(&predicateRows); err != nil {
		t.Fatalf(
			"execute mvcount predicate: %v\nSQL: %s\nargs: %#v",
			err,
			predicate.SQL,
			predicate.Args,
		)
	}
	if predicateRows != 1 {
		t.Fatalf("mvcount predicate rows = %d, want 1", predicateRows)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		dynamic,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("mvcount lowering expands event rows:\n%s", actions)
	}
}
