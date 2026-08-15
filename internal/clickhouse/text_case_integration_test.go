package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func testTextCaseAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	unicodeEvent := testStoredEvent("text-case-unicode", "textcase", indexTime)
	unicodeEvent.Event.Host = "München"
	unicodeEvent.Event.Raw = []byte("Straße RAW")
	unicodeEvent.Event.Message = new("Unicode fixture")
	unicodeEvent.Event.Fields = typedObjectValue(
		typedField("scalar", typedString("MÜNCHEN Straße")),
		typedField(
			"multi",
			typedList(typedString("MÜNCHEN"), typedString("Straße")),
		),
		typedField("numeric", typedSint(42)),
		typedField("nothing", typedNull()),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("unchanged"))),
		),
	)

	binaryEvent := testStoredEvent("text-case-binary", "textcase", indexTime)
	binaryEvent.Event.Raw = []byte("VALID ASCII MARKED BINARY")
	binaryEvent.Event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY
	binaryEvent.Event.Fields = typedObjectValue(
		typedField("scalar", typedString("BINARY EVENT")),
	)

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"textcase",
		"text-case-batch",
		90,
		unicodeEvent,
		binaryEvent,
	)

	scalars := compile(
		`index=textcase event_id="text-case-unicode"` +
			` | eval raw_lower=lower(_raw), host_upper=upper(host), scalar_lower=lower(scalar), scalar_upper=upper(scalar_lower)` +
			` | table raw_lower,host_upper,scalar_lower,scalar_upper`,
	)
	scalarControl := "SELECT raw_lower, host_upper, " +
		"dynamicType(scalar_lower), dynamicElement(scalar_lower, 'String'), " +
		"dynamicType(scalar_upper), dynamicElement(scalar_upper, 'String') FROM (" +
		scalars.SQL + ")"
	var (
		rawLower, hostUpper   string
		lowerType, lowerValue string
		upperType, upperValue string
	)
	if err := connection.QueryRow(
		queryContext,
		scalarControl,
		scalars.Args...,
	).Scan(
		&rawLower,
		&hostUpper,
		&lowerType,
		&lowerValue,
		&upperType,
		&upperValue,
	); err != nil {
		t.Fatalf(
			"execute scalar text case: %v\nSQL: %s\nargs: %#v",
			err,
			scalarControl,
			scalars.Args,
		)
	}
	if rawLower != "straße raw" || hostUpper != "MÜNCHEN" ||
		lowerType != "String" || lowerValue != "münchen straße" ||
		upperType != "String" || upperValue != "MÜNCHEN STRASSE" {
		t.Fatalf(
			"scalar text case = raw:%q host:%q lower:%q/%q upper:%q/%q",
			rawLower,
			hostUpper,
			lowerType,
			lowerValue,
			upperType,
			upperValue,
		)
	}

	dynamicMultivalue := compile(
		`index=textcase event_id="text-case-unicode"` +
			` | eval lowered=lower(multi), uppered=upper(multi)` +
			` | table lowered,uppered`,
	)
	multivalueControl := "SELECT dynamicType(lowered), " +
		"dynamicElement(lowered, 'Array(String)'), dynamicType(uppered), " +
		"dynamicElement(uppered, 'Array(String)') FROM (" +
		dynamicMultivalue.SQL + ")"
	var lowerArrayType, upperArrayType string
	var lowerArray, upperArray []string
	if err := connection.QueryRow(
		queryContext,
		multivalueControl,
		dynamicMultivalue.Args...,
	).Scan(
		&lowerArrayType,
		&lowerArray,
		&upperArrayType,
		&upperArray,
	); err != nil {
		t.Fatalf(
			"execute Dynamic multivalue text case: %v\nSQL: %s\nargs: %#v",
			err,
			multivalueControl,
			dynamicMultivalue.Args,
		)
	}
	if lowerArrayType != "Array(String)" ||
		!slices.Equal(lowerArray, []string{"münchen", "straße"}) ||
		upperArrayType != "Array(String)" ||
		!slices.Equal(upperArray, []string{"MÜNCHEN", "STRASSE"}) {
		t.Fatalf(
			"Dynamic multivalue text case = %q/%#v %q/%#v",
			lowerArrayType,
			lowerArray,
			upperArrayType,
			upperArray,
		)
	}

	unsupported := compile(
		`index=textcase event_id="text-case-unicode"` +
			` | eval numeric_result=lower(numeric), object_result=upper(object_value), null_result=lower(nothing), missing_result=upper(absent)` +
			` | table numeric_result,object_result,null_result,missing_result`,
	)
	unsupportedControl := "SELECT dynamicType(numeric_result), " +
		"dynamicType(object_result), dynamicType(null_result), " +
		"dynamicType(missing_result) FROM (" + unsupported.SQL + ")"
	var numericType, objectType, nullType, missingType string
	if err := connection.QueryRow(
		queryContext,
		unsupportedControl,
		unsupported.Args...,
	).Scan(
		&numericType,
		&objectType,
		&nullType,
		&missingType,
	); err != nil {
		t.Fatalf(
			"execute unsupported text-case inputs: %v\nSQL: %s\nargs: %#v",
			err,
			unsupportedControl,
			unsupported.Args,
		)
	}
	if numericType != "None" || objectType != "None" ||
		nullType != "None" || missingType != "None" {
		t.Fatalf(
			"unsupported text-case types = %q/%q/%q/%q, want None",
			numericType,
			objectType,
			nullType,
			missingType,
		)
	}

	fixedMultivalue := compile(
		`index=textcase event_id="text-case-unicode"` +
			` | stats values(scalar) AS collected` +
			` | eval lowered=lower(collected), uppered=upper(lowered)` +
			` | table lowered,uppered`,
	)
	var fixedLower, fixedUpper []string
	if err := connection.QueryRow(
		queryContext,
		fixedMultivalue.SQL,
		fixedMultivalue.Args...,
	).Scan(&fixedLower, &fixedUpper); err != nil {
		t.Fatalf(
			"execute fixed multivalue text case: %v\nSQL: %s\nargs: %#v",
			err,
			fixedMultivalue.SQL,
			fixedMultivalue.Args,
		)
	}
	if !slices.Equal(fixedLower, []string{"münchen straße"}) ||
		!slices.Equal(fixedUpper, []string{"MÜNCHEN STRASSE"}) {
		t.Fatalf(
			"fixed multivalue text case = %#v/%#v",
			fixedLower,
			fixedUpper,
		)
	}

	for _, test := range []struct {
		source string
		want   uint64
	}{
		{
			source: `index=textcase event_id="text-case-unicode" | where lower(scalar)="münchen straße" | stats count`,
			want:   1,
		},
		{
			source: `index=textcase event_id="text-case-unicode" | where lower(scalar)="MÜNCHEN STRASSE" | stats count`,
			want:   0,
		},
	} {
		compiled := compile(test.source)
		var count uint64
		if err := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(&count); err != nil {
			t.Fatalf(
				"execute text-case where %q: %v\nSQL: %s\nargs: %#v",
				test.source,
				err,
				compiled.SQL,
				compiled.Args,
			)
		}
		if count != test.want {
			t.Fatalf("text-case where %q count = %d, want %d", test.source, count, test.want)
		}
	}

	binary := compile(
		`index=textcase event_id="text-case-binary"` +
			` | eval normalized=lower(_raw)` +
			` | table normalized`,
	)
	var binaryResult *string
	if err := connection.QueryRow(
		queryContext,
		binary.SQL,
		binary.Args...,
	).Scan(&binaryResult); err != nil {
		t.Fatalf(
			"execute binary raw text case: %v\nSQL: %s\nargs: %#v",
			err,
			binary.SQL,
			binary.Args,
		)
	}
	if binaryResult != nil {
		t.Fatalf("binary raw text case = %q, want null", *binaryResult)
	}

	for _, aggregate := range []string{"values", "list"} {
		rawMultivalue := compile(
			`index=textcase | stats ` + aggregate + `(_raw) AS raws` +
				` | eval normalized=lower(raws) | table normalized`,
		)
		var normalized []string
		if err := connection.QueryRow(
			queryContext,
			rawMultivalue.SQL,
			rawMultivalue.Args...,
		).Scan(&normalized); err != nil {
			t.Fatalf(
				"execute %s raw text-case: %v\nSQL: %s\nargs: %#v",
				aggregate,
				err,
				rawMultivalue.SQL,
				rawMultivalue.Args,
			)
		}
		if !slices.Equal(normalized, []string{"straße raw"}) {
			t.Fatalf("%s raw text-case = %#v, want only UTF-8-declared raw", aggregate, normalized)
		}
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		dynamicMultivalue,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("text-case multivalue lowering expands event rows:\n%s", actions)
	}
}
