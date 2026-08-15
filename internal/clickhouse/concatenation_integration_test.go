package clickhouse

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func testConcatAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	scalarEvent := testStoredEvent("concat-scalars", "concat", indexTime)
	scalarEvent.Event.Fields = typedObjectValue(
		typedField("first", typedString("Grace")),
		typedField("last", typedString("Hopper")),
		typedField("text", typedString("value")),
		typedField("signed", typedSint(-42)),
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("floating", typedDouble(12.5)),
		typedField("tagged_decimal", typedDecimal("123.4500")),
		typedField("yes", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField(
			"multi",
			typedList(typedString("first"), typedString("second")),
		),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("unchanged"))),
		),
	)

	binaryEvent := testStoredEvent("concat-binary", "concat", indexTime)
	binaryEvent.Event.Raw = []byte{0xff, 0, 'b', 'y', 't', 'e', 's'}
	binaryEvent.Event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"concat",
		"concat-batch",
		118,
		scalarEvent,
		binaryEvent,
	)

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture concatenation visibility cutoff: %v", err)
	}
	insertConcatDynamicDecimalFixtures(
		ctx,
		t,
		connection,
		indexTime,
		visibilityCutoff,
	)

	official := compile(
		`index=concat event_id="concat-scalars"` +
			` | eval full=first." ".last, numeric="n=" . 42 . "/" . 12.5, ordered="" . first . ":" . "" . last . ":" . signed . ""` +
			` | table full,numeric,ordered`,
	)
	var full, numeric, ordered string
	if err := connection.QueryRow(
		queryContext,
		official.SQL,
		official.Args...,
	).Scan(&full, &numeric, &ordered); err != nil {
		t.Fatalf(
			"execute official concatenation examples: %v\nSQL: %s\nargs: %#v",
			err,
			official.SQL,
			official.Args,
		)
	}
	if full != "Grace Hopper" || numeric != "n=42/12.5" ||
		ordered != "Grace:Hopper:-42" {
		t.Fatalf(
			"official concatenation = full:%q numeric:%q ordered:%q",
			full,
			numeric,
			ordered,
		)
	}

	dynamic := compile(
		`index=concat event_id="concat-scalars"` +
			` | eval text_result="v=" . text, signed_result="v=" . signed, unsigned_result="v=" . unsigned, floating_result="v=" . floating, tagged_decimal_result="v=" . tagged_decimal, numeric_domain_result="v=" . round(floating, 1)` +
			` | table text_result,signed_result,unsigned_result,floating_result,tagged_decimal_result,numeric_domain_result`,
	)
	var textResult, signedResult, unsignedResult, floatingResult string
	var taggedDecimalResult, numericDomainResult string
	if err := connection.QueryRow(
		queryContext,
		dynamic.SQL,
		dynamic.Args...,
	).Scan(
		&textResult,
		&signedResult,
		&unsignedResult,
		&floatingResult,
		&taggedDecimalResult,
		&numericDomainResult,
	); err != nil {
		t.Fatalf(
			"execute Dynamic concatenation matrix: %v\nSQL: %s\nargs: %#v",
			err,
			dynamic.SQL,
			dynamic.Args,
		)
	}
	if textResult != "v=value" || signedResult != "v=-42" ||
		unsignedResult != "v=18446744073709551615" ||
		floatingResult != "v=12.5" ||
		taggedDecimalResult != "v=123.4500" ||
		numericDomainResult != "v=12.5" {
		t.Fatalf(
			"Dynamic concatenation = text:%q signed:%q unsigned:%q floating:%q tagged_decimal:%q numeric_domain:%q",
			textResult,
			signedResult,
			unsignedResult,
			floatingResult,
			taggedDecimalResult,
			numericDomainResult,
		)
	}
	testConcatenationBoundedDynamicScalarAgainstClickHouse(
		t,
		queryContext,
		connection,
		indexTime,
		"bounded Dynamic Text",
		"CAST('VALUE' AS Dynamic)",
		dynamicScalarDomainText,
		64,
		"v=VALUE",
	)

	nullsAndUnsupported := compile(
		`index=concat event_id="concat-scalars"` +
			` | eval empty_result="[" . "" . "]", null_result="[" . nothing . "]", missing_result="[" . absent . "]", bool_result="v=" . yes, list_result="v=" . multi, object_result="v=" . object_value, explicit_bool="flag=" . tostring(yes), literal_bool="flag=" . tostring(false)` +
			` | table empty_result,null_result,missing_result,bool_result,list_result,object_result,explicit_bool,literal_bool`,
	)
	var emptyResult, explicitBool, literalBool string
	var nullResult, missingResult, boolResult, listResult, objectResult *string
	if err := connection.QueryRow(
		queryContext,
		nullsAndUnsupported.SQL,
		nullsAndUnsupported.Args...,
	).Scan(
		&emptyResult,
		&nullResult,
		&missingResult,
		&boolResult,
		&listResult,
		&objectResult,
		&explicitBool,
		&literalBool,
	); err != nil {
		t.Fatalf(
			"execute null and unsupported concatenation matrix: %v\nSQL: %s\nargs: %#v",
			err,
			nullsAndUnsupported.SQL,
			nullsAndUnsupported.Args,
		)
	}
	if emptyResult != "[]" || nullResult != nil || missingResult != nil ||
		boolResult != nil || listResult != nil || objectResult != nil ||
		explicitBool != "flag=True" || literalBool != "flag=False" {
		t.Fatalf(
			"null and unsupported concatenation = empty:%q null:%v missing:%v bool:%v list:%v object:%v explicit_bool:%q literal_bool:%q",
			emptyResult,
			nullResult,
			missingResult,
			boolResult,
			listResult,
			objectResult,
			explicitBool,
			literalBool,
		)
	}

	decimals := compile(
		`index=concat event_id="concat-dynamic-decimals"` +
			` | eval malformed_result="v=" . malformed_decimal, oversized_result="v=" . oversized_decimal` +
			` | table malformed_result,oversized_result`,
	)
	var malformedResult, oversizedResult *string
	if err := connection.QueryRow(
		queryContext,
		decimals.SQL,
		decimals.Args...,
	).Scan(
		&malformedResult,
		&oversizedResult,
	); err != nil {
		t.Fatalf(
			"execute Dynamic Decimal concatenation: %v\nSQL: %s\nargs: %#v",
			err,
			decimals.SQL,
			decimals.Args,
		)
	}
	if malformedResult != nil || oversizedResult != nil {
		t.Fatalf(
			"tagged Dynamic Decimal concatenation = malformed:%v oversized:%v",
			malformedResult,
			oversizedResult,
		)
	}
	testConcatenationBoundedDynamicScalarAgainstClickHouse(
		t,
		queryContext,
		connection,
		indexTime,
		"physical Dynamic Decimal",
		"CAST(toDecimal64('12.3400', 4) AS Dynamic)",
		dynamicScalarDomainAny,
		32,
		"v=12.34",
	)

	sequential := compile(
		`index=concat event_id="concat-scalars"` +
			` | eval full=first." ".last, decorated="[" . full . "]", selected=if(decorated="[Grace Hopper]", decorated . ":" . tostring(yes), "wrong")` +
			` | where selected="[Grace Hopper]:True"` +
			` | stats count`,
	)
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		sequential.SQL,
		sequential.Args...,
	).Scan(&count); err != nil {
		t.Fatalf(
			"execute sequential concatenation pipeline: %v\nSQL: %s\nargs: %#v",
			err,
			sequential.SQL,
			sequential.Args,
		)
	}
	if count != 1 {
		t.Fatalf("sequential concatenation pipeline count = %d, want 1", count)
	}

	binary := compile(
		`index=concat event_id="concat-binary"` +
			` | eval copied="[" . _raw . "]", normalized=lower(copied)` +
			` | table copied,normalized`,
	)
	var copied []byte
	var normalized *string
	var copiedSemanticBytes uint8
	if err := connection.QueryRow(
		queryContext,
		binary.SQL,
		binary.Args...,
	).Scan(&copied, &normalized, &copiedSemanticBytes); err != nil {
		t.Fatalf(
			"execute binary concatenation provenance: %v\nSQL: %s\nargs: %#v",
			err,
			binary.SQL,
			binary.Args,
		)
	}
	wantCopied := []byte{'[', 0xff, 0, 'b', 'y', 't', 'e', 's', ']'}
	if !bytes.Equal(copied, wantCopied) || normalized != nil || copiedSemanticBytes != 1 {
		t.Fatalf(
			"binary concatenation = copied:%x normalized:%v semantic-bytes:%d, want copied:%x normalized:null semantic-bytes:1",
			copied,
			normalized,
			copiedSemanticBytes,
			wantCopied,
		)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		dynamic,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("concatenation expands event rows:\n%s", actions)
	}
}

func testConcatenationBoundedDynamicScalarAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	searchStart time.Time,
	name string,
	valueSQL string,
	domain dynamicScalarDomain,
	maxStringBytes uint64,
	want string,
) {
	t.Helper()

	state := compileState{
		visible: map[string]fieldState{
			"value": {
				valueSQL:       valueSQL,
				dynamicTypeSQL: "dynamicType(" + valueSQL + ")",
				maxStringBytes: maxStringBytes,
				existsSQL:      "1",
				kind:           fieldKindDynamic,
				dynamicDomain:  domain,
			},
		},
		context: newCompileContext(searchStart, "UTC"),
		blocked: make(map[string]struct{}),
	}
	compiled, err := compileScalarValue(
		&plan.ScalarCallExpression{
			Function: plan.ScalarFunctionConcat,
			Arguments: []plan.ScalarExpression{
				&plan.ScalarLiteralExpression{
					Value: plan.Value{Kind: plan.ValueKindString, String: "v="},
				},
				&plan.ScalarFieldExpression{
					Field: plan.FieldRef{Name: "value"},
				},
			},
		},
		state,
	)
	if err != nil {
		t.Fatalf("compile %s concatenation: %v", name, err)
	}

	var result string
	if err := connection.QueryRow(
		ctx,
		"SELECT "+compiled.valueSQL,
		compiled.valueArgs...,
	).Scan(&result); err != nil {
		t.Fatalf(
			"execute %s concatenation: %v\nSQL: %s\nargs: %#v",
			name,
			err,
			compiled.valueSQL,
			compiled.valueArgs,
		)
	}
	if result != want {
		t.Fatalf("%s concatenation = %q, want %q", name, result, want)
	}
}

func insertConcatDynamicDecimalFixtures(
	ctx context.Context,
	t *testing.T,
	connection clickhousedriver.Conn,
	indexTime time.Time,
	visibilityCutoff uint64,
) {
	t.Helper()

	document := clickhousedriver.NewJSON()
	document.SetValueAtPath(
		"malformed_decimal",
		clickhousedriver.NewDynamicWithType(map[string]string{
			"\x00open_splunk_type":  "decimal/v1",
			"\x00open_splunk_value": "malformed-secret-1e",
		}, "Map(String, String)"),
	)
	document.SetValueAtPath(
		"oversized_decimal",
		clickhousedriver.NewDynamicWithType(map[string]string{
			"\x00open_splunk_type": "decimal/v1",
			"\x00open_splunk_value": strings.Repeat(
				"9",
				MaximumExactNumericBinTextBytes+1,
			),
		}, "Map(String, String)"),
	)

	insertContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithSettings(
			insertSettings("concat-dynamic-decimal-envelope"),
		),
	)
	batch, err := connection.PrepareBatch(insertContext, eventsInsertSQL)
	if err != nil {
		t.Fatalf("prepare concatenation Dynamic Decimal fixture: %v", err)
	}
	defer func() { _ = batch.Close() }()

	fieldNames := []string{
		"malformed_decimal",
		"oversized_decimal",
	}
	fieldTypes := []uint8{
		uint8(eventfields.StoredValueTypeDecimal),
		uint8(eventfields.StoredValueTypeDecimal),
	}
	if err := batch.Append(
		"concat-dynamic-decimals",
		"tenant",
		"concat",
		indexTime.UTC(),
		indexTime.UTC().Truncate(time.Millisecond),
		nil,
		uint8(opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED),
		"concat-host",
		"concat-source",
		"concat",
		nil,
		uint8(opensplunkv1.LogSeverity_LOG_SEVERITY_INFO),
		nil,
		nil,
		[]byte("concat Dynamic Decimal fixture"),
		uint8(opensplunkv1.RawEncoding_RAW_ENCODING_UTF8),
		nil,
		nil,
		document,
		fieldNames,
		"concat-collector",
		uint8(ingest.IngestionSourceKindNativeCollector),
		"concat-collector",
		"concat-dynamic-decimal-batch",
		uint64(1),
		time.Date(2099, time.January, 1, 0, 0, 0, 0, time.UTC),
		visibilityCutoff,
		fieldTypes,
		eventfields.CurrentFieldMetadataVersion,
	); err != nil {
		t.Fatalf("append concatenation Dynamic Decimal fixture: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("send concatenation Dynamic Decimal fixture: %v", err)
	}
}
