package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func testToStringAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	scalarEvent := testStoredEvent("tostring-scalars", "tostring", indexTime)
	scalarEvent.Event.Host = "München"
	scalarEvent.Event.Raw = []byte("UTF-8 RAW")
	scalarEvent.Event.Fields = typedObjectValue(
		typedField("text", typedString("Straße")),
		typedField("signed", typedSint(-42)),
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("floating", typedDouble(12.5)),
		typedField("decimal", typedDecimal("123.4500")),
		typedField("yes", typedBool(true)),
		typedField("no", typedBool(false)),
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

	binaryEvent := testStoredEvent("tostring-binary", "tostring", indexTime)
	binaryEvent.Event.Raw = []byte("VALID ASCII MARKED BINARY")
	binaryEvent.Event.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_BINARY

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"tostring",
		"tostring-batch",
		93,
		scalarEvent,
		binaryEvent,
	)

	scalars := compile(
		`index=tostring event_id="tostring-scalars"` +
			` | eval literal=tostring(123), host_text=tostring(host), text_value=tostring(text), signed_value=tostring(signed), unsigned_value=tostring(unsigned), floating_value=tostring(floating), decimal_value=tostring(decimal), yes_value=tostring(yes), no_value=tostring(no), predicate=tostring(isnull(absent)), null_value=tostring(nothing), missing_value=tostring(absent)` +
			` | table literal,host_text,text_value,signed_value,unsigned_value,floating_value,decimal_value,yes_value,no_value,predicate,null_value,missing_value`,
	)
	var literal, hostText string
	var textValue, signedValue, unsignedValue, floatingValue, decimalValue *string
	var yesValue, noValue, predicate, nullValue, missingValue *string
	if err := connection.QueryRow(
		queryContext,
		scalars.SQL,
		scalars.Args...,
	).Scan(
		&literal,
		&hostText,
		&textValue,
		&signedValue,
		&unsignedValue,
		&floatingValue,
		&decimalValue,
		&yesValue,
		&noValue,
		&predicate,
		&nullValue,
		&missingValue,
	); err != nil {
		t.Fatalf(
			"execute tostring scalars: %v\nSQL: %s\nargs: %#v",
			err,
			scalars.SQL,
			scalars.Args,
		)
	}
	for name, got := range map[string]*string{
		"text":      textValue,
		"signed":    signedValue,
		"unsigned":  unsignedValue,
		"floating":  floatingValue,
		"decimal":   decimalValue,
		"yes":       yesValue,
		"no":        noValue,
		"predicate": predicate,
	} {
		if got == nil {
			t.Fatalf("%s tostring = null", name)
		}
	}
	if literal != "123" || hostText != "München" ||
		*textValue != "Straße" || *signedValue != "-42" ||
		*unsignedValue != "18446744073709551615" ||
		*floatingValue != "12.5" || *decimalValue != "123.4500" ||
		*yesValue != "True" ||
		*noValue != "False" || *predicate != "True" ||
		nullValue != nil || missingValue != nil {
		t.Fatalf(
			"tostring scalars = literal:%q host:%q text:%v signed:%v unsigned:%v floating:%v decimal:%v yes:%v no:%v predicate:%v null:%v missing:%v",
			literal,
			hostText,
			textValue,
			signedValue,
			unsignedValue,
			floatingValue,
			decimalValue,
			yesValue,
			noValue,
			predicate,
			nullValue,
			missingValue,
		)
	}

	insertMalformedDecimalIntegrationFixture(
		ctx,
		t,
		store,
		connection,
		indexTime,
		"tostring",
		"tostring-malformed-decimal",
		"tostring-malformed-decimal-envelope",
	)
	malformed := compile(
		`index=tostring event_id="tostring-malformed-decimal"` +
			` | eval rendered=tostring(malformed) | table rendered`,
	)
	var malformedValue *string
	if err := connection.QueryRow(
		queryContext,
		malformed.SQL,
		malformed.Args...,
	).Scan(&malformedValue); err != nil {
		t.Fatalf(
			"execute malformed Decimal tostring: %v\nSQL: %s\nargs: %#v",
			err,
			malformed.SQL,
			malformed.Args,
		)
	}
	if malformedValue != nil {
		t.Fatalf("malformed Decimal tostring = %q, want null", *malformedValue)
	}

	unsupported := compile(
		`index=tostring event_id="tostring-scalars"` +
			` | eval multi_value=tostring(multi), object_result=tostring(object_value)` +
			` | table multi_value,object_result`,
	)
	var multiValue, objectResult *string
	if err := connection.QueryRow(
		queryContext,
		unsupported.SQL,
		unsupported.Args...,
	).Scan(&multiValue, &objectResult); err != nil {
		t.Fatalf(
			"execute unsupported Dynamic tostring: %v\nSQL: %s\nargs: %#v",
			err,
			unsupported.SQL,
			unsupported.Args,
		)
	}
	if multiValue != nil || objectResult != nil {
		t.Fatalf("unsupported Dynamic tostring = %#v/%#v, want nulls", multiValue, objectResult)
	}

	predicateQuery := compile(
		`index=tostring event_id="tostring-scalars"` +
			` | where tostring(unsigned)="18446744073709551615" | stats count`,
	)
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		predicateQuery.SQL,
		predicateQuery.Args...,
	).Scan(&count); err != nil {
		t.Fatalf(
			"execute tostring predicate: %v\nSQL: %s\nargs: %#v",
			err,
			predicateQuery.SQL,
			predicateQuery.Args,
		)
	}
	if count != 1 {
		t.Fatalf("tostring predicate count = %d, want 1", count)
	}

	binary := compile(
		`index=tostring event_id="tostring-binary"` +
			` | eval copied=tostring(_raw), normalized=lower(copied)` +
			` | table copied,normalized`,
	)
	var copied string
	var normalized *string
	var copiedSemanticBytes uint8
	if err := connection.QueryRow(
		queryContext,
		binary.SQL,
		binary.Args...,
	).Scan(&copied, &normalized, &copiedSemanticBytes); err != nil {
		t.Fatalf(
			"execute binary tostring provenance: %v\nSQL: %s\nargs: %#v",
			err,
			binary.SQL,
			binary.Args,
		)
	}
	if copied != "VALID ASCII MARKED BINARY" || normalized != nil || copiedSemanticBytes != 1 {
		t.Fatalf(
			"binary tostring = copied:%q normalized:%#v semantic-bytes:%d",
			copied,
			normalized,
			copiedSemanticBytes,
		)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		scalars,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("tostring lowering expands event rows:\n%s", actions)
	}
}
