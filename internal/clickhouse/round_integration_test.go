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

func testRoundAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("round-scalars", "round", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("positive_half", typedDouble(3.5)),
		typedField("even_half", typedDouble(2.5)),
		typedField("negative_half", typedDouble(-2.5)),
		typedField("cents", typedDouble(2.555)),
		typedField("binary_low", typedDouble(15.275)),
		typedField("binary_high", typedDouble(17.275)),
		typedField("nested_precision", typedDouble(2.51)),
		typedField("signed", typedSint(-42)),
		typedField("unsigned", typedUint(math.MaxUint64)),
		typedField("decimal", typedDecimal("15.275")),
		typedField("decimal_integer_low", typedDecimal("9007199254740992")),
		typedField("decimal_integer_high", typedDecimal("9007199254740993")),
		typedField("text", typedString("2.555")),
		typedField("flag", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField(
			"multi",
			typedList(typedDouble(1.25), typedDouble(2.5)),
		),
		typedField(
			"object_value",
			typedObject(typedField("child", typedDouble(1.25))),
		),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"round",
		"round-batch",
		94,
		event,
	)

	literals := compile(
		`index=round event_id="round-scalars"` +
			` | eval positive=round(3.5), even=round(2.5), negative=round(-2.5), cents_value=round(2.555, 2), low=round(15.275, 2), high=round(17.275, 2)` +
			` | table positive,even,negative,cents_value,low,high`,
	)
	var positive, even, negative, cents, low, high float64
	if err := connection.QueryRow(
		queryContext,
		literals.SQL,
		literals.Args...,
	).Scan(&positive, &even, &negative, &cents, &low, &high); err != nil {
		t.Fatalf(
			"execute literal round matrix: %v\nSQL: %s\nargs: %#v",
			err,
			literals.SQL,
			literals.Args,
		)
	}
	if positive != 4 || even != 2 || negative != -2 ||
		cents != 2.56 || low != 15.28 || high != 17.27 {
		t.Fatalf(
			"literal round = %v/%v/%v/%v/%v/%v",
			positive,
			even,
			negative,
			cents,
			low,
			high,
		)
	}

	dynamic := compile(
		`index=round event_id="round-scalars"` +
			` | eval positive=round(positive_half), even=round(even_half), negative=round(negative_half), cents_value=round(cents, 2), low=round(binary_low, 2), high=round(binary_high, 2), nested_value=round(round(nested_precision, 1), 0), signed_value=round(signed, 18), unsigned_value=round(unsigned, 18), decimal_value=round(decimal, 2), decimal_integer_low_value=round(decimal_integer_low, 18), decimal_integer_high_value=round(decimal_integer_high, 18), text_value=round(text, 2), flag_value=round(flag), null_value=round(nothing), missing_value=round(absent), multi_value=round(multi), object_result=round(object_value)` +
			` | table positive,even,negative,cents_value,low,high,nested_value,signed_value,unsigned_value,decimal_value,decimal_integer_low_value,decimal_integer_high_value,text_value,flag_value,null_value,missing_value,multi_value,object_result`,
	)
	dynamicControl := `SELECT
		dynamicType(positive), dynamicElement(positive, 'Float64'),
		dynamicType(even), dynamicElement(even, 'Float64'),
		dynamicType(negative), dynamicElement(negative, 'Float64'),
		dynamicType(cents_value), dynamicElement(cents_value, 'Float64'),
		dynamicElement(low, 'Float64'), dynamicElement(high, 'Float64'),
		dynamicType(nested_value), dynamicElement(nested_value, 'Float64'),
		dynamicType(signed_value), dynamicElement(signed_value, 'Int64'),
		dynamicType(unsigned_value), dynamicElement(unsigned_value, 'UInt64'),
		dynamicType(decimal_value), dynamicElement(decimal_value, 'Float64'),
		dynamicType(decimal_integer_low_value), toString(dynamicElement(decimal_integer_low_value, 'Int256')),
		dynamicType(decimal_integer_high_value), toString(dynamicElement(decimal_integer_high_value, 'Int256')),
		dynamicType(text_value), dynamicType(flag_value),
		dynamicType(null_value), dynamicType(missing_value),
		dynamicType(multi_value), dynamicType(object_result)
		FROM (` + dynamic.SQL + `)`
	var (
		positiveType, evenType, negativeType, centsType string
		nestedType, signedType, unsignedType            string
		decimalType, decimalIntegerLowType              string
		decimalIntegerHighType                          string
		textType, flagType, nullType, missingType       string
		multiType, objectType                           string
	)
	var dynamicPositive, dynamicEven, dynamicNegative float64
	var dynamicCents, dynamicLow, dynamicHigh float64
	var dynamicNested float64
	var dynamicSigned int64
	var dynamicUnsigned uint64
	var dynamicDecimal float64
	var dynamicDecimalIntegerLow, dynamicDecimalIntegerHigh string
	if err := connection.QueryRow(
		queryContext,
		dynamicControl,
		dynamic.Args...,
	).Scan(
		&positiveType,
		&dynamicPositive,
		&evenType,
		&dynamicEven,
		&negativeType,
		&dynamicNegative,
		&centsType,
		&dynamicCents,
		&dynamicLow,
		&dynamicHigh,
		&nestedType,
		&dynamicNested,
		&signedType,
		&dynamicSigned,
		&unsignedType,
		&dynamicUnsigned,
		&decimalType,
		&dynamicDecimal,
		&decimalIntegerLowType,
		&dynamicDecimalIntegerLow,
		&decimalIntegerHighType,
		&dynamicDecimalIntegerHigh,
		&textType,
		&flagType,
		&nullType,
		&missingType,
		&multiType,
		&objectType,
	); err != nil {
		t.Fatalf(
			"execute Dynamic round matrix: %v\nSQL: %s\nargs: %#v",
			err,
			dynamicControl,
			dynamic.Args,
		)
	}
	if positiveType != "Float64" || dynamicPositive != 4 ||
		evenType != "Float64" || dynamicEven != 2 ||
		negativeType != "Float64" || dynamicNegative != -2 ||
		centsType != "Float64" || dynamicCents != 2.56 ||
		dynamicLow != 15.28 || dynamicHigh != 17.27 ||
		nestedType != "Float64" || dynamicNested != 2 ||
		signedType != "Int64" || dynamicSigned != -42 ||
		unsignedType != "UInt64" || dynamicUnsigned != math.MaxUint64 ||
		decimalType != "Float64" || dynamicDecimal != 15.28 ||
		decimalIntegerLowType != "Int256" ||
		dynamicDecimalIntegerLow != "9007199254740992" ||
		decimalIntegerHighType != "Int256" ||
		dynamicDecimalIntegerHigh != "9007199254740993" ||
		textType != "None" || flagType != "None" ||
		nullType != "None" || missingType != "None" ||
		multiType != "None" || objectType != "None" {
		t.Fatalf(
			"Dynamic round = float:%q/%v %q/%v %q/%v %q/%v %v/%v nested:%q/%v integer:%q/%d %q/%d decimal:%q/%v exact:%q/%q %q/%q unsupported:%q/%q/%q/%q/%q/%q",
			positiveType,
			dynamicPositive,
			evenType,
			dynamicEven,
			negativeType,
			dynamicNegative,
			centsType,
			dynamicCents,
			dynamicLow,
			dynamicHigh,
			nestedType,
			dynamicNested,
			signedType,
			dynamicSigned,
			unsignedType,
			dynamicUnsigned,
			decimalType,
			dynamicDecimal,
			decimalIntegerLowType,
			dynamicDecimalIntegerLow,
			decimalIntegerHighType,
			dynamicDecimalIntegerHigh,
			textType,
			flagType,
			nullType,
			missingType,
			multiType,
			objectType,
		)
	}

	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture round malformed visibility cutoff: %v", err)
	}
	binEdgeInsertRawDecimalEnvelopes(
		t,
		ctx,
		connection,
		"round-malformed-decimal-envelope",
		[]binEdgeRawDecimalEnvelope{{
			eventID:       "round-malformed-decimal",
			tenantID:      "tenant",
			indexName:     "round",
			eventTime:     indexTime,
			indexTime:     indexTime,
			visibilitySeq: visibilityCutoff,
			fieldName:     "malformed",
			fieldType:     eventfields.StoredValueTypeDecimal,
			envelope: map[string]string{
				"\x00open_splunk_type":  "decimal/v1",
				"\x00open_splunk_value": "malformed-secret-1e",
			},
		}},
	)
	malformed := compile(
		`index=round event_id="round-malformed-decimal"` +
			` | eval rounded=round(malformed, 2) | table rounded`,
	)
	var malformedType string
	if err := connection.QueryRow(
		queryContext,
		"SELECT dynamicType(rounded) FROM ("+malformed.SQL+")",
		malformed.Args...,
	).Scan(&malformedType); err != nil {
		t.Fatalf(
			"execute malformed Decimal round: %v\nSQL: %s\nargs: %#v",
			err,
			malformed.SQL,
			malformed.Args,
		)
	}
	if malformedType != "None" {
		t.Fatalf("malformed Decimal round type = %q, want None", malformedType)
	}

	predicate := compile(
		`index=round event_id="round-scalars"` +
			` | where round(unsigned, 18)=18446744073709551615 | stats count`,
	)
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		predicate.SQL,
		predicate.Args...,
	).Scan(&count); err != nil {
		t.Fatalf(
			"execute exact integer round predicate: %v\nSQL: %s\nargs: %#v",
			err,
			predicate.SQL,
			predicate.Args,
		)
	}
	if count != 1 {
		t.Fatalf("exact integer round predicate count = %d, want 1", count)
	}

	fixed := compile(
		`index=round event_id="round-scalars"` +
			` | stats count AS total | eval rounded=round(total, 18)` +
			` | table rounded`,
	)
	var fixedValue uint64
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedValue); err != nil {
		t.Fatalf(
			"execute fixed integer round: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if fixedValue != 1 {
		t.Fatalf("fixed integer round = %d, want 1", fixedValue)
	}

	textOnly := compile(
		`index=round event_id="round-scalars"` +
			` | eval rounded=round(lower(text), 2) | table rounded`,
	)
	var textOnlyValue *float64
	if err := connection.QueryRow(
		queryContext,
		textOnly.SQL,
		textOnly.Args...,
	).Scan(&textOnlyValue); err != nil {
		t.Fatalf(
			"execute text-only round: %v\nSQL: %s\nargs: %#v",
			err,
			textOnly.SQL,
			textOnly.Args,
		)
	}
	if textOnlyValue != nil {
		t.Fatalf("text-only round = %v, want null", *textOnlyValue)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		"EXPLAIN actions=1 ",
		dynamic,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("round lowering expands event rows:\n%s", actions)
	}
}
