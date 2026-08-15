package clickhouse

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testIntegralRoundingAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("integral-rounding-scalars", "integral-rounding", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("positive", typedDouble(1.2)),
		typedField("negative", typedDouble(-1.2)),
		typedField("negative_fraction", typedDouble(-0.2)),
		typedField("signed", typedSint(-42)),
		typedField("unsigned", typedUint(math.MaxUint64)),
		typedField("decimal_positive", typedDecimal("15.275")),
		typedField("decimal_negative", typedDecimal("-15.275")),
		typedField("decimal_integer_high", typedDecimal("9007199254740993")),
		typedField("text", typedString("2.5")),
		typedField("flag", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField(
			"multi",
			typedList(typedDouble(1.2), typedDouble(2.8)),
		),
		typedField(
			"object_value",
			typedObject(typedField("child", typedDouble(1.2))),
		),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"integral-rounding",
		"integral-rounding-batch",
		95,
		event,
	)

	literals := compile(
		`index=integral-rounding event_id="integral-rounding-scalars"` +
			` | eval up=ceil(1.2), alias=ceiling(-1.2), down=floor(1.2), negative_down=floor(-1.2), negative_zero=ceil(-0.2)` +
			` | table up,alias,down,negative_down,negative_zero`,
	)
	var up, alias, down, negativeDown, negativeZero float64
	if err := connection.QueryRow(
		queryContext,
		literals.SQL,
		literals.Args...,
	).Scan(&up, &alias, &down, &negativeDown, &negativeZero); err != nil {
		t.Fatalf(
			"execute literal integral-rounding matrix: %v\nSQL: %s\nargs: %#v",
			err,
			literals.SQL,
			literals.Args,
		)
	}
	if up != 2 || alias != -1 || down != 1 || negativeDown != -2 ||
		negativeZero != 0 || !math.Signbit(negativeZero) {
		t.Fatalf(
			"literal integral rounding = %v/%v/%v/%v/%v signbit=%v",
			up,
			alias,
			down,
			negativeDown,
			negativeZero,
			math.Signbit(negativeZero),
		)
	}

	dynamic := compile(
		`index=integral-rounding event_id="integral-rounding-scalars"` +
			` | eval up=ceil(positive), alias=ceiling(negative), down=floor(positive), negative_down=floor(negative), negative_zero=ceil(negative_fraction), signed_value=ceil(signed), unsigned_value=floor(unsigned), decimal_up=ceil(decimal_positive), decimal_down=floor(decimal_negative), exact_up=ceil(decimal_integer_high), exact_down=floor(decimal_integer_high), nested=floor(ceil(positive)), text_value=ceil(text), flag_value=floor(flag), null_value=ceil(nothing), missing_value=floor(absent), multi_value=ceil(multi), object_result=floor(object_value)` +
			` | table up,alias,down,negative_down,negative_zero,signed_value,unsigned_value,decimal_up,decimal_down,exact_up,exact_down,nested,text_value,flag_value,null_value,missing_value,multi_value,object_result`,
	)
	dynamicControl := `SELECT
		dynamicType(up), dynamicElement(up, 'Float64'),
		dynamicType(alias), dynamicElement(alias, 'Float64'),
		dynamicType(down), dynamicElement(down, 'Float64'),
		dynamicType(negative_down), dynamicElement(negative_down, 'Float64'),
		dynamicElement(negative_zero, 'Float64'),
		dynamicType(signed_value), dynamicElement(signed_value, 'Int64'),
		dynamicType(unsigned_value), dynamicElement(unsigned_value, 'UInt64'),
		dynamicType(decimal_up), dynamicElement(decimal_up, 'Float64'),
		dynamicType(decimal_down), dynamicElement(decimal_down, 'Float64'),
		dynamicType(exact_up), toString(dynamicElement(exact_up, 'Int256')),
		dynamicType(exact_down), toString(dynamicElement(exact_down, 'Int256')),
		dynamicType(nested), dynamicElement(nested, 'Float64'),
		dynamicType(text_value), dynamicType(flag_value),
		dynamicType(null_value), dynamicType(missing_value),
		dynamicType(multi_value), dynamicType(object_result)
		FROM (` + dynamic.SQL + `)`
	var (
		upType, aliasType, downType, negativeDownType string
		signedType, unsignedType                      string
		decimalUpType, decimalDownType                string
		exactUpType, exactDownType, nestedType        string
		textType, flagType, nullType, missingType     string
		multiType, objectType                         string
	)
	var dynamicUp, dynamicAlias, dynamicDown, dynamicNegativeDown float64
	var dynamicNegativeZero float64
	var dynamicSigned int64
	var dynamicUnsigned uint64
	var dynamicDecimalUp, dynamicDecimalDown float64
	var dynamicExactUp, dynamicExactDown string
	var dynamicNested float64
	if err := connection.QueryRow(
		queryContext,
		dynamicControl,
		dynamic.Args...,
	).Scan(
		&upType,
		&dynamicUp,
		&aliasType,
		&dynamicAlias,
		&downType,
		&dynamicDown,
		&negativeDownType,
		&dynamicNegativeDown,
		&dynamicNegativeZero,
		&signedType,
		&dynamicSigned,
		&unsignedType,
		&dynamicUnsigned,
		&decimalUpType,
		&dynamicDecimalUp,
		&decimalDownType,
		&dynamicDecimalDown,
		&exactUpType,
		&dynamicExactUp,
		&exactDownType,
		&dynamicExactDown,
		&nestedType,
		&dynamicNested,
		&textType,
		&flagType,
		&nullType,
		&missingType,
		&multiType,
		&objectType,
	); err != nil {
		t.Fatalf(
			"execute Dynamic integral-rounding matrix: %v\nSQL: %s\nargs: %#v",
			err,
			dynamicControl,
			dynamic.Args,
		)
	}
	if upType != "Float64" || dynamicUp != 2 ||
		aliasType != "Float64" || dynamicAlias != -1 ||
		downType != "Float64" || dynamicDown != 1 ||
		negativeDownType != "Float64" || dynamicNegativeDown != -2 ||
		dynamicNegativeZero != 0 || !math.Signbit(dynamicNegativeZero) ||
		signedType != "Int64" || dynamicSigned != -42 ||
		unsignedType != "UInt64" || dynamicUnsigned != math.MaxUint64 ||
		decimalUpType != "Float64" || dynamicDecimalUp != 16 ||
		decimalDownType != "Float64" || dynamicDecimalDown != -16 ||
		exactUpType != "Int256" || dynamicExactUp != "9007199254740993" ||
		exactDownType != "Int256" || dynamicExactDown != "9007199254740993" ||
		nestedType != "Float64" || dynamicNested != 2 ||
		textType != "None" || flagType != "None" ||
		nullType != "None" || missingType != "None" ||
		multiType != "None" || objectType != "None" {
		t.Fatalf(
			"Dynamic integral rounding = up:%q/%v alias:%q/%v down:%q/%v negative:%q/%v zero:%v/%v integer:%q/%d %q/%d decimal:%q/%v %q/%v exact:%q/%q %q/%q nested:%q/%v unsupported:%q/%q/%q/%q/%q/%q",
			upType,
			dynamicUp,
			aliasType,
			dynamicAlias,
			downType,
			dynamicDown,
			negativeDownType,
			dynamicNegativeDown,
			dynamicNegativeZero,
			math.Signbit(dynamicNegativeZero),
			signedType,
			dynamicSigned,
			unsignedType,
			dynamicUnsigned,
			decimalUpType,
			dynamicDecimalUp,
			decimalDownType,
			dynamicDecimalDown,
			exactUpType,
			dynamicExactUp,
			exactDownType,
			dynamicExactDown,
			nestedType,
			dynamicNested,
			textType,
			flagType,
			nullType,
			missingType,
			multiType,
			objectType,
		)
	}

	insertMalformedDecimalIntegrationFixture(
		ctx,
		t,
		store,
		connection,
		indexTime,
		"integral-rounding",
		"integral-rounding-malformed-decimal",
		"integral-rounding-malformed-decimal-envelope",
	)
	malformed := compile(
		`index=integral-rounding event_id="integral-rounding-malformed-decimal"` +
			` | eval up=ceil(malformed), down=floor(malformed) | table up,down`,
	)
	var malformedUpType, malformedDownType string
	if err := connection.QueryRow(
		queryContext,
		"SELECT dynamicType(up), dynamicType(down) FROM ("+malformed.SQL+")",
		malformed.Args...,
	).Scan(&malformedUpType, &malformedDownType); err != nil {
		t.Fatalf(
			"execute malformed Decimal integral rounding: %v\nSQL: %s\nargs: %#v",
			err,
			malformed.SQL,
			malformed.Args,
		)
	}
	if malformedUpType != "None" || malformedDownType != "None" {
		t.Fatalf(
			"malformed Decimal integral-rounding types = %q/%q, want None/None",
			malformedUpType,
			malformedDownType,
		)
	}

	predicate := compile(
		`index=integral-rounding event_id="integral-rounding-scalars"` +
			` | where floor(unsigned)=18446744073709551615 AND ceil(decimal_integer_high)=9007199254740993 | stats count`,
	)
	var count uint64
	if err := connection.QueryRow(
		queryContext,
		predicate.SQL,
		predicate.Args...,
	).Scan(&count); err != nil {
		t.Fatalf(
			"execute exact integral-rounding predicate: %v\nSQL: %s\nargs: %#v",
			err,
			predicate.SQL,
			predicate.Args,
		)
	}
	if count != 1 {
		t.Fatalf("exact integral-rounding predicate count = %d, want 1", count)
	}

	fixed := compile(
		`index=integral-rounding event_id="integral-rounding-scalars"` +
			` | stats count AS total | eval up=ceil(total), down=floor(total), missing_up=ceil(absent), missing_down=floor(absent)` +
			` | table up,down,missing_up,missing_down`,
	)
	var fixedUp, fixedDown uint64
	var fixedMissingUp, fixedMissingDown *float64
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedUp, &fixedDown, &fixedMissingUp, &fixedMissingDown); err != nil {
		t.Fatalf(
			"execute fixed integer integral rounding: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if fixedUp != 1 || fixedDown != 1 ||
		fixedMissingUp != nil || fixedMissingDown != nil {
		t.Fatalf(
			"fixed integer/missing integral rounding = %d/%d/%v/%v, want 1/1/null/null",
			fixedUp,
			fixedDown,
			fixedMissingUp,
			fixedMissingDown,
		)
	}

	textOnly := compile(
		`index=integral-rounding event_id="integral-rounding-scalars"` +
			` | eval up=ceil(lower(text)), down=floor(upper(text)) | table up,down`,
	)
	var textOnlyUp, textOnlyDown *float64
	if err := connection.QueryRow(
		queryContext,
		textOnly.SQL,
		textOnly.Args...,
	).Scan(&textOnlyUp, &textOnlyDown); err != nil {
		t.Fatalf(
			"execute text-only integral rounding: %v\nSQL: %s\nargs: %#v",
			err,
			textOnly.SQL,
			textOnly.Args,
		)
	}
	if textOnlyUp != nil || textOnlyDown != nil {
		t.Fatalf(
			"text-only integral rounding = %v/%v, want null/null",
			textOnlyUp,
			textOnlyDown,
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
		t.Fatalf("integral-rounding lowering expands event rows:\n%s", actions)
	}
}
