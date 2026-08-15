package clickhouse

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func testSubstringAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	unicodeEvent := testStoredEvent("substring-unicode", "substring", indexTime)
	unicodeEvent.Event.Host = "😀abcdef"
	unicodeEvent.Event.Raw = []byte("😀abcdef RAW")
	unicodeEvent.Event.Fields = typedObjectValue(
		typedField("scalar", typedString("😀abcdef")),
		typedField(
			"multi",
			typedList(typedString("first"), typedString("second")),
		),
		typedField("numeric", typedSint(42)),
		typedField("nothing", typedNull()),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("unchanged"))),
		),
	)

	binaryEvent := testStoredEvent("substring-binary", "substring", indexTime)
	binaryEvent.Event.Raw = []byte("VALID ASCII MARKED BINARY")
	binaryEvent.Event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"substring",
		"substring-batch",
		92,
		unicodeEvent,
		binaryEvent,
	)

	matrix := compile(
		`index=substring event_id="substring-unicode"` +
			` | eval positive=substr(scalar, 1, 3), zero_start=substr(scalar, 0, 3), negative_start=substr(scalar, -2, 2), suffix=substr(scalar, -3), zero_length=substr(scalar, 2, 0), negative_length=substr(scalar, 4, -2), far_right=substr(scalar, 99, 3), far_left=substr(scalar, -99), covering=substr(scalar, -99, 100), minimum_start=substr(scalar, -9223372036854775808), maximum_start=substr(scalar, 18446744073709551615), maximum_length=substr(scalar, 1, 18446744073709551615), huge_preceding=substr(scalar, 9223372036854775808, -9223372036854775808)` +
			` | table positive,zero_start,negative_start,suffix,zero_length,negative_length,far_right,far_left,covering,minimum_start,maximum_start,maximum_length,huge_preceding`,
	)
	var (
		positive, zeroStart, negativeStart, suffix string
		zeroLength, negativeLength, farRight       string
		farLeft, covering, minimumStart            string
		maximumStart, maximumLength, hugePreceding string
	)
	if err := connection.QueryRow(
		queryContext,
		matrix.SQL,
		matrix.Args...,
	).Scan(
		&positive,
		&zeroStart,
		&negativeStart,
		&suffix,
		&zeroLength,
		&negativeLength,
		&farRight,
		&farLeft,
		&covering,
		&minimumStart,
		&maximumStart,
		&maximumLength,
		&hugePreceding,
	); err != nil {
		t.Fatalf(
			"execute SQLite substring matrix: %v\nSQL: %s\nargs: %#v",
			err,
			matrix.SQL,
			matrix.Args,
		)
	}
	got := []string{
		positive,
		zeroStart,
		negativeStart,
		suffix,
		zeroLength,
		negativeLength,
		farRight,
		farLeft,
		covering,
		minimumStart,
		maximumStart,
		maximumLength,
		hugePreceding,
	}
	want := []string{
		"😀ab",
		"😀a",
		"ef",
		"def",
		"",
		"ab",
		"",
		"😀abcdef",
		"😀abcdef",
		"😀abcdef",
		"",
		"😀abcdef",
		"😀abcdef",
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf(
				"SQLite substring matrix column %d = %q, want %q; all=%#v",
				index,
				got[index],
				want[index],
				got,
			)
		}
	}
	if !containsArgument(matrix.Args, int64(math.MinInt64)) ||
		!containsArgument(matrix.Args, uint64(math.MaxUint64)) {
		t.Fatalf("substring matrix args lost extreme integers: %#v", matrix.Args)
	}

	unsupported := compile(
		`index=substring event_id="substring-unicode"` +
			` | eval numeric_part=substr(numeric, 1), multi_part=substr(multi, 1), object_part=substr(object_value, 1), null_part=substr(nothing, 1), missing_part=substr(absent, 1)` +
			` | table numeric_part,multi_part,object_part,null_part,missing_part`,
	)
	var numericPart, multiPart, objectPart, nullPart, missingPart *string
	if err := connection.QueryRow(
		queryContext,
		unsupported.SQL,
		unsupported.Args...,
	).Scan(
		&numericPart,
		&multiPart,
		&objectPart,
		&nullPart,
		&missingPart,
	); err != nil {
		t.Fatalf(
			"execute unsupported Dynamic substrings: %v\nSQL: %s\nargs: %#v",
			err,
			unsupported.SQL,
			unsupported.Args,
		)
	}
	if numericPart != nil || multiPart != nil || objectPart != nil ||
		nullPart != nil || missingPart != nil {
		t.Fatalf(
			"unsupported Dynamic substrings = %#v/%#v/%#v/%#v/%#v, want nulls",
			numericPart,
			multiPart,
			objectPart,
			nullPart,
			missingPart,
		)
	}

	fixed := compile(
		`index=substring event_id="substring-unicode"` +
			` | eval part=substr(host, 2, 3) | table part`,
	)
	var fixedPart string
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedPart); err != nil {
		t.Fatalf(
			"execute fixed String substring: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if fixedPart != "abc" {
		t.Fatalf("fixed String substring = %q, want abc", fixedPart)
	}

	for _, test := range []struct {
		source string
		want   uint64
	}{
		{
			source: `index=substring event_id="substring-unicode" | where substr(scalar, 1, 3)="😀ab" | stats count`,
			want:   1,
		},
		{
			source: `index=substring event_id="substring-unicode" | where substr(scalar, 1, 3)="😀ac" | stats count`,
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
				"execute substring where %q: %v\nSQL: %s\nargs: %#v",
				test.source,
				err,
				compiled.SQL,
				compiled.Args,
			)
		}
		if count != test.want {
			t.Fatalf(
				"substring where %q count = %d, want %d",
				test.source,
				count,
				test.want,
			)
		}
	}

	binary := compile(
		`index=substring event_id="substring-binary"` +
			` | eval part=substr(_raw, 1, 5) | table part`,
	)
	var binaryPart *string
	if err := connection.QueryRow(
		queryContext,
		binary.SQL,
		binary.Args...,
	).Scan(&binaryPart); err != nil {
		t.Fatalf(
			"execute binary raw substring: %v\nSQL: %s\nargs: %#v",
			err,
			binary.SQL,
			binary.Args,
		)
	}
	if binaryPart != nil {
		t.Fatalf("binary raw substring = %q, want null", *binaryPart)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		matrix,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("substring lowering expands event rows:\n%s", actions)
	}
}
