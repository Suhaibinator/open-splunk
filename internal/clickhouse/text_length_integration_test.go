package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

func testTextLengthAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	unicodeEvent := testStoredEvent("text-length-unicode", "textlength", indexTime)
	unicodeEvent.Event.Host = "München Straße"
	unicodeEvent.Event.Raw = []byte("München Straße RAW")
	unicodeEvent.Event.Fields = typedObjectValue(
		typedField("scalar", typedString("München Straße")),
		typedField("empty", typedString("")),
		typedField(
			"multi",
			typedList(typedString("München"), typedString("Straße")),
		),
		typedField("numeric", typedSint(42)),
		typedField("nothing", typedNull()),
		typedField(
			"object_value",
			typedObject(typedField("child", typedString("unchanged"))),
		),
	)

	binaryEvent := testStoredEvent("text-length-binary", "textlength", indexTime)
	binaryEvent.Event.Raw = []byte("VALID ASCII MARKED BINARY")
	binaryEvent.Event.RawEncoding = opensplunkv1.RawEncoding_RAW_ENCODING_BINARY

	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"textlength",
		"text-length-batch",
		91,
		unicodeEvent,
		binaryEvent,
	)

	scalars := compile(
		`index=textlength event_id="text-length-unicode"` +
			` | eval literal_size=len("München"), scalar_size=length(scalar), nested_size=len(lower(scalar)), raw_size=len(_raw), empty_size=length(empty)` +
			` | table literal_size,scalar_size,nested_size,raw_size,empty_size`,
	)
	var literalSize, scalarSize, nestedSize, rawSize, emptySize uint64
	if err := connection.QueryRow(
		queryContext,
		scalars.SQL,
		scalars.Args...,
	).Scan(
		&literalSize,
		&scalarSize,
		&nestedSize,
		&rawSize,
		&emptySize,
	); err != nil {
		t.Fatalf(
			"execute scalar text length: %v\nSQL: %s\nargs: %#v",
			err,
			scalars.SQL,
			scalars.Args,
		)
	}
	if literalSize != 7 || scalarSize != 14 || nestedSize != 14 ||
		rawSize != 18 || emptySize != 0 {
		t.Fatalf(
			"text lengths = literal:%d scalar:%d nested:%d raw:%d empty:%d",
			literalSize,
			scalarSize,
			nestedSize,
			rawSize,
			emptySize,
		)
	}

	unsupported := compile(
		`index=textlength event_id="text-length-unicode"` +
			` | eval numeric_size=len(numeric), multi_size=length(multi), object_size=len(object_value), null_size=len(nothing), missing_size=length(absent)` +
			` | table numeric_size,multi_size,object_size,null_size,missing_size`,
	)
	var numericSize, multiSize, objectSize, nullSize, missingSize *uint64
	if err := connection.QueryRow(
		queryContext,
		unsupported.SQL,
		unsupported.Args...,
	).Scan(
		&numericSize,
		&multiSize,
		&objectSize,
		&nullSize,
		&missingSize,
	); err != nil {
		t.Fatalf(
			"execute unsupported text lengths: %v\nSQL: %s\nargs: %#v",
			err,
			unsupported.SQL,
			unsupported.Args,
		)
	}
	if numericSize != nil || multiSize != nil || objectSize != nil ||
		nullSize != nil || missingSize != nil {
		t.Fatalf(
			"unsupported text lengths = %#v/%#v/%#v/%#v/%#v, want nulls",
			numericSize,
			multiSize,
			objectSize,
			nullSize,
			missingSize,
		)
	}

	for _, test := range []struct {
		source string
		want   uint64
	}{
		{
			source: `index=textlength event_id="text-length-unicode" | where len(scalar)=14 | stats count`,
			want:   1,
		},
		{
			source: `index=textlength event_id="text-length-unicode" | where length(scalar)=15 | stats count`,
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
				"execute text-length where %q: %v\nSQL: %s\nargs: %#v",
				test.source,
				err,
				compiled.SQL,
				compiled.Args,
			)
		}
		if count != test.want {
			t.Fatalf("text-length where %q count = %d, want %d", test.source, count, test.want)
		}
	}

	fixed := compile(
		`index=textlength event_id="text-length-unicode"` +
			` | stats min(host) AS selected` +
			` | eval size=len(selected) | table size`,
	)
	var fixedSize uint64
	if err := connection.QueryRow(
		queryContext,
		fixed.SQL,
		fixed.Args...,
	).Scan(&fixedSize); err != nil {
		t.Fatalf(
			"execute fixed String text length: %v\nSQL: %s\nargs: %#v",
			err,
			fixed.SQL,
			fixed.Args,
		)
	}
	if fixedSize != 14 {
		t.Fatalf("fixed String text length = %d, want 14", fixedSize)
	}

	binary := compile(
		`index=textlength event_id="text-length-binary"` +
			` | eval raw_size=len(_raw) | table raw_size`,
	)
	var binarySize *uint64
	if err := connection.QueryRow(
		queryContext,
		binary.SQL,
		binary.Args...,
	).Scan(&binarySize); err != nil {
		t.Fatalf(
			"execute binary raw text length: %v\nSQL: %s\nargs: %#v",
			err,
			binary.SQL,
			binary.Args,
		)
	}
	if binarySize != nil {
		t.Fatalf("binary raw text length = %d, want null", *binarySize)
	}

	actions := explainCompiledQuery(
		t,
		queryContext,
		connection,
		explainActionsPrefix,
		scalars,
	)
	if strings.Contains(actions, "ArrayJoin") {
		t.Fatalf("text-length lowering expands event rows:\n%s", actions)
	}
}
