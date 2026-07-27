package clickhouse

import (
	"context"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testMatchAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("match-scalars", "match", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("text", typedString("line1\nline2 ERROR api/v1")),
		typedField("final_newline", typedString("ERROR\n")),
		typedField("number", typedSint(42)),
		typedField("flag", typedBool(false)),
		typedField("nothing", typedNull()),
		typedField("multi", typedList(typedString("error"), typedString("ok"))),
		typedField("object", typedObject(typedField("child", typedString("error")))),
		typedField("binary", typedBytes([]byte{0xff, 'e', 'r', 'r', 'o', 'r'})),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"match",
		"match-batch",
		112,
		event,
	)

	for _, test := range []struct {
		name  string
		where string
		want  uint64
	}{
		{name: "substring", where: `match(text, "ERROR")`, want: 1},
		{name: "case sensitive", where: `match(text, "error")`, want: 0},
		{name: "inline case insensitive", where: `match(text, "(?i)error")`, want: 1},
		{name: "dot excludes newline", where: `match(text, "line1.line2")`, want: 0},
		{name: "explicit dotall", where: `match(text, "(?s)line1.line2")`, want: 1},
		{name: "dollar before final newline", where: `match(final_newline, "ERROR$")`, want: 1},
		{name: "strict end before final newline", where: `match(final_newline, "ERROR\z")`, want: 0},
		{name: "multiline dollar", where: `match(text, "(?m)line1$")`, want: 1},
		{name: "empty pattern", where: `match(text, "")`, want: 1},
		{name: "zero width", where: `match(text, "^")`, want: 1},
		{name: "fixed number conversion", where: `match(42, "^42$")`, want: 1},
		{name: "fixed Boolean conversion", where: `match(false, "^false$")`, want: 1},
		{name: "Dynamic number is not text", where: `match(number, "42")`, want: 0},
		{name: "Dynamic Boolean is not text", where: `match(flag, "false")`, want: 0},
		{name: "null", where: `match(nothing, "x")`, want: 0},
		{name: "missing", where: `match(absent, "x")`, want: 0},
		{name: "multivalue", where: `match(multi, "error")`, want: 0},
		{name: "object", where: `match(object, "error")`, want: 0},
		{name: "binary", where: `match(binary, "error")`, want: 0},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(
				`index=match event_id="match-scalars" | where ` + test.where + ` | stats count`,
			)
			var count uint64
			if err := connection.QueryRow(
				queryContext,
				compiled.SQL,
				compiled.Args...,
			).Scan(&count); err != nil {
				t.Fatalf(
					"execute match %s: %v\nSQL: %s\nargs: %#v",
					test.name,
					err,
					compiled.SQL,
					compiled.Args,
				)
			}
			if count != test.want {
				t.Fatalf("match %s count = %d, want %d", test.name, count, test.want)
			}
		})
	}

	composed := compile(
		`index=match event_id="match-scalars"` +
			` | eval label=if(match(text, "(?i)error"), "problem", "ok"), rendered=tostring(match(text, "^line1"))` +
			` | table label,rendered`,
	)
	var label, rendered string
	if err := connection.QueryRow(
		queryContext,
		composed.SQL,
		composed.Args...,
	).Scan(&label, &rendered); err != nil {
		t.Fatalf(
			"execute composed match: %v\nSQL: %s\nargs: %#v",
			err,
			composed.SQL,
			composed.Args,
		)
	}
	if label != "problem" || rendered != "True" {
		t.Fatalf("composed match = %q/%q, want problem/True", label, rendered)
	}
}
