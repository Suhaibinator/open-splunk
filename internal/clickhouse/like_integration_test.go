package clickhouse

import (
	"context"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testLikeAgainstClickHouse(
	ctx context.Context,
	t *testing.T,
	store *Store,
	connection clickhousedriver.Conn,
	indexTime time.Time,
) {
	t.Helper()

	event := testStoredEvent("like-scalars", "like", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("text", typedString("line1\nline2 ERROR api/v1 ¥")),
		typedField("empty", typedString("")),
		typedField("percent", typedString("50%off")),
		typedField("underscore", typedString("a_b")),
		typedField("slash", typedString(`a\b`)),
		typedField("ordinary_escape", typedString(`a\qb`)),
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
		"like",
		"like-batch",
		113,
		event,
	)

	for _, test := range []struct {
		name  string
		where string
		want  uint64
	}{
		{name: "exact whole string", where: `like("api", "api")`, want: 1},
		{name: "not a substring without wildcard", where: `like(text, "ERROR")`, want: 0},
		{name: "substring percent", where: `like(text, "%ERROR%")`, want: 1},
		{name: "case sensitive", where: `like(text, "%error%")`, want: 0},
		{name: "prefix", where: `like(text, "line1%")`, want: 1},
		{name: "suffix", where: `like(text, "%¥")`, want: 1},
		{name: "percent crosses newline", where: `like(text, "line1%line2%")`, want: 1},
		{name: "underscore is one Unicode code point", where: `like("¥", "_")`, want: 1},
		{name: "escaped percent", where: `like(percent, "50\%off")`, want: 1},
		{name: "escaped underscore", where: `like(underscore, "a\_b")`, want: 1},
		{name: "escaped backslash", where: `like(slash, "a\\\\b")`, want: 1},
		{name: "backslash before ordinary character is literal", where: `like(ordinary_escape, "a\qb")`, want: 1},
		{name: "empty pattern matches empty", where: `like(empty, "")`, want: 1},
		{name: "empty pattern does not match text", where: `like(text, "")`, want: 0},
		{name: "percent matches all text", where: `like(text, "%")`, want: 1},
		{name: "fixed number conversion", where: `like(42, "4_")`, want: 1},
		{name: "fixed Boolean conversion", where: `like(false, "false")`, want: 1},
		{name: "Dynamic number is not text", where: `like(number, "4_")`, want: 0},
		{name: "Dynamic Boolean is not text", where: `like(flag, "false")`, want: 0},
		{name: "null", where: `like(nothing, "%")`, want: 0},
		{name: "missing", where: `like(absent, "%")`, want: 0},
		{name: "multivalue", where: `like(multi, "%error%")`, want: 0},
		{name: "object", where: `like(object, "%error%")`, want: 0},
		{name: "binary", where: `like(binary, "%error%")`, want: 0},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			compiled := compile(
				`index=like event_id="like-scalars" | where ` + test.where + ` | stats count`,
			)
			var count uint64
			if err := connection.QueryRow(
				queryContext,
				compiled.SQL,
				compiled.Args...,
			).Scan(&count); err != nil {
				t.Fatalf(
					"execute like %s: %v\nSQL: %s\nargs: %#v",
					test.name,
					err,
					compiled.SQL,
					compiled.Args,
				)
			}
			if count != test.want {
				t.Fatalf("like %s count = %d, want %d", test.name, count, test.want)
			}
		})
	}

	composed := compile(
		`index=like event_id="like-scalars"` +
			` | eval label=if(like(text, "%ERROR%"), "problem", "ok"), rendered=tostring(like(text, "line1%"))` +
			` | table label,rendered`,
	)
	var label, rendered string
	if err := connection.QueryRow(
		queryContext,
		composed.SQL,
		composed.Args...,
	).Scan(&label, &rendered); err != nil {
		t.Fatalf(
			"execute composed like: %v\nSQL: %s\nargs: %#v",
			err,
			composed.SQL,
			composed.Args,
		)
	}
	if label != "problem" || rendered != "True" {
		t.Fatalf("composed like = %q/%q, want problem/True", label, rendered)
	}
}
