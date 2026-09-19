package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

func testReplacePathAgainstClickHouse(ctx context.Context, t *testing.T, store *Store, connection clickhousedriver.Conn, indexTime time.Time) {
	t.Helper()
	event := testStoredEvent("replace-path", "replacepath", indexTime)
	event.Event.Fields = typedObjectValue(
		typedField("path", typedString("/123/456")),
		typedField("nothing", typedNull()),
		typedField("binary", typedBytes([]byte("/123/456"))),
	)
	compile, queryContext := storeScalarFunctionIntegrationFixtures(ctx, t, store, indexTime, "replacepath", "replace-path-batch", 180, event)
	const pattern = `/(\d+|[0-9a-fA-F-]{36}|[0-9a-fA-F]{24,})(?=/|$)`
	for _, tc := range []struct{ input, replacement, want string }{
		{"/123/456", "/:id", "/:id/:id"}, {"/123/456/", "/:id", "/:id/:id/"},
		{"/abc/123/def/456", "/:id", "/abc/:id/def/:id"}, {"123/456", "/:id", "123/:id"},
		{"//123///456/", "/:id", "//:id///:id/"}, {"", "/:id", ""}, {"/", "/:id", "/"},
		{"/123x/456", "/:id", "/123x/:id"}, {"/123\n", "/:id", "/:id\n"},
		{"/123\n/456", "/:id", "/123\n/:id"}, {"/123\n\n", "/:id", "/123\n\n"},
		{"/123/456", "", ""}, {"/123/456", `/\1/id`, "/123/id/456/id"},
		{"/123/456", "/789/012", "/789/012/789/012"},
		{"/12345678-1234-1234-1234-123456789abc/abcdefabcdefabcdefabcdef", "/:id", "/:id/:id"},
		{strings.Repeat("/123", 10000), "/:id", strings.Repeat("/:id", 10000)},
	} {
		// Execute the production scalar compiler directly with bound input so the
		// fixture can include control characters and large values without SPL limits.
		state := replaceOutputTestState(uint64(len(tc.input)))
		field := state.visible["value"]
		field.valueSQL = "CAST(? AS String)"
		state.visible["value"] = field
		scalar, err := compileScalarValue(replaceOutputTestCall(replaceOutputTestField(), pattern, tc.replacement), state)
		if err != nil {
			t.Fatal(err)
		}
		var result *string
		scanScalarPackRow(t, queryContext, connection, fmt.Sprintf("path input length %d", len(tc.input)), CompiledQuery{SQL: "SELECT " + scalar.valueSQL, Args: append([]any{tc.input}, scalar.valueArgs...)}, &result)
		if result == nil || *result != tc.want {
			t.Fatalf("replace(%q,%q) = %v, want %q", tc.input, tc.replacement, result, tc.want)
		}
	}
	for _, tc := range []struct{ expression, want string }{
		{`replace(path, "/(\d+)(?=/|$)", "/:id")`, "/:id/:id"},
		{`replace("/ab/a", "/(a|ab)(?=/|$)", "/<\1>")`, "/<ab>/<a>"},
		{`replace("/123/456", "/(\d+)(\d+)(?=/|$)", "/\1-\2")`, "/12-3/45-6"},
		{`replace("/123/456", "/(\d+?)(\d+)(?=/|$)", "/\1-\2")`, "/1-23/4-56"},
		{`replace("/123/456", "/(\d+)(?=/|$)", "<\0>")`, "</123></456>"},
		{`replace(123, "/(\d+)(?=/|$)", "/:id")`, "123"},
		{`replace(nothing, "/(\d+)(?=/|$)", "/:id")`, "<null>"},
		{`replace(absent, "/(\d+)(?=/|$)", "/:id")`, "<null>"},
		{`replace(binary, "/(\d+)(?=/|$)", "/:id")`, "<null>"},
		{`replace(replace("/123/456", "/(\d+)(?=/|$)", "/789"), "/(\d+)(?=/|$)", "/:id")`, "/:id/:id"},
	} {
		compiled := compile(`index=replacepath | eval result=` + tc.expression + ` | table result`)
		var result *string
		scanScalarPackRow(t, queryContext, connection, tc.expression, compiled, &result)
		if scalarPackText(result) != tc.want {
			t.Fatalf("%s = %s, want %s", tc.expression, scalarPackText(result), tc.want)
		}
	}
}
