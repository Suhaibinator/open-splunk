package clickhouse

import (
	"reflect"
	"strings"
	"testing"
)

func TestCompileNormalizedIDIndexPreservesResidual(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"event_id", "trace_id", "span_id"} {
		t.Run(field, func(t *testing.T) {
			compiled := compileSPL(t, `index=gradethis `+field+`="NeEdLe"`)
			candidate := normalizedIDIndexExpressionSQL(quoteIdentifier(field)) + " = lowerUTF8(?)"
			residual := `(1 AND ifNull(lowerUTF8(toString("` + field + `")) = lowerUTF8(?), 0))`
			if !strings.Contains(compiled.SQL, "("+candidate+" AND "+residual+")") {
				t.Fatalf("normalized ID candidate lost the residual:\n%s", compiled.SQL)
			}
			if got := compiled.Args[len(compiled.Args)-2:]; !reflect.DeepEqual(got, []any{"NeEdLe", "NeEdLe"}) {
				t.Fatalf("candidate and residual arguments = %#v", got)
			}
			if strings.Contains(compiled.SQL, "NeEdLe") {
				t.Fatal("ID literal escaped parameter binding")
			}
		})
	}
}

func TestCompileNormalizedIDIndexRespectsLineageAndPolarity(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		pipeline   string
		candidates int
	}{
		{"AND", `trace_id="a" AND span_id="b"`, 2},
		{"OR", `trace_id="a" OR span_id="b"`, 2},
		{"empty", `trace_id=""`, 1},
		{"Unicode", `trace_id="Kİß"`, 1},
		{"wildcard", `trace_id="a*"`, 0},
		{"presence", `trace_id=*`, 0},
		{"null", `trace_id=null`, 0},
		{"numeric", `trace_id=123`, 0},
		{"inequality", `trace_id!="a"`, 0},
		{"negative", `NOT trace_id="a"`, 0},
		{"negative group", `NOT (trace_id="a" OR span_id="b")`, 0},
		{"where", `| where trace_id="a"`, 0},
		{"eval predicate", `| eval found=if(trace_id="a", 1, 0)`, 0},
		{"renamed source", `| rename trace_id AS id | search id="a"`, 1},
		{"projected source", `| table trace_id | search trace_id="a"`, 1},
		{"calculated shadow", `| eval trace_id="a" | search trace_id="a"`, 0},
		{"renamed shadow", `| rename host AS trace_id | search trace_id="a"`, 0},
		{"calculated copy", `| eval id=trace_id | search id="a"`, 0},
		{"transformed ID", `| eval trace_id=lower(trace_id) | search trace_id="a"`, 0},
		{"aggregate shadow", `| stats count AS trace_id | search trace_id="a"`, 0},
		{"dynamic shadow", `| rename request_id AS trace_id | search trace_id="a"`, 0},
		{"bytes-capable shadow", `| rename _raw AS trace_id | search trace_id="a"`, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			compiled := compileSPL(t, "index=gradethis "+test.pipeline)
			if got := strings.Count(compiled.SQL, "lowerUTF8(ifNull("); got != test.candidates {
				t.Fatalf("index candidates = %d, want %d:\n%s", got, test.candidates, compiled.SQL)
			}
		})
	}
}
