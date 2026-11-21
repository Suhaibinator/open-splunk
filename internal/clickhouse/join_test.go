package clickhouse

import (
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const sanitizerJoin = `| fields trace_id
| join type=left trace_id
    [ search index=gradethis message="request rejected by sanitizer" | fields trace_id, stage, error, verification_code_length, min_code_length, max_code_length ]
| stats count BY error, verification_code_length, min_code_length, max_code_length`

func TestSanitizerJoin(t *testing.T) {
	compiled := compileSPL(t, sanitizerJoin)
	if _, indexes, ok := compiled.ReadScope(); !ok || !slices.Equal(indexes, []string{"gradethis"}) {
		t.Fatal("join lost the scoped scan")
	}
	if !slices.Equal(compiled.OutputFields, []string{"error", "verification_code_length", "min_code_length", "max_code_length", "count"}) {
		t.Fatal(compiled.OutputFields)
	}
	query, err := spl.Parse(strings.Replace(sanitizerJoin, "index=gradethis", "index=private", 1))
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.Build(query, testChartScope())
	var diagnostic *plan.Diagnostic
	if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_INDEX_FORBIDDEN" || diagnostic.Range.Start.Line != 3 {
		t.Fatalf("subsearch index diagnostic = %v", err)
	}
}

func TestSanitizerJoinAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("requires ClickHouse")
	}
	ctx, conn := eventResultLimitDatabaseFixture(t, 0)
	// Three same-trace events, two right matches. A fourth event has no
	// match. Later matching events outside the tenant/index/snapshot must
	// never replace the authorized newest result.
	const fixture = `INSERT INTO open_splunk.events (event_id,tenant_id,index_name,event_time,index_time,host,source,sourcetype,body,trace_id,
		fields,field_names,field_types,field_metadata_version,collector_id,ingest_source_kind,ingest_source_id,batch_id,batch_sequence,visibility_seq,expires_at)
	SELECT toString(number),if(number=4,'other','tenant-1'),if(number=5,'private','gradethis'),
	toDateTime64('2026-07-21 01:00:00',9,'UTC')+toIntervalSecond(number),toDateTime64('2026-07-21 01:00:00',3,'UTC'),
	'host','source','test',if(number IN (0,3),'request','request rejected by sanitizer'),if(number=3,'other','same'),
	CAST(concat('{"stage":"check","error":"error-',toString(number),'","verification_code_length":3,"min_code_length":4,"max_code_length":6}') AS JSON),
	['error','max_code_length','min_code_length','stage','verification_code_length'],[2,3,3,2,3],1,
	'test',1,'test','batch',number,if(number=6,99,1),toDateTime64('2099-01-01 00:00:00',3,'UTC') FROM numbers(7)`
	if err := conn.Exec(ctx, fixture); err != nil {
		t.Fatal(err)
	}
	compiled := compileSPL(t, sanitizerJoin)
	var label, lengths string
	var count uint64
	sql := "SELECT toString(error), concat(toString(verification_code_length), '/', toString(min_code_length), '/', toString(max_code_length)), count FROM (" + compiled.SQL + ")"
	rows, err := conn.Query(ctx, sql, compiled.Args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("missing sanitizer statistics: %v", rows.Err())
	}
	if err := rows.Scan(&label, &lengths, &count); err != nil {
		t.Fatal(err)
	}
	if label != "error-2" || lengths != "3/4/6" || count != 3 || rows.Next() {
		t.Fatalf("unexpected group: %s %s %d", label, lengths, count)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		kind string
		want uint64
	}{{"left", 4}, {"outer", 4}, {"inner", 3}} {
		pipeline := strings.Split(sanitizerJoin, "| stats")[0] + "| stats count"
		result := compileSPL(t, strings.Replace(pipeline, "type=left", "type="+tc.kind, 1))
		if err := conn.QueryRow(ctx, result.SQL, result.Args...).Scan(&count); err != nil || count != tc.want {
			t.Fatalf("%s count = %d, want %d: %v", tc.kind, count, tc.want, err)
		}
	}

	t.Run("preserve prior values on unmatched rows", func(t *testing.T) {
		result := compileSPL(t, `| table trace_id, error | join type=left trace_id [search message="request rejected by sanitizer" | fields trace_id, error] | search error="error-3" | stats count`)
		var kept uint64
		if err := conn.QueryRow(ctx, result.SQL, result.Args...).Scan(&kept); err != nil || kept != 1 {
			t.Fatalf("unmatched rows with prior value = %d, want 1: %v", kept, err)
		}
	})
	t.Run("right branch sees admitted knowledge fields", func(t *testing.T) {
		program := testKnowledgeEvidenceProgram(t, "join")
		logical, err := plan.InjectKnowledgePrelude(buildPlan(t, `| fields trace_id | join trace_id [search knowledge_calculated=present | fields trace_id, knowledge_calculated] | search knowledge_calculated=present | stats count`), program)
		if err != nil {
			t.Fatal(err)
		}
		result, err := (Compiler{}).Compile(logical)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := result.KnowledgeSnapshotEvidenceFor(program); !ok {
			t.Fatal("missing join knowledge authority")
		}
		var matched uint64
		if err := conn.QueryRow(ctx, result.SQL, result.Args...).Scan(&matched); err != nil || matched != 4 {
			t.Fatalf("knowledge matches = %d, want 4: %v", matched, err)
		}
	})
}
