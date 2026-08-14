package clickhouse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const v03FillNullMVExpandSource = "v03-fillnull-mvexpand-regression"

// TestCompileV03FillNullDynamicThenMVExpandMaterializesThePublicField protects
// the physical-column boundary between two independently valid v0.3 stages.
// A stored Dynamic field is first materialized through fillnull's private
// lossless tuple. mvexpand must then publish its replacement without assuming
// that a same-named public physical column already exists.
func TestCompileV03FillNullDynamicThenMVExpandMaterializesThePublicField(t *testing.T) {
	t.Parallel()

	for _, source := range []string{
		`index=gradethis | fillnull value="fallback" optional_mv | mvexpand optional_mv | table event_id optional_mv`,
		`index=gradethis | fillnull value="fallback" optional_mv | fields event_id optional_mv | mvexpand optional_mv | table event_id optional_mv`,
		`index=gradethis | fillnull value="fallback" optional_mv | table event_id optional_mv | mvexpand optional_mv | table event_id optional_mv`,
	} {
		compiled := compileSPL(t, source)
		if !compiled.HasValidExecutionSeal() || !compiled.RequiresAtomicResult() {
			t.Fatalf("fillnull -> mvexpand query is not sealed atomic execution: %s", source)
		}
		if !reflect.DeepEqual(compiled.OutputFields, []string{"event_id", "optional_mv"}) {
			t.Fatalf("fillnull -> mvexpand fields = %v, want [event_id optional_mv]", compiled.OutputFields)
		}
		// Regression symptom: REPLACE(optional_mv) compiled while the relation
		// contained only __os_fillnull_value_N_M and no physical optional_mv.
		if !strings.Contains(compiled.SQL, ` AS "optional_mv",`) {
			t.Fatalf("fillnull did not materialize its Dynamic output publicly:\n%s", compiled.SQL)
		}
		if strings.Contains(compiled.SQL, ` AS "__os_fillnull_value_`) {
			t.Fatalf("fillnull retained a private-only Dynamic output:\n%s", compiled.SQL)
		}
	}
}

// TestV03FillNullDynamicThenMVExpandAgainstClickHouse is the authoritative
// pinned-engine regression for missing, null, supported list, scalar String,
// numeric, Boolean, and unsupported heterogeneous-list Dynamic cells. It also
// keeps fields/table projection barriers in the matrix.
func TestV03FillNullDynamicThenMVExpandAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"))
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	eventAnchor := time.Date(2026, time.July, 21, 12, 0, 0, 0, time.UTC)
	newEvent := func(id string, ordinal int, fields ...*opensplunkv1.TypedObjectField) *ingest.StoredEvent {
		event := testStoredEvent(id, "spl-v03-fillnull-mvexpand", indexTime)
		event.Event.Source = v03FillNullMVExpandSource
		event.Event.EventTime = timestamppb.New(eventAnchor.Add(time.Duration(ordinal) * time.Second))
		event.Event.CollectedAt = timestamppb.New(event.Event.EventTime.AsTime().Add(-time.Second))
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	fixtures := []*ingest.StoredEvent{
		newEvent("missing", 1),
		newEvent("null", 2, typedField("optional_mv", typedNull())),
		newEvent("list", 3, typedField("optional_mv", typedList(typedString("a"), typedNull(), typedString("β")))),
		newEvent("empty-list", 4, typedField("optional_mv", typedList())),
		newEvent("string", 5, typedField("optional_mv", typedString("scalar"))),
		newEvent("number", 6, typedField("optional_mv", typedSint(7))),
		newEvent("bool", 7, typedField("optional_mv", typedBool(true))),
		newEvent("mixed-list", 8, typedField("optional_mv", typedList(typedString("x"), typedSint(9)))),
		newEvent("nested-list", 9, typedField("optional_mv", typedList(typedList(typedString("x"))))),
		newEvent("object", 10, typedField("optional_mv", typedObject(typedField("child", typedString("x"))))),
	}
	compile, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx, t, store, indexTime, "spl-v03-fillnull-mvexpand",
		"v03-fillnull-mvexpand-regression", 1, fixtures...,
	)

	for _, barrier := range []string{
		"",
		" | fields event_id optional_mv",
		" | table event_id optional_mv",
	} {
		source := `index=spl-v03-fillnull-mvexpand source="` + v03FillNullMVExpandSource + `"` +
			` event_id!="mixed-list" event_id!="nested-list" event_id!="object" | sort 0 +event_id` +
			` | fillnull value="fallback" optional_mv` + barrier +
			` | mvexpand optional_mv | table event_id optional_mv`
		compiled := compile(source)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute fillnull -> %q -> mvexpand: %v\nSQL: %s", barrier, queryErr, compiled.SQL)
		}
		columns := rows.Columns()
		if !reflect.DeepEqual(columns, []string{"event_id", "optional_mv"}) {
			_ = rows.Close()
			t.Fatalf("fillnull -> %q -> mvexpand columns = %v", barrier, columns)
		}
		var got []string
		for rows.Next() {
			var id string
			var value clickhousedriver.Dynamic
			if scanErr := rows.Scan(&id, &value); scanErr != nil {
				_ = rows.Close()
				t.Fatalf("scan fillnull -> %q -> mvexpand: %v", barrier, scanErr)
			}
			got = append(got, id+"="+v03JSONText(value))
		}
		iterateErr := rows.Err()
		closeErr := rows.Close()
		if iterateErr != nil || closeErr != nil {
			t.Fatalf("iterate fillnull -> %q -> mvexpand = (%v, %v)", barrier, iterateErr, closeErr)
		}
		want := []string{
			"bool=true", "list=a", "list=<null>", "list=β", "missing=fallback",
			"null=fallback", "number=7", "string=scalar",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("fillnull -> %q -> mvexpand rows = %v, want %v", barrier, got, want)
		}
	}

	for _, id := range []string{"mixed-list", "nested-list", "object"} {
		unsupported := compile(`index=spl-v03-fillnull-mvexpand source="` +
			v03FillNullMVExpandSource + `" event_id="` + id + `"` +
			` | fillnull value="fallback" optional_mv` +
			` | fields event_id optional_mv | mvexpand optional_mv | table event_id`)
		if !unsupported.RequiresAtomicResult() {
			t.Fatalf("unsupported %s fillnull -> mvexpand query lost its atomic-result seal", id)
		}
		queryErr := executeCompiledExpectingNoRows(queryContext, connection, unsupported)
		var exception *clickhousedriver.Exception
		if !errors.As(queryErr, &exception) || exception.Code != 395 ||
			!strings.Contains(exception.Message, UnsupportedMVExpandValueMarker) {
			t.Fatalf("unsupported %s error = %v, want sanitized mvexpand marker", id, queryErr)
		}
	}

	// Runtime-wide pivots have a deliberately narrow source CTE. Keep the
	// fillnull container sidecar in that CTE even when every selected row is a
	// scalar: the compiler cannot discard the descendant authority based on the
	// fixture values, and ClickHouse resolves the prepared CTE structurally.
	for _, barrier := range []string{
		"",
		" | fields event_id optional_mv",
		" | table event_id optional_mv",
	} {
		charted := compile(`index=spl-v03-fillnull-mvexpand source="` +
			v03FillNullMVExpandSource + `"` +
			` event_id!="list" event_id!="empty-list" event_id!="number" event_id!="bool"` +
			` event_id!="mixed-list" event_id!="nested-list" event_id!="object"` +
			` | fillnull value="fallback" optional_mv` + barrier +
			` | chart count OVER optional_mv BY event_id`)
		want := []chartEdgeTransportRow{
			{ordinal: 0, row: "fallback", names: "0:missing|0:null|0:string", counts: "1|1|0"},
			{ordinal: 1, row: "scalar", names: "0:missing|0:null|0:string", counts: "0|0|1"},
		}
		if got := chartEdgeTransport(t, queryContext, connection, charted); !reflect.DeepEqual(got, want) {
			t.Fatalf("fillnull -> %q -> chart = %#v, want %#v", barrier, got, want)
		}
	}

	// Exercise every runtime-wide pivot lowering that crosses the same narrow
	// source boundary. The structural tests pin the exact sidecar projection;
	// these engine probes ensure ClickHouse can resolve it for both axes and for
	// count/numeric chart and timechart execution.
	pivotScope := `index=spl-v03-fillnull-mvexpand source="` +
		v03FillNullMVExpandSource + `"` +
		` event_id!="list" event_id!="empty-list" event_id!="number" event_id!="bool"` +
		` event_id!="mixed-list" event_id!="nested-list" event_id!="object"` +
		` | fillnull value="fallback" optional_mv`
	for _, suffix := range []string{
		` | chart sum(number) OVER optional_mv BY event_id`,
		` | chart sum(number) OVER event_id BY optional_mv`,
		` | timechart span=5m count BY optional_mv`,
		` | timechart span=5m sum(number) BY optional_mv`,
	} {
		compiled := compile(pivotScope + suffix)
		query := `SELECT count() FROM (` + compiled.SQL + `)`
		var rowCount uint64
		if queryErr := connection.QueryRow(queryContext, query, compiled.Args...).Scan(&rowCount); queryErr != nil {
			t.Fatalf("execute fillnull pivot %q: %v\nSQL: %s", suffix, queryErr, query)
		}
		if rowCount == 0 {
			t.Fatalf("fillnull pivot %q produced no transport rows", suffix)
		}
	}
}
