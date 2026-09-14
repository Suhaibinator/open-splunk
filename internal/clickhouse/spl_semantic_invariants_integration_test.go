package clickhouse

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// TestSPLSemanticInvariantsAgainstClickHouse checks pairs of SPL pipelines
// whose rewrites must not change the selected events. These comparisons are
// deliberately cross-feature: the same Dynamic values pass through eval,
// rename, fields, search, where, and multivalue scalar lowering.
func TestSPLSemanticInvariantsAgainstClickHouse(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the Docker integration test")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker CLI is unavailable: %v", err)
	}
	image, err := testsupport.ResolvePinnedClickHouseImage(
		os.Getenv("OPEN_SPLUNK_CLICKHOUSE_TEST_IMAGE"),
	)
	if err != nil {
		t.Fatalf("resolve pinned ClickHouse image: %v", err)
	}
	t.Logf("ClickHouse image: %s", image)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	connection, store := chartEdgeStartClickHouse(t, ctx)
	indexTime := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	newEvent := func(id string, fields ...*opensplunk.TypedObjectField) *ingest.StoredEvent {
		event := testStoredEvent(id, "semantic-invariants", indexTime)
		event.Event.Source = "semantic-invariants"
		event.Event.Fields = typedObjectValue(fields...)
		return event
	}
	events := []*ingest.StoredEvent{
		newEvent("all-equal",
			typedField("left", typedString("same")),
			typedField("right", typedString("same")),
			typedField("low", typedSint(1)),
			typedField("high", typedSint(2)),
			typedField("number", typedSint(1)),
			typedField("flag", typedBool(true)),
			typedField("other_flag", typedBool(true)),
			typedField("probe", typedString("present")),
			typedField("tags", typedList(typedString("x"), typedNull(), typedString("y"))),
		),
		newEvent("different",
			typedField("left", typedString("same")),
			typedField("right", typedString("different")),
			typedField("low", typedSint(-2)),
			typedField("high", typedDouble(-1.5)),
			typedField("number", typedDouble(1)),
			typedField("flag", typedBool(false)),
			typedField("other_flag", typedBool(true)),
			typedField("probe", typedNull()),
			typedField("tags", typedList(typedString("y"))),
		),
		newEvent("case-different",
			typedField("left", typedString("Same")),
			typedField("right", typedString("same")),
			typedField("low", typedSint(3)),
			typedField("high", typedSint(2)),
			typedField("number", typedDecimal("1.00")),
			typedField("flag", typedBool(true)),
			typedField("other_flag", typedBool(false)),
			typedField("tags", typedList(typedString("x"), typedString("x"))),
		),
		newEvent("null-fields",
			typedField("left", typedNull()),
			typedField("right", typedNull()),
			typedField("low", typedNull()),
			typedField("high", typedNull()),
			typedField("number", typedNull()),
			typedField("flag", typedNull()),
			typedField("other_flag", typedNull()),
			typedField("probe", typedNull()),
			typedField("tags", typedNull()),
		),
		newEvent("missing-fields"),
		newEvent("single-tag",
			typedField("left", typedString("other")),
			typedField("right", typedString("other")),
			typedField("low", typedUint(9)),
			typedField("high", typedDouble(10)),
			typedField("number", typedUint(1)),
			typedField("flag", typedBool(false)),
			typedField("other_flag", typedBool(false)),
			typedField("probe", typedString("")),
			typedField("tags", typedList(typedString("x"))),
		),
	}
	_, queryContext := storeScalarFunctionIntegrationFixtures(
		ctx,
		t,
		store,
		indexTime,
		"semantic-invariants",
		"semantic-invariants-batch",
		371,
		events...,
	)
	visibilityCutoff, err := store.VisibilityCutoff(ctx)
	if err != nil {
		t.Fatalf("capture semantic-invariant visibility cutoff: %v", err)
	}
	compile := func(t *testing.T, source string) CompiledQuery {
		t.Helper()
		return compileIntegrationSPLForIndex(
			t,
			source,
			indexTime.Add(10*time.Second),
			visibilityCutoff,
			"semantic-invariants",
		)
	}
	queryEventIDs := func(t *testing.T, source string) []string {
		t.Helper()
		compiled := compile(t, source+` | sort event_id | table event_id`)
		rows, queryErr := connection.Query(queryContext, compiled.SQL, compiled.Args...)
		if queryErr != nil {
			t.Fatalf("execute semantic invariant SPL %q: %v\nSQL: %s\nargs: %#v",
				source, queryErr, compiled.SQL, compiled.Args)
		}
		defer rows.Close()
		var result []string
		for rows.Next() {
			var eventID string
			if scanErr := rows.Scan(&eventID); scanErr != nil {
				t.Fatalf("scan semantic invariant SPL %q: %v", source, scanErr)
			}
			result = append(result, eventID)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			t.Fatalf("iterate semantic invariant SPL %q: %v", source, rowsErr)
		}
		return result
	}
	queryCount := func(t *testing.T, source string) uint64 {
		t.Helper()
		compiled := compile(t, source+` | table result`)
		var result uint64
		if queryErr := connection.QueryRow(
			queryContext,
			compiled.SQL,
			compiled.Args...,
		).Scan(&result); queryErr != nil {
			t.Fatalf("execute semantic count SPL %q: %v\nSQL: %s\nargs: %#v",
				source, queryErr, compiled.SQL, compiled.Args)
		}
		return result
	}

	base := `index=semantic-invariants source="semantic-invariants"`
	invariants := []struct {
		name  string
		left  string
		right string
	}{
		{
			name:  "field equality is symmetric",
			left:  base + ` | where left=right`,
			right: base + ` | where right=left`,
		},
		{
			name:  "relational operands can be swapped",
			left:  base + ` | where low<high`,
			right: base + ` | where high>low`,
		},
		{
			name:  "eval alias preserves field comparison",
			left:  base + ` | where left=right`,
			right: base + ` | eval copied=left | where copied=right`,
		},
		{
			name:  "rename preserves field comparison",
			left:  base + ` | where left=right`,
			right: base + ` | rename left AS copied | where copied=right`,
		},
		{
			name:  "projection placement preserves required fields",
			left:  base + ` | where left=right`,
			right: base + ` | fields event_id,left,right | where left=right`,
		},
		{
			name:  "unrelated eval commutes with filter",
			left:  base + ` | where left=right | eval marker="kept"`,
			right: base + ` | eval marker="kept" | where left=right`,
		},
		{
			name:  "boolean equality survives eval alias",
			left:  base + ` | where flag=other_flag`,
			right: base + ` | eval copied_flag=flag | where copied_flag=other_flag`,
		},
		{
			name:  "boolean negated inequality equals equality",
			left:  base + ` | where flag=other_flag`,
			right: base + ` | where NOT flag!=other_flag`,
		},
		{
			name:  "null predicates are complements",
			left:  base + ` | where isnull(probe)`,
			right: base + ` | where NOT isnotnull(probe)`,
		},
		{
			name:  "null predicate survives eval projection",
			left:  base + ` | where isnull(probe)`,
			right: base + ` | eval missing=if(isnull(probe),1,0) | where missing=1`,
		},
		{
			name:  "equivalent numeric literal spellings",
			left:  base + ` | where number=1`,
			right: base + ` | where number=1.0`,
		},
		{
			name:  "exponent numeric literal spelling",
			left:  base + ` | where number=1`,
			right: base + ` | where number=1e0`,
		},
		{
			name:  "single-value membership equals equality",
			left:  base + ` | where left="same"`,
			right: base + ` | where left IN ("same")`,
		},
		{
			name:  "numeric single-value membership equals equality",
			left:  base + ` | where number=1`,
			right: base + ` | where number IN (1)`,
		},
		{
			name:  "numeric eval alias preserves equality",
			left:  base + ` | where number=1`,
			right: base + ` | eval copied_number=number | where copied_number=1`,
		},
		{
			name:  "numeric rename preserves equality",
			left:  base + ` | where number=1`,
			right: base + ` | rename number AS renamed_number | where renamed_number=1`,
		},
		{
			name:  "isnotnull is negated isnull",
			left:  base + ` | where isnotnull(probe)`,
			right: base + ` | where NOT isnull(probe)`,
		},
		{
			name:  "if null predicate equals direct predicate",
			left:  base + ` | where isnull(probe)`,
			right: base + ` | where if(isnull(probe),true,false)`,
		},
		{
			name:  "multivalue function survives rename",
			left:  base + ` | where mvfind(tags,"^x$")>=0`,
			right: base + ` | rename tags AS renamed_tags | where mvfind(renamed_tags,"^x$")>=0`,
		},
		{
			name:  "multivalue function survives eval result",
			left:  base + ` | where mvfind(tags,"^x$")>=0`,
			right: base + ` | eval position=mvfind(tags,"^x$") | where position>=0`,
		},
		{
			name:  "multivalue count survives rename",
			left:  base + ` | where mvcount(tags)>0`,
			right: base + ` | rename tags AS renamed_tags | where mvcount(renamed_tags)>0`,
		},
		{
			name:  "multivalue count survives eval alias",
			left:  base + ` | where mvcount(tags)>0`,
			right: base + ` | eval copied_tags=tags | where mvcount(copied_tags)>0`,
		},
	}
	for _, invariant := range invariants {
		t.Run(invariant.name, func(t *testing.T) {
			left := queryEventIDs(t, invariant.left)
			right := queryEventIDs(t, invariant.right)
			if !slices.Equal(left, right) {
				t.Fatalf("equivalent SPL selected different events:\nleft  %q => %#v\nright %q => %#v",
					invariant.left, left, invariant.right, right)
			}
		})
	}

	countInvariants := []struct {
		name  string
		left  string
		right string
	}{
		{
			name:  "conditional count equals field equality filter",
			left:  base + ` | stats count(eval(left=right)) AS result`,
			right: base + ` | where left=right | stats count AS result`,
		},
		{
			name:  "conditional count equals null filter",
			left:  base + ` | stats count(eval(isnull(probe))) AS result`,
			right: base + ` | where isnull(probe) | stats count AS result`,
		},
		{
			name:  "conditional count equals boolean filter",
			left:  base + ` | stats count(eval(flag=other_flag)) AS result`,
			right: base + ` | where flag=other_flag | stats count AS result`,
		},
		{
			name:  "conditional count equals multivalue filter",
			left:  base + ` | stats count(eval(mvfind(tags,"^x$")>=0)) AS result`,
			right: base + ` | where mvfind(tags,"^x$")>=0 | stats count AS result`,
		},
		{
			name:  "field count equals presence filter",
			left:  base + ` | stats count(number) AS result`,
			right: base + ` | where isnotnull(number) | stats count AS result`,
		},
	}
	for _, invariant := range countInvariants {
		t.Run(invariant.name, func(t *testing.T) {
			left := queryCount(t, invariant.left)
			right := queryCount(t, invariant.right)
			if left != right {
				t.Fatalf("equivalent SPL produced different counts:\nleft  %q => %d\nright %q => %d",
					invariant.left, left, invariant.right, right)
			}
		})
	}
}
