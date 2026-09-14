package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTimechartClosedRelationContinuation(t *testing.T) {
	for _, suffix := range []string{
		`where 'west coast' > 0 | table _time 'west coast'`,
		`eval total='west coast'+east | sort 0 -total | head 2`,
		`stats sum(*)`,
		`fields _time east | timechart span=1h sum(east) AS total | head 1`,
	} {
		t.Run(suffix, func(t *testing.T) {
			compiled := compileSPL(t, `index=gradethis | timechart span=1h count BY host | `+suffix)
			if !compiled.HasContinuation() {
				t.Fatal("missing continuation")
			}
			columns := []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {"west coast", "UInt64"}, {"east", "UInt64"}}
			rows := [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(2), uint64(3)}}
			next, err := compiled.ContinueContext(context.Background(), columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(next.SQL, `"open_splunk"."events"`) {
				t.Fatal("continuation rescans events")
			}
			if !next.IsContinuationOf(compiled) {
				t.Fatal("missing ancestry")
			}
			if !next.HasValidExecutionSeal() {
				t.Fatal("invalid seal")
			}
			columns[1].Name = "mutated"
			rows[0][1] = uint64(42)
			if !next.HasValidExecutionSeal() {
				t.Fatal("caller mutation changed backing")
			}
			tables, err := next.ExternalTablesForExecution(context.Background())
			if err != nil || len(tables) != 1 {
				t.Fatalf("tables=%d err=%v", len(tables), err)
			}
		})
	}
}
func TestStaticTimechartSuffixUsesClosedSchema(t *testing.T) {
	compiled := compileSPL(t, `index=gradethis | timechart span=1h count | eval twice=count*2 | table twice`)
	if !slices.Equal(compiled.OutputFields, []string{"twice"}) || compiled.Timechart != nil || compiled.HasContinuation() {
		t.Fatalf("final output=%v timechart=%v", compiled.OutputFields, compiled.Timechart)
	}
	if strings.Count(compiled.SQL, `FROM "open_splunk"."events"`) != 1 || !strings.Contains(compiled.SQL, "__os_chronological_validation") {
		t.Fatal("missing single-scan validation barrier")
	}
}
