package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestCompileExactTimechartGrid(t *testing.T) {
	earliest := time.Date(2026, 3, 7, 8, 0, 0, 0, time.UTC)
	for _, axis := range []string{"span=250ms", "span=49h", "span=2d", "span=2w", "span=2q", "span=2y", "span=1h aligntime=earliest", "span=1month partial=false"} {
		t.Run(axis, func(t *testing.T) {
			query, err := spl.Parse("index=main | timechart " + axis + " count")
			if err != nil {
				t.Fatal(err)
			}
			latest := earliest.Add(time.Second)
			if axis != "span=250ms" {
				latest = earliest.AddDate(0, 0, 4)
			}
			logical, err := plan.Build(query, plan.Scope{TenantID: "tenant", AuthorizedIndexes: []string{"main"}, Earliest: earliest, Latest: latest, SearchStart: earliest, SearchTimezone: "America/Los_Angeles", IndexTimeCutoff: earliest, VisibilityCutoff: new(uint64)})
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := (Compiler{}).Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			if !compiled.Timechart.ExactGrid || len(compiled.Timechart.Boundaries) != int(compiled.Timechart.BucketCount)+1 {
				t.Fatalf("output=%+v", compiled.Timechart)
			}
			if strings.Count(compiled.SQL, "?") != len(compiled.Args) {
				t.Fatal("placeholder mismatch")
			}
			clone, ok := compiled.CloneForExecution()
			if !ok {
				t.Fatal("clone failed")
			}
			clone.Timechart.Boundaries[1] = clone.Timechart.Boundaries[1].Add(time.Nanosecond)
			if compiled.EqualForExecution(clone) {
				t.Fatal("boundary mutation escaped seal")
			}
			if compiled.Timechart.Boundaries[1].Equal(clone.Timechart.Boundaries[1]) {
				t.Fatal("boundary clone aliases")
			}
			if _, ok := compiled.RetainedBytes(); !ok {
				t.Fatal("retained accounting failed")
			}
			op := logical.Operators[len(logical.Operators)-1].(*plan.Timechart)
			op.GridBoundaries[1] = op.GridBoundaries[1].Add(time.Nanosecond)
			if _, err := (Compiler{}).Compile(logical); err == nil {
				t.Fatal("forged grid compiled")
			}
		})
	}
}
