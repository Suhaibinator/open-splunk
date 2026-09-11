package clickhouse

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestObservedTimechartCapturesAggregateSemantics(t *testing.T) {
	compiled := compileSPL(t, `index=gradethis | timechart span=1h fixedrange=false count(occurrence)`)
	if !strings.Contains(compiled.SQL, "arrayCount(") || !strings.Contains(compiled.SQL, "field_names") {
		t.Fatal("source capture omitted multivalue cardinality or object-parent presence")
	}
	columns := []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {timechartObservedMeasure, "UInt64"}}
	rows := [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(8)}}
	next, err := compiled.ContinueContext(context.Background(), columns, rows)
	if err != nil {
		t.Fatal(err)
	}
	if !next.relationInput.timechartOccurrences || !next.HasValidExecutionSeal() {
		t.Fatal("missing sealed cardinality authority")
	}
	next.relationInput.timechartOccurrences = false
	if next.HasValidExecutionSeal() {
		t.Fatal("cardinality authority mutation accepted")
	}
	columns[1].Name = "occurrence"
	if _, err := compiled.ContinueContext(context.Background(), columns, rows); err == nil {
		t.Fatal("incorrect discovery schema accepted")
	}
}

func TestObservedTimechartCapturesNumericSamplesBeforePresentation(t *testing.T) {
	for _, function := range []string{"sum", "avg", "perc95"} {
		compiled := compileSPL(t, `index=gradethis | timechart span=1h fixedrange=false `+function+`(metric) BY category`)
		for _, fragment := range []string{"arrayMap(sample", "dynamicElement", "descendant", "isValidUTF8"} {
			if !strings.Contains(compiled.SQL, fragment) {
				t.Fatalf("%s source omitted %s", function, fragment)
			}
		}
	}
}
