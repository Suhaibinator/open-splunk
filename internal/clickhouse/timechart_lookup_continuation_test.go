package clickhouse

import (
	"context"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"strings"
	"testing"
	"time"
)

func TestTimechartDeferredLookupAuthority(t *testing.T) {
	for _, chart := range []string{"timechart span=1h count BY host", "timechart span=1h fixedrange=false count"} {
		t.Run(chart, func(t *testing.T) {
			logical := buildPlan(t, `index=gradethis | `+chart+` | eval service="api" | lookup service_catalog service_id AS service OUTPUT owner | table owner`)
			resolution := testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}})
			compiler := Compiler{}
			var err error
			if strings.Contains(chart, "BY") {
				compiler, err = compiler.WithDeferredLookupResolutionsContext(context.Background(), []LookupResolution{resolution})
			} else {
				compiler, err = compiler.WithLookupResolutionsContext(context.Background(), []LookupResolution{resolution})
			}
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := compiler.Compile(logical)
			if err != nil {
				t.Fatal(err)
			}
			evidence, ok := compiled.LookupAssetVersions()
			if !ok || len(evidence) != 1 || evidence[0].DefinitionName() != "service_catalog" {
				t.Fatalf("evidence=%v valid=%v", evidence, ok)
			}
			columns := []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {"east", "UInt64"}}
			rows := [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(1)}}
			if compiled.RequiresTimechartInputDiscovery() {
				compiled, err = compiled.ContinueContext(context.Background(), columns[:1], [][]any{rows[0][:1]})
				if err != nil {
					t.Fatal(err)
				}
				columns[1].Name = "count"
			}
			next, err := compiled.ContinueContext(context.Background(), columns, rows)
			if err != nil {
				t.Fatal(err)
			}
			if !next.HasValidExecutionSeal() || len(next.lookupTables) != 1 || strings.Contains(next.SQL, `"open_splunk"."events"`) {
				t.Fatal("missing sealed external lookup")
			}
			if &next.lookupTables[0].backing.values[0][0] != &compiled.continuation.lookups[0].backing.values[0][0] {
				t.Fatal("continuation duplicated selected asset cells")
			}
		})
	}
}

func TestTimechartDeferredLookupRejectsMissingAuthority(t *testing.T) {
	logical := buildPlan(t, `index=gradethis | timechart span=1h count BY host | lookup service_catalog service_id AS service OUTPUT owner`)
	if _, err := (Compiler{}).Compile(logical); err == nil {
		t.Fatal("missing deferred authority accepted")
	}
}

func TestTimechartDeferredLookupWithAutomaticAdmission(t *testing.T) {
	authored := buildPlan(t, `index=gradethis | timechart span=1h count BY host | eval service="api" | lookup service_catalog service_id AS service OUTPUT owner | table owner`)
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := plan.InjectKnowledgePrelude(authored, empty)
	if err != nil {
		t.Fatal(err)
	}
	selector, err := knowledge.CompileSelector(knowledge.SelectorSpec{})
	if err != nil {
		t.Fatal(err)
	}
	binding := automaticLookupTestBinding(t, "automatic", "auto_a", "service", "owner", selector, []string{"service_id", "owner"}, [][]string{{"api", "automatic-owner"}})
	injected, compiler, err := (Compiler{}).WithAutomaticLookupBindings(admitted, []AutomaticLookupBinding{binding}, nil)
	if err != nil {
		t.Fatal(err)
	}
	compiler, err = compiler.WithDeferredLookupResolutionsContext(context.Background(), []LookupResolution{testLookupResolution(t, "tenant-1", [][]string{{"api", "platform"}})})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile(injected)
	if err != nil {
		t.Fatal(err)
	}
	evidence, ok := compiled.LookupAssetVersions()
	if !ok || len(evidence) != 2 {
		t.Fatalf("evidence=%v valid=%v", evidence, ok)
	}
	next, err := compiled.ContinueContext(context.Background(), []RelationColumn{{"_time", "DateTime64(9, 'UTC')"}, {"east", "UInt64"}}, [][]any{{time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC), uint64(1)}})
	if err != nil {
		t.Fatal(err)
	}
	if !next.IsContinuationOf(compiled) {
		t.Fatal("lost admitted ancestry")
	}
}
