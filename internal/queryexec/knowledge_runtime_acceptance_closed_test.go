//go:build !open_splunk_knowledge_runtime_acceptance

package queryexec

import (
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

const knowledgeRuntimeClosedCompilerSealError = "seal compiled ClickHouse execution: nonempty knowledge lowering is absent"

func TestKnowledgeRuntimeCompilerMatrixStopsOnlyAtDefaultSeal(t *testing.T) {
	const (
		indexName         = "knowledge-runtime"
		selectorIndexName = "selector-runtime"
		tenantID          = "knowledge-tenant"
	)
	base := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	indexTime := base.Add(10 * time.Minute)
	earliest := base
	latest := base.Add(2 * time.Minute)
	plans := buildKnowledgeRuntimeMatrixPlans(
		t,
		knowledgeRuntimeProgram(t),
		tenantID,
		indexName,
		selectorIndexName,
		base,
		indexTime,
		earliest,
		latest,
	)
	compiler := clickhouse.Compiler{}

	tests := []struct {
		name string
		run  func() (bool, error)
	}{
		{name: "ordinary", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.ordinary)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "selector controls", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.controls)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "chart", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.chart)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "timechart", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.timechart)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "stats", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.stats)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "alias event overflow", run: func() (bool, error) {
			compiled, err := compiler.Compile(plans.overflow)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}},
		{name: "timeline", run: func() (bool, error) {
			compiled, err := compiler.CompileTimeline(plans.timeline, plans.timelineSpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledTimeline{}), err
		}},
		{name: "field catalog", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldCatalog(plans.analysis, plans.catalogSpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldCatalog{}), err
		}},
		{name: "field summary", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldSummary(plans.analysis, plans.summarySpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldSummary{}), err
		}},
		{name: "field suggestions", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldSuggestions(
				plans.analysis,
				plans.suggestionSpec,
			)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldSuggestions{}), err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nonzero, err := test.run()
			if err == nil || err.Error() != knowledgeRuntimeClosedCompilerSealError || nonzero {
				t.Fatalf(
					"default compiler closure = nonzero %t, error %v; want zero/%q",
					nonzero,
					err,
					knowledgeRuntimeClosedCompilerSealError,
				)
			}
		})
	}
}
