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
	program := knowledgeRuntimeProgram(t)
	plans := buildKnowledgeRuntimeMatrixPlans(
		t,
		program,
		tenantID,
		indexName,
		selectorIndexName,
		base,
		indexTime,
		earliest,
		latest,
	)
	compiler := clickhouse.Compiler{}

	type compilerSealTest struct {
		name string
		run  func() (bool, error)
	}
	tests := make([]compilerSealTest, 0, 13)
	descriptors := knowledgeRuntimePublicCompilerCases(t, plans, program, tenantID, indexName)
	knowledgeRuntimeRequirePublicCompilerCaseNames(t, descriptors)
	for _, descriptor := range descriptors {
		descriptor := descriptor
		tests = append(tests, compilerSealTest{name: descriptor.name, run: func() (bool, error) {
			compiled, err := compiler.Compile(descriptor.logical)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledQuery{}), err
		}})
	}
	tests = append(tests,
		compilerSealTest{name: "timeline", run: func() (bool, error) {
			compiled, err := compiler.CompileTimeline(plans.timeline, plans.timelineSpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledTimeline{}), err
		}},
		compilerSealTest{name: "field catalog", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldCatalog(plans.analysis, plans.catalogSpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldCatalog{}), err
		}},
		compilerSealTest{name: "field summary", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldSummary(plans.analysis, plans.summarySpec)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldSummary{}), err
		}},
		compilerSealTest{name: "field suggestions", run: func() (bool, error) {
			compiled, err := compiler.CompileFieldSuggestions(
				plans.analysis,
				plans.suggestionSpec,
			)
			return !reflect.DeepEqual(compiled, clickhouse.CompiledFieldSuggestions{}), err
		}},
	)
	if len(tests) != 13 {
		t.Fatalf("default compiler matrix cases = %d, want 13", len(tests))
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
