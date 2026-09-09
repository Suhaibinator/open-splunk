package clickhouse

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestEventResultLimitRetainsEmptyKnowledgeAdmissionAndExcludesRuntimeKnowledge(t *testing.T) {
	t.Parallel()
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("prepare empty knowledge: %v", err)
	}
	for _, test := range []struct {
		name     string
		program  knowledgeprogram.Program
		eligible bool
	}{
		{name: "empty admitted knowledge", program: empty, eligible: true},
		{name: "runtime knowledge validation", program: deferredMixedKnowledgeProgramForTest(t)},
	} {
		t.Run(test.name, func(t *testing.T) {
			logical, injectErr := plan.InjectKnowledgePrelude(buildPlan(t, `index=gradethis | table event_id`), test.program)
			if injectErr != nil {
				t.Fatalf("inject knowledge: %v", injectErr)
			}
			canonical, compileErr := (Compiler{}).Compile(logical)
			if compileErr != nil {
				t.Fatalf("compile admitted source: %v", compileErr)
			}
			if _, eligible, limitErr := CompileEventResultLimitContext(context.Background(), canonical, 11); limitErr != nil || eligible != test.eligible {
				t.Fatalf("knowledge limit = eligible %t, want %t; err %v", eligible, test.eligible, limitErr)
			}
		})
	}
}

func TestEventResultLimitPreservesCanonicalAuthorityAndOrdering(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis`,
		`index=gradethis level=error | table _time event_id`,
		`index=gradethis | regex _raw="error" | fields - message`,
		`index=gradethis | sort 0 status | fields - status | tail 50`,
		`index=gradethis | head 20 | search level=error`,
		`index=gradethis | fields host*`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			canonical := compileSPL(t, source)
			digest, valid := canonical.ExecutionAuthorityDigest()
			if !valid {
				t.Fatal("canonical query is unsigned")
			}
			sql := canonical.SQL
			args := append([]any(nil), canonical.Args...)
			for _, maximumRows := range []uint64{1, 2, 10_001, 100_001, math.MaxUint64} {
				limited, eligible, err := CompileEventResultLimitContext(context.Background(), canonical, maximumRows)
				if err != nil || !eligible {
					t.Fatalf("compile limit %d = eligible %t, err %v", maximumRows, eligible, err)
				}
				if limited.SQL != sql+" LIMIT "+strconv.FormatUint(maximumRows, 10) ||
					!reflect.DeepEqual(limited.Args, args) || !limited.HasValidExecutionSeal() {
					t.Fatalf("limit moved a pipeline/order boundary or lost authority: %#v", limited)
				}
				cloned, ok, cloneErr := limited.CloneForExecutionContext(context.Background())
				if cloneErr != nil || !ok || !cloned.HasValidExecutionSeal() {
					t.Fatalf("clone limited = valid %t, err %v", ok, cloneErr)
				}
				cloned.Args[0] = "other-tenant"
				if !limited.HasValidExecutionSeal() {
					t.Fatal("derived clones share mutable arguments")
				}
			}
			after, valid := canonical.ExecutionAuthorityDigest()
			if !valid || after != digest || canonical.SQL != sql || !reflect.DeepEqual(canonical.Args, args) {
				t.Fatal("search limit changed retained/export authority")
			}
			cloned, ok := canonical.CloneForExecution()
			if !ok {
				t.Fatal("clone canonical query failed")
			}
			if _, eligible, err := CompileEventResultLimitContext(context.Background(), cloned, 11); err != nil || !eligible {
				t.Fatalf("canonical clone lost limit proof: %t, %v", eligible, err)
			}
		})
	}
}

func TestEventResultLimitExcludesCompleteValidationAndTransformingPipelines(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | where severity+1 > 2`,
		`index=gradethis | where status IN (200,500)`,
		`index=gradethis | eval lowered=lower(host)`,
		`index=gradethis | rex field=_raw "(?<value>[A-Z]+)"`,
		`index=gradethis | spath path=foo output=value`,
		`index=gradethis | stats count`,
		`index=gradethis | eventstats count AS total`,
		`index=gradethis | streamstats count AS total`,
		`index=gradethis | timechart span=1h count`,
		`index=gradethis | chart count OVER host`,
		`index=gradethis | dedup host`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			canonical := compileSPL(t, source)
			if limited, eligible, err := CompileEventResultLimitContext(context.Background(), canonical, 11); err != nil || eligible || limited.SQL != "" {
				t.Fatalf("unsafe specialization: eligible %t, err %v, SQL %s", eligible, err, limited.SQL)
			}
		})
	}
}

func TestEventResultLimitRejectsTamperedSourceAndFinalizerProof(t *testing.T) {
	t.Parallel()
	canonical := compileSPL(t, `index=gradethis | table event_id`)
	other := compileSPL(t, `index=gradethis level=error | table event_id`)
	for name, mutate := range map[string]func(*CompiledQuery){
		"SQL":         func(query *CompiledQuery) { query.SQL += " LIMIT 1" },
		"arguments":   func(query *CompiledQuery) { query.Args[0] = "other-tenant" },
		"output":      func(query *CompiledQuery) { query.OutputFields[0] = "other" },
		"proof":       func(query *CompiledQuery) { query.eventResultLimit.seal[0] ^= 1 },
		"eligibility": func(query *CompiledQuery) { query.eventResultLimit.ordinaryEventRows = false },
		"transplant":  func(query *CompiledQuery) { query.eventResultLimit = other.eventResultLimit },
	} {
		t.Run(name, func(t *testing.T) {
			candidate, ok := canonical.CloneForExecution()
			if !ok {
				t.Fatal("clone source failed")
			}
			mutate(&candidate)
			if _, eligible, err := CompileEventResultLimitContext(context.Background(), candidate, 11); err == nil || eligible {
				t.Fatalf("accepted tampered source: eligible %t, err %v", eligible, err)
			}
		})
	}
	// Absence can only opt out of the optimization. It cannot produce a
	// bounded executable or change the canonical source's result semantics.
	absent := canonical
	absent.eventResultLimit = eventResultLimitProof{}
	if _, eligible, err := CompileEventResultLimitContext(context.Background(), absent, 11); err != nil || eligible {
		t.Fatalf("absent proof = eligible %t, err %v", eligible, err)
	}
}

func TestEventResultLimitSealsDerivedSurfaceAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	canonical := compileSPL(t, `index=gradethis | table event_id`)
	limited, eligible, err := CompileEventResultLimitContext(context.Background(), canonical, 11)
	if err != nil || !eligible {
		t.Fatalf("compile limit: %t, %v", eligible, err)
	}
	for name, mutate := range map[string]func(*CompiledEventResultLimit){
		"SQL":       func(query *CompiledEventResultLimit) { query.SQL += " LIMIT 1" },
		"arguments": func(query *CompiledEventResultLimit) { query.Args[0] = "other-tenant" },
		"ceiling":   func(query *CompiledEventResultLimit) { query.MaximumOutputRows++ },
		"source":    func(query *CompiledEventResultLimit) { query.executionAuthority.sourceDigest[0] ^= 1 },
		"scope":     func(query *CompiledEventResultLimit) { query.readScope.tenantID = "other-tenant" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate, ok, cloneErr := limited.CloneForExecutionContext(context.Background())
			if cloneErr != nil || !ok {
				t.Fatalf("clone: %t, %v", ok, cloneErr)
			}
			mutate(&candidate)
			if candidate.HasValidExecutionSeal() {
				t.Fatal("accepted mutated derived execution")
			}
			if _, ok, err := candidate.CloneForExecutionContext(context.Background()); err != nil || ok {
				t.Fatalf("cloned mutated derived execution: %t, %v", ok, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := CompileEventResultLimitContext(ctx, canonical, 11); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled derivation = %v", err)
	}
	if _, _, err := limited.CloneForExecutionContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled clone = %v", err)
	}
	if _, _, err := CompileEventResultLimitContext(context.Background(), canonical, 0); err == nil || !strings.Contains(err.Error(), "positive") {
		t.Fatalf("zero ceiling = %v", err)
	}
	var nilContext context.Context
	if _, _, err := CompileEventResultLimitContext(nilContext, canonical, 11); err == nil {
		t.Fatal("nil context succeeded")
	}
}
