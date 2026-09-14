package queryexec

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func TestTimechartContinuationPreflightsLookupAndRelationTogether(t *testing.T) {
	lookupOnly := compileLookupExternalTableFixture(t)
	lookupPrefix, combined := compileTimechartExternalTableFixture(t, true)
	_, relationOnly := compileTimechartExternalTableFixture(t, false)
	if !combined.HasValidExecutionSeal() || !combined.IsContinuationOf(lookupPrefix) ||
		!combined.HasLookupAuthority() {
		t.Fatal("combined lookup and relation fixture lacks sealed continuation authority")
	}

	lookupMinimum := minimumExternalTableBudget(t, lookupOnly)
	relationMinimum := minimumExternalTableBudget(t, relationOnly)
	combinedMinimum := minimumExternalTableBudget(t, combined)
	individualMaximum := max(lookupMinimum, relationMinimum)
	if combinedMinimum <= individualMaximum+1 {
		t.Fatalf(
			"combined external-table minimum=%d, lookup=%d relation=%d",
			combinedMinimum,
			lookupMinimum,
			relationMinimum,
		)
	}
	remaining := individualMaximum + (combinedMinimum-individualMaximum)/2
	if remaining <= individualMaximum || remaining >= combinedMinimum {
		t.Fatalf("remaining budget %d is not between %d and %d", remaining, individualMaximum, combinedMinimum)
	}

	connection := &fakeQueryConnection{}
	sink := &fakeSink{}
	ctx := searchlimits.WithRemainingExecutionBytes(context.Background(), remaining)
	err := mustExecutor(t, connection).Execute(ctx, combined, sink)
	if !errors.Is(err, searchjobs.ErrExecutionLimit) {
		t.Fatalf("combined external-table preflight error = %v", err)
	}
	if connection.query != "" || len(connection.args) != 0 {
		t.Fatalf("combined external-table preflight issued query %q with %#v", connection.query, connection.args)
	}
	if sink.setCalls != 0 || len(sink.schema.Columns) != 0 || len(sink.rows) != 0 {
		t.Fatalf(
			"combined external-table preflight published output: schema calls=%d schema=%#v rows=%d",
			sink.setCalls,
			sink.schema,
			len(sink.rows),
		)
	}
}

func compileLookupExternalTableFixture(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()
	logical := buildReadAdmissionPlan(
		t,
		`index=target | eval service="api" | lookup service_catalog service_id AS service OUTPUT owner | table owner`,
	)
	resolution := lookupExternalTableFixtureResolution(t, logical)
	compiled, err := (clickhouse.Compiler{}).CompileWithLookupResolutions(
		logical,
		[]clickhouse.LookupResolution{resolution},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !compiled.HasValidExecutionSeal() || !compiled.HasLookupAuthority() {
		t.Fatal("lookup-only fixture lacks sealed lookup authority")
	}
	return compiled
}

func compileTimechartExternalTableFixture(
	t *testing.T,
	withLookup bool,
) (clickhouse.CompiledQuery, clickhouse.CompiledQuery) {
	t.Helper()
	source := `index=target | timechart span=1h count BY host | table _time east`
	if withLookup {
		source = `index=target | timechart span=1h count BY host | eval service="api" | lookup service_catalog service_id AS service OUTPUT owner | table owner`
	}
	logical := buildReadAdmissionPlan(t, source)
	compiler := clickhouse.Compiler{}
	if withLookup {
		var err error
		compiler, err = compiler.WithDeferredLookupResolutionsContext(
			context.Background(),
			[]clickhouse.LookupResolution{lookupExternalTableFixtureResolution(t, logical)},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	prefix, err := compiler.Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	continued, err := prefix.ContinueContext(
		context.Background(),
		[]clickhouse.RelationColumn{
			{Name: "_time", Type: "DateTime64(9, 'UTC')"},
			{Name: "east", Type: "UInt64"},
		},
		[][]any{{time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC), uint64(1)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !continued.HasValidExecutionSeal() || !continued.IsContinuationOf(prefix) {
		t.Fatal("relation fixture lacks sealed continuation authority")
	}
	return prefix, continued
}

func lookupExternalTableFixtureResolution(
	t *testing.T,
	logical *plan.Query,
) clickhouse.LookupResolution {
	t.Helper()
	var contract plan.Lookup
	found := false
	for index, operator := range logical.Operators {
		if lookup, ok := operator.(*plan.Lookup); ok {
			contract, found = *lookup, true
			break
		}
		continuation, ok := logical.TimechartContinuationAt(index)
		if !ok {
			continue
		}
		contracts, err := continuation.LookupContracts()
		if err != nil {
			t.Fatal(err)
		}
		if len(contracts) != 0 {
			contract, found = contracts[0], true
			break
		}
	}
	if !found {
		t.Fatal("lookup fixture lacks a logical contract")
	}
	const content = "service_id,owner\napi,platform\n"
	resolution, err := clickhouse.NewLookupResolution(
		"tenant-read",
		"service_catalog",
		"asset-7",
		7,
		uint64(len(content)),
		sha256.Sum256([]byte(content)),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	resolution, err = resolution.WithLogicalContract(contract, "lookup-service-catalog", 1)
	if err != nil {
		t.Fatal(err)
	}
	return resolution
}

func minimumExternalTableBudget(t *testing.T, compiled clickhouse.CompiledQuery) uint64 {
	t.Helper()
	maximum := uint64(1)
	for {
		ctx := searchlimits.WithRemainingExecutionBytes(context.Background(), maximum)
		tables, err := compiled.ExternalTablesForExecution(ctx)
		if err == nil {
			if len(tables) == 0 {
				t.Fatal("sealed external-table fixture materialized no tables")
			}
			break
		}
		if !errors.Is(err, clickhouse.ErrTimechartResourceLimit) {
			t.Fatalf("external-table budget probe at %d bytes: %v", maximum, err)
		}
		if maximum > 1<<62 {
			t.Fatal("external-table budget probe overflowed")
		}
		maximum *= 2
	}
	minimum := uint64(0)
	for minimum+1 < maximum {
		middle := minimum + (maximum-minimum)/2
		ctx := searchlimits.WithRemainingExecutionBytes(context.Background(), middle)
		_, err := compiled.ExternalTablesForExecution(ctx)
		if err == nil {
			maximum = middle
			continue
		}
		if !errors.Is(err, clickhouse.ErrTimechartResourceLimit) {
			t.Fatalf("external-table budget probe at %d bytes: %v", middle, err)
		}
		minimum = middle
	}
	return maximum
}
