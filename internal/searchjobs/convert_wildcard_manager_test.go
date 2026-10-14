package searchjobs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestConvertWildcardManagerRetainsOrderedEvidenceAndSharedBudget(t *testing.T) {
	for _, knowledge := range []bool{false, true} {
		name := "ordinary"
		if knowledge {
			name = "knowledge"
		}
		t.Run(name, func(t *testing.T) {
			const maximum = 500 * time.Millisecond
			executor := &statsWildcardTestExecutor{inventoryDelay: 20 * time.Millisecond}
			config := Config{Executor: executor, CleanupInterval: -1, MaxRuntime: maximum, NewID: sequenceIDs("convert-" + name)}
			request := validRequest()
			request.SPL = `index=main | convert num(by*) | convert num(del*) | stats sum(*)`
			if knowledge {
				resolver, appID := newEmptyKnowledgeResolver(t)
				config.KnowledgeResolver = resolver
				request.AppID = appID
			}
			manager := newTestManager(t, config)
			created, err := manager.Create(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			waitForState(t, manager, created.ID, StateCompleted)
			inventory, executed := executor.calls()
			if inventory != 3 || executed != 1 {
				t.Fatalf("calls=%d/%d", inventory, executed)
			}
			if budget := executor.executionTimeBudget(); budget > maximum-50*time.Millisecond {
				t.Fatalf("inventory time not debited across all stages: %s", budget)
			}
			snapshot, err := manager.CompletedExecutionSnapshotFor(context.Background(), AccessScope{TenantID: request.TenantID, OwnerID: request.OwnerID}, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			evidence, present, err := snapshot.OpenRetainedStatsWildcardExpansion()
			if err != nil || !present {
				t.Fatalf("evidence=%t/%v", present, err)
			}
			parsed, err := spl.Parse(request.SPL)
			if err != nil {
				t.Fatal(err)
			}
			scope := plan.Scope{TenantID: snapshot.TenantID, AuthorizedIndexes: snapshot.EffectiveIndexes, RequestedIndexes: snapshot.EffectiveIndexes, SearchJobID: snapshot.ID, Earliest: snapshot.Earliest, Latest: snapshot.Latest, SearchStart: snapshot.SearchStart, SearchTimezone: snapshot.SearchTimezone, IndexTimeCutoff: snapshot.IndexTimeCutoff, VisibilityCutoff: &snapshot.VisibilityCutoff}
			if _, err := plan.BuildWithStatsWildcardExpansion(parsed, scope, evidence); err != nil {
				t.Fatalf("retained replay: %v", err)
			}
			stripped := snapshot
			stripped.StatsWildcardExpansion = plan.StatsWildcardExpansion{}
			if stripped.ValidKnowledgeAuthority() {
				t.Fatal("stripped ordered evidence retained signature")
			}
		})
	}
}

func TestConvertWildcardValidationNeverExecutesDiscovery(t *testing.T) {
	executor := &statsWildcardTestExecutor{}
	manager := newTestManager(t, Config{Executor: executor, CleanupInterval: -1})
	start := time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC)
	for _, source := range []string{`index=main | convert num(*) none(src_ip)`, `index=main | convert num(by*) | convert num(del*) | stats sum(*)`} {
		result, err := manager.Validate(context.Background(), ValidateRequest{SPL: source, TenantID: "tenant", AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, TimeRange: mustAbsoluteTimeRange(start, start.Add(time.Hour))})
		if err != nil || !result.Valid {
			t.Fatalf("Validate=%#v/%v", result, err)
		}
	}
	if inventory, execute := executor.calls(); inventory != 0 || execute != 0 {
		t.Fatalf("validation executed storage %d/%d", inventory, execute)
	}
}

type substitutedConvertInventoryExecutor struct {
	statsWildcardTestExecutor
	replacement plan.StatsWildcardExpansion
}

func (executor *substitutedConvertInventoryExecutor) ExecuteStatsWildcardInventory(context.Context, clickhouse.CompiledStatsWildcardInventory) (plan.StatsWildcardExpansion, error) {
	return executor.replacement.Clone(), nil
}

func TestConvertWildcardExecutorCannotSubstituteFutureStageEvidence(t *testing.T) {
	source := `index=main | convert num(by*) | convert num(del*)`
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatal(err)
	}
	request := validRequest()
	scope := plan.Scope{SearchTimezone: "UTC", TenantID: request.TenantID, AuthorizedIndexes: request.AuthorizedIndexes, RequestedIndexes: request.RequestedIndexes, Earliest: request.TimeRange.Earliest(), Latest: request.TimeRange.Latest(), SearchStart: time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC), IndexTimeCutoff: time.Date(2026, time.October, 14, 0, 0, 0, 0, time.UTC), VisibilityCutoff: func() *uint64 { value := uint64(1); return &value }()}
	preparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	first, err := plan.ValidateStatsWildcardInventory(preparation.Request(), []plan.StatsWildcardInventoryMatch{{Ordinal: 0, Field: "bytes"}})
	if err != nil {
		t.Fatal(err)
	}
	preparation, err = plan.PrepareNextStatsWildcard(parsed, scope, first)
	if err != nil {
		t.Fatal(err)
	}
	future, err := plan.ValidateStatsWildcardInventory(preparation.Request(), []plan.StatsWildcardInventoryMatch{{Ordinal: 0, Field: "delay"}})
	if err != nil {
		t.Fatal(err)
	}
	executor := &substitutedConvertInventoryExecutor{replacement: future}
	manager := newTestManager(t, Config{Executor: executor, CleanupInterval: -1})
	firstPreparation, err := plan.PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	returned, _, _, err := manager.executeStatsWildcardInventory(context.Background(), clickhouse.Compiler{}, firstPreparation.Prefix(), firstPreparation.Request(), time.Second)
	if !errors.Is(err, ErrInvalidResult) || !returned.IsZero() {
		t.Fatalf("future-stage evidence accepted: zero=%t err=%v", returned.IsZero(), err)
	}
}
