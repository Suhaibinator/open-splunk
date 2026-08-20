package searchjobs

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type lookupResolverFunc func(
	context.Context,
	LookupAdmissionResolutionScope,
) ([]clickhouse.LookupResolution, error)

func (resolver lookupResolverFunc) ResolveLookupAdmission(
	ctx context.Context,
	scope LookupAdmissionResolutionScope,
) (LookupAdmissionResolution, error) {
	explicit, err := resolver(ctx, scope)
	return LookupAdmissionResolution{Explicit: explicit}, err
}

var _ LookupResolver = lookupResolverFunc(nil)

func TestLookupAdmissionResolvesAndSealsBeforeExecution(t *testing.T) {
	knowledgeResolver, appID := newEmptyKnowledgeResolver(t)
	request := validRequest()
	request.AppID = appID
	request.SPL = "index=main | lookup service_catalog service_id AS service OUTPUT owner | table owner"
	resolution := testLookupResolution(t, request.TenantID, "service_catalog")

	var calls atomic.Int32
	var gotScope LookupAdmissionResolutionScope
	lookupResolver := lookupResolverFunc(func(
		ctx context.Context,
		scope LookupAdmissionResolutionScope,
	) ([]clickhouse.LookupResolution, error) {
		calls.Add(1)
		gotScope = scope
		return []clickhouse.LookupResolution{resolution}, ctx.Err()
	})
	executed := make(chan clickhouse.CompiledQuery, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, compiled clickhouse.CompiledQuery, _ ResultSink) error {
			executed <- compiled
			return errors.New("stop after observing compiled lookup authority")
		}),
		Snapshotter:       snapshotterFunc(func(context.Context) (uint64, error) { return 9, nil }),
		KnowledgeResolver: knowledgeResolver,
		LookupResolver:    lookupResolver,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("lookup-admission"),
	})
	if !manager.LookupAdmissionEnabled() {
		t.Fatal("lookup-capable manager did not report lookup admission")
	}

	created, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("Create(lookup): %v", err)
	}
	if calls.Load() != 1 || gotScope.TenantID != request.TenantID ||
		gotScope.PrincipalID != request.OwnerID || gotScope.AppID != appID ||
		len(gotScope.Names) != 1 || gotScope.Names[0] != "service_catalog" {
		t.Fatalf("lookup resolution = calls %d, scope %#v", calls.Load(), gotScope)
	}
	select {
	case compiled := <-executed:
		if !compiled.HasValidExecutionSeal() {
			t.Fatal("executed lookup query lacks a valid authority seal")
		}
		clone, ok := compiled.CloneForExecution()
		if !ok || !clone.EqualForExecution(compiled) {
			t.Fatal("lookup authority did not detach for execution")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lookup query did not reach execution")
	}
	_ = waitForState(t, manager, created.ID, StateFailed)
}

func TestConfiguredLookupResolverIsConsultedWithoutAuthoredLookup(t *testing.T) {
	knowledgeResolver, appID := newEmptyKnowledgeResolver(t)
	request := validRequest()
	request.AppID = appID
	request.SPL = "index=main | head 1"

	var calls atomic.Int32
	resolver := lookupResolverFunc(func(
		ctx context.Context,
		scope LookupAdmissionResolutionScope,
	) ([]clickhouse.LookupResolution, error) {
		calls.Add(1)
		if scope.TenantID != request.TenantID || scope.PrincipalID != request.OwnerID ||
			scope.AppID != appID || len(scope.Names) != 0 {
			t.Fatalf("automatic discovery scope = %#v", scope)
		}
		return nil, ctx.Err()
	})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		KnowledgeResolver: knowledgeResolver,
		LookupResolver:    resolver,
		MaxConcurrent:     1,
		CleanupInterval:   -1,
		NewID:             sequenceIDs("automatic-discovery"),
	})
	created, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("Create(non-lookup with automatic discovery): %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("lookup admission resolution calls = %d, want 1", calls.Load())
	}
	_ = waitForState(t, manager, created.ID, StateFailed)
}

func TestLookupAdmissionFailsClosedBeforeJobWhenResolverIsMissingOrDiverges(t *testing.T) {
	knowledgeResolver, appID := newEmptyKnowledgeResolver(t)
	request := validRequest()
	request.AppID = appID
	request.SPL = "index=main | lookup service_catalog service_id AS service OUTPUT owner"

	for _, test := range []struct {
		name     string
		resolver LookupResolver
	}{
		{name: "missing"},
		{
			name: "wrong tenant",
			resolver: lookupResolverFunc(func(context.Context, LookupAdmissionResolutionScope) ([]clickhouse.LookupResolution, error) {
				return []clickhouse.LookupResolution{testLookupResolution(t, "other-tenant", "service_catalog")}, nil
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(
					context.Context,
					clickhouse.CompiledQuery,
					ResultSink,
				) error {
					return nil
				}),
				Snapshotter:       snapshotterFunc(func(context.Context) (uint64, error) { return 4, nil }),
				KnowledgeResolver: knowledgeResolver,
				LookupResolver:    test.resolver,
				CleanupInterval:   -1,
			})
			if _, err := manager.Create(t.Context(), request); !errors.Is(err, ErrKnowledgeUnavailable) {
				t.Fatalf("Create() error = %v, want ErrKnowledgeUnavailable", err)
			}
			assertEmptyManagerAdmissionState(t, manager)
		})
	}
}

func TestLookupAdmissionMapsCombinedCatalogBudgetToCapacity(t *testing.T) {
	knowledgeResolver, appID := newEmptyKnowledgeResolver(t)
	request := validRequest()
	request.AppID = appID
	request.SPL = "index=main | lookup service_catalog service_id AS service OUTPUT owner"
	resolver := lookupResolverFunc(func(
		context.Context,
		LookupAdmissionResolutionScope,
	) ([]clickhouse.LookupResolution, error) {
		return nil, ErrCapacity
	})
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		KnowledgeResolver: knowledgeResolver,
		LookupResolver:    resolver,
		CleanupInterval:   -1,
	})
	if _, err := manager.Create(t.Context(), request); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(over combined lookup budget) error = %v, want ErrCapacity", err)
	}
	assertEmptyManagerAdmissionState(t, manager)
}

func TestLookupCreateRejectsMissingScopeOrAuthorityBeforeIDAndJournal(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name              string
		appID             string
		knowledgeResolver KnowledgeResolver
		lookupResolver    LookupResolver
	}{
		{
			name: "missing app scope",
			knowledgeResolver: knowledgeResolverFunc(func(
				context.Context,
				knowledgecatalog.ResolutionScope,
			) (knowledgecatalog.Resolution, error) {
				t.Fatal("app-less lookup consulted knowledge resolver")
				return knowledgecatalog.Resolution{}, nil
			}),
			lookupResolver: lookupResolverFunc(func(
				context.Context,
				LookupAdmissionResolutionScope,
			) ([]clickhouse.LookupResolution, error) {
				t.Fatal("app-less lookup consulted resolver")
				return nil, nil
			}),
		},
		{
			name:  "missing lookup resolver",
			appID: "app-main",
			knowledgeResolver: knowledgeResolverFunc(func(
				context.Context,
				knowledgecatalog.ResolutionScope,
			) (knowledgecatalog.Resolution, error) {
				t.Fatal("lookup without asset authority consulted knowledge resolver")
				return knowledgecatalog.Resolution{}, nil
			}),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var idCalls, journalCalls, snapshotCalls atomic.Int32
			manager := newTestManager(t, Config{
				Executor: executorFunc(func(
					context.Context,
					clickhouse.CompiledQuery,
					ResultSink,
				) error {
					return nil
				}),
				Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
					snapshotCalls.Add(1)
					return 1, nil
				}),
				Journal: jobJournalFunc{admit: func(context.Context, Job) error {
					journalCalls.Add(1)
					return nil
				}},
				KnowledgeResolver: test.knowledgeResolver,
				LookupResolver:    test.lookupResolver,
				NewID: func() string {
					idCalls.Add(1)
					return "lookup-missing-authority"
				},
				CleanupInterval: -1,
			})
			request := validRequest()
			request.AppID = test.appID
			request.SPL = "index=main | lookup service_catalog service_id AS service OUTPUT owner"
			if _, err := manager.Create(t.Context(), request); !errors.Is(err, ErrKnowledgeUnavailable) {
				t.Fatalf("Create() error = %v, want ErrKnowledgeUnavailable", err)
			}
			if idCalls.Load() != 0 || journalCalls.Load() != 0 || snapshotCalls.Load() != 0 {
				t.Fatalf(
					"pre-admission side effects = IDs %d, journal %d, snapshots %d",
					idCalls.Load(),
					journalCalls.Load(),
					snapshotCalls.Load(),
				)
			}
			assertEmptyManagerAdmissionState(t, manager)
		})
	}
}

func TestNewRejectsLookupResolverWithoutKnowledgeResolver(t *testing.T) {
	t.Parallel()

	resolver := lookupResolverFunc(func(
		context.Context,
		LookupAdmissionResolutionScope,
	) ([]clickhouse.LookupResolution, error) {
		t.Fatal("invalid resolver configuration was consulted")
		return nil, nil
	})
	manager, err := New(Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 0, nil
		}),
		LookupResolver:  resolver,
		CleanupInterval: -1,
		CursorKey:       testCursorKey,
	})
	if manager != nil || err == nil ||
		!strings.Contains(err.Error(), "lookup resolver requires a knowledge resolver") {
		t.Fatalf("New() = manager %v, error %v", manager, err)
	}
}

func TestMissingLookupAuthorityDoesNotChangeLegacyNonLookupCreate(t *testing.T) {
	t.Parallel()

	var idCalls, journalCalls atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(
			context.Context,
			clickhouse.CompiledQuery,
			ResultSink,
		) error {
			return nil
		}),
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) {
			return 1, nil
		}),
		Journal: jobJournalFunc{admit: func(context.Context, Job) error {
			journalCalls.Add(1)
			return nil
		}},
		NewID: func() string {
			idCalls.Add(1)
			return "legacy-non-lookup"
		},
		CleanupInterval: -1,
	})
	if manager.LookupAdmissionEnabled() {
		t.Fatal("manager without a lookup resolver reported lookup admission")
	}
	request := validRequest()
	request.SPL = "index=main | head 1"
	created, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("Create(non-lookup): %v", err)
	}
	if created.ID != "legacy-non-lookup" || idCalls.Load() != 1 || journalCalls.Load() != 1 {
		t.Fatalf(
			"legacy admission = job %#v, IDs %d, journal %d",
			created,
			idCalls.Load(),
			journalCalls.Load(),
		)
	}
}

func TestLookupValidationAnalyzesWithoutMintingExecutableAuthority(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			return errors.New("validation must not execute")
		}),
		CleanupInterval: -1,
	})
	result, err := manager.Validate(t.Context(), validValidationRequest(
		"index=main | lookup service_catalog service_id AS service OUTPUT owner",
	))
	if err != nil {
		t.Fatalf("Validate(lookup): %v", err)
	}
	if !result.Valid || len(result.Diagnostics) != 0 {
		t.Fatalf("lookup validation = %#v", result)
	}
}

func testLookupResolution(t *testing.T, tenantID, name string) clickhouse.LookupResolution {
	t.Helper()
	asset, err := lookupasset.ParseCSV(
		strings.NewReader("service_id,owner\napi,alice\n"),
		lookupasset.Limits{},
	)
	if err != nil {
		t.Fatalf("ParseCSV(): %v", err)
	}
	version := lookupasset.Version{
		Ref: lookupasset.VersionRef{
			TenantID:      tenantID,
			LookupAssetID: "asset-service-catalog",
			Version:       1,
			SizeBytes:     uint64(len(asset.CanonicalCSV())),
			ContentSHA256: asset.ContentSHA256(),
		},
		SourceSHA256: asset.SourceSHA256(),
		SourceBytes:  asset.SourceBytes(),
		CreatedAt:    time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC),
		Asset:        asset,
	}
	resolution, err := clickhouse.NewLookupResolutionFromVersion(name, version)
	if err != nil {
		t.Fatalf("NewLookupResolutionFromVersion(): %v", err)
	}
	key, err := plan.ResolveField("service", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service): %v", err)
	}
	output, err := plan.ResolveField("owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(owner): %v", err)
	}
	resolution, err = resolution.WithLogicalContract(plan.Lookup{
		DefinitionName: name,
		Keys: []plan.LookupKey{{
			LookupField: "service_id",
			EventField:  key,
		}},
		Outputs: []plan.LookupOutput{{
			LookupField: "owner",
			EventField:  output,
		}},
		WriteMode: plan.LookupWriteModeOverwrite,
	}, "lookup-"+name, 1)
	if err != nil {
		t.Fatalf("WithLogicalContract(): %v", err)
	}
	return resolution
}

var _ LookupResolver = lookupResolverFunc(nil)
