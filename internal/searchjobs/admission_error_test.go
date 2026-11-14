package searchjobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestAdmissionDiagnosticsDistinguishCapacityBudgets(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"job_capacity", "metadata_capacity"} {
		t.Run(stage, func(t *testing.T) {
			config := Config{
				Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
				Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 1, nil }),
				CleanupInterval: -1,
			}
			if stage == "job_capacity" {
				config.MaxJobs = 1
			} else {
				config.MaxMetadataBytes = 1
			}
			manager := newTestManager(t, config)
			if stage == "job_capacity" {
				if _, err := manager.Create(t.Context(), validRequest()); err != nil {
					t.Fatal(err)
				}
			}
			_, err := manager.Create(t.Context(), validRequest())
			diagnostic, ok := errors.AsType[*AdmissionError](err)
			if !errors.Is(err, ErrCapacity) || !ok || diagnostic.Stage != stage {
				t.Fatalf("expected %s capacity diagnostic, got %v", stage, err)
			}
		})
	}
}

func TestKnowledgeAdmissionDiagnosticsIdentifyFailedOperation(t *testing.T) {
	for _, step := range []string{"catalog_resolve", "resolution_validate", "lookup_resolve", "query_compile"} {
		t.Run(step, func(t *testing.T) {
			resolver, appID := newEmptyKnowledgeResolver(t)
			privateCause := fmt.Errorf("private-path private-query: %w", knowledgecatalog.ErrCorrupt)
			config := Config{
				Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
					t.Error("rejected admission executed")
					return nil
				}),
				KnowledgeResolver: resolver,
				CleanupInterval:   -1,
				NewID:             func() string { t.Error("rejected admission allocated an ID"); return "unexpected" },
			}
			request := validRequest()
			request.AppID = appID
			switch step {
			case "catalog_resolve":
				config.KnowledgeResolver = knowledgeResolverFunc(func(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
					return knowledgecatalog.Resolution{}, privateCause
				})
			case "resolution_validate":
				config.KnowledgeResolver = knowledgeResolverFunc(func(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
					return knowledgecatalog.Resolution{}, nil
				})
			case "lookup_resolve":
				config.LookupResolver = lookupResolverFunc(func(context.Context, LookupAdmissionResolutionScope) ([]clickhouse.LookupResolution, error) {
					return nil, privateCause
				})
			case "query_compile":
				request.SPL = `index=main | eval value=if(status=200, "private-value", 1) | table value`
			}
			manager := newTestManager(t, config)
			_, err := manager.Create(t.Context(), request)
			diagnostic, ok := errors.AsType[*AdmissionError](err)
			if !errors.Is(err, ErrKnowledgeUnavailable) || !ok {
				t.Fatalf("expected private knowledge diagnostic, got %v", err)
			}
			if diagnostic.Stage != "knowledge" || diagnostic.Step != step || diagnostic.Cause == nil {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
			if diagnostic.Elapsed <= 0 {
				t.Fatal("failed preparation omitted whole-stage elapsed time")
			}
			if step == "query_compile" {
				compilerDiagnostic, found := errors.AsType[*plan.Diagnostic](diagnostic.Cause)
				if !found || compilerDiagnostic.Code != "SPL_UNSUPPORTED_IF_BRANCH_TYPE" {
					t.Fatalf("compiler cause = %v", diagnostic.Cause)
				}
			}
			if errors.Is(err, privateCause) || strings.Contains(err.Error(), "private-") {
				t.Fatal("private diagnostic escaped public error")
			}
			assertEmptyManagerAdmissionState(t, manager)
		})
	}
}

func TestKnowledgeAdmissionDiagnosticPreservesDeadlineAndCapacitySemantics(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor:        executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		CleanupInterval: -1,
	})
	for _, test := range []struct{ cause, want error }{
		{context.DeadlineExceeded, ErrKnowledgeUnavailable},
		{control.ErrCapacityExceeded, ErrCapacity},
		{knowledgesnapshot.ErrResourceLimit, ErrCapacity},
		{nil, ErrKnowledgeUnavailable},
	} {
		err := manager.safeKnowledgeAdmissionError(t.Context(), "snapshot_finalize", test.cause)
		diagnostic, ok := errors.AsType[*AdmissionError](err)
		if !ok || diagnostic.Step != "snapshot_finalize" || !errors.Is(err, test.want) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cause %v: diagnostic = %#v", test.cause, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := manager.safeKnowledgeAdmissionError(ctx, "catalog_resolve", context.DeadlineExceeded)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation = %v", err)
	}
}
