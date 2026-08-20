package searchsnapshot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type compilerVersionSnapshotExecutor struct{}

func (compilerVersionSnapshotExecutor) Execute(
	_ context.Context,
	query clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	columns := make([]searchjobs.Column, len(query.OutputFields))
	for index, field := range query.OutputFields {
		columns[index] = searchjobs.Column{
			Name: field,
			Kind: searchjobs.ValueKindString,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

type compilerVersionSnapshotter struct{}

func (compilerVersionSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return 73, nil
}

func TestExecutionPlanRebuildRequiresCurrentCompilerVersion(t *testing.T) {
	current := mintCompilerVersionSnapshot(t)
	logical, err := BuildExecutionPlan(current)
	if err != nil || logical == nil {
		t.Fatalf("BuildExecutionPlan(current) = (%#v, %v), want success", logical, err)
	}

	incompatible := current
	incompatible.CompilerVersion = "0.1"
	builders := []struct {
		name  string
		build func(searchjobs.ExecutionSnapshot) (*plan.Query, error)
	}{
		{name: "ordinary", build: BuildExecutionPlan},
		{
			name: "candidate knowledge prelude",
			build: func(snapshot searchjobs.ExecutionSnapshot) (*plan.Query, error) {
				return BuildExecutionPlanWithKnowledgePrelude(
					snapshot,
					knowledgeprogram.Program{},
				)
			},
		},
	}
	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			logical, err := builder.build(incompatible)
			if logical != nil || !errors.Is(err, ErrCompilerVersionMismatch) {
				t.Fatalf(
					"rebuild incompatible snapshot = (%#v, %v), want ErrCompilerVersionMismatch",
					logical,
					err,
				)
			}
		})
	}
}

func mintCompilerVersionSnapshot(
	t *testing.T,
) searchjobs.ExecutionSnapshot {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        compilerVersionSnapshotExecutor{},
		Snapshotter:     compilerVersionSnapshotter{},
		CleanupInterval: -1,
		RetentionTTL:    time.Hour,
		Now:             func() time.Time { return now },
		NewID:           func() string { return "compiler-version-snapshot" },
		CursorKey:       []byte("compiler-version-snapshot-cursor-key-0001"),
	})
	if err != nil {
		t.Fatalf("searchjobs.New(): %v", err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	timeRange, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatalf("searchtime.NewAbsoluteRange(): %v", err)
	}
	created, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL:               `index=main | table _raw`,
		OwnerID:           "compiler-version-owner",
		TenantID:          "compiler-version-tenant",
		AuthorizedIndexes: []string{"main"},
		RequestedIndexes:  []string{"main"},
		TimeRange:         timeRange,
	})
	if err != nil {
		t.Fatalf("Manager.Create(): %v", err)
	}
	access := searchjobs.AccessScope{
		TenantID: "compiler-version-tenant",
		OwnerID:  "compiler-version-owner",
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		job, getErr := manager.GetFor(access, created.ID)
		if getErr != nil {
			t.Fatalf("Manager.GetFor(): %v", getErr)
		}
		if job.State == searchjobs.StateCompleted {
			break
		}
		if job.State.Terminal() {
			t.Fatalf("compiler-version fixture reached %s: %#v", job.State, job.Failure)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for compiler-version fixture")
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, err := manager.CompletedExecutionSnapshotFor(ctx, access, created.ID)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	if snapshot.CompilerVersion != spl.CompatibilityVersion || !snapshot.ValidKnowledgeAuthority() {
		t.Fatalf(
			"minted snapshot version/authority = %q/%t, want %q/true",
			snapshot.CompilerVersion,
			snapshot.ValidKnowledgeAuthority(),
			spl.CompatibilityVersion,
		)
	}
	return snapshot
}
