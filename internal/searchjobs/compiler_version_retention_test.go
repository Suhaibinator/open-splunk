package searchjobs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

func TestManagerRetainsImmutableCompilerVersionAcrossEveryProjection(t *testing.T) {
	t.Parallel()

	const version = "SPLExpressionV02-build.2026-08-11"
	admitted := make(chan Job, 1)
	finalized := make(chan Job, 1)
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(_ context.Context, _ clickhouse.CompiledQuery, sink ResultSink) error {
			return sink.SetSchema(messageSchema())
		}),
		Journal: jobJournalFunc{
			admit: func(_ context.Context, job Job) error {
				admitted <- job
				job.CompilerVersion = "journal-mutated-admission"
				return nil
			},
			finalize: func(_ context.Context, job Job) error {
				finalized <- job
				job.CompilerVersion = "journal-mutated-finalization"
				return nil
			},
		},
		CompilerVersion: version,
		CleanupInterval: -1,
		NewID:           sequenceIDs("compiler-version"),
	})

	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if created.CompilerVersion != version {
		t.Fatalf("created compiler version = %q, want %q", created.CompilerVersion, version)
	}
	created.CompilerVersion = "caller-mutated-created"
	if journaled := receiveCompilerVersionJob(t, admitted, "admission"); journaled.CompilerVersion != version {
		t.Fatalf("admission compiler version = %q, want %q", journaled.CompilerVersion, version)
	}

	completed := waitForState(t, manager, created.ID, StateCompleted)
	if completed.CompilerVersion != version {
		t.Fatalf("completed compiler version = %q, want %q", completed.CompilerVersion, version)
	}
	if journaled := receiveCompilerVersionJob(t, finalized, "finalization"); journaled.CompilerVersion != version {
		t.Fatalf("finalization compiler version = %q, want %q", journaled.CompilerVersion, version)
	}

	stored, err := manager.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.CompilerVersion != version {
		t.Fatalf("Get() compiler version = %q, want %q", stored.CompilerVersion, version)
	}
	stored.CompilerVersion = "caller-mutated-get"

	access := AccessScope{TenantID: stored.TenantID, OwnerID: stored.OwnerID}
	listed := manager.ListFor(access)
	if len(listed) != 1 || listed[0].CompilerVersion != version {
		t.Fatalf("ListFor() compiler version projection = %#v", listed)
	}
	listed[0].CompilerVersion = "caller-mutated-list"

	page, err := manager.ListPageFor(context.Background(), access, JobListRequest{})
	if err != nil {
		t.Fatalf("ListPageFor() error = %v", err)
	}
	if len(page.Jobs) != 1 || page.Jobs[0].CompilerVersion != version {
		t.Fatalf("ListPageFor() compiler version projection = %#v", page.Jobs)
	}
	page.Jobs[0].CompilerVersion = "caller-mutated-page"

	snapshot, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, created.ID)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor() error = %v", err)
	}
	if snapshot.CompilerVersion != version {
		t.Fatalf("execution snapshot compiler version = %q, want %q", snapshot.CompilerVersion, version)
	}
	if _, err := snapshot.ValidateRetainedKnowledgeAuthority(); err != nil {
		t.Fatalf("ValidateRetainedKnowledgeAuthority() error = %v", err)
	}
	tampered := snapshot
	tampered.CompilerVersion = "forged-compatible-version"
	if _, err := tampered.ValidateRetainedKnowledgeAuthority(); !errors.Is(err, ErrResultsUnavailable) {
		t.Fatalf("tampered compiler version validation error = %v, want ErrResultsUnavailable", err)
	}

	fresh, err := manager.Get(created.ID)
	if err != nil || fresh.CompilerVersion != version {
		t.Fatalf("fresh Get() = (%q, %v), want immutable %q", fresh.CompilerVersion, err, version)
	}
	freshSnapshot, err := manager.CompletedExecutionSnapshotFor(context.Background(), access, created.ID)
	if err != nil || freshSnapshot.CompilerVersion != version {
		t.Fatalf("fresh execution snapshot = (%q, %v), want immutable %q", freshSnapshot.CompilerVersion, err, version)
	}
}

func TestManagerCompilerVersionDefaultAndValidation(t *testing.T) {
	t.Parallel()

	executor := executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil })
	manager := newTestManager(t, Config{
		Executor:        executor,
		CleanupInterval: -1,
	})
	created, err := manager.Create(context.Background(), validRequest())
	if err != nil {
		t.Fatalf("Create(default compiler version) error = %v", err)
	}
	if defaultCompilerVersion != "0.2" {
		t.Fatalf("defaultCompilerVersion = %q, want 0.2", defaultCompilerVersion)
	}
	if created.CompilerVersion != defaultCompilerVersion {
		t.Fatalf("default compiler version = %q, want 0.2", created.CompilerVersion)
	}

	for _, test := range []struct {
		name    string
		version string
	}{
		{name: "leading whitespace", version: " v0.2"},
		{name: "trailing whitespace", version: "v0.2 "},
		{name: "control", version: "v0.2\nforged"},
		{name: "C1 control", version: "v0.2\u0080forged"},
		{name: "invalid UTF-8", version: string([]byte{0xff})},
		{name: "oversized", version: strings.Repeat("v", MaximumCompilerVersionBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			invalid, err := New(Config{
				Executor:        executor,
				Snapshotter:     snapshotterFunc(func(context.Context) (uint64, error) { return 0, nil }),
				CompilerVersion: test.version,
				CleanupInterval: -1,
				CursorKey:       testCursorKey,
			})
			if err == nil {
				_ = invalid.Close()
				t.Fatalf("New(CompilerVersion=%q) unexpectedly succeeded", test.version)
			}
		})
	}
}

func TestValidCompilerVersionCanonicalContract(t *testing.T) {
	t.Parallel()

	maximum := strings.Repeat("v", MaximumCompilerVersionBytes)
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "plain", value: "0.2", want: true},
		{name: "maximum bytes", value: maximum, want: true},
		{name: "empty"},
		{name: "leading Unicode whitespace", value: "\u00a00.2"},
		{name: "trailing Unicode whitespace", value: "0.2\u3000"},
		{name: "ASCII control", value: "0.2\nforged"},
		{name: "C1 control", value: "0.2\u0080forged"},
		{name: "invalid UTF-8", value: string([]byte{0xff})},
		{name: "oversized", value: maximum + "v"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := ValidCompilerVersion(test.value); got != test.want {
				t.Fatalf("ValidCompilerVersion(%q) = %t, want %t", test.value, got, test.want)
			}
		})
	}
}

func receiveCompilerVersionJob(t *testing.T, jobs <-chan Job, operation string) Job {
	t.Helper()
	select {
	case job := <-jobs:
		return job
	case <-time.After(3 * time.Second):
		t.Fatalf("journal %s callback did not arrive", operation)
		return Job{}
	}
}
