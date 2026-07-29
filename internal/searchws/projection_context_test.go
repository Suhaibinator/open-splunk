package searchws

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestProjectionUsesOneDeadlineAcrossPreviewFallback(t *testing.T) {
	job := scopedSearchJob("search-shared-projection-deadline")
	reader := &deadlineFallbackSearchSnapshots{job: job}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = reader
		config.MaximumPreviewRows = 1
		config.ProjectionTimeout = time.Second
	})

	_, err := service.loadProjection(
		context.Background(),
		targetKey{kind: targetKindSearch, id: job.ID},
		1,
	)
	if err != nil {
		t.Fatalf("loadProjection() error = %v", err)
	}
	reader.mu.Lock()
	previewDeadline, previewOK := reader.previewDeadline, reader.previewDeadlineOK
	getDeadline, getOK := reader.getDeadline, reader.getDeadlineOK
	reader.mu.Unlock()
	if !previewOK || !getOK {
		t.Fatalf("provider deadlines present = preview:%t get:%t", previewOK, getOK)
	}
	if !previewDeadline.Equal(getDeadline) {
		t.Fatalf("fallback deadlines differ: preview=%s get=%s", previewDeadline, getDeadline)
	}
}

func TestProjectionTimeoutCancelsPreviewProviderAndReleasesPermit(t *testing.T) {
	job := scopedSearchJob("search-preview-projection-timeout")
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}}
	release := make(chan struct{})
	reader := &blockingPreviewRefreshSnapshots{
		job: job,
		snapshot: searchjobs.PreviewSnapshot{
			Job:      job,
			Revision: job.Version,
		},
		entered: make(chan struct{}, 1),
		release: release,
	}
	t.Cleanup(func() { close(release) })
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = reader
		config.MaximumPreviewRows = 1
		config.ProjectionTimeout = minimumProjectionTimeout
	})

	_, err := service.loadProjection(
		context.Background(),
		targetKey{kind: targetKindSearch, id: job.ID},
		1,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("loadProjection() error = %v, want context deadline exceeded", err)
	}
	if got := reader.active.Load(); got != 0 {
		t.Fatalf("active preview providers = %d, want 0", got)
	}
	if got := len(service.projectionGate); got != 0 {
		t.Fatalf("projection gate occupancy = %d, want 0", got)
	}
}

func TestProjectionTimeoutCancelsExportProviderAndReleasesPermit(t *testing.T) {
	reader := &blockingExportSnapshots{
		started: make(chan struct{}),
		exited:  make(chan struct{}),
	}
	service := adversarialNewService(t, func(config *Config) {
		config.Exports = reader
		config.ProjectionTimeout = minimumProjectionTimeout
	})

	_, err := service.loadProjection(
		context.Background(),
		targetKey{kind: targetKindExport, id: "export-projection-timeout"},
		0,
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("loadProjection() error = %v, want context deadline exceeded", err)
	}
	select {
	case <-reader.exited:
	default:
		t.Fatal("timed-out export projection returned before its provider exited")
	}
	if got := reader.active.Load(); got != 0 {
		t.Fatalf("active export providers = %d, want 0", got)
	}
	if got := len(service.projectionGate); got != 0 {
		t.Fatalf("projection gate occupancy = %d, want 0", got)
	}
}

func TestProjectionPreservesCancellationDuringPreviewMaterialization(t *testing.T) {
	job := scopedSearchJob("search-preview-materialization-cancellation")
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{
		Name: "message",
		Kind: searchjobs.ValueKindString,
	}}}
	reader := materializationSearchSnapshots{
		snapshot: searchjobs.PreviewSnapshot{
			Job:      job,
			Rows:     []searchjobs.ResultRow{{Values: []searchjobs.Value{searchjobs.StringValue("value")}}},
			Revision: job.Version,
		},
	}
	service := adversarialNewService(t, func(config *Config) {
		config.Searches = reader
		config.MaximumPreviewRows = 1
	})
	ctx := newCancelOnSecondErrContext()

	_, err := service.loadProjectionWithPermit(
		ctx,
		targetKey{kind: targetKindSearch, id: job.ID},
		1,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("loadProjectionWithPermit() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, errTargetNotFound) {
		t.Fatalf("loadProjectionWithPermit() error = %v, unexpectedly mapped to target not found", err)
	}
}

type deadlineFallbackSearchSnapshots struct {
	mu sync.Mutex

	job               searchjobs.Job
	previewDeadline   time.Time
	previewDeadlineOK bool
	getDeadline       time.Time
	getDeadlineOK     bool
}

func (reader *deadlineFallbackSearchSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	deadline, ok := ctx.Deadline()
	reader.mu.Lock()
	reader.getDeadline = deadline
	reader.getDeadlineOK = ok
	reader.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return searchjobs.Job{}, err
	}
	if id != reader.job.ID || scope.TenantID != reader.job.TenantID || scope.OwnerID != reader.job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.job), nil
}

func (reader *deadlineFallbackSearchSnapshots) PreviewForBytesContext(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
	_ int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	deadline, ok := ctx.Deadline()
	reader.mu.Lock()
	reader.previewDeadline = deadline
	reader.previewDeadlineOK = ok
	reader.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (*deadlineFallbackSearchSnapshots) MaximumPreviewRows() uint32 { return 1 }

type blockingExportSnapshots struct {
	started chan struct{}
	exited  chan struct{}
	once    sync.Once
	active  atomic.Int32
}

func (reader *blockingExportSnapshots) Snapshot(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
) (exportjobs.Job, error) {
	reader.active.Add(1)
	defer reader.active.Add(-1)
	reader.once.Do(func() { close(reader.started) })
	<-ctx.Done()
	close(reader.exited)
	return exportjobs.Job{}, ctx.Err()
}

type materializationSearchSnapshots struct {
	snapshot searchjobs.PreviewSnapshot
}

func (reader materializationSearchSnapshots) GetForContext(
	_ context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	if id != reader.snapshot.Job.ID ||
		scope.TenantID != reader.snapshot.Job.TenantID ||
		scope.OwnerID != reader.snapshot.Job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.snapshot.Job), nil
}

func (reader materializationSearchSnapshots) PreviewForBytesContext(
	_ context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	if id != reader.snapshot.Job.ID ||
		scope.TenantID != reader.snapshot.Job.TenantID ||
		scope.OwnerID != reader.snapshot.Job.OwnerID {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrNotFound
	}
	if limit != 1 {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrPageSize
	}
	return clonePreviewSnapshot(reader.snapshot), nil
}

func (materializationSearchSnapshots) MaximumPreviewRows() uint32 { return 1 }

type cancelOnSecondErrContext struct {
	calls atomic.Int32
	done  chan struct{}
	once  sync.Once
}

func newCancelOnSecondErrContext() *cancelOnSecondErrContext {
	return &cancelOnSecondErrContext{done: make(chan struct{})}
}

func (*cancelOnSecondErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }

func (ctx *cancelOnSecondErrContext) Done() <-chan struct{} { return ctx.done }

func (ctx *cancelOnSecondErrContext) Err() error {
	if ctx.calls.Add(1) < 2 {
		return nil
	}
	ctx.once.Do(func() { close(ctx.done) })
	return context.Canceled
}

func (*cancelOnSecondErrContext) Value(any) any { return nil }
