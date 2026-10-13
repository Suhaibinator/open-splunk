package searchartifacts

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type finalUploadBlockingBlobs struct {
	*conditionalTestBlobs
	started      chan struct{}
	allowCleanup chan struct{}
	startOnce    sync.Once
}

func (blobs *finalUploadBlockingBlobs) Put(ctx context.Context, _ string, _ io.Reader, _ int64) (BlobIdentity, error) {
	blobs.startOnce.Do(func() { close(blobs.started) })
	<-ctx.Done()
	return BlobIdentity{}, ctx.Err()
}
func (blobs *finalUploadBlockingBlobs) Delete(ctx context.Context, identity BlobIdentity) error {
	select {
	case <-blobs.allowCleanup:
		return blobs.conditionalTestBlobs.Delete(ctx, identity)
	default:
		return errors.New("object cleanup temporarily unavailable")
	}
}

type finalUploadExecutor struct{}

func (finalUploadExecutor) Execute(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}); err != nil {
		return err
	}
	return sink.AddRow([]searchjobs.Value{searchjobs.StringValue("private final chunk")})
}

type finalUploadSnapshotter struct{}

func (finalUploadSnapshotter) VisibilityCutoff(context.Context) (uint64, error) { return 1, nil }

func TestCancelInterruptsFinalCaptureUploadAndPreservesCleanupIntent(t *testing.T) {
	store, memory, clock := captureTestStore(t)
	blobs := &finalUploadBlockingBlobs{conditionalTestBlobs: memory, started: make(chan struct{}), allowCleanup: make(chan struct{})}
	store.blobs = blobs
	manager, err := searchjobs.New(searchjobs.Config{Executor: finalUploadExecutor{}, Snapshotter: finalUploadSnapshotter{}, Compiler: clickhouse.Compiler{}, Journal: store, CaptureFactory: store, CleanupInterval: -1, Now: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	timeRange, err := searchtime.Resolve("-1h", "now", nil, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	job, err := manager.Create(context.Background(), searchjobs.CreateRequest{SPL: "index=main | table message", TenantID: "tenant", OwnerID: "owner", OrganizationID: "1", AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, TimeRange: timeRange})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-blobs.started:
	case <-time.After(5 * time.Second):
		t.Fatal("final private upload did not start")
	}
	canceled := make(chan error, 1)
	go func() {
		canceled <- manager.CancelFor(searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner", OrganizationID: "1"}, job.ID)
	}()
	select {
	case err := <-canceled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation waited behind the private upload")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	var manifests, pending int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM search_capture_manifests WHERE job_id=$1`, job.ID).Scan(&manifests); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM search_capture_objects o JOIN search_capture_sessions s ON s.capture_id=o.capture_id WHERE s.job_id=$1 AND s.reclaiming`, job.ID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if manifests != 0 || pending != 1 {
		t.Fatalf("canceled publication manifests=%d durable cleanup intents=%d", manifests, pending)
	}
	record, err := store.Get(context.Background(), searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner", OrganizationID: "1"}, job.ID, AccessAuthorization)
	if err != nil || record.State != StateCanceled || record.ArtifactPresent || record.Job.CaptureComplete {
		t.Fatalf("canceled source exposed completion: state=%v artifact=%v err=%v", record.State, record.ArtifactPresent, err)
	}
	close(blobs.allowCleanup)
	if err := store.ReconcileCaptures(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM search_capture_sessions WHERE job_id=$1`, job.ID).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("reconciled canceled capture retained sessions=%d err=%v", pending, err)
	}
}

func TestPreparedCaptureCannotSealAfterReclamationClaim(t *testing.T) {
	store, _, clock := captureTestStore(t)
	job, transaction := beginTestCapture(t, store, clock, "prepared-reclamation-fence")
	if err := transaction.Append(context.Background(), searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("private row")}}); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount, completed.CaptureComplete = 1, true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	capture := transaction.(*captureTransaction)
	if err := capture.PreparePublication(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.claimCaptureReclamation(context.Background(), capture.id, false)
	if err != nil || !claimed {
		t.Fatalf("private reclamation claim=%v err=%v", claimed, err)
	}
	if err := capture.Publish(context.Background(), completed); !errors.Is(err, ErrConflict) {
		t.Fatalf("prepared capture bypassed reclamation fence: %v", err)
	}
	var manifests int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM search_capture_manifests WHERE job_id=$1`, job.ID).Scan(&manifests); err != nil || manifests != 0 {
		t.Fatalf("reclaimed capture published manifest=%d err=%v", manifests, err)
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCapturePublicationUsesActiveAuthorityUntilManifestCommit(t *testing.T) {
	store, _, clock := captureTestStore(t)
	freshPrivilege := true
	denied := errors.New("admission privilege revoked")
	store.captureOptions.Authorize = func(_ context.Context, job searchjobs.Job) error {
		if job.State.Terminal() || job.CaptureComplete {
			return errors.New("uncommitted capture used completed-read authority")
		}
		if !freshPrivilege {
			return denied
		}
		return nil
	}
	job, transaction := beginTestCapture(t, store, clock, "capture-active-publication-authority")
	if err := transaction.Append(context.Background(), searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("private row")}}); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount, completed.CaptureComplete = 1, true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	capture := transaction.(*captureTransaction)
	if err := capture.PreparePublication(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	freshPrivilege = false
	if err := capture.Publish(context.Background(), completed); !errors.Is(err, denied) {
		t.Fatalf("revoked admission privilege sealed private capture: %v", err)
	}
	var manifests int
	if err := store.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM search_capture_manifests WHERE job_id=$1`, job.ID).Scan(&manifests); err != nil || manifests != 0 {
		t.Fatalf("revoked private capture manifest=%d err=%v", manifests, err)
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
}
