package searchartifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type conditionalTestBlobs struct {
	mu        sync.Mutex
	objects   map[string][]byte
	maxUpload int64
}

func (store *conditionalTestBlobs) Put(ctx context.Context, key string, reader io.Reader, size int64) (BlobIdentity, error) {
	if err := ctx.Err(); err != nil {
		return BlobIdentity{}, err
	}
	payload, err := io.ReadAll(reader)
	if err != nil {
		return BlobIdentity{}, err
	}
	if size >= 0 && int64(len(payload)) != size {
		return BlobIdentity{}, ErrCorrupt
	}
	hash := sha256.Sum256(payload)
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, exists := store.objects[key]; exists {
		return BlobIdentity{}, ErrConflict
	}
	store.objects[key] = payload
	if int64(len(payload)) > store.maxUpload {
		store.maxUpload = int64(len(payload))
	}
	return BlobIdentity{Key: key, ETag: hex.EncodeToString(hash[:]), Bytes: int64(len(payload))}, nil
}
func (store *conditionalTestBlobs) Open(ctx context.Context, identity BlobIdentity) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	payload, exists := store.objects[identity.Key]
	if !exists {
		return nil, ErrNotFound
	}
	hash := sha256.Sum256(payload)
	if identity.ETag != hex.EncodeToString(hash[:]) {
		return nil, ErrCorrupt
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}
func (store *conditionalTestBlobs) Delete(_ context.Context, identity BlobIdentity) error {
	store.mu.Lock()
	delete(store.objects, identity.Key)
	store.mu.Unlock()
	return nil
}

func captureTestStore(t *testing.T) (*Store, *conditionalTestBlobs, *testClock) {
	t.Helper()
	ctx := context.Background()
	db, err := control.Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	blobs := &conditionalTestBlobs{objects: map[string][]byte{}}
	clock := &testClock{now: time.Now().UTC().Truncate(time.Microsecond)}
	store, err := New(ctx, Config{DB: db.SQLDB(), Directory: t.TempDir(), Blobs: blobs, Clock: clock.Now, CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, blobs, clock
}

func beginTestCapture(t *testing.T, store *Store, clock *testClock, id string) (searchjobs.Job, searchjobs.CaptureTransaction) {
	t.Helper()
	job := testQueuedJob(t, id, clock.Now())
	job.OrganizationID = "1"
	job.Earliest = clock.Now().Add(-time.Hour)
	job.Latest = clock.Now()
	job.IndexTimeCutoff = clock.Now()
	job.EffectiveIndexes = []string{"main"}
	if err := store.Admit(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	parsed, err := spl.Parse("index=main | table message")
	if err != nil {
		t.Fatal(err)
	}
	cutoff := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: job.TenantID, AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, Earliest: job.Earliest, Latest: job.Latest, SearchStart: job.CreatedAt, SearchTimezone: "UTC", IndexTimeCutoff: job.IndexTimeCutoff, VisibilityCutoff: &cutoff})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.BeginCapture(context.Background(), job, compiled, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}); err != nil {
		t.Fatal(err)
	}
	return job, capture
}

func TestCaptureChunkPublicationSeekAndIntegrity(t *testing.T) {
	store, blobs, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "capture-rows")
	const rows = 4097
	for ordinal := range uint64(rows) {
		value := fmt.Sprintf("%05d:", ordinal) + strings.Repeat("x", 4096)
		if err := capture.Append(context.Background(), searchjobs.ResultRow{Ordinal: ordinal, Values: []searchjobs.Value{searchjobs.StringValue(value)}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Acquire(context.Background(), testAccess(), job.ID); !errors.Is(err, ErrNotReady) {
		t.Fatalf("unpublished chunks readable: %v", err)
	}
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = rows
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	completed.CaptureComplete = true
	if err := capture.Publish(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(context.Background(), testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close() }()
	seekable := lease.(SeekableResultLease)
	for _, ordinal := range []uint64{0, 255, 256, 2048, rows - 1} {
		if err := seekable.Seek(context.Background(), ordinal); err != nil {
			t.Fatal(err)
		}
		row, present, err := lease.Next(context.Background())
		if err != nil || !present || row.Ordinal != ordinal {
			t.Fatalf("seek %d: %#v %v %v", ordinal, row, present, err)
		}
	}
	references, err := store.ReferencedBlobs(context.Background())
	if err != nil || len(references) < 2 {
		t.Fatalf("chunk references: %d %v", len(references), err)
	}
	blobs.mu.Lock()
	maxUpload := blobs.maxUpload
	blobs.mu.Unlock()
	if maxUpload > CaptureChunkBytes {
		t.Fatalf("unbounded chunk upload=%d", maxUpload)
	}
	if lease.Generation() != 7 || lease.RowCount() != rows || lease.ResultsTruncated() {
		t.Fatal("capture snapshot metadata changed")
	}
}

func TestCaptureAbortReleasesObjectsAndQuota(t *testing.T) {
	store, blobs, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "capture-abort")
	for ordinal := range uint64(2200) {
		if err := capture.Append(context.Background(), searchjobs.ResultRow{Ordinal: ordinal, Values: []searchjobs.Value{searchjobs.StringValue(strings.Repeat("x", 4096))}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	blobs.mu.Lock()
	objects := len(blobs.objects)
	blobs.mu.Unlock()
	if objects != 0 {
		t.Fatalf("aborted capture retained %d objects", objects)
	}
	var sessions int
	if err := store.db.QueryRowContext(context.Background(), `SELECT count(*) FROM search_capture_sessions WHERE job_id=$1`, job.ID).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("reserved storage leaked: %d %v", sessions, err)
	}
	if _, err := store.Acquire(context.Background(), testAccess(), job.ID); !errors.Is(err, ErrNotReady) {
		t.Fatalf("aborted capture readable: %v", err)
	}
}

func TestCaptureAndExportShareTransactionalRetentionQuota(t *testing.T) {
	store, _, clock := captureTestStore(t)
	store.captureOptions = CaptureOptions{MaximumRows: 1000, MaximumBytes: 1 << 20, OrganizationBytes: 2 << 20, GlobalBytes: 3 << 20}
	if err := store.ReserveExport(context.Background(), "1", "export-budget", 2<<20); err != nil {
		t.Fatal(err)
	}
	job, capture := beginTestCapture(t, store, clock, "quota-capture")
	if err := capture.Append(context.Background(), searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("captured")}}); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = 1
	completed.CaptureComplete = true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.Publish(context.Background(), completed); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capture ignored export reservation: %v", err)
	}
	if err := capture.Abort(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveExport(context.Background(), "2", "other-org", 2<<20); !errors.Is(err, ErrCapacity) {
		t.Fatalf("organization ignored instance quota: %v", err)
	}
	if err := store.ReleaseExport(context.Background(), "export-budget"); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveExport(context.Background(), "2", "other-org", 1<<20); err != nil {
		t.Fatal(err)
	}
	usage, err := store.RetentionUsage(context.Background(), "2")
	if err != nil || usage.OrganizationBytes != 1<<20 || usage.GlobalBytes != 1<<20 {
		t.Fatalf("unified usage=%#v %v", usage, err)
	}
}

func TestEmptyCaptureSealsSchemaAndSurvivesReconcile(t *testing.T) {
	store, _, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "empty-capture")
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = 0
	completed.CaptureComplete = true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.Publish(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	if err := store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireCapture(context.Background(), testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if lease.RowCount() != 0 || !lease.RowCountExact() || len(lease.Schema().Columns) != 1 {
		t.Fatal("empty snapshot lost exact schema")
	}
	if _, present, err := lease.Next(context.Background()); err != nil || present {
		t.Fatalf("empty snapshot row=%v %v", present, err)
	}
	references, err := store.ReferencedBlobs(context.Background())
	if err != nil || len(references) != 0 {
		t.Fatalf("empty snapshot objects=%d %v", len(references), err)
	}
}

func TestCaptureAcquisitionSlidesRetentionButAuthorizationMetadataDoesNot(t *testing.T) {
	store, _, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "sliding-capture")
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = 0
	completed.CaptureComplete = true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.Publish(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(30 * time.Minute)
	lease, err := store.AcquireCapture(context.Background(), testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = lease.Close()
	record, err := store.Get(context.Background(), testAccess(), job.ID, AccessInspect)
	if err != nil {
		t.Fatal(err)
	}
	if !record.ExpiresAt.Equal(clock.now.Add(time.Hour)) {
		t.Fatalf("retention was not extended: %v", record.ExpiresAt)
	}
	clock.now = record.ExpiresAt.Add(time.Second)
	authority, err := store.AuthorizationJob(context.Background(), testAccess(), job.ID)
	if err != nil || !authority.ExpiresAt.Equal(record.ExpiresAt) {
		t.Fatalf("authorization metadata refreshed TTL: %v %#v", err, authority)
	}
	if _, err := store.AcquireCapture(context.Background(), testAccess(), job.ID); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired results resurrected: %v", err)
	}
}

func TestUnpublishedCleanupNeverReclaimsCommittedCapture(t *testing.T) {
	store, blobs, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "committed-cleanup-barrier")
	if err := capture.Append(context.Background(), searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("complete")}}); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = 1
	completed.CaptureComplete = true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.Publish(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	transaction := capture.(*captureTransaction)
	if err := store.deleteCapture(context.Background(), transaction.id, false); err != nil {
		t.Fatal(err)
	}
	blobs.mu.Lock()
	remaining := len(blobs.objects)
	blobs.mu.Unlock()
	if remaining != 1 {
		t.Fatal("unpublished cleanup removed a committed blob")
	}
	lease, err := store.AcquireCapture(context.Background(), testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if _, present, err := lease.Next(context.Background()); err != nil || !present {
		t.Fatalf("committed snapshot damaged by cleanup: %v %v", present, err)
	}
}

func TestRevokedCaptureReadDoesNotRefreshRetention(t *testing.T) {
	store, _, clock := captureTestStore(t)
	job, capture := beginTestCapture(t, store, clock, "revoked-capture-retention")
	completed := completeTestJob(job, clock.Now(), time.Hour)
	completed.RowCount = 0
	completed.CaptureComplete = true
	completed.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.Publish(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(30 * time.Minute)
	denied := errors.New("source grant revoked")
	store.captureOptions.Authorize = func(context.Context, searchjobs.Job) error { return denied }
	if _, err := store.AcquireCapture(context.Background(), testAccess(), job.ID); !errors.Is(err, denied) {
		t.Fatalf("revoked capture read=%v", err)
	}
	var expires int64
	if err := store.db.QueryRowContext(context.Background(), `SELECT expires_at_us FROM durable_search_jobs WHERE id=$1`, job.ID).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	if expires != completed.ExpiresAt.UnixMicro() {
		t.Fatal("denied capture request extended retention")
	}
}
