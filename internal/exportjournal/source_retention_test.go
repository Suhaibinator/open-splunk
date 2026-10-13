package exportjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type sourceRetentionBlobs struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (blobs *sourceRetentionBlobs) Put(ctx context.Context, key string, reader io.Reader, _ int64) (searchartifacts.BlobIdentity, error) {
	if err := ctx.Err(); err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	if _, exists := blobs.objects[key]; exists {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrConflict
	}
	blobs.objects[key] = data
	digest := sha256.Sum256(data)
	return searchartifacts.BlobIdentity{Key: key, ETag: hex.EncodeToString(digest[:]), Version: "retention-version", Bytes: int64(len(data))}, nil
}
func (blobs *sourceRetentionBlobs) Open(_ context.Context, identity searchartifacts.BlobIdentity) (io.ReadCloser, error) {
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	data, ok := blobs.objects[identity.Key]
	if !ok {
		return nil, searchartifacts.ErrNotFound
	}
	digest := sha256.Sum256(data)
	if identity.Version != "retention-version" || identity.ETag != hex.EncodeToString(digest[:]) {
		return nil, searchartifacts.ErrCorrupt
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (blobs *sourceRetentionBlobs) Delete(_ context.Context, identity searchartifacts.BlobIdentity) error {
	blobs.mu.Lock()
	delete(blobs.objects, identity.Key)
	blobs.mu.Unlock()
	return nil
}

type sourceRetentionExecutor struct{}

func (sourceRetentionExecutor) Execute(_ context.Context, _ clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	if err := sink.SetSchema(searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}); err != nil {
		return err
	}
	return sink.AddRow([]searchjobs.Value{searchjobs.StringValue("retained source")})
}

type sourceRetentionSnapshotter struct{}

func (sourceRetentionSnapshotter) VisibilityCutoff(context.Context) (uint64, error) { return 1, nil }

func TestIndependentExportRetainsOnlySourceAuthorityAndRechecksPermissions(t *testing.T) {
	journal, db := openTestJournal(t)
	var micros atomic.Int64
	micros.Store(time.Now().UTC().Truncate(time.Microsecond).UnixMicro())
	now := func() time.Time { return time.UnixMicro(micros.Load()).UTC() }
	blobs := &sourceRetentionBlobs{objects: map[string][]byte{}}
	var allowed atomic.Bool
	allowed.Store(true)
	denied := errors.New("current source permission revoked")
	store, err := searchartifacts.New(context.Background(), searchartifacts.Config{DB: db.SQLDB(), Directory: t.TempDir(), Blobs: blobs, Clock: now, TombstoneRetention: time.Minute, CleanupInterval: -1, CaptureOptions: searchartifacts.CaptureOptions{Authorize: func(context.Context, searchjobs.Job) error {
		if !allowed.Load() {
			return denied
		}
		return nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	searches, err := searchjobs.New(searchjobs.Config{Executor: sourceRetentionExecutor{}, Snapshotter: sourceRetentionSnapshotter{}, Compiler: clickhouse.Compiler{}, Journal: store, CaptureFactory: store, RetentionTTL: time.Minute, Now: now, CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = searches.Close() })
	window, err := searchtime.Resolve("-1h", "now", nil, now())
	if err != nil {
		t.Fatal(err)
	}
	search, err := searches.Create(context.Background(), searchjobs.CreateRequest{SPL: "index=main | table message", TenantID: "tenant", OwnerID: "owner", OrganizationID: "1", AuthorizedIndexes: []string{"main"}, RequestedIndexes: []string{"main"}, TimeRange: window})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for search.State != searchjobs.StateCompleted {
		if time.Now().After(deadline) || search.State.Terminal() {
			t.Fatalf("capture completion state=%v", search.State)
		}
		time.Sleep(time.Millisecond)
		search, err = searches.Get(search.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	source, err := exportjobs.NewCaptureSource(store)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := exportjobs.New(exportjobs.Config{ArtifactDir: t.TempDir(), Source: source, Journal: journal, Blobs: blobs, RetentionQuota: store, ArtifactTTL: 30 * time.Minute, Now: now, CleanupInterval: -1, Authorize: func(ctx context.Context, access searchjobs.AccessScope, id string) error {
		_, err := store.AuthorizationJob(ctx, access, id)
		return err
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "owner", Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner", OrganizationID: "1"}
	exported, err := manager.Create(ctx, access, exportjobs.CreateRequest{SearchJobID: search.ID, Format: exportjobs.FormatJSONLines})
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for exported.State != exportjobs.StateCompleted {
		if time.Now().After(deadline) || (exported.State == exportjobs.StateFailed || exported.State == exportjobs.StateCanceled || exported.State == exportjobs.StateExpired) {
			t.Fatalf("export completion state=%v failure=%#v", exported.State, exported.Failure)
		}
		time.Sleep(time.Millisecond)
		exported, err = manager.Get(ctx, access, exported.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	micros.Add(int64(2 * time.Minute / time.Microsecond))
	if err := store.Reap(context.Background()); err != nil {
		t.Fatal(err)
	}
	micros.Add(int64(2 * time.Minute / time.Microsecond))
	if err := store.Reap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireCapture(context.Background(), access, search.ID); !errors.Is(err, searchartifacts.ErrExpired) {
		t.Fatalf("expired capture resurrected: %v", err)
	}
	var manifests int
	if err := db.SQLDB().QueryRowContext(context.Background(), `SELECT count(*) FROM search_capture_manifests WHERE job_id=$1`, search.ID).Scan(&manifests); err != nil || manifests != 0 {
		t.Fatalf("expired capture objects retained: manifests=%d err=%v", manifests, err)
	}
	authority, err := store.AuthorizationJob(context.Background(), access, search.ID)
	if err != nil || len(authority.EffectiveIndexes) != 1 || authority.EffectiveIndexes[0] != "main" {
		t.Fatalf("export lost source authority: %#v %v", authority, err)
	}
	lease, err := manager.AcquireDownloadFor(ctx, access, exported.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(lease)
	closeErr := lease.Close()
	if readErr != nil || closeErr != nil || string(data) != "{\"message\":\"retained source\"}\n" {
		t.Fatalf("independent export bytes=%q read=%v close=%v", data, readErr, closeErr)
	}
	allowed.Store(false)
	if _, err := manager.AcquireDownloadFor(ctx, access, exported.ID); !errors.Is(err, denied) {
		t.Fatalf("independent export bypassed current permission: %v", err)
	}
	allowed.Store(true)
	micros.Add(int64(time.Hour / time.Microsecond))
	if err := manager.Cleanup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Reap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AuthorizationJob(context.Background(), access, search.ID); !errors.Is(err, searchartifacts.ErrNotFound) {
		t.Fatalf("unneeded source authority retained: %v", err)
	}
}
