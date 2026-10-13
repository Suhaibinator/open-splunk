package export

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

type remoteTestBlobs struct {
	mu        sync.Mutex
	objects   map[string][]byte
	deleteErr error
}

func (blobs *remoteTestBlobs) Put(ctx context.Context, key string, reader io.Reader, _ int64) (searchartifacts.BlobIdentity, error) {
	payload, err := io.ReadAll(reader)
	if err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	if err := ctx.Err(); err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	if _, ok := blobs.objects[key]; ok {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrConflict
	}
	blobs.objects[key] = payload
	digest := sha256.Sum256(payload)
	return searchartifacts.BlobIdentity{Key: key, ETag: hex.EncodeToString(digest[:]), Version: "version-1", Bytes: int64(len(payload))}, nil
}
func (blobs *remoteTestBlobs) Open(_ context.Context, id searchartifacts.BlobIdentity) (io.ReadCloser, error) {
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	p, ok := blobs.objects[id.Key]
	if !ok {
		return nil, searchartifacts.ErrNotFound
	}
	digest := sha256.Sum256(p)
	if id.ETag != hex.EncodeToString(digest[:]) || id.Version != "version-1" {
		return nil, searchartifacts.ErrCorrupt
	}
	return io.NopCloser(bytes.NewReader(p)), nil
}
func (blobs *remoteTestBlobs) Delete(_ context.Context, id searchartifacts.BlobIdentity) error {
	blobs.mu.Lock()
	defer blobs.mu.Unlock()
	if blobs.deleteErr != nil {
		return blobs.deleteErr
	}
	delete(blobs.objects, id.Key)
	return nil
}

type completedRejectJournal struct{ *memoryExportJournal }

func (journal completedRejectJournal) Update(ctx context.Context, row DurableJob) error {
	if row.Job.State == StateCompleted {
		return errors.New("injected completed journal failure")
	}
	return journal.memoryExportJournal.Update(ctx, row)
}

func TestRemoteExportPublicationJournalFailureDoesNotPublish(t *testing.T) {
	journal := completedRejectJournal{newMemoryExportJournal()}
	blobs := &remoteTestBlobs{objects: map[string][]byte{}, deleteErr: errors.New("injected physical reclamation failure")}
	source := &exportTestSource{datasets: map[string]exportTestDataset{"search": {schema: basicExportSchema(), rows: basicExportRows()}}}
	manager := newExportTestManager(t, source, func(config *Config) { config.Journal = journal; config.Blobs = blobs })
	accepted, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatCSV})
	if err != nil {
		t.Fatal(err)
	}
	failed := waitExportState(t, manager, testAccess, accepted.ID, StateFailed)
	if failed.Artifact != nil || failed.Failure == nil || failed.Failure.Code != FailureStorageUnavailable {
		t.Fatalf("unrecorded completed projection exposed: %#v", failed)
	}
	if _, err := manager.AcquireDownloadFor(context.Background(), testAccess, failed.ID); err == nil {
		t.Fatal("failed export downloadable")
	}
	journal.mu.Lock()
	retained := journal.rows[failed.ID]
	journal.mu.Unlock()
	if retained.Job.State != StateFailed || retained.ArtifactName == "" {
		t.Fatalf("durable cleanup intent lost: %#v", retained)
	}
	blobs.mu.Lock()
	blobs.deleteErr = nil
	blobs.mu.Unlock()
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemoteExportLateSourceFailureNeverPublishesPrefix(t *testing.T) {
	blobs := &remoteTestBlobs{objects: map[string][]byte{}}
	source := &exportTestSource{datasets: map[string]exportTestDataset{"search": {schema: basicExportSchema(), rows: basicExportRows(), nextErr: errors.New("late source failure")}}}
	manager := newExportTestManager(t, source, func(config *Config) { config.Blobs = blobs })
	accepted, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search", Format: FormatJSONLines})
	if err != nil {
		t.Fatal(err)
	}
	failed := waitExportState(t, manager, testAccess, accepted.ID, StateFailed)
	if failed.Artifact != nil {
		t.Fatal("late failed prefix published")
	}
}
