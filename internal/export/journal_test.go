package export

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type memoryExportJournal struct {
	mu            sync.Mutex
	rows          map[string]DurableJob
	keys          map[string]requestidempotency.Receipt
	failAdmission error
	admissions    int
}

func newMemoryExportJournal() *memoryExportJournal {
	return &memoryExportJournal{rows: map[string]DurableJob{}, keys: map[string]requestidempotency.Receipt{}}
}
func (journal *memoryExportJournal) Admit(_ context.Context, access searchjobs.AccessScope, job Job) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.failAdmission != nil {
		return journal.failAdmission
	}
	journal.admissions++
	journal.rows[job.ID] = DurableJob{Access: access, Job: cloneJob(job)}
	return nil
}
func (journal *memoryExportJournal) AdmitIdempotent(ctx context.Context, access searchjobs.AccessScope, job Job, intent requestidempotency.Intent) error {
	if err := journal.Admit(ctx, access, job); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.keys[intent.ClientRequestID] = requestidempotency.Receipt{Intent: intent, Target: requestidempotency.Target{Kind: "export_job", ID: job.ID, Version: job.Version}}
	return nil
}
func (journal *memoryExportJournal) Update(_ context.Context, row DurableJob) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	row.Job = cloneJob(row.Job)
	journal.rows[row.Job.ID] = row
	return nil
}
func (journal *memoryExportJournal) Get(_ context.Context, access searchjobs.AccessScope, id string) (DurableJob, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	row, ok := journal.rows[id]
	if !ok || row.Access != access {
		return DurableJob{}, ErrNotFound
	}
	row.Job = cloneJob(row.Job)
	return row, nil
}
func (journal *memoryExportJournal) Restore(_ context.Context, _ int) ([]DurableJob, error) {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	rows := make([]DurableJob, 0, len(journal.rows))
	for _, row := range journal.rows {
		row.Job = cloneJob(row.Job)
		rows = append(rows, row)
	}
	return rows, nil
}
func (journal *memoryExportJournal) Lookup(ctx context.Context, access searchjobs.AccessScope, intent requestidempotency.Intent) (Job, bool, error) {
	journal.mu.Lock()
	receipt, ok := journal.keys[intent.ClientRequestID]
	journal.mu.Unlock()
	if !ok {
		return Job{}, false, nil
	}
	if receipt.Intent != intent {
		return Job{}, true, requestidempotency.ErrConflict
	}
	row, err := journal.Get(ctx, access, receipt.Target.ID)
	return row.Job, true, err
}

func TestDurableExportAdmissionFailureNeverEnqueues(t *testing.T) {
	journal := newMemoryExportJournal()
	journal.failAdmission = errors.New("commit unavailable")
	source := &exportTestSource{datasets: map[string]exportTestDataset{"search-1": {schema: basicExportSchema(), rows: basicExportRows()}}}
	manager := newExportTestManager(t, source, func(config *Config) { config.Journal = journal })
	_, err := manager.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search-1", Format: FormatCSV})
	if !errors.Is(err, journal.failAdmission) {
		t.Fatalf("admission failure = %v", err)
	}
	manager.mu.RLock()
	jobs := len(manager.jobs)
	queued := len(manager.queue)
	manager.mu.RUnlock()
	if jobs != 0 || queued != 0 || manager.totalBytes != 0 {
		t.Fatalf("failed admission published work: jobs=%d queue=%d bytes=%d", jobs, queued, manager.totalBytes)
	}
}

func TestDurableExportRestartPreservesArtifactAndMissingFileFailsWithoutRerun(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "missing"}[missing], func(t *testing.T) {
			journal := newMemoryExportJournal()
			directory := t.TempDir()
			source := &exportTestSource{datasets: map[string]exportTestDataset{"search-1": {schema: basicExportSchema(), rows: basicExportRows()}}}
			first := newExportTestManager(t, source, func(config *Config) { config.Journal = journal; config.ArtifactDir = directory })
			accepted, err := first.Create(context.Background(), testAccess, CreateRequest{SearchJobID: "search-1", Format: FormatCSV})
			if err != nil {
				t.Fatal(err)
			}
			completed := waitExportState(t, first, testAccess, accepted.ID, StateCompleted)
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			if missing {
				if err := os.Remove(filepath.Join(directory, "durable", completed.Artifact.FileName)); err != nil {
					t.Fatal(err)
				}
			}
			second := newExportTestManager(t, source, func(config *Config) { config.Journal = journal; config.ArtifactDir = directory })
			got, err := second.Get(context.Background(), testAccess, accepted.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := StateCompleted
			if missing {
				want = StateFailed
			}
			if got.State != want {
				t.Fatalf("restored state=%v want=%v", got.State, want)
			}
			if !missing {
				if got.Version != completed.Version || got.Artifact == nil {
					t.Fatalf("restored metadata changed: %#v", got)
				}
				if actual, err := os.ReadFile(filepath.Join(directory, "durable", got.Artifact.FileName)); err != nil || len(actual) == 0 {
					t.Fatal("restored artifact empty")
				}
			}
			source.mu.Lock()
			acquires := source.acquires
			source.mu.Unlock()
			if acquires != 1 {
				t.Fatalf("restart reran source: %d acquisitions", acquires)
			}
		})
	}
}

func TestDurableExportKeyReplayAtCapacityReturnsCurrentMetadata(t *testing.T) {
	journal := newMemoryExportJournal()
	source := &exportTestSource{datasets: map[string]exportTestDataset{"search-1": {schema: basicExportSchema(), rows: basicExportRows()}}}
	manager := newExportTestManager(t, source, func(config *Config) { config.Journal = journal; config.MaxJobs = 1 })
	intent := requestidempotency.Intent{TenantID: testAccess.TenantID, ActorKind: "browser", ActorID: testAccess.OwnerID, Route: requestidempotency.RouteCreateExportJob, ClientRequestID: "logical-export-request", CanonicalVersion: 1}
	var group sync.WaitGroup
	identities := make(chan string, 12)
	for range 12 {
		group.Go(func() {
			job, _, err := manager.CreateIdempotent(context.Background(), testAccess, CreateRequest{SearchJobID: "search-1", Format: FormatCSV}, intent)
			if err != nil {
				t.Error(err)
				return
			}
			identities <- job.ID
		})
	}
	group.Wait()
	close(identities)
	id := ""
	for candidate := range identities {
		if id != "" && candidate != id {
			t.Fatal("parallel retry created another identity")
		}
		id = candidate
	}
	current := waitExportState(t, manager, testAccess, id, StateCompleted)
	replay, replayed, err := manager.CreateIdempotent(context.Background(), testAccess, CreateRequest{}, intent)
	if err != nil || !replayed || replay.ID != id || replay.Version != current.Version {
		t.Fatalf("replay after source/default changes = %#v,%v,%v", replay, replayed, err)
	}
	changed := intent
	changed.RequestSHA256[0] = 1
	if _, _, err := manager.CreateIdempotent(context.Background(), testAccess, CreateRequest{}, changed); !errors.Is(err, requestidempotency.ErrConflict) {
		t.Fatalf("changed intent = %v", err)
	}
	journal.mu.Lock()
	admissions := journal.admissions
	journal.mu.Unlock()
	if admissions != 1 {
		t.Fatalf("admissions = %d", admissions)
	}
}

func TestDurableQueuedExportRestartsInterruptedWithoutEnqueue(t *testing.T) {
	journal := newMemoryExportJournal()
	now := time.Now().UTC()
	journal.rows["accepted-before-crash"] = DurableJob{Access: testAccess, Job: Job{ID: "accepted-before-crash", Version: 1, SearchJobID: "search-1", Format: FormatCSV, Columns: []string{"message"}, RowLimit: 100, ByteLimit: 1024, State: StateQueued, CreatedAt: now, Progress: Progress{UpdatedAt: now}}}
	source := &exportTestSource{}
	manager := newExportTestManager(t, source, func(config *Config) { config.Journal = journal })
	job, err := manager.Get(context.Background(), testAccess, "accepted-before-crash")
	if err != nil || job.State != StateFailed || job.Failure == nil {
		t.Fatalf("interrupted accepted job = %#v,%v", job, err)
	}
	if len(manager.queue) != 0 || source.acquires != 0 {
		t.Fatal("restart enqueued interrupted work")
	}
}
