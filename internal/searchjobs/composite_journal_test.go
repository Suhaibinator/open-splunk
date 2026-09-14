package searchjobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
)

type idempotencyReceiptTestJournal struct {
	recordingJournal
	target          requestidempotency.Target
	idempotentCalls int
	idempotentErr   error
}

func (journal *idempotencyReceiptTestJournal) LookupIdempotencyReceipt(
	context.Context,
	requestidempotency.Intent,
) (requestidempotency.Target, bool, error) {
	return journal.target, true, nil
}

func (journal *idempotencyReceiptTestJournal) AdmitIdempotent(
	_ context.Context,
	job Job,
	_ requestidempotency.Intent,
) error {
	journal.idempotentCalls++
	journal.admitted = append(journal.admitted, job.ID)
	return journal.idempotentErr
}

func TestCompositeJournalReceiptConflictCompensatesOnlyEarlierProjection(t *testing.T) {
	target := &recordingJournal{}
	receipts := &idempotencyReceiptTestJournal{idempotentErr: requestidempotency.ErrConflict}
	journal := NewCompositeJournal(target, receipts)
	err := journal.AdmitIdempotent(
		t.Context(),
		Job{ID: "losing-job", State: StateQueued, Version: 1, CreatedAt: time.Now().UTC()},
		requestidempotency.Intent{},
	)
	if !errors.Is(err, requestidempotency.ErrConflict) {
		t.Fatalf("AdmitIdempotent error = %v", err)
	}
	if len(target.finalized) != 1 || len(receipts.finalized) != 0 {
		t.Fatalf("compensation = target %v receipt %v", target.finalized, receipts.finalized)
	}
}

type idempotencyTargetTestJournal struct {
	recordingJournal
	job Job
}

func (journal *idempotencyTargetTestJournal) ReadIdempotencyTarget(
	_ context.Context,
	_ AccessScope,
	target requestidempotency.Target,
) (Job, error) {
	if target.ID != journal.job.ID {
		return Job{}, requestidempotency.ErrUnavailable
	}
	return journal.job, nil
}

func TestCompositeJournalIdempotencyUsesOneReceiptAndCurrentDurableTarget(t *testing.T) {
	target := &idempotencyTargetTestJournal{job: Job{
		ID: "search-idempotent", TenantID: "tenant", OwnerID: "owner", Version: 4,
	}}
	receipts := &idempotencyReceiptTestJournal{target: requestidempotency.Target{
		Kind: requestidempotency.TargetSearchJob, ID: target.job.ID, Version: 1,
	}}
	journal := NewCompositeJournal(target, receipts)
	intent := requestidempotency.Intent{
		TenantID: "tenant", ActorKind: "browser", ActorID: "owner",
		Route:           requestidempotency.RouteCreateSearchJob,
		ClientRequestID: "search request 01", CanonicalVersion: requestidempotency.CanonicalVersion,
	}
	current, found, err := journal.LookupIdempotent(
		t.Context(), AccessScope{TenantID: "tenant", OwnerID: "owner"}, intent,
	)
	if err != nil || !found || current.Version != 4 {
		t.Fatalf("lookup = (%+v, %t, %v)", current, found, err)
	}
	if err := journal.AdmitIdempotent(t.Context(), Job{ID: "new-job"}, intent); err != nil {
		t.Fatal(err)
	}
	if receipts.idempotentCalls != 1 || len(target.admitted) != 1 || len(receipts.admitted) != 1 {
		t.Fatalf("admission calls = target %v receipt %v/%d", target.admitted, receipts.admitted, receipts.idempotentCalls)
	}
}

type recordingJournal struct {
	mu        sync.Mutex
	admitted  []string
	finalized []string
	finalJobs []Job
	admitErr  error
	finalErr  error
}

func (journal *recordingJournal) Admit(_ context.Context, job Job) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.admitted = append(journal.admitted, job.ID)
	return journal.admitErr
}

func (journal *recordingJournal) Finalize(_ context.Context, job Job) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.finalized = append(journal.finalized, job.ID)
	journal.finalJobs = append(journal.finalJobs, job)
	return journal.finalErr
}

func TestCompositeJournalOrdersAdmissionAndJoinsFinalization(t *testing.T) {
	first := &recordingJournal{}
	secondErr := errors.New("second journal failed")
	second := &recordingJournal{admitErr: secondErr, finalErr: secondErr}
	third := &recordingJournal{}
	journal := NewCompositeJournal(first, second, third)
	job := Job{ID: "job-1"}
	if err := journal.Admit(context.Background(), job); !errors.Is(err, secondErr) {
		t.Fatalf("Admit() error = %v", err)
	}
	if len(first.admitted) != 1 || len(second.admitted) != 1 || len(third.admitted) != 0 {
		t.Fatalf("admission order = %v, %v, %v", first.admitted, second.admitted, third.admitted)
	}
	if len(first.finalJobs) != 1 || first.finalJobs[0].State != StateCanceled ||
		first.finalJobs[0].ExpiresAt.IsZero() || len(second.finalJobs) != 1 {
		t.Fatalf("admission compensation = first %#v second %#v", first.finalJobs, second.finalJobs)
	}
	first.finalized = nil
	second.finalized = nil
	third.finalized = nil
	second.finalErr = secondErr
	if err := journal.Finalize(context.Background(), job); !errors.Is(err, secondErr) {
		t.Fatalf("Finalize() error = %v", err)
	}
	if len(first.finalized) != 1 || len(second.finalized) != 1 || len(third.finalized) != 1 {
		t.Fatalf("finalize fan-out = %v, %v, %v", first.finalized, second.finalized, third.finalized)
	}
}

type recordingResultJournal struct {
	recordingJournal
	published bool
}

func (journal *recordingResultJournal) FinalizeResults(_ context.Context, _ Job, _ ResultLease) error {
	journal.mu.Lock()
	journal.published = true
	journal.mu.Unlock()
	return nil
}

func TestCompositeCompletedPublicationPrecedesMetadataProjection(t *testing.T) {
	t.Parallel()

	publication := &recordingResultJournal{}
	projectionErr := errors.New("history unavailable")
	projection := &recordingJournal{finalErr: projectionErr}
	journal := NewCompositeJournal(projection, publication)
	job := Job{ID: "publish-before-project", State: StateCompleted}
	lease := &stubResultLease{}

	outcome := journal.finalizeCompleted(
		context.Background(), job, lease,
	)
	if outcome.Finalize != nil || outcome.Results != nil || !errors.Is(outcome.Projection, projectionErr) {
		t.Fatalf("finalizeCompleted outcome = %#v", outcome)
	}
	publication.mu.Lock()
	published := publication.published
	publication.mu.Unlock()
	projection.mu.Lock()
	projected := len(projection.finalized)
	projection.mu.Unlock()
	if !published || projected != 1 {
		t.Fatalf("publication=%t projected=%d", published, projected)
	}
}

func TestCompositeCompletedPublicationFailureProjectsTerminalStorageFailure(t *testing.T) {
	t.Parallel()

	publicationErr := errors.New("artifact unavailable")
	publication := &failingResultJournal{recordingJournal: recordingJournal{}, err: publicationErr}
	projection := &recordingJournal{}
	journal := NewCompositeJournal(projection, publication)
	job := Job{ID: "failed-publication", State: StateCompleted}

	outcome := journal.finalizeCompleted(
		context.Background(), job, &stubResultLease{},
	)
	if outcome.Finalize != nil || !errors.Is(outcome.Results, publicationErr) || outcome.Projection != nil {
		t.Fatalf("finalizeCompleted outcome = %#v", outcome)
	}
	projection.mu.Lock()
	projected := append([]Job(nil), projection.finalJobs...)
	projection.mu.Unlock()
	publication.mu.Lock()
	publicationJobs := append([]Job(nil), publication.finalJobs...)
	publication.mu.Unlock()
	if len(projected) != 1 || projected[0].State != StateFailed ||
		projected[0].Failure == nil || projected[0].Failure.Code != FailureStorageUnavailable {
		t.Fatalf("terminal metadata compensation = %#v", projected)
	}
	if len(publicationJobs) != 2 || publicationJobs[0].State != StateCompleted ||
		publicationJobs[1].State != StateFailed {
		t.Fatalf("publication compensation = %#v", publicationJobs)
	}
	if projected[0].Schema != nil || projected[0].RowCount != 0 || projected[0].ResultBytes != 0 {
		t.Fatalf("compensation exposed unavailable results: %#v", projected[0])
	}
}

type failingResultJournal struct {
	recordingJournal
	err error
}

func (journal *failingResultJournal) FinalizeResults(context.Context, Job, ResultLease) error {
	return journal.err
}

type stubResultLease struct{}

func (*stubResultLease) Schema() Schema         { return Schema{} }
func (*stubResultLease) RowCount() uint64       { return 0 }
func (*stubResultLease) RowCountExact() bool    { return true }
func (*stubResultLease) ResultsTruncated() bool { return false }
func (*stubResultLease) Generation() uint64     { return 1 }
func (*stubResultLease) Next(context.Context) (ResultRow, bool, error) {
	return ResultRow{}, false, nil
}
func (*stubResultLease) Close() error { return nil }

func TestAdmissionCompensationUsesDeterministicRetentionWindow(t *testing.T) {
	t.Parallel()

	created := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	job := Job{
		ID: "compensated", State: StateQueued, Version: 1, CreatedAt: created,
		RetentionLifetime: 37 * time.Minute,
	}
	compensating := admissionCompensation(job)
	if compensating.State != StateCanceled || compensating.Version != 2 ||
		!compensating.FinishedAt.Equal(created) ||
		!compensating.ExpiresAt.Equal(created.Add(37*time.Minute)) {
		t.Fatalf("compensation = %#v", compensating)
	}
}

type panickingAdmissionJournal struct {
	finalized []Job
}

func (*panickingAdmissionJournal) Admit(context.Context, Job) error {
	panic("sensitive admission failure")
}

func (journal *panickingAdmissionJournal) Finalize(_ context.Context, job Job) error {
	journal.finalized = append(journal.finalized, job)
	return nil
}

func TestCompositeAdmissionPanicStillCompensatesEarlierTargets(t *testing.T) {
	t.Parallel()

	first := &recordingJournal{}
	second := &panickingAdmissionJournal{}
	journal := NewCompositeJournal(first, second)
	job := Job{
		ID: "panic-compensation", State: StateQueued, Version: 1,
		CreatedAt: time.Date(2026, time.August, 30, 13, 0, 0, 0, time.UTC),
	}
	err := journal.Admit(context.Background(), job)
	if err == nil || err.Error() != "search job journal callback panicked" {
		t.Fatalf("Admit panic error = %v", err)
	}
	if len(first.finalJobs) != 1 || first.finalJobs[0].State != StateCanceled ||
		len(second.finalized) != 1 || second.finalized[0].State != StateCanceled {
		t.Fatalf("panic compensation = first %#v second %#v", first.finalJobs, second.finalized)
	}
}
