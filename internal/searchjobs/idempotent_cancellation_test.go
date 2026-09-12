package searchjobs

import (
	"context"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
)

type canceledAcknowledgementJournal struct {
	recordingJournal
	cancel  context.CancelFunc
	current Job
}

func (journal *canceledAcknowledgementJournal) AdmitIdempotent(_ context.Context, job Job, _ requestidempotency.Intent) error {
	journal.current = job
	journal.cancel()
	return context.Canceled
}

func (journal *canceledAcknowledgementJournal) LookupIdempotent(ctx context.Context, _ AccessScope, _ requestidempotency.Intent) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	return journal.current, journal.current.ID != "", nil
}

func TestIdempotentSearchReconcilesCanceledAdmissionAcknowledgement(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	journal := &canceledAcknowledgementJournal{cancel: cancel}
	manager := newTestManager(t, Config{
		Journal: journal,
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error {
			t.Error("ambiguous admission must not enqueue work")
			return nil
		}),
		CleanupInterval: -1,
	})
	intent := requestidempotency.Intent{
		TenantID: "tenant", ActorKind: "browser", ActorID: "owner",
		Route:           requestidempotency.RouteCreateSearchJob,
		ClientRequestID: "canceled-search-admission", CanonicalVersion: requestidempotency.CanonicalVersion,
	}
	current, replayed, err := manager.CreateIdempotent(ctx, validRequest(), intent)
	if err != nil || !replayed || current.ID == "" || current.ID != journal.current.ID {
		t.Fatalf("committed cancellation replay = %+v, %t, %v", current, replayed, err)
	}
}
