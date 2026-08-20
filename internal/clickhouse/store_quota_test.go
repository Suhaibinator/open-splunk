package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestStorePassesFreshQuotaAdmissionToVisibility(t *testing.T) {
	t.Parallel()

	quota := &ingestquota.Admission{}
	evaluatedAt := time.Date(2026, 8, 1, 12, 34, 56, 789_123_456, time.UTC)
	sequencer := &quotaCapturingVisibilitySequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 1},
		},
	}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{batch: &fakeWriteBatch{}},
		fixedRetention(time.Hour),
		sequencer,
	)
	batch := validStoreBatch()
	batch.QuotaAdmission = quota
	batch.QuotaEvaluatedAt = evaluatedAt

	if _, err := store.Store(context.Background(), batch); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if sequencer.request.QuotaAdmission != quota {
		t.Fatal("visibility reservation did not receive the store quota admission")
	}
	if !sequencer.request.QuotaEvaluatedAt.Equal(evaluatedAt) {
		t.Fatalf(
			"visibility quota evaluation time = %v, want %v",
			sequencer.request.QuotaEvaluatedAt,
			evaluatedAt,
		)
	}
}

func TestStoreMapsVisibilityQuotaDenial(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name           string
		kind           ingestquota.ScopeKind
		wantThrottle   opensplunk.ThrottleReason
		retryAfter     time.Duration
		wantRetryAfter time.Duration
	}{
		{
			name:           "token",
			kind:           ingestquota.ScopeKindToken,
			wantThrottle:   opensplunk.ThrottleReason_THROTTLE_REASON_TOKEN_QUOTA,
			retryAfter:     37 * time.Second,
			wantRetryAfter: 37 * time.Second,
		},
		{
			name:           "index",
			kind:           ingestquota.ScopeKindIndex,
			wantThrottle:   opensplunk.ThrottleReason_THROTTLE_REASON_INDEX_QUOTA,
			retryAfter:     2 * time.Hour,
			wantRetryAfter: time.Hour,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sequencer := &fakeVisibilitySequencer{reserveErr: &ingestquota.ExceededError{
				Scope: ingestquota.ScopeKey{
					Kind:     test.kind,
					TenantID: "tenant",
					Identity: "scope",
				},
				RetryAfter: test.retryAfter,
			}}
			connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)

			_, err := store.Store(context.Background(), validStoreBatch())
			var transient *ingest.TransientStoreError
			if !errors.As(err, &transient) {
				t.Fatalf("Store error = %v, want TransientStoreError", err)
			}
			if transient.Reason != opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED {
				t.Fatalf("retry reason = %v, want RATE_LIMITED", transient.Reason)
			}
			if transient.ThrottleReason != test.wantThrottle {
				t.Fatalf("throttle reason = %v, want %v", transient.ThrottleReason, test.wantThrottle)
			}
			if transient.RetryAfter != test.wantRetryAfter {
				t.Fatalf("retry after = %v, want %v", transient.RetryAfter, test.wantRetryAfter)
			}
			if connection.prepareCalls != 0 {
				t.Fatalf("ClickHouse prepare calls = %d, want zero", connection.prepareCalls)
			}
		})
	}
}

type quotaCapturingVisibilitySequencer struct {
	*fakeVisibilitySequencer
	request visibility.ReserveRequest
}

func (sequencer *quotaCapturingVisibilitySequencer) Reserve(
	ctx context.Context,
	request visibility.ReserveRequest,
) (visibility.Reservation, error) {
	sequencer.request = request
	return sequencer.fakeVisibilitySequencer.Reserve(ctx, request)
}
