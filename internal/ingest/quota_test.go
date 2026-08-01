package ingest

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"google.golang.org/protobuf/proto"
)

func TestProcessBatchBuildsAtomicAcceptedEventQuotaAdmission(t *testing.T) {
	t.Parallel()

	authorization := Authorization{
		SubjectID:   "token-1",
		TenantID:    "tenant-a",
		CollectorID: "collector-a",
		TokenRateLimits: ingestquota.Limits{
			MaxEventsPerSecond:            10,
			MaxUncompressedBytesPerSecond: 10_000,
		},
		AuthorizedIndexes: []IndexPolicy{
			{
				Name: "main", Version: 1, DefaultSourcetype: "policy:default",
				IngestionRateLimits: ingestquota.Limits{
					MaxEventsPerSecond:            5,
					MaxUncompressedBytesPerSecond: 5_000,
				},
			},
			{
				Name: "audit", Version: 1,
				IngestionRateLimits: ingestquota.Limits{
					MaxEventsPerSecond:            2,
					MaxUncompressedBytesPerSecond: 2_000,
				},
			},
		},
	}
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{
			Accepted:    uint32(len(batch.Events)),
			CommittedAt: validationTestNow,
		}, nil
	})
	config := testServiceConfig()
	config.SessionManager = newTestCollectorSessionManager(
		AuthorizerFunc(func(context.Context, string) (Authorization, error) {
			return authorization, nil
		}),
	)
	service, err := NewService(
		config,
		AuthorizerFunc(func(context.Context, string) (Authorization, error) {
			return authorization, nil
		}),
		store,
	)
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	resolved, ok := service.resolveAuthorizedIndexPolicies(
		authorization.AuthorizedIndexes,
		validationTestNow,
	)
	if !ok {
		t.Fatal("resolveAuthorizedIndexPolicies() failed")
	}
	authorization.AuthorizedIndexes = resolved.policies
	state := &streamState{
		collectorID:   "collector-a",
		protocolMajor: 1,
		protocolMinor: 0,
		authorization: authorization,
		indexPolicies: resolved.byName,
	}
	mainEvent := validTestEvent("event-main", "main")
	mainEvent.Sourcetype = ""
	mainEvent.Raw = []byte(`{"token":"source-secret","status":200}`)
	mainEvent.Fields = object(
		stringField("token", "source-secret"),
		stringField("status", "200"),
	)
	mainSource := proto.Clone(mainEvent).(*opensplunkv1.LogEvent)
	auditEvent := validTestEvent("event-audit", "audit")
	unauthorizedEvent := validTestEvent("event-secret", "secret")
	batch := validTestBatch(
		"collector-a",
		"batch-quota-charge",
		1,
		mainEvent,
		auditEvent,
		unauthorizedEvent,
	)
	response, err := service.processBatch(
		context.Background(),
		batch,
		state,
		validationTestNow,
	)
	if err != nil || response.GetBatchAck().GetAcceptedEventCount() != 2 ||
		len(response.GetBatchAck().GetRejectedEvents()) != 1 {
		t.Fatalf("processBatch() = (%+v, %v)", response, err)
	}
	if stored.QuotaAdmission == nil || stored.QuotaEvaluatedAt != validationTestNow ||
		len(stored.QuotaAdmission.Charges) != 3 {
		t.Fatalf("stored quota admission = %+v", stored.QuotaAdmission)
	}
	if !proto.Equal(mainEvent, mainSource) {
		t.Fatalf("source event was mutated:\n got: %+v\nwant: %+v", mainEvent, mainSource)
	}
	var storedMain *opensplunkv1.LogEvent
	for _, event := range stored.Events {
		if event.Event.GetEventId() == mainEvent.GetEventId() {
			storedMain = event.Event
			break
		}
	}
	if storedMain == nil || storedMain.GetSourcetype() != "policy:default" ||
		proto.Equal(storedMain, mainSource) ||
		bytes.Contains(storedMain.GetRaw(), []byte("source-secret")) {
		t.Fatalf("normalized main event = %+v, source = %+v", storedMain, mainSource)
	}
	mainBytes := uint64(proto.Size(mainSource))
	auditBytes := UncompressedEventBytes([]*opensplunkv1.LogEvent{auditEvent})
	want := []ingestquota.Charge{
		{
			Scope: ingestquota.ScopeKey{
				Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "token-1",
			},
			Limits: authorization.TokenRateLimits,
			Events: 2, UncompressedBytes: mainBytes + auditBytes,
		},
		{
			Scope: ingestquota.ScopeKey{
				Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "audit",
			},
			Limits: authorization.AuthorizedIndexes[0].IngestionRateLimits,
			Events: 1, UncompressedBytes: auditBytes,
		},
		{
			Scope: ingestquota.ScopeKey{
				Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "main",
			},
			Limits: authorization.AuthorizedIndexes[1].IngestionRateLimits,
			Events: 1, UncompressedBytes: mainBytes,
		},
	}
	for index, charge := range stored.QuotaAdmission.Charges {
		if charge.State != nil || charge.Scope != want[index].Scope ||
			charge.Limits != want[index].Limits ||
			charge.Events != want[index].Events ||
			charge.UncompressedBytes != want[index].UncompressedBytes {
			t.Fatalf("charge %d = %+v, want %+v", index, charge, want[index])
		}
	}
}

func TestCollectRefreshesQuotaRatesBeforeNextStore(t *testing.T) {
	t.Parallel()

	initialTokenLimits := ingestquota.Limits{
		MaxEventsPerSecond:            11,
		MaxUncompressedBytesPerSecond: 1_100,
	}
	initialIndexLimits := ingestquota.Limits{
		MaxEventsPerSecond:            7,
		MaxUncompressedBytesPerSecond: 700,
	}
	tests := []struct {
		name         string
		updatedToken ingestquota.Limits
		updatedIndex ingestquota.Limits
		wantToken    ingestquota.Limits
		wantIndex    ingestquota.Limits
	}{
		{
			name: "token rate only",
			updatedToken: ingestquota.Limits{
				MaxEventsPerSecond:            23,
				MaxUncompressedBytesPerSecond: 2_300,
			},
			updatedIndex: initialIndexLimits,
			wantToken: ingestquota.Limits{
				MaxEventsPerSecond:            23,
				MaxUncompressedBytesPerSecond: 2_300,
			},
			wantIndex: initialIndexLimits,
		},
		{
			name:         "index rate only",
			updatedToken: initialTokenLimits,
			updatedIndex: ingestquota.Limits{
				MaxEventsPerSecond:            13,
				MaxUncompressedBytesPerSecond: 1_300,
			},
			wantToken: initialTokenLimits,
			wantIndex: ingestquota.Limits{
				MaxEventsPerSecond:            13,
				MaxUncompressedBytesPerSecond: 1_300,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			initial := Authorization{
				SubjectID:       "token-1",
				TenantID:        "tenant-a",
				CollectorID:     "collector-a",
				TokenRateLimits: initialTokenLimits,
				AuthorizedIndexes: []IndexPolicy{{
					Name: "main", Version: 1, RetentionPeriod: time.Hour,
					IngestionRateLimits: initialIndexLimits,
				}},
			}
			updated := cloneAuthorization(initial)
			updated.TokenRateLimits = test.updatedToken
			updated.AuthorizedIndexes[0].IngestionRateLimits = test.updatedIndex
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return initial, nil
			})
			manager := newTestCollectorSessionManager(authorizer)
			manager.admitFunc = func(
				_ context.Context,
				_ string,
				request CollectorSessionAdmissionRequest,
			) (CollectorSessionAdmission, error) {
				return manager.admissionFor(initial, request), nil
			}
			manager.authorizeFunc = func(
				context.Context,
				string,
				collectorfleet.Lease,
				time.Time,
			) (Authorization, error) {
				return updated, nil
			}
			stored := make(chan StoreBatch, 1)
			store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
				stored <- batch
				return StoreResult{
					Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow,
				}, nil
			})
			config := testServiceConfig()
			config.SessionManager = manager
			harness := newServiceHarness(t, config, authorizer, store)
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1, 1, 0)
			_ = recvResponse(t, stream)

			batch := validTestBatch(
				"collector-a",
				"batch-refreshed-quota",
				1,
				validTestEvent("event-refreshed-quota", "main"),
			)
			if err := stream.Send(batchRequest(2, batch)); err != nil {
				t.Fatal(err)
			}
			if ack := recvResponse(t, stream).GetBatchAck(); ack == nil ||
				ack.GetAcceptedEventCount() != 1 {
				t.Fatalf("batch acknowledgment = %#v", ack)
			}

			got := <-stored
			if got.QuotaAdmission == nil || len(got.QuotaAdmission.Charges) != 2 {
				t.Fatalf("quota admission = %+v", got.QuotaAdmission)
			}
			if limits := got.QuotaAdmission.Charges[0].Limits; limits != test.wantToken {
				t.Fatalf("token quota limits = %+v, want %+v", limits, test.wantToken)
			}
			if limits := got.QuotaAdmission.Charges[1].Limits; limits != test.wantIndex {
				t.Fatalf("index quota limits = %+v, want %+v", limits, test.wantIndex)
			}
		})
	}
}

func TestCollectSequencesQuotaRetryThenThrottle(t *testing.T) {
	t.Parallel()

	store := EventStoreFunc(func(context.Context, StoreBatch) (StoreResult, error) {
		return StoreResult{}, &TransientStoreError{
			Err:            errors.New("index quota"),
			Reason:         opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED,
			RetryAfter:     2 * time.Hour,
			ThrottleReason: opensplunkv1.ThrottleReason_THROTTLE_REASON_INDEX_QUOTA,
		}
	})
	config := testServiceConfig()
	var clockMu sync.Mutex
	clockNow := validationTestNow
	config.Clock = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		now := clockNow
		clockNow = clockNow.Add(-time.Millisecond)
		return now
	}
	harness := newServiceHarness(
		t,
		config,
		staticTestAuthorizer(),
		store,
	)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1, 1, 0)
	_ = recvResponse(t, stream)
	batch := validTestBatch(
		"collector-a",
		"batch-quota-throttle",
		1,
		validTestEvent("event-a", "main"),
	)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	retryResponse := recvResponse(t, stream)
	retry := retryResponse.GetRetryBatch()
	if retryResponse.GetStreamSequence() != 2 || retry == nil ||
		retry.GetReason() != opensplunkv1.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED ||
		retry.GetRetryAfter().AsDuration() != ingestquota.MaximumRetryAfter ||
		retry.GetMessage() != "ingestion rate limit reached" {
		t.Fatalf("retry response = %+v", retryResponse)
	}
	throttleResponse := recvResponse(t, stream)
	throttle := throttleResponse.GetThrottle()
	if throttleResponse.GetStreamSequence() != 3 || throttle == nil ||
		throttle.GetReason() != opensplunkv1.ThrottleReason_THROTTLE_REASON_INDEX_QUOTA ||
		throttle.GetMinimumSendDelay().AsDuration() != ingestquota.MaximumRetryAfter ||
		throttleResponse.GetSentAt() == nil || throttle.GetEffectiveUntil() == nil ||
		throttle.GetEffectiveUntil().AsTime().Sub(throttleResponse.GetSentAt().AsTime()) !=
			ingestquota.MaximumRetryAfter ||
		throttle.GetMaxInFlightBatches() != 0 ||
		throttle.GetMaxBatchEvents() != 0 ||
		throttle.GetMaxBatchBytes() != 0 {
		t.Fatalf("throttle response = %+v", throttleResponse)
	}
}
