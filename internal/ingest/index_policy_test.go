package ingest

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCollectAppliesEachPerIndexLimitToOnlyThatIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limits     func(*opensplunkv1.LogEvent) IndexLimits
		mutate     func(*opensplunkv1.LogEvent)
		rejectCode opensplunkv1.EventRejectionCode
	}{
		{
			name: "encoded event bytes",
			limits: func(event *opensplunkv1.LogEvent) IndexLimits {
				return IndexLimits{MaxEventBytes: uint64(proto.Size(event) - 1)}
			},
			mutate: func(event *opensplunkv1.LogEvent) {
				event.Raw = []byte(strings.Repeat("x", 1024))
			},
			rejectCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE,
		},
		{
			name: "field count",
			limits: func(*opensplunkv1.LogEvent) IndexLimits {
				return IndexLimits{MaxFieldCount: 1}
			},
			mutate: func(event *opensplunkv1.LogEvent) {
				event.Fields = object(stringField("one", "1"), stringField("two", "2"))
			},
			rejectCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
		},
		{
			name: "nesting depth",
			limits: func(*opensplunkv1.LogEvent) IndexLimits {
				return IndexLimits{MaxNestingDepth: 1}
			},
			mutate: func(event *opensplunkv1.LogEvent) {
				event.Fields = object(objectField("nested", object(stringField("leaf", "value"))))
			},
			rejectCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP,
		},
		{
			name: "future skew",
			limits: func(*opensplunkv1.LogEvent) IndexLimits {
				return IndexLimits{MaximumFutureSkew: time.Second}
			},
			mutate: func(event *opensplunkv1.LogEvent) {
				event.EventTime = timestamppb.New(validationTestNow.Add(2 * time.Second))
				event.CollectedAt = timestamppb.New(validationTestNow.Add(2 * time.Second))
			},
			rejectCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
		{
			name: "event age",
			limits: func(*opensplunkv1.LogEvent) IndexLimits {
				return IndexLimits{MaximumEventAge: time.Hour}
			},
			mutate: func(event *opensplunkv1.LogEvent) {
				event.EventTime = timestamppb.New(validationTestNow.Add(-2 * time.Hour))
				event.CollectedAt = timestamppb.New(validationTestNow.Add(-2 * time.Hour))
			},
			rejectCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			tight := validTestEvent("event-tight", "tight")
			loose := validTestEvent("event-loose", "loose")
			test.mutate(tight)
			test.mutate(loose)
			authorization := Authorization{
				SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
				AuthorizedIndexes: []IndexPolicy{
					{Name: "tight", Version: 1, RetentionPeriod: time.Hour, Limits: test.limits(tight)},
					{Name: "loose", Version: 1, RetentionPeriod: 2 * time.Hour},
				},
			}
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return authorization, nil
			})
			var stored StoreBatch
			store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
				stored = batch
				return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
			})
			harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_ = recvResponse(t, stream)
			batchID := "batch-limit-" + strings.ReplaceAll(test.name, " ", "-")
			if err := stream.Send(batchRequest(2, validTestBatch(
				"collector-a", batchID, 1, tight, loose,
			))); err != nil {
				t.Fatal(err)
			}
			ack := recvResponse(t, stream).GetBatchAck()
			if ack == nil || ack.GetAcceptedEventCount() != 1 || len(ack.GetRejectedEvents()) != 1 {
				t.Fatalf("batch acknowledgment = %#v", ack)
			}
			if got := ack.GetRejectedEvents()[0].GetCode(); got != test.rejectCode {
				t.Fatalf("tight-index rejection = %v, want %v", got, test.rejectCode)
			}
			if len(stored.Events) != 1 || stored.Events[0].Event.GetIndexName() != "loose" {
				t.Fatalf("stored events = %#v, want only loose-index event", stored.Events)
			}
		})
	}
}

func TestResolveAuthorizedIndexPoliciesSortsDetachesAndOnlyTightens(t *testing.T) {
	t.Parallel()

	config := testServiceConfig()
	config.Limits.MaxBatchBytes = 8 << 10
	config.Limits.MaxEventBytes = 2 << 10
	config.Limits.MaxFields = 20
	config.Limits.MaxNestingDepth = 10
	config.Limits.MaxFutureSkew = time.Minute
	config.Limits.MaxEventAge = 24 * time.Hour
	authorizer := staticTestAuthorizer()
	config = withTestSessionManager(config, authorizer)
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	input := []IndexPolicy{
		{
			Name: "loosen", Version: 3, RetentionPeriod: 2 * time.Hour,
			DefaultSourcetype: "explicit:type",
			Limits: IndexLimits{
				MaxEventBytes: 4 << 10, MaxFieldCount: 40, MaxNestingDepth: 12,
				MaximumFutureSkew: 2 * time.Minute, MaximumEventAge: 48 * time.Hour,
			},
		},
		{Name: "inherit", Version: 2},
		{
			Name: "tight", Version: 1, RetentionPeriod: time.Hour,
			Limits: IndexLimits{
				MaxEventBytes: 1 << 10, MaxFieldCount: 10, MaxNestingDepth: 5,
				MaximumFutureSkew: 30 * time.Second, MaximumEventAge: 12 * time.Hour,
			},
		},
	}
	resolved, ok := service.resolveAuthorizedIndexPolicies(input, validationTestNow)
	if !ok {
		t.Fatal("resolveAuthorizedIndexPolicies rejected valid policies")
	}
	if got, want := authorizedIndexPolicyNames(resolved.policies), []string{"inherit", "loosen", "tight"}; !equalStrings(got, want) {
		t.Fatalf("resolved names = %v, want %v", got, want)
	}
	if got := resolved.byName["inherit"].validator.limits; got != config.Limits {
		t.Fatalf("inherited limits = %+v, want %+v", got, config.Limits)
	}
	if got := resolved.byName["loosen"].validator.limits; got != config.Limits {
		t.Fatalf("attempted-loosen limits = %+v, want capped %+v", got, config.Limits)
	}
	wantTight := config.Limits
	wantTight.MaxEventBytes = 1 << 10
	wantTight.MaxFields = 10
	wantTight.MaxNestingDepth = 5
	wantTight.MaxFutureSkew = 30 * time.Second
	wantTight.MaxEventAge = 12 * time.Hour
	if got := resolved.byName["tight"].validator.limits; got != wantTight {
		t.Fatalf("tight limits = %+v, want %+v", got, wantTight)
	}
	if got := resolved.byName["inherit"].retentionPeriod; got != config.DefaultIndexRetention {
		t.Fatalf("inherited retention = %v, want %v", got, config.DefaultIndexRetention)
	}

	input[0].Name = "mutated"
	input[0].DefaultSourcetype = "mutated"
	input[0].Limits.MaxEventBytes = 1
	if _, exists := resolved.byName["mutated"]; exists ||
		resolved.byName["loosen"].defaultSourcetype != "explicit:type" ||
		resolved.byName["loosen"].validator.limits.MaxEventBytes != config.Limits.MaxEventBytes {
		t.Fatalf("resolved policies alias caller input: %#v", resolved)
	}
}

func TestCollectAppliesDefaultSourcetypeAndSnapshotsExactRetention(t *testing.T) {
	t.Parallel()

	authorization := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{
			{Name: "main", Version: 7, RetentionPeriod: time.Hour, DefaultSourcetype: "policy:json"},
			{Name: "audit", Version: 9, DefaultSourcetype: "policy:audit"},
		},
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	defaulted := validTestEvent("event-defaulted", "main")
	defaulted.Sourcetype = ""
	explicit := validTestEvent("event-explicit", "main")
	explicit.Sourcetype = "explicit:type"
	audit := validTestEvent("event-audit", "audit")
	audit.Sourcetype = ""
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "batch-policy-snapshot", 1, defaulted, explicit, audit,
	))); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 3 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
	if got := []string{
		stored.Events[0].Event.GetSourcetype(),
		stored.Events[1].Event.GetSourcetype(),
		stored.Events[2].Event.GetSourcetype(),
	}; !equalStrings(got, []string{"policy:json", "explicit:type", "policy:audit"}) {
		t.Fatalf("stored sourcetypes = %v", got)
	}
	if got, want := stored.RetentionByIndex["main"], time.Hour; got != want {
		t.Fatalf("main retention = %v, want %v", got, want)
	}
	if got, want := stored.RetentionByIndex["audit"], DefaultIndexRetention; got != want {
		t.Fatalf("audit retention = %v, want inherited %v", got, want)
	}
	if len(stored.RetentionByIndex) != 2 {
		t.Fatalf("retention snapshot = %#v, want exactly accepted indexes", stored.RetentionByIndex)
	}
}

func TestCollectUsesServerReceiveTimeForPerIndexTimestampLimits(t *testing.T) {
	t.Parallel()

	authorization := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{
			Name: "main", Version: 1, RetentionPeriod: time.Hour,
			Limits: IndexLimits{MaximumEventAge: time.Hour},
		}},
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	event := validTestEvent("event-reference", "main")
	event.EventTime = timestamppb.New(validationTestNow.Add(-30 * time.Minute))
	event.CollectedAt = timestamppb.New(validationTestNow.Add(-30 * time.Minute))
	batch := validTestBatch("collector-a", "batch-reference", 1, event)
	// The collector created this batch well before its event was collected. An
	// event-relative check against created_at would misclassify both event
	// timestamps as far-future; the server receive boundary is authoritative.
	batch.CreatedAt = timestamppb.New(validationTestNow.Add(-2 * time.Hour))
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	if ack := recvResponse(t, stream).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
}

func TestCollectKeepsAuthorizationBoundaryTimeThroughPolicyRefresh(t *testing.T) {
	t.Parallel()

	boundary := validationTestNow.Truncate(time.Millisecond)
	authorization := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{
			Name: "main", Version: 1, RetentionPeriod: time.Hour,
			Limits: IndexLimits{MaximumEventAge: time.Hour},
		}},
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(authorization, request), nil
	}
	var clockNanos atomic.Int64
	clockNanos.Store(boundary.UnixNano())
	var checkedAtNanos atomic.Int64
	manager.authorizeFunc = func(
		_ context.Context,
		_ string,
		_ collectorfleet.Lease,
		checkedAt time.Time,
	) (Authorization, error) {
		checkedAtNanos.Store(checkedAt.UnixNano())
		clockNanos.Store(boundary.Add(2 * time.Hour).UnixNano())
		return authorization, nil
	}
	config := testServiceConfig()
	config.Clock = func() time.Time {
		return time.Unix(0, clockNanos.Load()).UTC()
	}
	config.SessionManager = manager
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: batch.ReceivedAt}, nil
	})
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	event := validTestEvent("event-boundary", "main")
	event.EventTime = timestamppb.New(boundary.Add(-30 * time.Minute))
	event.CollectedAt = timestamppb.New(boundary.Add(-30 * time.Minute))
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "batch-boundary", 1, event,
	))); err != nil {
		t.Fatal(err)
	}
	if ack := recvResponse(t, stream).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
	if checkedAt := time.Unix(0, checkedAtNanos.Load()).UTC(); !checkedAt.Equal(boundary) {
		t.Fatalf("policy checked at %v, want boundary %v", checkedAt, boundary)
	}
	if !stored.ReceivedAt.Equal(boundary) || len(stored.Events) != 1 ||
		!stored.Events[0].IndexTime.Equal(boundary) {
		t.Fatalf("stored boundary = received %v, events %#v; want %v", stored.ReceivedAt, stored.Events, boundary)
	}
}

func TestCollectUsesOneServerTimeSnapshotAcrossHeartbeatBoundary(t *testing.T) {
	t.Parallel()

	boundary := validationTestNow.Truncate(time.Millisecond)
	authorization := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(authorization, request), nil
	}
	var checkedAt time.Time
	manager.authorizeFunc = func(
		_ context.Context,
		_ string,
		_ collectorfleet.Lease,
		at time.Time,
	) (Authorization, error) {
		checkedAt = at
		return authorization, nil
	}
	heartbeats := make(chan collectorfleet.Heartbeat, 1)
	manager.heartbeatFunc = func(
		_ context.Context,
		_ collectorfleet.Lease,
		heartbeat collectorfleet.Heartbeat,
	) (bool, error) {
		heartbeats <- heartbeat
		return true, nil
	}
	var measure atomic.Bool
	var measuredCalls atomic.Int64
	config := testServiceConfig()
	config.Clock = func() time.Time {
		if !measure.Load() {
			return boundary
		}
		if measuredCalls.Add(1) == 1 {
			return boundary
		}
		return boundary.Add(2 * 365 * 24 * time.Hour)
	}
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	measure.Store(true)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(boundary),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: &opensplunkv1.CollectorHeartbeat{
			CollectorId: "collector-a", InstanceId: "instance-a", ObservedAt: timestamppb.New(boundary),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case heartbeat := <-heartbeats:
		if measuredCalls.Load() != 1 || !checkedAt.Equal(boundary) || !heartbeat.ReceivedAt.Equal(boundary) {
			t.Fatalf(
				"heartbeat boundary = calls:%d auth:%v persisted:%v, want one/%v",
				measuredCalls.Load(),
				checkedAt,
				heartbeat.ReceivedAt,
				boundary,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat was not persisted")
	}
	measure.Store(false)
	_ = stream.CloseSend()
}

func TestCollectMutableIndexAuthorityFailureBlocksHeartbeatBeforePersistence(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "no active index", err: ErrNoActiveIndexAuthority, wantCode: codes.Unauthenticated},
		{name: "invalid index policy", err: ErrInvalidIndexAuthority, wantCode: codes.Unavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			authorization := Authorization{
				SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
				AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
			}
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return authorization, nil
			})
			manager := newTestCollectorSessionManager(authorizer)
			manager.admitFunc = func(
				_ context.Context,
				_ string,
				request CollectorSessionAdmissionRequest,
			) (CollectorSessionAdmission, error) {
				return manager.admissionFor(authorization, request), nil
			}
			manager.authorizeFunc = func(
				context.Context,
				string,
				collectorfleet.Lease,
				time.Time,
			) (Authorization, error) {
				partial := authorization
				partial.AuthorizedIndexes = nil
				return partial, test.err
			}
			var persisted atomic.Uint32
			manager.heartbeatFunc = func(
				context.Context,
				collectorfleet.Lease,
				collectorfleet.Heartbeat,
			) (bool, error) {
				persisted.Add(1)
				return true, nil
			}
			config := testServiceConfig()
			config.SessionManager = manager
			harness := newServiceHarness(t, config, authorizer, acceptingStore())
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_ = recvResponse(t, stream)
			if err := stream.Send(&opensplunkv1.CollectRequest{
				StreamSequence: 2,
				SentAt:         timestamppb.New(validationTestNow),
				Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: &opensplunkv1.CollectorHeartbeat{
					CollectorId: "collector-a", InstanceId: "instance-a", ObservedAt: timestamppb.New(validationTestNow),
				}},
			}); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != test.wantCode {
				t.Fatalf("heartbeat = (%#v, %v), want nil/%v", response, err, test.wantCode)
			}
			if persisted.Load() != 0 {
				t.Fatalf("heartbeat persistence calls = %d, want 0", persisted.Load())
			}
		})
	}
}

func TestValidatorDefaultSourcetypeNeverMutatesInputAndPreservesExplicitValue(t *testing.T) {
	t.Parallel()

	validator := newTestValidator(t, DefaultLimits())
	for _, test := range []struct {
		name string
		got  string
		want string
	}{
		{name: "omitted", got: "", want: "policy:json"},
		{name: "explicit", got: "explicit:type", want: "explicit:type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			event := validTestEvent("event-"+test.name, "main")
			event.Sourcetype = test.got
			stored, eventErr := validator.ValidateAndNormalizeEvent(event, EventContext{
				ReceivedAt: validationTestNow, DefaultSourcetype: "policy:json",
			})
			if eventErr != nil {
				t.Fatal(eventErr)
			}
			if got := stored.Event.GetSourcetype(); got != test.want {
				t.Fatalf("stored sourcetype = %q, want %q", got, test.want)
			}
			if got := event.GetSourcetype(); got != test.got {
				t.Fatalf("input sourcetype mutated to %q, want %q", got, test.got)
			}
		})
	}
}

func TestValidatorDefaultSourcetypeRechecksEffectiveEventSize(t *testing.T) {
	t.Parallel()

	event := validTestEvent("event-default-size", "main")
	event.Sourcetype = ""
	limits := DefaultLimits()
	limits.MaxEventBytes = uint64(proto.Size(event))
	validator := newTestValidator(t, limits)
	if _, rejection := validator.ValidateAndNormalizeEvent(event, EventContext{
		ReceivedAt:        validationTestNow,
		DefaultSourcetype: "policy:default-that-expands-the-event",
	}); rejection == nil ||
		rejection.Code != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE ||
		rejection.Violations[0].GetCode() != "event_too_large_after_redaction" {
		t.Fatalf("default-expanded rejection = %#v", rejection)
	}
	if event.GetSourcetype() != "" {
		t.Fatalf("rejected input sourcetype mutated to %q", event.GetSourcetype())
	}
}

func TestCollectAppliesIndexTimestampPolicyFromServerReceiveTime(t *testing.T) {
	t.Parallel()

	authorization := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{
			{Name: "tight", Version: 1, Limits: IndexLimits{MaximumEventAge: time.Hour}},
			{Name: "loose", Version: 1},
		},
	}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return authorization, nil
	})
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	collectorReference := validationTestNow.Add(-2 * time.Hour)
	tight := validTestEvent("event-tight-time", "tight")
	tight.EventTime = timestamppb.New(collectorReference)
	tight.CollectedAt = timestamppb.New(collectorReference)
	loose := validTestEvent("event-loose-time", "loose")
	loose.EventTime = timestamppb.New(collectorReference)
	loose.CollectedAt = timestamppb.New(collectorReference)
	batch := validTestBatch("collector-a", "batch-server-time-policy", 1, tight, loose)
	batch.CreatedAt = timestamppb.New(collectorReference)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 || len(ack.GetRejectedEvents()) != 1 ||
		ack.GetRejectedEvents()[0].GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_TIMESTAMP ||
		ack.GetRejectedEvents()[0].GetViolations()[0].GetCode() != "timestamp_too_old" {
		t.Fatalf("server-time policy acknowledgment = %#v", ack)
	}
	if len(stored.Events) != 1 || stored.Events[0].Event.GetEventId() != "event-loose-time" {
		t.Fatalf("stored events = %#v", stored.Events)
	}
}

func TestNewServiceValidatesDefaultIndexRetention(t *testing.T) {
	t.Parallel()

	authorizer := staticTestAuthorizer()
	for name, retention := range map[string]time.Duration{
		"negative":             -time.Millisecond,
		"submillisecond":       time.Nanosecond,
		"past storage horizon": 8_000_000_000 * time.Second,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			config := withTestSessionManager(testServiceConfig(), authorizer)
			config.DefaultIndexRetention = retention
			if _, err := NewService(config, authorizer, acceptingStore()); err == nil {
				t.Fatal("NewService accepted invalid default index retention")
			}
		})
	}
}

func TestCollectClassifiesInvalidAndUnauthorizedIndexesBeforeDeepValidation(t *testing.T) {
	t.Parallel()

	authorizer := staticTestAuthorizer()
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{Accepted: uint32(len(batch.Events)), CommittedAt: validationTestNow}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	unauthorized := &opensplunkv1.LogEvent{EventId: "shared-id", IndexName: "forbidden"}
	duplicate := validTestEvent("shared-id", "main")
	invalid := &opensplunkv1.LogEvent{EventId: "invalid-index", IndexName: "Invalid Name"}
	invalidID := &opensplunkv1.LogEvent{EventId: " bad ", IndexName: "Invalid Name"}
	valid := validTestEvent("event-valid", "main")
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "batch-index-precheck", 1,
		unauthorized, duplicate, invalid, invalidID, valid,
	))); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 || len(ack.GetRejectedEvents()) != 4 {
		t.Fatalf("batch acknowledgment = %#v", ack)
	}
	if got := ack.GetRejectedEvents()[0].GetCode(); got != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX {
		t.Fatalf("unauthorized event rejection = %v", got)
	}
	if got := ack.GetRejectedEvents()[1]; got.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID ||
		got.GetViolations()[0].GetCode() != "duplicate_event_id" {
		t.Fatalf("duplicate event rejection = %#v", got)
	}
	if got := ack.GetRejectedEvents()[2].GetCode(); got != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX {
		t.Fatalf("invalid-index event rejection = %v", got)
	}
	if got := ack.GetRejectedEvents()[3].GetCode(); got != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID {
		t.Fatalf("invalid-ID event rejection = %v", got)
	}
	if len(stored.Events) != 1 || stored.Events[0].Event.GetEventId() != "event-valid" {
		t.Fatalf("stored events = %#v", stored.Events)
	}
}

func TestCollectRefreshesResolvedIndexPolicyAtEveryBatchBoundary(t *testing.T) {
	t.Parallel()

	initial := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{
			Name: "main", Version: 1, RetentionPeriod: time.Hour,
		}},
	}
	updated := cloneAuthorization(initial)
	updated.AuthorizedIndexes[0].Version = 2
	updated.AuthorizedIndexes[0].RetentionPeriod = 2 * time.Hour
	updated.AuthorizedIndexes[0].Limits.MaxFieldCount = 1
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
	var refreshes int
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		refreshes++
		if refreshes == 1 {
			return initial, nil
		}
		return updated, nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	store := &recoverableTestStore{}
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)

	first := validTestEvent("event-first", "main")
	first.Fields = object(stringField("one", "1"), stringField("two", "2"))
	if err := stream.Send(batchRequest(2, validTestBatch("collector-a", "batch-first-policy", 1, first))); err != nil {
		t.Fatal(err)
	}
	if ack := recvResponse(t, stream).GetBatchAck(); ack == nil || ack.GetAcceptedEventCount() != 1 {
		t.Fatalf("first policy acknowledgment = %#v", ack)
	}
	second := validTestEvent("event-second", "main")
	second.Fields = object(stringField("one", "1"), stringField("two", "2"))
	if err := stream.Send(batchRequest(3, validTestBatch("collector-a", "batch-second-policy", 2, second))); err != nil {
		t.Fatal(err)
	}
	if reject := recvResponse(t, stream).GetBatchReject(); reject == nil ||
		reject.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS {
		t.Fatalf("refreshed policy response = %#v", reject)
	}
	if refreshes != 2 || store.storeCalls != 1 || store.rejectCalls != 1 ||
		store.first.RetentionByIndex["main"] != time.Hour {
		t.Fatalf("refreshes=%d store=%+v", refreshes, store)
	}
}

func TestCollectDurableRetryPrecedesMutableIndexAuthorityFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		authErr  error
		wantCode codes.Code
	}{
		{name: "no active index", authErr: ErrNoActiveIndexAuthority, wantCode: codes.Unauthenticated},
		{name: "invalid index policy", authErr: ErrInvalidIndexAuthority, wantCode: codes.Unavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			initial := Authorization{
				SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
				AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
			}
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
			var refreshes int
			manager.authorizeFunc = func(
				context.Context,
				string,
				collectorfleet.Lease,
				time.Time,
			) (Authorization, error) {
				refreshes++
				if refreshes == 1 {
					return initial, nil
				}
				partial := initial
				partial.AuthorizedIndexes = nil
				return partial, test.authErr
			}
			config := testServiceConfig()
			config.SessionManager = manager
			store := &recoverableTestStore{}
			harness := newServiceHarness(t, config, authorizer, store)
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_ = recvResponse(t, stream)

			batch := validTestBatch(
				"collector-a",
				"batch-policy-replay",
				1,
				validTestEvent("event-main", "main"),
				validTestEvent("event-audit", "audit"),
			)
			if err := stream.Send(batchRequest(2, batch)); err != nil {
				t.Fatal(err)
			}
			first := recvResponse(t, stream).GetBatchAck()
			if first == nil || first.GetAcceptedEventCount() != 1 || len(first.GetRejectedEvents()) != 1 {
				t.Fatalf("first acknowledgment = %#v", first)
			}

			if err := stream.Send(batchRequest(3, batch)); err != nil {
				t.Fatal(err)
			}
			replayed := recvResponse(t, stream).GetBatchAck()
			if replayed == nil || replayed.GetAcceptedEventCount() != 0 ||
				replayed.GetDuplicateEventCount() != 1 || len(replayed.GetRejectedEvents()) != 1 ||
				replayed.GetRejectedEvents()[0].GetEventId() != "event-audit" {
				t.Fatalf("replayed acknowledgment = %#v", replayed)
			}

			fresh := validTestBatch(
				"collector-a", "batch-policy-miss", 2, validTestEvent("event-fresh", "main"),
			)
			if err := stream.Send(batchRequest(4, fresh)); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != test.wantCode {
				t.Fatalf("fresh batch = (%#v, %v), want nil/%v", response, err, test.wantCode)
			}
			if refreshes != 3 || store.storeCalls != 1 || store.lookupCalls != 3 {
				t.Fatalf(
					"refreshes=%d store calls=%d lookup calls=%d, want 3/1/3",
					refreshes,
					store.storeCalls,
					store.lookupCalls,
				)
			}
		})
	}
}

func TestCollectRejectedRetryPrecedesMutableIndexAuthorityFailure(t *testing.T) {
	t.Parallel()

	initial := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
	}
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
		partial := initial
		partial.AuthorizedIndexes = nil
		return partial, ErrNoActiveIndexAuthority
	}
	batch := validTestBatch(
		"collector-a", "batch-rejected-policy-replay", 1, validTestEvent("event-main", "main"),
	)
	identity, err := batchFingerprint(batch)
	if err != nil {
		t.Fatal(err)
	}
	rejection := batchRejection(
		batch,
		opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE,
		"original durable policy rejection",
		"events",
		"original_policy_limit",
	)
	store := &recoverableTestStore{
		identity: StoreBatchIdentity{
			TenantID:          initial.TenantID,
			CollectorID:       initial.CollectorID,
			BatchID:           batch.GetBatchId(),
			BatchSequence:     batch.GetBatchSequence(),
			SourceBatchSHA256: identity.contentHash,
		},
		result:      StoreResult{BatchRejection: rejection},
		storedState: StoredBatchRejected,
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream, 1)
	_ = recvResponse(t, stream)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	replayed := recvResponse(t, stream).GetBatchReject()
	if !proto.Equal(replayed, rejection) {
		t.Fatalf("replayed rejection = %#v, want %#v", replayed, rejection)
	}
	if store.lookupCalls != 1 || store.rejectCalls != 0 || store.storeCalls != 0 {
		t.Fatalf(
			"durable calls = lookup:%d reject:%d store:%d, want 1/0/0",
			store.lookupCalls,
			store.rejectCalls,
			store.storeCalls,
		)
	}
}

func TestCollectPendingRetryPrecedesMutableIndexAuthorityFailure(t *testing.T) {
	t.Parallel()

	for _, authorityErr := range []error{ErrNoActiveIndexAuthority, ErrInvalidIndexAuthority} {
		t.Run(authorityErr.Error(), func(t *testing.T) {
			t.Parallel()

			initial := Authorization{
				SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
				AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
			}
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
				partial := initial
				partial.AuthorizedIndexes = nil
				return partial, authorityErr
			}
			acknowledged := uint64(1)
			store := &pendingAuthorityTestStore{result: StoreResult{
				Accepted: 1, AcknowledgedThrough: &acknowledged, CommittedAt: validationTestNow,
				OriginalEventCount: 1,
			}}
			config := testServiceConfig()
			config.SessionManager = manager
			harness := newServiceHarness(t, config, authorizer, store)
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_ = recvResponse(t, stream)
			if err := stream.Send(batchRequest(2, validTestBatch(
				"collector-a", "batch-pending-policy", 1, validTestEvent("event-pending", "main"),
			))); err != nil {
				t.Fatal(err)
			}
			ack := recvResponse(t, stream).GetBatchAck()
			if ack == nil || ack.GetAcceptedEventCount() != 1 ||
				store.lookupCalls != 1 || store.resumeCalls != 1 || store.storeCalls != 0 {
				t.Fatalf("pending replay ack=%#v store=%+v", ack, store)
			}
		})
	}
}

func TestCollectFatalOrMalformedLeaseAuthorizationNeverLooksUpDurableBatch(t *testing.T) {
	t.Parallel()

	initial := Authorization{
		SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		AuthorizedIndexes: []IndexPolicy{{Name: "main", Version: 1, RetentionPeriod: time.Hour}},
	}
	tests := []struct {
		name     string
		result   Authorization
		err      error
		wantCode codes.Code
	}{
		{name: "revoked credential", err: ErrUnauthorized, wantCode: codes.Unauthenticated},
		{name: "stale lease", err: ErrCollectorLeaseNotCurrent, wantCode: codes.Aborted},
		{name: "typed error without identity", err: ErrNoActiveIndexAuthority, wantCode: codes.Unavailable},
		{name: "successful empty projection", result: Authorization{
			SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
		}, wantCode: codes.Unavailable},
		{name: "typed error with policies", result: initial, err: ErrNoActiveIndexAuthority, wantCode: codes.Unavailable},
		{name: "typed error with changed subject", result: Authorization{
			SubjectID: "token-2", TenantID: "tenant-a", CollectorID: "collector-a",
		}, err: ErrNoActiveIndexAuthority, wantCode: codes.PermissionDenied},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

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
				return test.result, test.err
			}
			config := testServiceConfig()
			config.SessionManager = manager
			store := &recoverableTestStore{}
			harness := newServiceHarness(t, config, authorizer, store)
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_ = recvResponse(t, stream)
			if err := stream.Send(batchRequest(2, validTestBatch(
				"collector-a", "batch-no-lookup", 1, validTestEvent("event-no-lookup", "main"),
			))); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != test.wantCode {
				t.Fatalf("batch = (%#v, %v), want nil/%v", response, err, test.wantCode)
			}
			if store.lookupCalls != 0 || store.storeCalls != 0 {
				t.Fatalf("durable calls = lookup:%d store:%d, want zero", store.lookupCalls, store.storeCalls)
			}
		})
	}
}

func TestProcessBatchDeferredIndexAuthorityMasksEveryUnprovenDurableOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		store deferredAuthorityTestStore
		setup func(*testing.T, *Service, *streamState, *opensplunkv1.EventBatch)
	}{
		{
			name: "hard envelope rejection",
			setup: func(_ *testing.T, _ *Service, _ *streamState, batch *opensplunkv1.EventBatch) {
				batch.CollectorId = "other-collector"
			},
		},
		{
			name: "fingerprint failure",
			setup: func(_ *testing.T, _ *Service, _ *streamState, batch *opensplunkv1.EventBatch) {
				batch.Events[0].Host = string([]byte{0xff})
			},
		},
		{
			name: "pending identity conflict",
			setup: func(t *testing.T, service *Service, state *streamState, _ *opensplunkv1.EventBatch) {
				other := validTestBatch(
					"collector-a", "other-batch", 1, validTestEvent("other-event", "main"),
				)
				identity, err := batchFingerprint(other)
				if err != nil {
					t.Fatalf("batchFingerprint(other): %v", err)
				}
				if _, rejection, atCapacity := recordBatchIdentity(
					state,
					other.GetBatchSequence(),
					identity,
					validationTestNow,
					service.config.MaxInFlightBatches,
				); rejection != nil || atCapacity {
					t.Fatalf("seed pending identity = rejection:%#v capacity:%t", rejection, atCapacity)
				}
			},
		},
		{name: "lookup failure", store: deferredAuthorityTestStore{lookupErr: errors.New("private lookup failure")}},
		{name: "invalid stored state", store: deferredAuthorityTestStore{lookupState: StoredBatchState(255)}},
		{
			name: "pending resume identity conflict",
			store: deferredAuthorityTestStore{
				lookupState: StoredBatchPending,
				resumeErr: &DurableIdentityConflictError{
					Err: errors.New("private pending identity conflict"),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := test.store
			config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
			service, err := NewService(config, staticTestAuthorizer(), &store)
			if err != nil {
				t.Fatal(err)
			}
			state := testBatchStreamState(service)
			batch := validTestBatch(
				"collector-a", "batch-deferred-authority", 1, validTestEvent("event-one", "main"),
			)
			if test.setup != nil {
				test.setup(t, service, state, batch)
			}
			response, err := service.processBatchWithDeferredAuthority(
				context.Background(),
				batch,
				state,
				validationTestNow,
				ErrNoActiveIndexAuthority,
			)
			if response != nil || status.Code(err) != codes.Unauthenticated {
				t.Fatalf("processBatch = (%#v, %v), want nil/Unauthenticated", response, err)
			}
			if store.storeCalls != 0 {
				t.Fatalf("Store calls = %d, want 0", store.storeCalls)
			}
			switch test.name {
			case "hard envelope rejection", "fingerprint failure", "pending identity conflict":
				if store.lookupCalls != 0 || store.resumeCalls != 0 {
					t.Fatalf("durable calls = lookup:%d resume:%d, want zero", store.lookupCalls, store.resumeCalls)
				}
			case "pending resume identity conflict":
				if store.lookupCalls != 1 || store.resumeCalls != 1 {
					t.Fatalf("durable calls = lookup:%d resume:%d, want 1/1", store.lookupCalls, store.resumeCalls)
				}
			default:
				if store.lookupCalls != 1 || store.resumeCalls != 0 {
					t.Fatalf("durable calls = lookup:%d resume:%d, want 1/0", store.lookupCalls, store.resumeCalls)
				}
			}
		})
	}
}

func TestCollectFailsClosedForInvalidAdmittedIndexPolicies(t *testing.T) {
	t.Parallel()

	valid := IndexPolicy{Name: "main", Version: 1, RetentionPeriod: time.Hour}
	tests := map[string][]IndexPolicy{
		"zero version":           {{Name: "main", RetentionPeriod: time.Hour}},
		"duplicate name":         {valid, {Name: "main", Version: 2, RetentionPeriod: 2 * time.Hour}},
		"negative retention":     {{Name: "main", Version: 1, RetentionPeriod: -time.Hour}},
		"submillisecond retain":  {{Name: "main", Version: 1, RetentionPeriod: time.Nanosecond}},
		"retention past horizon": {{Name: "main", Version: 1, RetentionPeriod: 8_000_000_000 * time.Second}},
		"event bytes above hard": {{Name: "main", Version: 1, RetentionPeriod: time.Hour, Limits: IndexLimits{MaxEventBytes: HardMaxEventBytes + 1}}},
		"field count above hard": {{Name: "main", Version: 1, RetentionPeriod: time.Hour, Limits: IndexLimits{MaxFieldCount: HardMaxFields + 1}}},
		"nesting above hard":     {{Name: "main", Version: 1, RetentionPeriod: time.Hour, Limits: IndexLimits{MaxNestingDepth: HardMaxNestingDepth + 1}}},
		"negative future skew":   {{Name: "main", Version: 1, RetentionPeriod: time.Hour, Limits: IndexLimits{MaximumFutureSkew: -time.Second}}},
		"negative event age":     {{Name: "main", Version: 1, RetentionPeriod: time.Hour, Limits: IndexLimits{MaximumEventAge: -time.Second}}},
		"malformed sourcetype":   {{Name: "main", Version: 1, RetentionPeriod: time.Hour, DefaultSourcetype: " surrounding "}},
	}
	for name, policies := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
				return Authorization{
					SubjectID: "token-1", TenantID: "tenant-a", CollectorID: "collector-a",
					AuthorizedIndexes: policies,
				}, nil
			})
			harness := newServiceHarness(t, testServiceConfig(), authorizer, acceptingStore())
			stream := harness.stream(t, "Bearer good-token")
			sendHello(t, stream, 1)
			_, err := stream.Recv()
			if got := status.Code(err); got != codes.Unavailable {
				t.Fatalf("Collect error = %v (%v), want Unavailable", err, got)
			}
		})
	}
}
