package ingest

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
)

func TestCompileEventAuthorizationMatchesFullValues(t *testing.T) {
	t.Parallel()

	matcher, err := compileEventAuthorization(Authorization{
		AllowedHostRegexes: []string{`api-[0-9]+`, `host-a|host-b`},
		AllowedSourceRegexes: []string{
			`/srv/[^/]+\.json`,
			`/var/log/(?:app|api)\.log`,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		host   string
		source string
		want   bool
	}{
		{name: "first alternatives", host: "host-a", source: "/var/log/app.log", want: true},
		{name: "second alternatives", host: "api-42", source: "/srv/audit.json", want: true},
		{name: "host prefix is not full match", host: "prefix-host-a", source: "/var/log/app.log"},
		{name: "host suffix is not full match", host: "host-a.example", source: "/var/log/app.log"},
		{name: "source suffix is not full match", host: "host-a", source: "/var/log/app.log.1"},
		{name: "dimensions are anded", host: "host-a", source: "/tmp/app.log"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := matcher.allows(test.host, test.source); got != test.want {
				t.Fatalf("allows(%q, %q) = %t, want %t", test.host, test.source, got, test.want)
			}
		})
	}
}

func TestCompileEventAuthorizationTreatsEmptyDimensionsAsUnrestricted(t *testing.T) {
	t.Parallel()

	unrestricted, err := compileEventAuthorization(Authorization{})
	if err != nil {
		t.Fatal(err)
	}
	if !unrestricted.allows("", "") || !unrestricted.allows("any-host", "/any/source") {
		t.Fatal("empty authorization filters restricted an event")
	}

	hostOnly, err := compileEventAuthorization(Authorization{
		AllowedHostRegexes: []string{`allowed`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hostOnly.allows("allowed", "anything") || hostOnly.allows("denied", "anything") {
		t.Fatal("host-only authorization did not leave source unrestricted")
	}

	sourceOnly, err := compileEventAuthorization(Authorization{
		AllowedSourceRegexes: []string{`/allowed`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !sourceOnly.allows("anything", "/allowed") || sourceOnly.allows("anything", "/denied") {
		t.Fatal("source-only authorization did not leave host unrestricted")
	}
}

func TestCompileEventAuthorizationHonorsInlineModesWithinFullMatch(t *testing.T) {
	t.Parallel()

	matcher, err := compileEventAuthorization(Authorization{
		AllowedHostRegexes:   []string{`(?i:api-[0-9]+)`},
		AllowedSourceRegexes: []string{`(?s:/var/log/first.second)`},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !matcher.allows("API-42", "/var/log/first\nsecond") {
		t.Fatal("inline case-insensitive and dot-all modes were not honored")
	}
	if matcher.allows("prefix-API-42", "/var/log/first\nsecond") ||
		matcher.allows("API-42", "/var/log/first\nsecond-suffix-outside-match") {
		t.Fatal("inline modes escaped full-value authorization matching")
	}
}

func TestCompileEventAuthorizationRejectsCorruptOrUnboundedPatterns(t *testing.T) {
	t.Parallel()

	invalidUTF8 := string([]byte{utf8.RuneSelf, 0xff})
	tooMany := make([]string, maximumEventAuthorizationRegexesPerDimension+1)
	for index := range tooMany {
		tooMany[index] = "x"
	}
	tooLargeTotal := make([]string, 9)
	for index := range tooLargeTotal {
		tooLargeTotal[index] = strings.Repeat(
			"x",
			maximumEventAuthorizationRegexBytes-1,
		) + string(rune('a'+index))
	}
	for _, test := range []struct {
		name          string
		authorization Authorization
	}{
		{
			name:          "too many host patterns",
			authorization: Authorization{AllowedHostRegexes: tooMany},
		},
		{
			name:          "too many source patterns",
			authorization: Authorization{AllowedSourceRegexes: tooMany},
		},
		{
			name: "host pattern too large",
			authorization: Authorization{AllowedHostRegexes: []string{
				strings.Repeat("x", maximumEventAuthorizationRegexBytes+1),
			}},
		},
		{
			name: "source pattern too large",
			authorization: Authorization{AllowedSourceRegexes: []string{
				strings.Repeat("x", maximumEventAuthorizationRegexBytes+1),
			}},
		},
		{
			name:          "host total too large",
			authorization: Authorization{AllowedHostRegexes: tooLargeTotal},
		},
		{
			name:          "source total too large",
			authorization: Authorization{AllowedSourceRegexes: tooLargeTotal},
		},
		{
			name:          "invalid host utf8",
			authorization: Authorization{AllowedHostRegexes: []string{invalidUTF8}},
		},
		{
			name:          "invalid source utf8",
			authorization: Authorization{AllowedSourceRegexes: []string{invalidUTF8}},
		},
		{
			name:          "invalid host regexp",
			authorization: Authorization{AllowedHostRegexes: []string{`(`}},
		},
		{
			name:          "invalid source regexp",
			authorization: Authorization{AllowedSourceRegexes: []string{`[z-a]`}},
		},
		{
			name:          "empty expression",
			authorization: Authorization{AllowedHostRegexes: []string{""}},
		},
		{
			name:          "embedded nul",
			authorization: Authorization{AllowedSourceRegexes: []string{"prefix\x00suffix"}},
		},
		{
			name: "noncanonical order",
			authorization: Authorization{AllowedHostRegexes: []string{
				`z`,
				`a`,
			}},
		},
		{
			name: "compiled program too complex",
			authorization: Authorization{AllowedHostRegexes: []string{
				strings.Repeat("a{1000}", 5),
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := compileEventAuthorization(test.authorization); err == nil {
				t.Fatal("compileEventAuthorization accepted invalid authorization")
			}
		})
	}
}

func TestCloneAuthorizationDetachesEventFilters(t *testing.T) {
	t.Parallel()

	original := Authorization{
		AllowedHostRegexes:   []string{`host-a`},
		AllowedSourceRegexes: []string{`/var/log/app\.log`},
	}
	cloned := cloneAuthorization(original)
	cloned.AllowedHostRegexes[0] = `host-b`
	cloned.AllowedSourceRegexes[0] = `/tmp/other`
	if original.AllowedHostRegexes[0] != `host-a` ||
		original.AllowedSourceRegexes[0] != `/var/log/app\.log` {
		t.Fatalf("clone aliases original filters: original=%+v clone=%+v", original, cloned)
	}
}

func TestCollectAppliesEventAuthorizationAfterNormalizationBeforeQuota(t *testing.T) {
	t.Parallel()

	authorization := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	authorization.AllowedHostRegexes = []string{`api-[0-9]+`, `host-a`}
	authorization.AllowedSourceRegexes = []string{`/srv/[^/]+\.json`, `/var/log/[^/]+\.log`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(authorization), nil
	})
	var stored StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = batch
		return StoreResult{
			Accepted:           uint32(len(batch.Events)),
			OriginalEventCount: batch.OriginalEventCount,
			RejectedEvents:     batch.RejectedEvents,
			CommittedAt:        validationTestNow,
		}, nil
	})
	harness := newServiceHarness(t, testServiceConfig(), authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)

	acceptedA := validTestEvent("accepted-a", "main")
	acceptedB := validTestEvent("accepted-b", "main")
	acceptedB.Host = "api-42"
	acceptedB.Source = "/srv/audit.json"
	badHost := validTestEvent("bad-host", "main")
	badHost.Host = "host-a.example"
	badSource := validTestEvent("bad-source", "main")
	badSource.Source = "/var/log/app.log.1"
	invalidBeforeAuthorization := validTestEvent("invalid-utf8", "main")
	invalidBeforeAuthorization.Raw = []byte{0xff}
	invalidBeforeAuthorization.RawEncoding = opensplunk.RawEncoding_RAW_ENCODING_UTF8
	invalidBeforeAuthorization.Source = "/denied"
	batch := validTestBatch(
		"collector-a",
		"event-authorization-partial",
		1,
		acceptedA,
		acceptedB,
		badHost,
		badSource,
		invalidBeforeAuthorization,
	)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	ack := recvResponse(t, stream).GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 2 || len(ack.GetRejectedEvents()) != 3 {
		t.Fatalf("partial authorization acknowledgment = %#v", ack)
	}
	for index, want := range []struct {
		code      opensplunk.EventRejectionCode
		path      string
		violation string
	}{
		{opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST, "host", "unauthorized_host"},
		{opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE, "source", "unauthorized_source"},
		{opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID, "raw", "invalid_utf8"},
	} {
		got := ack.GetRejectedEvents()[index]
		if got.GetCode() != want.code || len(got.GetViolations()) != 1 ||
			got.GetViolations()[0].GetFieldPath() != want.path ||
			got.GetViolations()[0].GetCode() != want.violation {
			t.Fatalf("rejection %d = %#v, want %+v", index, got, want)
		}
	}
	if len(stored.Events) != 2 || stored.Events[0].Event.GetEventId() != "accepted-a" ||
		stored.Events[1].Event.GetEventId() != "accepted-b" || len(stored.RejectedEvents) != 3 {
		t.Fatalf("stored authorization outcome = %+v", stored)
	}
	if stored.QuotaAdmission == nil || len(stored.QuotaAdmission.Charges) == 0 ||
		stored.QuotaAdmission.Charges[0].Events != 2 {
		t.Fatalf("quota admission = %+v", stored.QuotaAdmission)
	}
	wantBytes := uint64(proto.Size(acceptedA) + proto.Size(acceptedB))
	if got := stored.QuotaAdmission.Charges[0].UncompressedBytes; got != wantBytes {
		t.Fatalf("quota bytes = %d, want accepted-only %d", got, wantBytes)
	}
}

func TestCollectRefreshesEventAuthorizationAtEveryBatchBoundary(t *testing.T) {
	t.Parallel()

	initial := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	initial.AllowedHostRegexes = []string{`host-a`}
	initial.AllowedSourceRegexes = []string{`/var/log/app\.log`}
	updated := cloneAuthorization(initial)
	updated.AllowedHostRegexes = []string{`host-b`}
	updated.AllowedSourceRegexes = []string{`/srv/audit\.json`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(initial), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(initial, request), nil
	}
	refreshes := 0
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		refreshes++
		if refreshes == 1 {
			return cloneAuthorization(initial), nil
		}
		return cloneAuthorization(updated), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	var stored []StoreBatch
	store := EventStoreFunc(func(_ context.Context, batch StoreBatch) (StoreResult, error) {
		stored = append(stored, batch)
		return StoreResult{
			Accepted:           uint32(len(batch.Events)),
			OriginalEventCount: batch.OriginalEventCount,
			RejectedEvents:     batch.RejectedEvents,
			CommittedAt:        validationTestNow,
		}, nil
	})
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)

	firstAllowed := validTestEvent("first-allowed", "main")
	firstDenied := validTestEvent("first-denied", "main")
	firstDenied.Host = "host-b"
	firstDenied.Source = "/srv/audit.json"
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "event-authorization-first", 1, firstAllowed, firstDenied,
	))); err != nil {
		t.Fatal(err)
	}
	first := recvResponse(t, stream).GetBatchAck()
	if first == nil || first.GetAcceptedEventCount() != 1 ||
		len(first.GetRejectedEvents()) != 1 ||
		first.GetRejectedEvents()[0].GetEventId() != "first-denied" {
		t.Fatalf("first event authority acknowledgment = %#v", first)
	}

	secondDenied := validTestEvent("second-denied", "main")
	secondAllowed := validTestEvent("second-allowed", "main")
	secondAllowed.Host = "host-b"
	secondAllowed.Source = "/srv/audit.json"
	if err := stream.Send(batchRequest(3, validTestBatch(
		"collector-a", "event-authorization-second", 2, secondDenied, secondAllowed,
	))); err != nil {
		t.Fatal(err)
	}
	second := recvResponse(t, stream).GetBatchAck()
	if second == nil || second.GetAcceptedEventCount() != 1 ||
		len(second.GetRejectedEvents()) != 1 ||
		second.GetRejectedEvents()[0].GetEventId() != "second-denied" {
		t.Fatalf("second event authority acknowledgment = %#v", second)
	}
	if refreshes != 2 || len(stored) != 2 ||
		stored[0].Events[0].Event.GetEventId() != "first-allowed" ||
		stored[1].Events[0].Event.GetEventId() != "second-allowed" {
		t.Fatalf("refreshes=%d stored=%+v", refreshes, stored)
	}
}

func TestCollectDurablyReplaysAllRejectedEventAuthorization(t *testing.T) {
	t.Parallel()

	restricted := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	restricted.AllowedHostRegexes = []string{`other-host`}
	unrestricted := cloneAuthorization(restricted)
	unrestricted.AllowedHostRegexes = nil
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(restricted), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(restricted, request), nil
	}
	refreshes := 0
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		refreshes++
		if refreshes == 1 {
			return cloneAuthorization(restricted), nil
		}
		return cloneAuthorization(unrestricted), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	store := &recoverableTestStore{}
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)

	batch := validTestBatch(
		"collector-a", "event-authorization-replay", 1, validTestEvent("denied", "main"),
	)
	if err := stream.Send(batchRequest(2, batch)); err != nil {
		t.Fatal(err)
	}
	first := recvResponse(t, stream).GetBatchReject()
	if first == nil || first.GetCode() != opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS ||
		len(first.GetViolations()) != 1 || first.GetViolations()[0].GetCode() != "unauthorized_host" {
		t.Fatalf("first all-rejected response = %#v", first)
	}
	if err := stream.Send(batchRequest(3, batch)); err != nil {
		t.Fatal(err)
	}
	replayed := recvResponse(t, stream).GetBatchReject()
	if !proto.Equal(replayed, first) {
		t.Fatalf("replayed rejection = %#v, want %#v", replayed, first)
	}
	if refreshes != 2 || store.rejectCalls != 1 || store.storeCalls != 0 || store.lookupCalls != 2 {
		t.Fatalf("refreshes=%d store=%+v", refreshes, store)
	}
}

func TestCollectFailsClosedOnCorruptRefreshedEventAuthorization(t *testing.T) {
	t.Parallel()

	initial := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	initial.AllowedHostRegexes = []string{`host-a`}
	corrupt := cloneAuthorization(initial)
	corrupt.AllowedHostRegexes = []string{`(`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(initial), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(initial, request), nil
	}
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		return cloneAuthorization(corrupt), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	store := &deferredAuthorityTestStore{lookupState: StoredBatchNotFound}
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "corrupt-event-authorization", 1, validTestEvent("event", "main"),
	))); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("corrupt event authorization response = (%#v, %v), want nil/Unavailable", response, err)
	}
	if store.lookupCalls != 1 || store.storeCalls != 0 || store.rejectCalls != 0 {
		t.Fatalf("corrupt event authority durable calls = %+v", store)
	}
}

func TestCollectDefersTypedInvalidEventAuthorityUntilAfterDurableLookup(t *testing.T) {
	t.Parallel()

	initial := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	initial.AllowedHostRegexes = []string{`host-a`}
	partial := cloneAuthorization(initial)
	partial.AuthorizedIndexes = nil
	partial.AllowedHostRegexes = nil
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(initial), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(initial, request), nil
	}
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		return cloneAuthorization(partial), ErrInvalidEventAuthority
	}
	config := testServiceConfig()
	config.SessionManager = manager
	store := &deferredAuthorityTestStore{lookupState: StoredBatchNotFound}
	harness := newServiceHarness(t, config, authorizer, store)
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	_ = recvResponse(t, stream)
	if err := stream.Send(batchRequest(2, validTestBatch(
		"collector-a", "typed-invalid-event-authorization", 1, validTestEvent("event", "main"),
	))); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("invalid event authority response = (%#v, %v), want nil/Unavailable", response, err)
	}
	if store.lookupCalls != 1 || store.storeCalls != 0 || store.rejectCalls != 0 {
		t.Fatalf("invalid event authority durable calls = %+v", store)
	}
}

func TestInvalidRefreshedEventAuthorizationDefersToEveryDurableOutcome(t *testing.T) {
	t.Parallel()

	batch := validTestBatch(
		"collector-a",
		"event-authorization-durable-precedence",
		1,
		validTestEvent("event", "main"),
	)
	acknowledged := batch.GetBatchSequence()
	rejection := batchRejection(
		batch,
		opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS,
		"original durable event-authorization rejection",
		"host",
		"unauthorized_host",
	)
	for _, test := range []struct {
		name       string
		state      StoredBatchState
		lookup     StoreResult
		resume     StoreResult
		wantReject *opensplunk.BatchReject
		wantResume int
	}{
		{
			name:  "committed acknowledgment",
			state: StoredBatchCommitted,
			lookup: StoreResult{
				Duplicate:           1,
				OriginalEventCount:  1,
				AcknowledgedThrough: &acknowledged,
				CommittedAt:         validationTestNow,
			},
		},
		{
			name:       "committed rejection",
			state:      StoredBatchRejected,
			lookup:     StoreResult{BatchRejection: rejection},
			wantReject: rejection,
		},
		{
			name:  "pending resume",
			state: StoredBatchPending,
			resume: StoreResult{
				Accepted:            1,
				OriginalEventCount:  1,
				AcknowledgedThrough: &acknowledged,
				CommittedAt:         validationTestNow,
			},
			wantResume: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			store := &eventAuthorityReplayStore{
				state:        test.state,
				lookupResult: test.lookup,
				resumeResult: test.resume,
			}
			config := withTestSessionManager(testServiceConfig(), staticTestAuthorizer())
			service, err := NewService(config, staticTestAuthorizer(), store)
			if err != nil {
				t.Fatal(err)
			}
			response, processErr := service.processBatchWithDeferredAuthority(
				context.Background(),
				batch,
				testBatchStreamState(service),
				validationTestNow,
				ErrInvalidEventAuthority,
			)
			if processErr != nil {
				t.Fatalf("durable replay failed behind event authority error: %v", processErr)
			}
			if test.wantReject != nil {
				if !proto.Equal(response.GetBatchReject(), test.wantReject) {
					t.Fatalf("durable rejection = %#v, want %#v", response, test.wantReject)
				}
			} else if ack := response.GetBatchAck(); ack == nil ||
				ack.GetAcceptedEventCount()+ack.GetDuplicateEventCount() != 1 {
				t.Fatalf("durable acknowledgment = %#v", response)
			}
			if store.lookupCalls != 1 || store.resumeCalls != test.wantResume ||
				store.storeCalls != 0 || store.rejectCalls != 0 {
				t.Fatalf("durable calls = %+v", store)
			}
		})
	}
}

func TestCollectRejectsCorruptAdmittedEventAuthorization(t *testing.T) {
	t.Parallel()

	preliminary := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	admitted := cloneAuthorization(preliminary)
	admitted.AllowedSourceRegexes = []string{`(`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(preliminary), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.admitFunc = func(
		_ context.Context,
		_ string,
		request CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		return manager.admissionFor(admitted, request), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	sendHello(t, stream)
	if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("corrupt admitted event authorization = (%#v, %v), want nil/Unavailable", response, err)
	}
}

func TestCollectRejectsCorruptPreliminaryEventAuthorizationBeforeAdmission(t *testing.T) {
	t.Parallel()

	corrupt := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	corrupt.AllowedHostRegexes = []string{`(`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(corrupt), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	var admitCalled atomic.Bool
	manager.admitFunc = func(
		context.Context,
		string,
		CollectorSessionAdmissionRequest,
	) (CollectorSessionAdmission, error) {
		admitCalled.Store(true)
		return CollectorSessionAdmission{}, errors.New("unexpected admission")
	}
	config := testServiceConfig()
	config.SessionManager = manager
	harness := newServiceHarness(t, config, authorizer, acceptingStore())
	stream := harness.stream(t, "Bearer good-token")
	if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("corrupt preliminary event authorization = (%#v, %v), want nil/Unavailable", response, err)
	}
	if admitCalled.Load() {
		t.Fatal("corrupt preliminary event authority reached durable admission")
	}
}

func TestEventAuthorizationRejectsHostBeforeSource(t *testing.T) {
	t.Parallel()

	matcher, err := compileEventAuthorization(Authorization{
		AllowedHostRegexes:   []string{`allowed-host`},
		AllowedSourceRegexes: []string{`/allowed-source`},
	})
	if err != nil {
		t.Fatal(err)
	}
	stored := &StoredEvent{Event: &opensplunk.LogEvent{Host: "bad-host", Source: "/bad-source"}}
	rejection := matcher.rejection(stored)
	if rejection == nil ||
		rejection.Code != opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST ||
		len(rejection.Violations) != 1 || rejection.Violations[0].GetCode() != "unauthorized_host" {
		t.Fatalf("dual-dimension rejection = %#v", rejection)
	}
}

func TestRefreshEventAuthorizationCorruptionKeepsLastValidSnapshot(t *testing.T) {
	t.Parallel()

	initial := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	initial.AllowedHostRegexes = []string{`host-a`}
	matcher, err := compileEventAuthorization(initial)
	if err != nil {
		t.Fatal(err)
	}
	corrupt := cloneAuthorization(initial)
	corrupt.AllowedHostRegexes = []string{`(`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(initial), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		return cloneAuthorization(corrupt), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	resolved, ok := service.resolveAuthorizedIndexPolicies(initial.AuthorizedIndexes, validationTestNow)
	if !ok {
		t.Fatal("initial index authority did not resolve")
	}
	state := streamState{
		collectorID:        initial.CollectorID,
		authorization:      cloneAuthorization(initial),
		indexPolicies:      resolved.byName,
		eventAuthorization: matcher,
	}
	deferred, fatal := service.refreshLeaseAuthorization(
		context.Background(),
		"good-token",
		collectorfleet.Lease{TenantID: initial.TenantID},
		validationTestNow,
		&state,
	)
	if fatal != nil || !errors.Is(deferred, ErrInvalidEventAuthority) {
		t.Fatalf("refresh corruption = deferred:%v fatal:%v", deferred, fatal)
	}
	if state.authorization.AllowedHostRegexes[0] != `host-a` ||
		!state.eventAuthorization.allows("host-a", "/anything") ||
		state.eventAuthorization.allows("host-b", "/anything") {
		t.Fatalf("corrupt refresh replaced last valid snapshot: %+v", state.authorization)
	}
}

func TestRefreshEventAuthorizationRecompilesOnlyChangedDimensions(t *testing.T) {
	t.Parallel()

	initial := boundTestAuthorization("subject-a", "tenant-a", "collector-a")
	initial.AllowedHostRegexes = []string{`host-a`}
	initial.AllowedSourceRegexes = []string{`/var/log/app\.log`}
	matcher, err := compileEventAuthorization(initial)
	if err != nil {
		t.Fatal(err)
	}
	updated := cloneAuthorization(initial)
	updated.AllowedHostRegexes = []string{`host-b`}
	authorizer := AuthorizerFunc(func(context.Context, string) (Authorization, error) {
		return cloneAuthorization(initial), nil
	})
	manager := newTestCollectorSessionManager(authorizer)
	manager.authorizeFunc = func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error) {
		return cloneAuthorization(updated), nil
	}
	config := testServiceConfig()
	config.SessionManager = manager
	service, err := NewService(config, authorizer, acceptingStore())
	if err != nil {
		t.Fatal(err)
	}
	resolved, ok := service.resolveAuthorizedIndexPolicies(initial.AuthorizedIndexes, validationTestNow)
	if !ok {
		t.Fatal("initial index authority did not resolve")
	}
	state := streamState{
		collectorID:        initial.CollectorID,
		authorization:      cloneAuthorization(initial),
		indexPolicies:      resolved.byName,
		eventAuthorization: matcher,
	}
	originalHost := state.eventAuthorization.hosts[0]
	originalSource := state.eventAuthorization.sources[0]
	deferred, fatal := service.refreshLeaseAuthorization(
		context.Background(),
		"good-token",
		collectorfleet.Lease{TenantID: initial.TenantID},
		validationTestNow,
		&state,
	)
	if deferred != nil || fatal != nil {
		t.Fatalf("refresh = deferred:%v fatal:%v", deferred, fatal)
	}
	if state.eventAuthorization.hosts[0] == originalHost {
		t.Fatal("changed host projection reused stale compiled expression")
	}
	if state.eventAuthorization.sources[0] != originalSource {
		t.Fatal("unchanged source projection was recompiled")
	}
	if !state.eventAuthorization.allows("host-b", "/var/log/app.log") ||
		state.eventAuthorization.allows("host-a", "/var/log/app.log") {
		t.Fatal("refreshed host projection has stale authorization behavior")
	}
}

type eventAuthorityReplayStore struct {
	state        StoredBatchState
	lookupResult StoreResult
	resumeResult StoreResult
	storeCalls   int
	lookupCalls  int
	resumeCalls  int
	rejectCalls  int
}

func (store *eventAuthorityReplayStore) Store(context.Context, StoreBatch) (StoreResult, error) {
	store.storeCalls++
	return StoreResult{}, errors.New("fresh store must not run behind invalid event authority")
}

func (store *eventAuthorityReplayStore) LookupBatch(
	context.Context,
	StoreBatchIdentity,
) (StoredBatchState, StoreResult, error) {
	store.lookupCalls++
	return store.state, store.lookupResult, nil
}

func (store *eventAuthorityReplayStore) ResumeBatch(
	context.Context,
	StoreBatchIdentity,
) (StoreResult, error) {
	store.resumeCalls++
	return store.resumeResult, nil
}

func (store *eventAuthorityReplayStore) RejectBatch(
	context.Context,
	StoreBatchRejection,
) (StoreResult, error) {
	store.rejectCalls++
	return StoreResult{}, errors.New("fresh rejection must not run behind invalid event authority")
}
