package ingest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"google.golang.org/protobuf/proto"
)

type admissionTestStagingStore struct {
	storeCalls  int
	stageCalls  int
	staged      []StoreBatch
	stageCtx    context.Context
	stageResult StageResult
	stageErr    error
}

func (store *admissionTestStagingStore) Store(
	context.Context,
	StoreBatch,
) (StoreResult, error) {
	store.storeCalls++
	return StoreResult{}, errors.New("unexpected synchronous store call")
}

func (store *admissionTestStagingStore) Stage(
	ctx context.Context,
	batch StoreBatch,
) (StageResult, error) {
	store.stageCalls++
	store.stageCtx = ctx
	store.staged = append(store.staged, batch)
	return store.stageResult, store.stageErr
}

func TestAdmissionPreparerPrepareHECNormalizesAndPlansPolicy(t *testing.T) {
	store := &admissionTestStagingStore{}
	defaultRetention := 72 * time.Hour
	preparer, err := NewAdmissionPreparer(AdmissionConfig{
		DefaultIndexRetention: defaultRetention,
		Redaction: RedactionPolicy{
			AdditionalSensitiveFields: []string{"customer_secret"},
			Replacement:               "<masked>",
		},
	}, store)
	if err != nil {
		t.Fatalf("NewAdmissionPreparer(): %v", err)
	}

	tokenLimits := ingestquota.Limits{
		MaxEventsPerSecond:            100,
		MaxUncompressedBytesPerSecond: 50_000,
	}
	auditLimits := ingestquota.Limits{
		MaxEventsPerSecond:            20,
		MaxUncompressedBytesPerSecond: 10_000,
	}
	mainLimits := ingestquota.Limits{
		MaxEventsPerSecond:            50,
		MaxUncompressedBytesPerSecond: 20_000,
	}
	request := admissionTestHECRequest(
		AdmissionEvent{Event: validTestEvent("event-main", "main"), UncompressedBytes: 111},
		AdmissionEvent{Event: validTestEvent("event-audit", "audit"), UncompressedBytes: 222},
	)
	request.Authorization.TokenRateLimits = tokenLimits
	// Supply policies in reverse lexical order to prove that quota plans are
	// canonical rather than dependent on the authentication projection order.
	request.Authorization.AuthorizedIndexes = []IndexPolicy{
		{
			Name:              "main",
			Version:           3,
			DefaultSourcetype: "policy:main",
			Limits: IndexLimits{
				MaxEventBytes:   4 << 10,
				MaxFieldCount:   4,
				MaxNestingDepth: 2,
			},
			IngestionRateLimits: mainLimits,
		},
		{
			Name:                "audit",
			Version:             7,
			RetentionPeriod:     6 * time.Hour,
			DefaultSourcetype:   "policy:audit",
			IngestionRateLimits: auditLimits,
		},
	}
	request.Authorization.AllowedHostRegexes = []string{`host-[ab]`}
	request.Authorization.AllowedSourceRegexes = []string{`/var/log/.*`}
	request.HECAdmission = &HECStageAdmission{
		TokenID:               "hec-token-1",
		TokenVersion:          9,
		RequestID:             request.BatchID,
		AcknowledgmentEnabled: true,
		Channel:               "14c0d562-7a02-4b00-9b28-2e72a79bb28c",
		CreatedAt:             validationTestNow,
	}

	mainEvent := request.Events[0].Event
	mainEvent.Sourcetype = ""
	mainEvent.Raw = []byte(`{"status":200,"customer_secret":"raw-secret"}`)
	mainEvent.Fields = object(
		stringField("status", "200"),
		stringField("customer_secret", "structured-secret"),
	)
	auditEvent := request.Events[1].Event
	auditEvent.Host = "host-b"
	auditEvent.Source = "/var/log/audit.log"
	mainBefore := proto.Clone(mainEvent).(*opensplunkv1.LogEvent)
	auditBefore := proto.Clone(auditEvent).(*opensplunkv1.LogEvent)

	batch, err := preparer.Prepare(request)
	if err != nil {
		t.Fatalf("Prepare(): %v", err)
	}
	if store.storeCalls != 0 || store.stageCalls != 0 {
		t.Fatalf("Prepare() persistence calls = (store %d, stage %d), want zero", store.storeCalls, store.stageCalls)
	}
	if !proto.Equal(mainEvent, mainBefore) || !proto.Equal(auditEvent, auditBefore) {
		t.Fatal("Prepare() mutated a source event")
	}
	if batch.TenantID != "tenant-a" || batch.CollectorID != "" ||
		batch.Source != HECSource("hec-token-1") || batch.BatchID != "hec-batch-1" ||
		batch.BatchSequence != 1 || batch.OriginalEventCount != 2 ||
		batch.SourceBatchSHA256 != request.SourceBatchSHA256 ||
		!batch.ReceivedAt.Equal(request.ReceivedAt) ||
		!batch.QuotaEvaluatedAt.Equal(request.QuotaEvaluatedAt) {
		t.Fatalf("prepared batch identity = %+v", batch)
	}
	if batch.HECAdmission == nil || batch.HECAdmission == request.HECAdmission ||
		batch.HECAdmission.TokenID != request.HECAdmission.TokenID ||
		batch.HECAdmission.TokenVersion != request.HECAdmission.TokenVersion ||
		len(batch.HECAdmission.AuthorizedIndexes) != 2 ||
		batch.HECAdmission.AuthorizedIndexes[0] != (HECIndexAuthority{Name: "audit", Version: 7}) ||
		batch.HECAdmission.AuthorizedIndexes[1] != (HECIndexAuthority{Name: "main", Version: 3}) {
		t.Fatalf("prepared HEC admission = %+v, want detached selected index authority", batch.HECAdmission)
	}
	if batch.RejectedEvents == nil || len(batch.RejectedEvents) != 0 {
		t.Fatalf("prepared rejected events = %#v, want nonnil empty list", batch.RejectedEvents)
	}
	if got, want := batch.RetentionByIndex["main"], defaultRetention; got != want {
		t.Fatalf("main retention = %v, want inherited %v", got, want)
	}
	if got, want := batch.RetentionByIndex["audit"], 6*time.Hour; got != want {
		t.Fatalf("audit retention = %v, want %v", got, want)
	}
	if len(batch.RetentionByIndex) != 2 || len(batch.Events) != 2 {
		t.Fatalf("prepared policy/event cardinality = (%d, %d), want (2, 2)", len(batch.RetentionByIndex), len(batch.Events))
	}

	storedMain := batch.Events[0]
	if storedMain.Event == mainEvent || storedMain.Event.GetSourcetype() != "policy:main" ||
		storedMain.Source != HECSource("hec-token-1") || storedMain.CollectorID != "" ||
		storedMain.TenantID != "tenant-a" || storedMain.BatchID != "hec-batch-1" ||
		!storedMain.IndexTime.Equal(validationTestNow) {
		t.Fatalf("normalized main event metadata = %+v", storedMain)
	}
	if bytes.Contains(storedMain.Event.GetRaw(), []byte("raw-secret")) ||
		!bytes.Contains(storedMain.Event.GetRaw(), []byte("masked")) {
		t.Fatalf("normalized raw payload was not redacted: %s", storedMain.Event.GetRaw())
	}
	if got := admissionTestStringField(storedMain.Event, "customer_secret"); got != "<masked>" {
		t.Fatalf("normalized structured secret = %q, want %q", got, "<masked>")
	}
	if got := batch.Events[1].Event.GetSourcetype(); got != "json" {
		t.Fatalf("explicit audit sourcetype = %q, want preserved %q", got, "json")
	}

	wantCharges := []ingestquota.Charge{
		{
			Scope:  ingestquota.ScopeKey{Kind: ingestquota.ScopeKindToken, TenantID: "tenant-a", Identity: "hec-token-1"},
			Limits: tokenLimits, Events: 2, UncompressedBytes: uint64(proto.Size(mainBefore) + proto.Size(auditBefore)),
		},
		{
			Scope:  ingestquota.ScopeKey{Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "audit"},
			Limits: auditLimits, Events: 1, UncompressedBytes: uint64(proto.Size(auditBefore)),
		},
		{
			Scope:  ingestquota.ScopeKey{Kind: ingestquota.ScopeKindIndex, TenantID: "tenant-a", Identity: "main"},
			Limits: mainLimits, Events: 1, UncompressedBytes: uint64(proto.Size(mainBefore)),
		},
	}
	if got, normalized := wantCharges[2].UncompressedBytes, uint64(proto.Size(batch.Events[0].Event)); got == normalized {
		t.Fatalf("redacted source and normalized sizes unexpectedly match at %d bytes; test cannot prove pre-redaction quota accounting", got)
	}
	if batch.QuotaAdmission == nil || len(batch.QuotaAdmission.Charges) != len(wantCharges) {
		t.Fatalf("quota admission = %+v, want %d charges", batch.QuotaAdmission, len(wantCharges))
	}
	for index, want := range wantCharges {
		got := batch.QuotaAdmission.Charges[index]
		if got.Scope != want.Scope || got.Limits != want.Limits || got.Events != want.Events ||
			got.UncompressedBytes != want.UncompressedBytes || got.State != nil {
			t.Fatalf("quota charge %d = %+v, want %+v", index, got, want)
		}
	}
}

func TestAdmissionPreparerStageIsRequestAtomicAtLowestFailure(t *testing.T) {
	store := &admissionTestStagingStore{}
	preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
	request := admissionTestHECRequest(
		AdmissionEvent{Event: validTestEvent("event-good", "main"), UncompressedBytes: 100},
		AdmissionEvent{Event: validTestEvent("event-first-bad", "main"), UncompressedBytes: 100},
		AdmissionEvent{Event: validTestEvent("event-later-bad", "secret"), UncompressedBytes: 100},
	)
	request.Authorization.AuthorizedIndexes[0].Limits.MaxFieldCount = 1
	request.Events[1].Event.Fields = object(
		stringField("first", "1"),
		stringField("second", "2"),
	)
	request.Events[2].Event.EventId = ""

	_, err := preparer.Stage(context.Background(), request)
	var failure *AdmissionFailure
	if !errors.As(err, &failure) {
		t.Fatalf("Stage() error = %v, want *AdmissionFailure", err)
	}
	if failure.EventIndex != 1 || failure.EventID != "event-first-bad" ||
		failure.Failure == nil ||
		failure.Failure.Code != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS {
		t.Fatalf("Stage() failure = %+v, want ordinal 1 field-limit failure", failure)
	}
	if store.storeCalls != 0 || store.stageCalls != 0 || len(store.staged) != 0 {
		t.Fatalf("failed atomic Stage() persistence calls = (store %d, stage %d, batches %d), want zero",
			store.storeCalls, store.stageCalls, len(store.staged))
	}
}

func TestAdmissionPreparerStageDelegatesPreparedBatchAndResult(t *testing.T) {
	storeFailure := errors.New("durable stage unavailable")
	tests := []struct {
		name        string
		stageResult StageResult
		stageErr    error
	}{
		{
			name: "success",
			stageResult: StageResult{
				VisibilitySequence:  42,
				State:               StoredBatchPending,
				HECRequestSequence:  17,
				HECAcknowledgmentID: 9,
			},
		},
		{name: "store error", stageErr: storeFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &admissionTestStagingStore{stageResult: test.stageResult, stageErr: test.stageErr}
			preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
			request := admissionTestHECRequest(
				AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 321},
			)
			type contextKey string
			const key contextKey = "admission-test"
			ctx := context.WithValue(context.Background(), key, "sentinel")

			result, err := preparer.Stage(ctx, request)
			if !errors.Is(err, test.stageErr) {
				t.Fatalf("Stage() error = %v, want %v", err, test.stageErr)
			}
			if result.VisibilitySequence != test.stageResult.VisibilitySequence ||
				result.State != test.stageResult.State ||
				result.HECRequestSequence != test.stageResult.HECRequestSequence ||
				result.HECAcknowledgmentID != test.stageResult.HECAcknowledgmentID {
				t.Fatalf("Stage() result = %+v, want %+v", result, test.stageResult)
			}
			if store.storeCalls != 0 || store.stageCalls != 1 || len(store.staged) != 1 {
				t.Fatalf("Stage() calls = (store %d, stage %d, batches %d), want (0, 1, 1)",
					store.storeCalls, store.stageCalls, len(store.staged))
			}
			if store.stageCtx == nil || store.stageCtx.Value(key) != "sentinel" {
				t.Fatalf("Stage() context = %v, want caller context", store.stageCtx)
			}
			staged := store.staged[0]
			wantBytes := uint64(proto.Size(request.Events[0].Event))
			if staged.Source != HECSource("hec-token-1") || len(staged.Events) != 1 ||
				staged.Events[0].Event.GetEventId() != "event-1" ||
				staged.QuotaAdmission == nil || staged.QuotaAdmission.Charges[0].UncompressedBytes != wantBytes {
				t.Fatalf("delegated batch = %+v", staged)
			}
			if test.stageErr == nil && (result.AcceptedEvents != 1 || result.UncompressedBytes != wantBytes) {
				t.Fatalf("Stage() accounting = %+v, want 1 event/%d bytes", result, wantBytes)
			}
		})
	}
}

func TestAdmissionPreparerRejectsInvalidSourceAuthorityAndIdentity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AdmissionRequest)
		wantIs error
		want   string
	}{
		{
			name: "HEC source carries collector identity",
			mutate: func(request *AdmissionRequest) {
				request.Source.CollectorID = "collector-a"
			},
			want: "ingestion admission source",
		},
		{
			name: "HEC authority carries collector identity",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.CollectorID = "collector-a"
			},
			want: "HEC ingestion admission cannot carry collector authority",
		},
		{
			name: "native authority mismatches source",
			mutate: func(request *AdmissionRequest) {
				request.Source = NativeCollectorSource("collector-a")
				request.CollectorID = "collector-a"
				request.Authorization.CollectorID = "collector-b"
			},
			want: "native ingestion admission authority does not match its source",
		},
		{
			name: "batch identity incomplete",
			mutate: func(request *AdmissionRequest) {
				request.SourceBatchSHA256 = [32]byte{}
			},
			want: "ingestion admission identity is incomplete",
		},
		{
			name: "no active index authority",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AuthorizedIndexes = nil
			},
			wantIs: ErrNoActiveIndexAuthority,
		},
		{
			name: "duplicate index authority",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AuthorizedIndexes = append(
					request.Authorization.AuthorizedIndexes,
					request.Authorization.AuthorizedIndexes[0],
				)
			},
			wantIs: ErrInvalidIndexAuthority,
		},
		{
			name: "invalid host constraint",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AllowedHostRegexes = []string{"("}
			},
			wantIs: ErrInvalidEventAuthority,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &admissionTestStagingStore{}
			preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
			request := admissionTestHECRequest(
				AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 100},
			)
			test.mutate(&request)

			_, err := preparer.Stage(context.Background(), request)
			if err == nil {
				t.Fatal("Stage() error = nil, want rejection")
			}
			if test.wantIs != nil && !errors.Is(err, test.wantIs) {
				t.Fatalf("Stage() error = %v, want errors.Is(%v)", err, test.wantIs)
			}
			if test.want != "" && !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Stage() error = %q, want substring %q", err, test.want)
			}
			if store.storeCalls != 0 || store.stageCalls != 0 {
				t.Fatalf("rejected Stage() persistence calls = (store %d, stage %d), want zero", store.storeCalls, store.stageCalls)
			}
		})
	}
}

func TestAdmissionPreparerEnforcesEventAuthorityAndIndexLimits(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*AdmissionRequest)
		wantCode opensplunkv1.EventRejectionCode
	}{
		{
			name: "host constraint",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AllowedHostRegexes = []string{`allowed-host`}
			},
			wantCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST,
		},
		{
			name: "source constraint",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AllowedSourceRegexes = []string{`/allowed/.*`}
			},
			wantCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE,
		},
		{
			name: "index field limit",
			mutate: func(request *AdmissionRequest) {
				request.Authorization.AuthorizedIndexes[0].Limits.MaxFieldCount = 1
				request.Events[0].Event.Fields = object(stringField("one", "1"), stringField("two", "2"))
			},
			wantCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
		},
		{
			name: "unauthorized index",
			mutate: func(request *AdmissionRequest) {
				request.Events[0].Event.IndexName = "secret"
			},
			wantCode: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &admissionTestStagingStore{}
			preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
			request := admissionTestHECRequest(
				AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 100},
			)
			test.mutate(&request)

			_, err := preparer.Stage(context.Background(), request)
			var failure *AdmissionFailure
			if !errors.As(err, &failure) || failure.EventIndex != 0 ||
				failure.Failure == nil || failure.Failure.Code != test.wantCode {
				t.Fatalf("Stage() failure = %+v (error %v), want ordinal 0 code %v", failure, err, test.wantCode)
			}
			if store.storeCalls != 0 || store.stageCalls != 0 {
				t.Fatalf("rejected Stage() persistence calls = (store %d, stage %d), want zero", store.storeCalls, store.stageCalls)
			}
		})
	}
}

func TestAdmissionPreparerRejectsRequestResourceBounds(t *testing.T) {
	t.Run("event count", func(t *testing.T) {
		store := &admissionTestStagingStore{}
		limits := DefaultLimits()
		limits.MaxBatchEvents = 1
		preparer := admissionTestPreparer(t, AdmissionConfig{Limits: limits}, store)
		request := admissionTestHECRequest(
			AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 100},
			AdmissionEvent{Event: validTestEvent("event-2", "main"), UncompressedBytes: 100},
		)

		if _, err := preparer.Stage(context.Background(), request); err == nil ||
			!strings.Contains(err.Error(), "event count is outside bounds") {
			t.Fatalf("Stage() error = %v, want event-count bound", err)
		}
		if store.stageCalls != 0 {
			t.Fatalf("Stage() durable calls = %d, want zero", store.stageCalls)
		}
	})

	t.Run("uncompressed byte charge", func(t *testing.T) {
		store := &admissionTestStagingStore{}
		limits := DefaultLimits()
		limits.MaxBatchBytes = 5_000
		limits.MaxEventBytes = 5_000
		preparer := admissionTestPreparer(t, AdmissionConfig{Limits: limits}, store)
		request := admissionTestHECRequest(
			AdmissionEvent{Event: validTestEvent("event-1", "main"), UncompressedBytes: 3_000},
			AdmissionEvent{Event: validTestEvent("event-2", "main"), UncompressedBytes: 2_001},
		)
		request.Events[0].Event.Raw = bytes.Repeat([]byte{'a'}, 3_000)
		request.Events[1].Event.Raw = bytes.Repeat([]byte{'b'}, 2_001)

		if _, err := preparer.Stage(context.Background(), request); err == nil ||
			!strings.Contains(err.Error(), "request exceeds its byte limit") {
			t.Fatalf("Stage() error = %v, want byte-charge bound", err)
		}
		if store.stageCalls != 0 {
			t.Fatalf("Stage() durable calls = %d, want zero", store.stageCalls)
		}
	})

	t.Run("caller byte estimate cannot undercharge", func(t *testing.T) {
		store := &admissionTestStagingStore{}
		store.stageResult = StageResult{State: StoredBatchPending, HECRequestSequence: 1}
		preparer := admissionTestPreparer(t, AdmissionConfig{}, store)
		request := admissionTestHECRequest(
			AdmissionEvent{Event: validTestEvent("event-1", "main")},
		)

		result, err := preparer.Stage(context.Background(), request)
		if err != nil || result.UncompressedBytes == 0 {
			t.Fatalf("Stage() result = %+v error=%v, want recomputed positive charge", result, err)
		}
		if store.stageCalls != 1 {
			t.Fatalf("Stage() durable calls = %d, want one", store.stageCalls)
		}
	})
}

func TestNewAdmissionPreparerValidatesConfiguration(t *testing.T) {
	if _, err := NewAdmissionPreparer(AdmissionConfig{}, nil); err == nil ||
		!strings.Contains(err.Error(), "staging store is required") {
		t.Fatalf("NewAdmissionPreparer(nil store) error = %v", err)
	}

	store := &admissionTestStagingStore{}
	limits := DefaultLimits()
	limits.MaxIDBytes = 0
	if _, err := NewAdmissionPreparer(AdmissionConfig{Limits: limits}, store); err == nil {
		t.Fatal("NewAdmissionPreparer(invalid limits) error = nil")
	}
	if _, err := NewAdmissionPreparer(AdmissionConfig{
		DefaultIndexRetention: -time.Millisecond,
	}, store); err == nil || !strings.Contains(err.Error(), "default retention") {
		t.Fatalf("NewAdmissionPreparer(invalid retention) error = %v", err)
	}
}

func admissionTestPreparer(
	t *testing.T,
	config AdmissionConfig,
	store StagingEventStore,
) *AdmissionPreparer {
	t.Helper()
	preparer, err := NewAdmissionPreparer(config, store)
	if err != nil {
		t.Fatalf("NewAdmissionPreparer(): %v", err)
	}
	return preparer
}

func admissionTestHECRequest(events ...AdmissionEvent) AdmissionRequest {
	return AdmissionRequest{
		Authorization: Authorization{
			SubjectID: "hec-token-1",
			TenantID:  "tenant-a",
			AuthorizedIndexes: []IndexPolicy{{
				Name:              "main",
				Version:           1,
				DefaultSourcetype: "policy:default",
			}},
		},
		Source:            HECSource("hec-token-1"),
		BatchID:           "hec-batch-1",
		BatchSequence:     1,
		SourceBatchSHA256: [32]byte{1},
		ReceivedAt:        validationTestNow,
		QuotaEvaluatedAt:  validationTestNow.Add(time.Second),
		Events:            events,
		HECAdmission: &HECStageAdmission{
			TokenID:      "hec-token-1",
			TokenVersion: 1,
			RequestID:    "hec-batch-1",
			CreatedAt:    validationTestNow,
		},
	}
}

func admissionTestStringField(event *opensplunkv1.LogEvent, name string) string {
	for _, field := range event.GetFields().GetFields() {
		if field.GetName() == name {
			return field.GetValue().GetStringValue()
		}
	}
	return ""
}
