package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectoradmission"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeIndexPoliciesRefreshWithoutCollectorReconnect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database, err := control.Open(ctx, filepath.Join(t.TempDir(), "index-policy-runtime.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})

	mainIndex, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:              "main",
		DisplayName:       "Main",
		RetentionPeriod:   time.Hour,
		IngestionEnabled:  true,
		SearchEnabled:     true,
		DefaultSourcetype: "runtime:main:v1",
		Limits: control.IndexLimits{
			MaxFieldCount: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	auditIndex, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:              "audit",
		DisplayName:       "Audit",
		RetentionPeriod:   2 * time.Hour,
		IngestionEnabled:  true,
		SearchEnabled:     true,
		DefaultSourcetype: "runtime:audit:v1",
		Limits: control.IndexLimits{
			MaxNestingDepth: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	const (
		tenantID    = "tenant-index-policy-runtime"
		collectorID = "collector-index-policy-runtime"
		streamID    = "stream-index-policy-runtime"
	)
	tokens, err := auth.NewStore(database, []byte("index-policy-runtime-digest-key-32b"))
	if err != nil {
		t.Fatal(err)
	}
	issued, err := tokens.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name:              "index policy runtime collector",
		AllowedIndexNames: []string{"main", "audit"},
		BoundCollectorID:  collectorID,
	})
	if err != nil {
		t.Fatal(err)
	}
	fleet, err := collectorfleet.New(database)
	if err != nil {
		t.Fatal(err)
	}
	admissions, err := collectoradmission.New(database, tokens, tenantID)
	if err != nil {
		t.Fatal(err)
	}

	now := issued.Token.CreatedAt.Add(time.Second).Truncate(time.Microsecond).UTC()
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time { return now }
	config.NewStreamID = func() string { return streamID }
	config.ServerInstanceID = "server-index-policy-runtime"
	config.ServerVersion = "index-policy-runtime-test"
	heartbeatRuntime := newCommandHeartbeatRuntime(t, fleet, config.HeartbeatInterval)
	config.SessionManager = collectorSessionManager{
		admission:  admissions,
		fleet:      fleet,
		heartbeats: heartbeatRuntime,
	}
	capture := &indexPolicyRuntimeEventStore{}
	service, err := ingest.NewService(
		config,
		collectorAuthorizer{store: tokens, tenantID: tenantID},
		capture,
	)
	if err != nil {
		t.Fatal(err)
	}
	stream := openIndexPolicyRuntimeStream(t, service, issued.Secret.Plaintext())

	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(now),
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: &opensplunkv1.CollectorHello{
			CollectorId:      collectorID,
			InstanceId:       "instance-index-policy-runtime",
			ProtocolMajor:    1,
			ProtocolMinor:    0,
			CollectorVersion: "index-policy-runtime-test",
			Hostname:         "index-policy-runtime-host",
			StartedAt:        timestamppb.New(now.Add(-time.Hour)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if ready := readyResponse.GetReady(); ready == nil || ready.GetStreamId() != streamID ||
		!slices.Equal(ready.GetAuthorizedIndexes(), []string{"audit", "main"}) {
		t.Fatalf("collector Ready = %#v", readyResponse)
	}

	initialBatch := indexPolicyRuntimeBatch(t, collectorID, "batch-index-policy-v1", 1, now,
		indexPolicyRuntimeEvent(t, "event-main-default-v1", "main", "", now,
			indexPolicyRuntimeStringField("one", "1")),
		indexPolicyRuntimeEvent(t, "event-main-explicit-v1", "main", "explicit:main", now,
			indexPolicyRuntimeStringField("one", "1")),
		indexPolicyRuntimeEvent(t, "event-main-fields-v1", "main", "", now,
			indexPolicyRuntimeStringField("one", "1"),
			indexPolicyRuntimeStringField("two", "2")),
		indexPolicyRuntimeEvent(t, "event-audit-default-v1", "audit", "", now,
			indexPolicyRuntimeStringField("one", "1")),
		indexPolicyRuntimeEvent(t, "event-audit-depth-v1", "audit", "", now,
			indexPolicyRuntimeObjectField("nested",
				indexPolicyRuntimeStringField("leaf", "value"))),
	)
	initialResponse := sendIndexPolicyRuntimeBatch(t, stream, 2, initialBatch, now)
	assertIndexPolicyRuntimeAck(t, initialResponse, "batch-index-policy-v1", 1, 3, []indexPolicyRuntimeRejection{
		{eventIndex: 2, eventID: "event-main-fields-v1", code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS},
		{eventIndex: 4, eventID: "event-audit-depth-v1", code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_NESTING_TOO_DEEP},
	})
	stored := capture.snapshot()
	if len(stored) != 1 {
		t.Fatalf("stored batches = %d, want 1", len(stored))
	}
	assertIndexPolicyRuntimeStoredBatch(
		t,
		stored[0],
		[]string{"runtime:main:v1", "explicit:main", "runtime:audit:v1"},
		map[string]time.Duration{"main": time.Hour, "audit": 2 * time.Hour},
	)

	updatedDefinition := mainIndex.Definition
	updatedDefinition.RetentionPeriod = 3 * time.Hour
	updatedDefinition.DefaultSourcetype = "runtime:main:v2"
	updatedDefinition.Limits.MaxFieldCount = 2
	mainIndex, err = database.UpdateIndex(ctx, mainIndex.ID, mainIndex.Version, updatedDefinition)
	if err != nil {
		t.Fatal(err)
	}
	updatedBatch := indexPolicyRuntimeBatch(t, collectorID, "batch-index-policy-v2", 2, now,
		indexPolicyRuntimeEvent(t, "event-main-default-v2", "main", "", now,
			indexPolicyRuntimeStringField("one", "1"),
			indexPolicyRuntimeStringField("two", "2")),
		indexPolicyRuntimeEvent(t, "event-main-fields-v2", "main", "", now,
			indexPolicyRuntimeStringField("one", "1"),
			indexPolicyRuntimeStringField("two", "2"),
			indexPolicyRuntimeStringField("three", "3")),
		indexPolicyRuntimeEvent(t, "event-audit-explicit-v2", "audit", "explicit:audit", now,
			indexPolicyRuntimeStringField("one", "1")),
	)
	updatedResponse := sendIndexPolicyRuntimeBatch(t, stream, 3, updatedBatch, now)
	assertIndexPolicyRuntimeAck(t, updatedResponse, "batch-index-policy-v2", 2, 2, []indexPolicyRuntimeRejection{{
		eventIndex: 1,
		eventID:    "event-main-fields-v2",
		code:       opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_TOO_MANY_FIELDS,
	}})
	stored = capture.snapshot()
	if len(stored) != 2 {
		t.Fatalf("stored batches after policy update = %d, want 2", len(stored))
	}
	assertIndexPolicyRuntimeStoredBatch(
		t,
		stored[1],
		[]string{"runtime:main:v2", "explicit:audit"},
		map[string]time.Duration{"main": 3 * time.Hour, "audit": 2 * time.Hour},
	)

	disabledDefinition := mainIndex.Definition
	disabledDefinition.IngestionEnabled = false
	if _, err := database.UpdateIndex(ctx, mainIndex.ID, mainIndex.Version, disabledDefinition); err != nil {
		t.Fatal(err)
	}
	disabledBatch := indexPolicyRuntimeBatch(t, collectorID, "batch-index-policy-disabled", 3, now,
		indexPolicyRuntimeEvent(t, "event-main-disabled", "main", "", now,
			indexPolicyRuntimeStringField("one", "1")),
	)
	disabledResponse := sendIndexPolicyRuntimeBatch(t, stream, 4, disabledBatch, now)
	disabledReject := disabledResponse.GetBatchReject()
	if disabledReject == nil ||
		disabledReject.GetBatchId() != "batch-index-policy-disabled" ||
		disabledReject.GetBatchSequence() != 3 ||
		disabledReject.GetCode() != opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_NO_AUTHORIZED_EVENTS ||
		len(disabledReject.GetViolations()) != 1 ||
		disabledReject.GetViolations()[0].GetFieldPath() != "events[0].index_name" ||
		disabledReject.GetViolations()[0].GetCode() != "unauthorized_index" {
		t.Fatalf(
			"disabled-index response = %v, violations = %v",
			disabledReject,
			disabledReject.GetViolations(),
		)
	}
	if calls := len(capture.snapshot()); calls != 2 {
		t.Fatalf("stored batches after disabled-index rejection = %d, want 2", calls)
	}

	auditBatch := indexPolicyRuntimeBatch(t, collectorID, "batch-index-policy-audit", 4, now,
		indexPolicyRuntimeEvent(t, "event-audit-after-main-disable", "audit", "", now,
			indexPolicyRuntimeStringField("one", "1")),
	)
	auditResponse := sendIndexPolicyRuntimeBatch(t, stream, 5, auditBatch, now)
	assertIndexPolicyRuntimeAck(t, auditResponse, "batch-index-policy-audit", 4, 1, nil)
	stored = capture.snapshot()
	if len(stored) != 3 {
		t.Fatalf("stored batches after audit batch = %d, want 3", len(stored))
	}
	assertIndexPolicyRuntimeStoredBatch(
		t,
		stored[2],
		[]string{"runtime:audit:v1"},
		map[string]time.Duration{"audit": auditIndex.Definition.RetentionPeriod},
	)

	auditDefinition := auditIndex.Definition
	auditDefinition.IngestionEnabled = false
	if _, err := database.UpdateIndex(ctx, auditIndex.ID, auditIndex.Version, auditDefinition); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 6,
		SentAt:         timestamppb.New(now),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: auditBatch},
	}); err != nil {
		t.Fatal(err)
	}
	replayedResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	replayed := replayedResponse.GetBatchAck()
	if replayed == nil || replayed.GetBatchId() != auditBatch.GetBatchId() ||
		replayed.GetBatchSequence() != auditBatch.GetBatchSequence() ||
		replayed.GetAcceptedEventCount() != 0 || replayed.GetDuplicateEventCount() != 1 {
		t.Fatalf("disabled-authority durable replay = %#v", replayedResponse)
	}
	if calls := len(capture.snapshot()); calls != 3 {
		t.Fatalf("stored batches after durable replay = %d, want 3", calls)
	}

	freshBatch := indexPolicyRuntimeBatch(t, collectorID, "batch-index-policy-no-authority", 5, now,
		indexPolicyRuntimeEvent(t, "event-no-authority", "audit", "", now,
			indexPolicyRuntimeStringField("one", "1")),
	)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 7,
		SentAt:         timestamppb.New(now),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: freshBatch},
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); response != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatalf("fresh batch without active authority = (%#v, %v), want nil/Unauthenticated", response, err)
	}
	if calls := len(capture.snapshot()); calls != 3 {
		t.Fatalf("stored batches after fresh authority miss = %d, want 3", calls)
	}
}

type indexPolicyRuntimeCollectStream interface {
	Send(*opensplunkv1.CollectRequest) error
	Recv() (*opensplunkv1.CollectResponse, error)
}

func openIndexPolicyRuntimeStream(
	t *testing.T,
	service opensplunkv1.CollectorIngestServiceServer,
	plaintextToken string,
) indexPolicyRuntimeCollectStream {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	connection, err := grpc.NewClient(
		"passthrough:///index-policy-runtime",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	streamContext, cancelStream := context.WithTimeout(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs("authorization", "Bearer "+plaintextToken),
		),
		10*time.Second,
	)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).Collect(streamContext)
	if err != nil {
		cancelStream()
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancelStream()
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
		if err := shutdownGRPCServer(grpcServer, time.Second); err != nil {
			t.Error(err)
		}
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-serveDone:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("serve collector: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("collector gRPC server did not stop")
		}
	})
	return stream
}

func indexPolicyRuntimeEvent(
	t *testing.T,
	eventID string,
	indexName string,
	sourcetype string,
	at time.Time,
	fields ...*opensplunkv1.TypedObjectField,
) *opensplunkv1.LogEvent {
	t.Helper()

	message := eventID
	return &opensplunkv1.LogEvent{
		EventId:     eventID,
		IndexName:   indexName,
		EventTime:   timestamppb.New(at.Add(-time.Second)),
		CollectedAt: timestamppb.New(at),
		EventTimeSource: opensplunkv1.
			EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:        "index-policy-runtime-host",
		Source:      "/var/log/index-policy-runtime.log",
		Sourcetype:  sourcetype,
		Severity:    opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
		Message:     &message,
		Raw:         []byte(message),
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		Fields:      &opensplunkv1.TypedObject{Fields: fields},
	}
}

func indexPolicyRuntimeStringField(name, value string) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{
		Name: name,
		Value: &opensplunkv1.TypedValue{
			Kind: &opensplunkv1.TypedValue_StringValue{StringValue: value},
		},
	}
}

func indexPolicyRuntimeObjectField(
	name string,
	fields ...*opensplunkv1.TypedObjectField,
) *opensplunkv1.TypedObjectField {
	return &opensplunkv1.TypedObjectField{
		Name: name,
		Value: &opensplunkv1.TypedValue{
			Kind: &opensplunkv1.TypedValue_ObjectValue{
				ObjectValue: &opensplunkv1.TypedObject{Fields: fields},
			},
		},
	}
}

func indexPolicyRuntimeBatch(
	t *testing.T,
	collectorID string,
	batchID string,
	batchSequence uint64,
	createdAt time.Time,
	events ...*opensplunkv1.LogEvent,
) *opensplunkv1.EventBatch {
	t.Helper()

	return &opensplunkv1.EventBatch{
		CollectorId:           collectorID,
		BatchId:               batchID,
		BatchSequence:         batchSequence,
		CreatedAt:             timestamppb.New(createdAt),
		Events:                events,
		UncompressedSizeBytes: ingest.UncompressedEventBytes(events),
		EventIdsSha256:        ingest.EventIDDigest(events),
		ProtocolMajor:         1,
		ProtocolMinor:         0,
	}
}

func sendIndexPolicyRuntimeBatch(
	t *testing.T,
	stream indexPolicyRuntimeCollectStream,
	streamSequence uint64,
	batch *opensplunkv1.EventBatch,
	sentAt time.Time,
) *opensplunkv1.CollectResponse {
	t.Helper()

	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: streamSequence,
		SentAt:         timestamppb.New(sentAt),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: batch},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return response
}

type indexPolicyRuntimeRejection struct {
	eventIndex uint32
	eventID    string
	code       opensplunkv1.EventRejectionCode
}

func assertIndexPolicyRuntimeAck(
	t *testing.T,
	response *opensplunkv1.CollectResponse,
	batchID string,
	batchSequence uint64,
	accepted uint32,
	wantRejections []indexPolicyRuntimeRejection,
) {
	t.Helper()

	ack := response.GetBatchAck()
	if ack == nil || ack.GetBatchId() != batchID || ack.GetBatchSequence() != batchSequence ||
		ack.GetAcknowledgedThroughBatchSequence() != batchSequence ||
		ack.GetAcceptedEventCount() != accepted || ack.GetDuplicateEventCount() != 0 ||
		len(ack.GetRejectedEvents()) != len(wantRejections) ||
		ack.GetDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		t.Fatalf("batch acknowledgment = %#v", response)
	}
	for index, want := range wantRejections {
		got := ack.GetRejectedEvents()[index]
		if got.GetEventIndex() != want.eventIndex || got.GetEventId() != want.eventID || got.GetCode() != want.code {
			t.Fatalf("rejection[%d] = %#v, want %+v", index, got, want)
		}
	}
}

func assertIndexPolicyRuntimeStoredBatch(
	t *testing.T,
	batch ingest.StoreBatch,
	wantSourcetypes []string,
	wantRetention map[string]time.Duration,
) {
	t.Helper()

	if len(batch.Events) != len(wantSourcetypes) || len(batch.RetentionByIndex) != len(wantRetention) {
		t.Fatalf("stored batch = %#v", batch)
	}
	for index, want := range wantSourcetypes {
		if got := batch.Events[index].Event.GetSourcetype(); got != want {
			t.Fatalf("stored sourcetype[%d] = %q, want %q", index, got, want)
		}
	}
	for indexName, want := range wantRetention {
		if got := batch.RetentionByIndex[indexName]; got != want {
			t.Fatalf("retention[%q] = %v, want %v", indexName, got, want)
		}
	}
}

type indexPolicyRuntimeEventStore struct {
	mu      sync.Mutex
	batches []ingest.StoreBatch
	results map[ingest.StoreBatchIdentity]ingest.StoreResult
}

func (store *indexPolicyRuntimeEventStore) Store(
	_ context.Context,
	batch ingest.StoreBatch,
) (ingest.StoreResult, error) {
	detached := indexPolicyRuntimeCloneBatch(batch)
	acknowledged := batch.BatchSequence
	result := ingest.StoreResult{
		Accepted:            uint32(len(batch.Events)),
		AcknowledgedThrough: &acknowledged,
		CommittedAt:         batch.ReceivedAt,
		OriginalEventCount:  batch.OriginalEventCount,
		RejectedEvents:      indexPolicyRuntimeCloneRejections(batch.RejectedEvents),
	}
	identity := ingest.StoreBatchIdentity{
		TenantID: batch.TenantID, CollectorID: batch.CollectorID, BatchID: batch.BatchID,
		BatchSequence: batch.BatchSequence, SourceBatchSHA256: batch.SourceBatchSHA256,
	}
	store.mu.Lock()
	if store.results == nil {
		store.results = make(map[ingest.StoreBatchIdentity]ingest.StoreResult)
	}
	store.batches = append(store.batches, detached)
	store.results[identity] = result
	store.mu.Unlock()
	return result, nil
}

func (store *indexPolicyRuntimeEventStore) LookupBatch(
	_ context.Context,
	identity ingest.StoreBatchIdentity,
) (ingest.StoredBatchState, ingest.StoreResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	result, ok := store.results[identity]
	if !ok {
		return ingest.StoredBatchNotFound, ingest.StoreResult{}, nil
	}
	if result.BatchRejection != nil {
		result.BatchRejection = proto.Clone(result.BatchRejection).(*opensplunkv1.BatchReject)
		return ingest.StoredBatchRejected, result, nil
	}
	result.Duplicate = result.Accepted
	result.Accepted = 0
	result.RejectedEvents = indexPolicyRuntimeCloneRejections(result.RejectedEvents)
	return ingest.StoredBatchCommitted, result, nil
}

func (store *indexPolicyRuntimeEventStore) RejectBatch(
	_ context.Context,
	rejected ingest.StoreBatchRejection,
) (ingest.StoreResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.results == nil {
		store.results = make(map[ingest.StoreBatchIdentity]ingest.StoreResult)
	}
	if existing, ok := store.results[rejected.Identity]; ok {
		if existing.BatchRejection != nil {
			existing.BatchRejection = proto.Clone(existing.BatchRejection).(*opensplunkv1.BatchReject)
		} else {
			existing.Duplicate = existing.Accepted
			existing.Accepted = 0
			existing.RejectedEvents = indexPolicyRuntimeCloneRejections(existing.RejectedEvents)
		}
		return existing, nil
	}
	result := ingest.StoreResult{
		BatchRejection: proto.Clone(rejected.Rejection).(*opensplunkv1.BatchReject),
	}
	store.results[rejected.Identity] = result
	return ingest.StoreResult{
		BatchRejection: proto.Clone(result.BatchRejection).(*opensplunkv1.BatchReject),
	}, nil
}

func (*indexPolicyRuntimeEventStore) ResumeBatch(
	context.Context,
	ingest.StoreBatchIdentity,
) (ingest.StoreResult, error) {
	return ingest.StoreResult{}, errors.New("runtime test store has no pending batch")
}

func (store *indexPolicyRuntimeEventStore) snapshot() []ingest.StoreBatch {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]ingest.StoreBatch(nil), store.batches...)
}

func indexPolicyRuntimeCloneBatch(batch ingest.StoreBatch) ingest.StoreBatch {
	detached := batch
	detached.Events = make([]*ingest.StoredEvent, len(batch.Events))
	for index, event := range batch.Events {
		if event == nil {
			continue
		}
		cloned := *event
		if event.Event != nil {
			cloned.Event = proto.Clone(event.Event).(*opensplunkv1.LogEvent)
		}
		detached.Events[index] = &cloned
	}
	detached.RetentionByIndex = make(map[string]time.Duration, len(batch.RetentionByIndex))
	for indexName, retention := range batch.RetentionByIndex {
		detached.RetentionByIndex[indexName] = retention
	}
	detached.RejectedEvents = indexPolicyRuntimeCloneRejections(batch.RejectedEvents)
	return detached
}

func indexPolicyRuntimeCloneRejections(
	values []*opensplunkv1.EventRejection,
) []*opensplunkv1.EventRejection {
	cloned := make([]*opensplunkv1.EventRejection, len(values))
	for index, rejection := range values {
		if rejection != nil {
			cloned[index] = proto.Clone(rejection).(*opensplunkv1.EventRejection)
		}
	}
	return cloned
}
