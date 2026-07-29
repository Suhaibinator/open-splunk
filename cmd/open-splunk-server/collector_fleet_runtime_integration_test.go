package main

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync/atomic"
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
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRuntimeCollectorLifecyclePersistsHeartbeatAndFencesDisabledBatch(
	t *testing.T,
) {
	ctx := context.Background()
	database, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "collector-fleet-runtime.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatal(err)
	}

	const (
		tenantID    = "tenant-runtime-fleet"
		collectorID = "collector-runtime-fleet"
		instanceID  = "instance-runtime-fleet"
		bootEpoch   = "boot-runtime-fleet"
		streamID    = "stream-runtime-fleet"
		inputID     = "input-runtime-fleet"
	)
	tokens, err := auth.NewStore(
		database,
		[]byte("collector-runtime-digest-key-32b"),
	)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := tokens.CreateCollectorToken(
		ctx,
		auth.CreateCollectorTokenRequest{
			Name:              "runtime fleet collector",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  collectorID,
		},
	)
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

	base := issued.Token.CreatedAt.
		Add(time.Second).
		Truncate(time.Microsecond).
		UTC()
	var clockUnixMicro atomic.Int64
	clockUnixMicro.Store(base.UnixMicro())
	var storeCalls atomic.Uint32
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time {
		return time.UnixMicro(clockUnixMicro.Load()).UTC()
	}
	config.NewStreamID = func() string { return streamID }
	config.ServerInstanceID = bootEpoch
	config.ServerVersion = "runtime-fleet-test"
	config.SessionManager = collectorSessionManager{
		admission: admissions,
		fleet:     fleet,
	}
	service, err := ingest.NewService(
		config,
		collectorAuthorizer{store: tokens, tenantID: tenantID},
		ingest.EventStoreFunc(func(
			context.Context,
			ingest.StoreBatch,
		) (ingest.StoreResult, error) {
			storeCalls.Add(1)
			return ingest.StoreResult{
				Accepted:    1,
				CommittedAt: time.UnixMicro(clockUnixMicro.Load()).UTC(),
			}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
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

	connection, err := grpc.NewClient(
		"passthrough:///runtime-collector-fleet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	streamContext, cancelStream := context.WithTimeout(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer "+issued.Secret.Plaintext(),
			),
		),
		10*time.Second,
	)
	t.Cleanup(cancelStream)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(base),
		Payload: &opensplunkv1.CollectRequest_Hello{
			Hello: &opensplunkv1.CollectorHello{
				CollectorId:      collectorID,
				InstanceId:       instanceID,
				ProtocolMajor:    1,
				ProtocolMinor:    0,
				CollectorVersion: "runtime-fleet-test",
				Hostname:         "runtime-fleet-host",
				OperatingSystem:  "linux",
				Architecture:     "amd64",
				StartedAt:        timestamppb.New(base.Add(-time.Minute)),
				Capabilities: []opensplunkv1.CollectorCapability{
					opensplunkv1.
						CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
				},
				Inputs: []*opensplunkv1.CollectorInputRegistration{{
					InputId: inputID,
					InputType: opensplunkv1.
						CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
					IndexName: "main",
				}},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	ready := readyResponse.GetReady()
	if ready == nil ||
		ready.GetStreamId() != streamID ||
		ready.GetServerInstanceId() != bootEpoch {
		t.Fatalf("collector Ready = %#v", readyResponse)
	}

	heartbeatAt := base.Add(time.Second)
	clockUnixMicro.Store(heartbeatAt.UnixMicro())
	lastSent := uint64(9)
	lastAcknowledged := uint64(8)
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(heartbeatAt),
		Payload: &opensplunkv1.CollectRequest_Heartbeat{
			Heartbeat: &opensplunkv1.CollectorHeartbeat{
				CollectorId: collectorID,
				InstanceId:  instanceID,
				ObservedAt:  timestamppb.New(heartbeatAt),
				Queue: &opensplunkv1.CollectorQueueStats{
					QueuedEvents:            3,
					QueuedBytes:             4096,
					OldestEventAge:          durationpb.New(2 * time.Second),
					SentEventsTotal:         12,
					AcknowledgedEventsTotal: 10,
					RetriedBatchesTotal:     2,
					RejectedEventsTotal:     1,
				},
				Inputs: []*opensplunkv1.CollectorInputHealth{{
					InputId: inputID,
					State: opensplunkv1.
						CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY,
					StatusMessage:     "healthy",
					DiscoveredSources: 2,
					ActiveSources:     1,
					EventsReadTotal:   12,
					BytesReadTotal:    2048,
					LastEventAt:       timestamppb.New(heartbeatAt),
				}},
				LastSentBatchSequence:         &lastSent,
				LastAcknowledgedBatchSequence: &lastAcknowledged,
				ProcessResidentMemoryBytes:    64 << 20,
				ProcessCpuPercent:             7.5,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	scope := collectorfleet.Scope{TenantID: tenantID}
	persisted := waitForRuntimeFleetObservation(
		t,
		fleet,
		scope,
		collectorID,
		2,
	)
	if persisted.ActiveLease == nil ||
		persisted.ActiveLease.BootEpoch != bootEpoch ||
		persisted.ActiveLease.StreamID != streamID ||
		persisted.ActiveLease.InstanceID != instanceID ||
		persisted.ObservationSequence != 2 ||
		!persisted.ObservedAt.Equal(heartbeatAt) ||
		!persisted.LastSeenAt.Equal(heartbeatAt) ||
		persisted.Queue.QueuedEvents != 3 ||
		persisted.Queue.QueuedBytes != 4096 ||
		persisted.LastSentBatchSequence == nil ||
		*persisted.LastSentBatchSequence != lastSent ||
		len(persisted.InputHealth) != 1 ||
		persisted.InputHealth[0].InputID != inputID ||
		persisted.InputHealth[0].StatusMessage != "healthy" {
		t.Fatalf("persisted collector heartbeat = %#v", persisted)
	}

	disabledAt := base.Add(2 * time.Second)
	clockUnixMicro.Store(disabledAt.UnixMicro())
	disabled, err := fleet.UpdateAdministration(
		ctx,
		scope,
		collectorID,
		persisted.Version,
		collectorfleet.Administration{
			State: collectorfleet.AdministrativeStateDisabled,
		},
		disabledAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	disabledState, err := fleet.Get(ctx, scope, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.AdministrativeState !=
		collectorfleet.AdministrativeStateDisabled ||
		disabledState.Version != disabled.Version ||
		disabledState.AdministrativeState !=
			collectorfleet.AdministrativeStateDisabled ||
		disabledState.ActiveLease != nil ||
		disabledState.DisconnectedAt == nil ||
		!disabledState.DisconnectedAt.Equal(disabledAt) {
		t.Fatalf(
			"disabled collector = administration:%#v fleet:%#v",
			disabled,
			disabledState,
		)
	}

	message := "must not be stored after collector disable"
	event := &opensplunkv1.LogEvent{
		EventId:     "event-after-disable",
		IndexName:   "main",
		EventTime:   timestamppb.New(disabledAt.Add(-time.Second)),
		CollectedAt: timestamppb.New(disabledAt),
		EventTimeSource: opensplunkv1.
			EventTimeSource_EVENT_TIME_SOURCE_PARSED,
		Host:        "runtime-fleet-host",
		Source:      "/var/log/runtime-fleet.log",
		Sourcetype:  "runtime:fleet",
		Severity:    opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
		Message:     &message,
		Raw:         []byte(message),
		RawEncoding: opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
	}
	batch := &opensplunkv1.EventBatch{
		CollectorId:   collectorID,
		BatchId:       "batch-after-disable",
		BatchSequence: 1,
		CreatedAt:     timestamppb.New(disabledAt),
		Events:        []*opensplunkv1.LogEvent{event},
		UncompressedSizeBytes: ingest.UncompressedEventBytes(
			[]*opensplunkv1.LogEvent{event},
		),
		EventIdsSha256: ingest.EventIDDigest(
			[]*opensplunkv1.LogEvent{event},
		),
		ProtocolMajor: 1,
		ProtocolMinor: 0,
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 3,
		SentAt:         timestamppb.New(disabledAt),
		Payload: &opensplunkv1.CollectRequest_Batch{
			Batch: batch,
		},
	}); err != nil {
		t.Fatal(err)
	}
	response, err := stream.Recv()
	if response != nil || status.Code(err) != codes.Aborted {
		t.Fatalf(
			"batch after disable = (%#v, %v), want nil/Aborted",
			response,
			err,
		)
	}
	if calls := storeCalls.Load(); calls != 0 {
		t.Fatalf("event store calls after disable = %d, want 0", calls)
	}

	final, err := fleet.Get(ctx, scope, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	if final.AdministrativeState !=
		collectorfleet.AdministrativeStateDisabled ||
		final.Version != disabled.Version ||
		final.TelemetryRevision != disabledState.TelemetryRevision ||
		final.ActiveLease != nil ||
		final.DisconnectedAt == nil ||
		!final.DisconnectedAt.Equal(disabledAt) ||
		final.ObservationSequence != 2 ||
		final.Queue.QueuedEvents != 3 ||
		len(final.InputHealth) != 1 {
		t.Fatalf("final disabled collector state = %#v", final)
	}
}

func TestRuntimeCollectorLifecycleDisconnectsActiveLeaseOnGoodbye(
	t *testing.T,
) {
	ctx := context.Background()
	database, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "collector-fleet-cleanup.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := database.CreateIndex(ctx, control.IndexDefinition{
		Name:             "main",
		DisplayName:      "Main",
		IngestionEnabled: true,
		SearchEnabled:    true,
	}); err != nil {
		t.Fatal(err)
	}

	const (
		tenantID    = "tenant-runtime-cleanup"
		collectorID = "collector-runtime-cleanup"
		instanceID  = "instance-runtime-cleanup"
		bootEpoch   = "boot-runtime-cleanup"
		streamID    = "stream-runtime-cleanup"
	)
	tokens, err := auth.NewStore(
		database,
		[]byte("collector-cleanup-digest-key-32b"),
	)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := tokens.CreateCollectorToken(
		ctx,
		auth.CreateCollectorTokenRequest{
			Name:              "runtime cleanup collector",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  collectorID,
		},
	)
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

	base := issued.Token.CreatedAt.
		Add(time.Second).
		Truncate(time.Microsecond).
		UTC()
	var clockUnixMicro atomic.Int64
	clockUnixMicro.Store(base.UnixMicro())
	config := ingest.DefaultConfig()
	config.Clock = func() time.Time {
		return time.UnixMicro(clockUnixMicro.Load()).UTC()
	}
	config.NewStreamID = func() string { return streamID }
	config.ServerInstanceID = bootEpoch
	config.ServerVersion = "runtime-cleanup-test"
	config.SessionManager = collectorSessionManager{
		admission: admissions,
		fleet:     fleet,
	}
	service, err := ingest.NewService(
		config,
		collectorAuthorizer{store: tokens, tenantID: tenantID},
		ingest.EventStoreFunc(func(
			context.Context,
			ingest.StoreBatch,
		) (ingest.StoreResult, error) {
			return ingest.StoreResult{}, errors.New(
				"event storage is unexpected in collector cleanup test",
			)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(grpcServer, service)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
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

	connection, err := grpc.NewClient(
		"passthrough:///runtime-collector-cleanup",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	streamContext, cancelStream := context.WithTimeout(
		metadata.NewOutgoingContext(
			context.Background(),
			metadata.Pairs(
				"authorization",
				"Bearer "+issued.Secret.Plaintext(),
			),
		),
		10*time.Second,
	)
	t.Cleanup(cancelStream)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(base),
		Payload: &opensplunkv1.CollectRequest_Hello{
			Hello: &opensplunkv1.CollectorHello{
				CollectorId:      collectorID,
				InstanceId:       instanceID,
				ProtocolMajor:    1,
				ProtocolMinor:    0,
				CollectorVersion: "runtime-cleanup-test",
				Hostname:         "runtime-cleanup-host",
				OperatingSystem:  "linux",
				Architecture:     "amd64",
				StartedAt:        timestamppb.New(base.Add(-time.Minute)),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	ready := readyResponse.GetReady()
	if ready == nil ||
		ready.GetStreamId() != streamID ||
		ready.GetServerInstanceId() != bootEpoch {
		t.Fatalf("collector Ready = %#v", readyResponse)
	}

	scope := collectorfleet.Scope{TenantID: tenantID}
	active, err := fleet.Get(ctx, scope, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	if active.ActiveLease == nil ||
		active.ActiveLease.BootEpoch != bootEpoch ||
		active.ActiveLease.StreamID != streamID ||
		active.ActiveLease.InstanceID != instanceID ||
		active.TelemetryRevision == 0 ||
		active.DisconnectedAt != nil {
		t.Fatalf("active collector before Goodbye = %#v", active)
	}

	disconnectedAt := base.Add(2 * time.Second)
	clockUnixMicro.Store(disconnectedAt.UnixMicro())
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(disconnectedAt),
		Payload: &opensplunkv1.CollectRequest_Goodbye{
			Goodbye: &opensplunkv1.CollectorGoodbye{
				Reason: opensplunkv1.
					CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); response != nil || !errors.Is(err, io.EOF) {
		t.Fatalf(
			"collector Goodbye result = (%#v, %v), want nil/EOF",
			response,
			err,
		)
	}

	disconnected, err := fleet.Get(ctx, scope, collectorID)
	if err != nil {
		t.Fatal(err)
	}
	if disconnected.ActiveLease != nil ||
		disconnected.DisconnectedAt == nil ||
		!disconnected.DisconnectedAt.Equal(disconnectedAt) ||
		!disconnected.LastSeenAt.Equal(disconnectedAt) ||
		disconnected.TelemetryRevision != active.TelemetryRevision+1 ||
		disconnected.LeaseGeneration != active.LeaseGeneration ||
		disconnected.Version != active.Version {
		t.Fatalf(
			"collector after exact Goodbye cleanup = before:%#v after:%#v",
			active,
			disconnected,
		)
	}
}

func waitForRuntimeFleetObservation(
	t *testing.T,
	fleet *collectorfleet.Store,
	scope collectorfleet.Scope,
	collectorID string,
	observationSequence uint64,
) collectorfleet.Collector {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		collector, err := fleet.Get(ctx, scope, collectorID)
		if err != nil {
			t.Fatal(err)
		}
		if collector.ObservationSequence == observationSequence {
			return collector
		}
		select {
		case <-ctx.Done():
			t.Fatalf(
				"collector observation sequence = %d, want %d",
				collector.ObservationSequence,
				observationSequence,
			)
		case <-ticker.C:
		}
	}
}
