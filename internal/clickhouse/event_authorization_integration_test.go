package clickhouse

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testEventAuthorizationFilteringAgainstClickHouse(
	t *testing.T,
	ctx context.Context,
	config Config,
	queryConnection clickhousedriver.Conn,
	receivedAt time.Time,
) {
	t.Helper()

	controlDB, err := control.Open(
		ctx,
		filepath.Join(t.TempDir(), "event-authorization.sqlite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	store, err := Open(config, fixedRetention(30*24*time.Hour), sequencer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	const (
		bearer      = "event-authorization-token"
		tenantID    = "event-authorization-tenant"
		collectorID = "event-authorization-collector"
		indexName   = "event-authorization-index"
		batchID     = "event-authorization-partial-batch"
		acceptedID  = "event-authorization-accepted-event"
		rejectedID  = "event-authorization-rejected-event"
	)
	authorization := ingest.Authorization{
		SubjectID:   "event-authorization-token-id",
		TenantID:    tenantID,
		CollectorID: collectorID,
		AuthorizedIndexes: []ingest.IndexPolicy{{
			Name:            indexName,
			Version:         1,
			RetentionPeriod: 30 * 24 * time.Hour,
		}},
		AllowedHostRegexes:   []string{`allowed-host`},
		AllowedSourceRegexes: []string{`/var/log/allowed\.log`},
	}
	authorizer := ingest.AuthorizerFunc(func(
		_ context.Context,
		gotBearer string,
	) (ingest.Authorization, error) {
		if gotBearer != bearer {
			return ingest.Authorization{}, ingest.ErrUnauthorized
		}
		return cloneClickHouseConstraintAuthorization(authorization), nil
	})
	manager := &clickHouseConstraintSessionManager{
		bearer:        bearer,
		authorization: authorization,
	}
	ingestConfig := ingest.DefaultConfig()
	ingestConfig.Clock = func() time.Time { return receivedAt }
	ingestConfig.NewStreamID = func() string { return "event-authorization-stream" }
	ingestConfig.ServerInstanceID = "event-authorization-server"
	ingestConfig.ServerVersion = "event-authorization-integration"
	ingestConfig.SessionManager = manager
	service, err := ingest.NewService(ingestConfig, authorizer, store)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	opensplunkv1.RegisterCollectorIngestServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	connection, err := grpc.NewClient(
		"passthrough:///event-authorization-clickhouse",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	streamContext := metadata.NewOutgoingContext(
		ctx,
		metadata.Pairs("authorization", "Bearer "+bearer),
	)
	stream, err := opensplunkv1.NewCollectorIngestServiceClient(connection).
		Collect(streamContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 1,
		SentAt:         timestamppb.New(receivedAt),
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: &opensplunkv1.CollectorHello{
			CollectorId:      collectorID,
			InstanceId:       "event-authorization-instance",
			ProtocolMajor:    1,
			ProtocolMinor:    0,
			CollectorVersion: "event-authorization-integration",
			Hostname:         "event-authorization-host",
			StartedAt:        timestamppb.New(receivedAt.Add(-time.Hour)),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	readyResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if readyResponse.GetReady() == nil {
		t.Fatalf("collector ready response = %#v", readyResponse)
	}

	newEvent := func(eventID, host, message string) *opensplunkv1.LogEvent {
		return &opensplunkv1.LogEvent{
			EventId:         eventID,
			IndexName:       indexName,
			EventTime:       timestamppb.New(receivedAt.Add(-time.Second)),
			CollectedAt:     timestamppb.New(receivedAt),
			EventTimeSource: opensplunkv1.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            host,
			Source:          "/var/log/allowed.log",
			Sourcetype:      "json",
			Severity:        opensplunkv1.LogSeverity_LOG_SEVERITY_INFO,
			Message:         &message,
			Raw:             []byte(message),
			RawEncoding:     opensplunkv1.RawEncoding_RAW_ENCODING_UTF8,
		}
	}
	acceptedEvent := newEvent(
		acceptedID,
		"allowed-host",
		"this authorized event must reach ClickHouse",
	)
	// This contains the allowed expression but is not a complete match. It
	// catches an accidental substring match while exercising mixed filtering.
	rejectedHost := "prefix-allowed-host-suffix"
	rejectedEvent := newEvent(
		rejectedID,
		rejectedHost,
		"this host-rejected event must not reach ClickHouse",
	)
	events := []*opensplunkv1.LogEvent{acceptedEvent, rejectedEvent}
	batch := &opensplunkv1.EventBatch{
		CollectorId:           collectorID,
		BatchId:               batchID,
		BatchSequence:         1,
		CreatedAt:             timestamppb.New(receivedAt),
		Events:                events,
		UncompressedSizeBytes: ingest.UncompressedEventBytes(events),
		EventIdsSha256:        ingest.EventIDDigest(events),
		ProtocolMajor:         1,
		ProtocolMinor:         0,
	}
	if err := stream.Send(&opensplunkv1.CollectRequest{
		StreamSequence: 2,
		SentAt:         timestamppb.New(receivedAt),
		Payload:        &opensplunkv1.CollectRequest_Batch{Batch: batch},
	}); err != nil {
		t.Fatal(err)
	}
	ackResponse, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	ack := ackResponse.GetBatchAck()
	if ack == nil || ack.GetAcceptedEventCount() != 1 ||
		ack.GetDuplicateEventCount() != 0 || len(ack.GetRejectedEvents()) != 1 {
		t.Fatalf("partial event authorization response = %#v", ackResponse)
	}
	rejection := ack.GetRejectedEvents()[0]
	if rejection.GetEventIndex() != 1 || rejection.GetEventId() != rejectedID ||
		rejection.GetCode() != opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST ||
		len(rejection.GetViolations()) != 1 ||
		rejection.GetViolations()[0].GetFieldPath() != "host" ||
		rejection.GetViolations()[0].GetCode() != "unauthorized_host" {
		t.Fatalf("host rejection = %#v", rejection)
	}
	if strings.Contains(rejection.String(), rejectedHost) ||
		strings.Contains(rejection.String(), authorization.AllowedHostRegexes[0]) {
		t.Fatalf("host rejection disclosed constraint data: %#v", rejection)
	}

	var acceptedRows, rejectedRows, batchRows uint64
	if err := queryConnection.QueryRow(
		ctx,
		`SELECT
			countIf(event_id = ?),
			countIf(event_id = ?),
			countIf(batch_id = ?)
		 FROM open_splunk.events`,
		acceptedID,
		rejectedID,
		batchID,
	).Scan(&acceptedRows, &rejectedRows, &batchRows); err != nil {
		t.Fatal(err)
	}
	if acceptedRows != 1 || rejectedRows != 0 || batchRows != 1 {
		t.Fatalf(
			"partial authorization ClickHouse rows = accepted %d rejected %d total %d, want 1/0/1",
			acceptedRows,
			rejectedRows,
			batchRows,
		)
	}
}

type clickHouseConstraintSessionManager struct {
	bearer        string
	authorization ingest.Authorization
}

func (manager *clickHouseConstraintSessionManager) Admit(
	_ context.Context,
	bearer string,
	request ingest.CollectorSessionAdmissionRequest,
) (ingest.CollectorSessionAdmission, error) {
	if bearer != manager.bearer ||
		request.CollectorID != manager.authorization.CollectorID {
		return ingest.CollectorSessionAdmission{}, ingest.ErrUnauthorized
	}
	return ingest.CollectorSessionAdmission{
		Authorization: cloneClickHouseConstraintAuthorization(manager.authorization),
		Lease: collectorfleet.Lease{
			Scope:       collectorfleet.Scope{TenantID: manager.authorization.TenantID},
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			Generation:  1,
		},
	}, nil
}

func (*clickHouseConstraintSessionManager) Activate(collectorfleet.Lease) error {
	return nil
}

func (manager *clickHouseConstraintSessionManager) AuthorizeLease(
	_ context.Context,
	bearer string,
	_ collectorfleet.Lease,
	_ time.Time,
) (ingest.Authorization, error) {
	if bearer != manager.bearer {
		return ingest.Authorization{}, ingest.ErrUnauthorized
	}
	return cloneClickHouseConstraintAuthorization(manager.authorization), nil
}

func (*clickHouseConstraintSessionManager) RecordHeartbeat(
	context.Context,
	collectorfleet.Lease,
	collectorfleet.Heartbeat,
) (bool, error) {
	return true, nil
}

func (*clickHouseConstraintSessionManager) Disconnect(
	context.Context,
	collectorfleet.Lease,
	time.Time,
) (bool, error) {
	return true, nil
}

func cloneClickHouseConstraintAuthorization(
	authorization ingest.Authorization,
) ingest.Authorization {
	authorization.AuthorizedIndexes = append(
		[]ingest.IndexPolicy(nil),
		authorization.AuthorizedIndexes...,
	)
	authorization.AllowedHostRegexes = append(
		[]string(nil),
		authorization.AllowedHostRegexes...,
	)
	authorization.AllowedSourceRegexes = append(
		[]string(nil),
		authorization.AllowedSourceRegexes...,
	)
	return authorization
}
