package ingest

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config controls collector protocol negotiation and ingestion enforcement.
type Config struct {
	Limits             Limits
	Redaction          RedactionPolicy
	ProtocolMajor      uint32
	ProtocolMinor      uint32
	ServerInstanceID   string
	ServerVersion      string
	HeartbeatInterval  time.Duration
	MaxInFlightBatches uint32
	DefaultRetryAfter  time.Duration
	Clock              func() time.Time
	NewStreamID        func() string
}

func DefaultConfig() Config {
	return Config{
		Limits:             DefaultLimits(),
		ProtocolMajor:      1,
		ProtocolMinor:      0,
		ServerVersion:      "development",
		HeartbeatInterval:  15 * time.Second,
		MaxInFlightBatches: 1,
		DefaultRetryAfter:  time.Second,
		Clock:              time.Now,
		NewStreamID:        randomID,
	}
}

// Service is the authenticated collector gRPC ingestion boundary.
type Service struct {
	opensplunkv1.UnimplementedCollectorIngestServiceServer

	config     Config
	validator  *Validator
	authorizer Authorizer
	store      EventStore
}

func NewService(config Config, authorizer Authorizer, store EventStore) (*Service, error) {
	defaults := DefaultConfig()
	if config.Limits == (Limits{}) {
		config.Limits = defaults.Limits
	}
	if config.ProtocolMajor == 0 {
		config.ProtocolMajor = defaults.ProtocolMajor
	}
	if config.ServerInstanceID == "" {
		config.ServerInstanceID = randomID()
	}
	if config.ServerVersion == "" {
		config.ServerVersion = defaults.ServerVersion
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.MaxInFlightBatches == 0 {
		config.MaxInFlightBatches = defaults.MaxInFlightBatches
	}
	if config.DefaultRetryAfter == 0 {
		config.DefaultRetryAfter = defaults.DefaultRetryAfter
	}
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.NewStreamID == nil {
		config.NewStreamID = defaults.NewStreamID
	}
	if authorizer == nil {
		return nil, errors.New("ingest authorizer is required")
	}
	if store == nil {
		return nil, errors.New("ingest event store is required")
	}
	if config.HeartbeatInterval <= 0 {
		return nil, errors.New("heartbeat interval must be positive")
	}
	if config.DefaultRetryAfter <= 0 {
		return nil, errors.New("default retry delay must be positive")
	}
	if !validIdentifier(config.ServerInstanceID, config.Limits.MaxIDBytes) {
		return nil, errors.New("server instance ID has an invalid format")
	}
	validator, err := NewValidator(config.Limits, config.Redaction)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:     config,
		validator:  validator,
		authorizer: authorizer,
		store:      store,
	}, nil
}

func (s *Service) Collect(stream opensplunkv1.CollectorIngestService_CollectServer) error {
	token, err := bearerToken(stream.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, "valid bearer authentication is required")
	}
	authorization, err := s.authorizer.Authorize(stream.Context(), token)
	if err != nil {
		return status.Error(codes.Unauthenticated, "collector authentication failed")
	}
	authorizedIndexes := normalizedAuthorizedIndexes(authorization.AuthorizedIndexes, s.config.Limits.MaxIDBytes)
	authorizedSet := make(map[string]struct{}, len(authorizedIndexes))
	for _, index := range authorizedIndexes {
		authorizedSet[index] = struct{}{}
	}

	request, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "first request must be CollectorHello")
	}
	if err != nil {
		return err
	}
	if err := s.validateRequestEnvelope(request, 1); err != nil {
		return err
	}
	hello := request.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first request must be CollectorHello")
	}
	if err := s.validateHello(hello, authorization); err != nil {
		return err
	}

	state := streamState{
		collectorID:       hello.GetCollectorId(),
		instanceID:        hello.GetInstanceId(),
		protocolMajor:     hello.GetProtocolMajor(),
		protocolMinor:     hello.GetProtocolMinor(),
		authorization:     authorization,
		authorizedIndexes: authorizedSet,
		batchesBySequence: make(map[uint64]batchIdentity),
		sequenceByBatchID: make(map[string]uint64),
	}
	responseSequence := uint64(1)
	streamID := s.config.NewStreamID()
	if !validIdentifier(streamID, s.config.Limits.MaxIDBytes) {
		return status.Error(codes.Internal, "failed to allocate collector stream ID")
	}
	if err := stream.Send(&opensplunkv1.CollectResponse{
		StreamSequence: responseSequence,
		SentAt:         timestamppb.New(s.config.Clock().UTC()),
		Payload: &opensplunkv1.CollectResponse_Ready{Ready: &opensplunkv1.CollectorReady{
			StreamId:                 streamID,
			ServerInstanceId:         s.config.ServerInstanceID,
			ServerVersion:            s.config.ServerVersion,
			ProtocolMajor:            s.config.ProtocolMajor,
			ProtocolMinor:            hello.GetProtocolMinor(),
			ServerTime:               timestamppb.New(s.config.Clock().UTC()),
			HeartbeatInterval:        durationpb.New(s.config.HeartbeatInterval),
			MaxInFlightBatches:       s.config.MaxInFlightBatches,
			MaxBatchEvents:           s.config.Limits.MaxBatchEvents,
			MaxBatchBytes:            s.config.Limits.MaxBatchBytes,
			MaxEventBytes:            s.config.Limits.MaxEventBytes,
			AuthorizedIndexes:        authorizedIndexes,
			AcknowledgmentDurability: opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		}},
	}); err != nil {
		return err
	}

	expectedRequestSequence := uint64(2)
	for {
		request, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := s.validateRequestEnvelope(request, expectedRequestSequence); err != nil {
			return err
		}
		if expectedRequestSequence == math.MaxUint64 {
			return status.Error(codes.ResourceExhausted, "collector stream sequence exhausted")
		}
		expectedRequestSequence++

		switch payload := request.GetPayload().(type) {
		case *opensplunkv1.CollectRequest_Hello:
			return status.Error(codes.InvalidArgument, "CollectorHello may only appear as the first request")
		case *opensplunkv1.CollectRequest_Heartbeat:
			if err := s.validateHeartbeat(payload.Heartbeat, &state); err != nil {
				return err
			}
			continue
		case *opensplunkv1.CollectRequest_Goodbye:
			if payload.Goodbye == nil {
				return status.Error(codes.InvalidArgument, "goodbye payload is required")
			}
			return nil
		case *opensplunkv1.CollectRequest_Batch:
			response, err := s.processBatch(stream.Context(), payload.Batch, &state)
			if err != nil {
				return err
			}
			if responseSequence == math.MaxUint64 {
				return status.Error(codes.ResourceExhausted, "server stream sequence exhausted")
			}
			responseSequence++
			response.StreamSequence = responseSequence
			response.SentAt = timestamppb.New(s.config.Clock().UTC())
			if err := stream.Send(response); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "collector request payload is required")
		}
	}
}

func (s *Service) validateRequestEnvelope(request *opensplunkv1.CollectRequest, expectedSequence uint64) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "collector request is required")
	}
	if request.GetStreamSequence() != expectedSequence {
		return status.Errorf(codes.InvalidArgument, "stream_sequence must be %d", expectedSequence)
	}
	if request.GetPayload() == nil {
		return status.Error(codes.InvalidArgument, "collector request payload is required")
	}
	if err := s.validator.validateTimestamp(request.GetSentAt(), s.config.Clock()); err != nil {
		return status.Error(codes.InvalidArgument, "sent_at is invalid or outside accepted bounds")
	}
	return nil
}

func (s *Service) validateHello(hello *opensplunkv1.CollectorHello, authorization Authorization) error {
	if hello == nil {
		return status.Error(codes.InvalidArgument, "hello payload is required")
	}
	if !validIdentifier(hello.GetCollectorId(), s.config.Limits.MaxIDBytes) {
		return status.Error(codes.InvalidArgument, "collector_id has an invalid format")
	}
	if !validIdentifier(hello.GetInstanceId(), s.config.Limits.MaxIDBytes) {
		return status.Error(codes.InvalidArgument, "instance_id has an invalid format")
	}
	if authorization.CollectorID != "" && authorization.CollectorID != hello.GetCollectorId() {
		return status.Error(codes.PermissionDenied, "token is not authorized for this collector_id")
	}
	if hello.GetProtocolMajor() != s.config.ProtocolMajor || hello.GetProtocolMinor() > s.config.ProtocolMinor {
		return status.Error(codes.FailedPrecondition, "collector protocol version is not supported")
	}
	if hello.GetStartedAt() == nil || hello.GetStartedAt().CheckValid() != nil {
		return status.Error(codes.InvalidArgument, "started_at is invalid")
	}
	for _, value := range []string{
		hello.GetCollectorVersion(), hello.GetHostname(), hello.GetOperatingSystem(), hello.GetArchitecture(),
	} {
		if !utf8.ValidString(value) {
			return status.Error(codes.InvalidArgument, "hello contains invalid UTF-8")
		}
	}
	for _, input := range hello.GetInputs() {
		if input == nil || !validIdentifier(input.GetInputId(), s.config.Limits.MaxIDBytes) ||
			!validIdentifier(input.GetIndexName(), s.config.Limits.MaxIDBytes) {
			return status.Error(codes.InvalidArgument, "hello contains an invalid input registration")
		}
	}
	return nil
}

func (s *Service) validateHeartbeat(heartbeat *opensplunkv1.CollectorHeartbeat, state *streamState) error {
	if heartbeat == nil {
		return status.Error(codes.InvalidArgument, "heartbeat payload is required")
	}
	if heartbeat.GetCollectorId() != state.collectorID || heartbeat.GetInstanceId() != state.instanceID {
		return status.Error(codes.InvalidArgument, "heartbeat collector identity does not match hello")
	}
	if err := s.validator.validateTimestamp(heartbeat.GetObservedAt(), s.config.Clock()); err != nil {
		return status.Error(codes.InvalidArgument, "heartbeat observed_at is invalid or outside accepted bounds")
	}
	if math.IsNaN(heartbeat.GetProcessCpuPercent()) || math.IsInf(heartbeat.GetProcessCpuPercent(), 0) || heartbeat.GetProcessCpuPercent() < 0 {
		return status.Error(codes.InvalidArgument, "heartbeat CPU percentage is invalid")
	}
	return nil
}

type streamState struct {
	collectorID          string
	instanceID           string
	protocolMajor        uint32
	protocolMinor        uint32
	authorization        Authorization
	authorizedIndexes    map[string]struct{}
	batchesBySequence    map[uint64]batchIdentity
	sequenceByBatchID    map[string]uint64
	highestBatchSequence uint64
}

type batchIdentity struct {
	batchID string
	digest  string
	size    uint64
}

func bearerToken(ctx context.Context) (string, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return "", errors.New("exactly one authorization value is required")
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return "", errors.New("authorization must use the Bearer scheme")
	}
	return parts[1], nil
}

func normalizedAuthorizedIndexes(indexes []string, maxIDBytes uint32) []string {
	seen := make(map[string]struct{}, len(indexes))
	result := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if !validIdentifier(index, maxIDBytes) {
			continue
		}
		if _, duplicate := seen[index]; duplicate {
			continue
		}
		seen[index] = struct{}{}
		result = append(result, index)
	}
	sort.Strings(result)
	return result
}

func randomID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(bytes[:])
}

func digestsEqual(left, right []byte) bool {
	return len(left) == sha256Size && len(right) == sha256Size && subtle.ConstantTimeCompare(left, right) == 1
}

const sha256Size = 32

var _ opensplunkv1.CollectorIngestServiceServer = (*Service)(nil)
