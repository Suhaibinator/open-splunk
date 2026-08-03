package ingest

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/buildmetadata"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Config controls collector protocol negotiation and ingestion enforcement.
type Config struct {
	Limits                Limits
	Redaction             RedactionPolicy
	DefaultIndexRetention time.Duration
	ProtocolMajor         uint32
	ProtocolMinor         uint32
	ServerInstanceID      string
	ServerVersion         string
	Build                 *opensplunkv1.BuildMetadata
	HeartbeatInterval     time.Duration
	MaxInFlightBatches    uint32
	MaxStreamsPerSubject  uint32
	DefaultRetryAfter     time.Duration
	SessionCleanupTimeout time.Duration
	Clock                 func() time.Time
	NewStreamID           func() string
	SessionManager        CollectorSessionManager
	StreamRegistry        CollectorStreamRegistry
	SessionErrorHandler   func(error)
}

const defaultServerVersion = "development"

// DefaultIndexRetention is the shared deployment default used by the native
// service and the server CLI when an index keeps the zero inheritance sentinel.
const DefaultIndexRetention = indexpolicy.DefaultRetention

const maximumTrustedAuthorizationIdentityBytes = 255
const maximumAuthorizedCollectorIndexes = 256

const defaultCollectorSessionCleanupTimeout = 15 * time.Second
const collectorSessionCleanupAttempts = 3
const collectorSessionCleanupRetryDelay = 10 * time.Millisecond

func DefaultConfig() Config {
	return Config{
		Limits:                DefaultLimits(),
		DefaultIndexRetention: DefaultIndexRetention,
		ProtocolMajor:         1,
		ProtocolMinor:         0,
		HeartbeatInterval:     15 * time.Second,
		MaxInFlightBatches:    1,
		MaxStreamsPerSubject:  4,
		DefaultRetryAfter:     time.Second,
		SessionCleanupTimeout: defaultCollectorSessionCleanupTimeout,
		Clock:                 time.Now,
		NewStreamID:           randomID,
	}
}

// Service is the authenticated collector gRPC ingestion boundary.
type Service struct {
	opensplunkv1.UnimplementedCollectorIngestServiceServer

	config         Config
	validator      *Validator
	authorizer     Authorizer
	sessionManager CollectorSessionManager
	streamRegistry CollectorStreamRegistry
	store          EventStore
	admissionMu    sync.Mutex
	nextAdmission  uint64
	admissions     map[string]map[CollectorStreamKey]collectorStreamAdmissionEntry
	finalizersMu   sync.Mutex
	finalizers     map[CollectorStreamKey]*collectorSessionFinalizer
}

func NewService(config Config, authorizer Authorizer, store EventStore) (*Service, error) {
	defaults := DefaultConfig()
	if config.Limits == (Limits{}) {
		config.Limits = defaults.Limits
	}
	if config.ProtocolMajor == 0 {
		config.ProtocolMajor = defaults.ProtocolMajor
	}
	if config.DefaultIndexRetention == 0 {
		config.DefaultIndexRetention = defaults.DefaultIndexRetention
	}
	if config.ServerInstanceID == "" {
		config.ServerInstanceID = randomID()
	}
	config.ServerVersion = strings.TrimSpace(config.ServerVersion)
	if config.Build != nil {
		clonedBuild, serverVersion, err := buildmetadata.Normalize(config.Build, config.ServerVersion)
		if err != nil {
			if errors.Is(err, buildmetadata.ErrVersionMismatch) {
				return nil, errors.New("ingest server version does not match structured build metadata")
			}
			return nil, fmt.Errorf("ingest build metadata: %w", err)
		}
		config.ServerVersion = serverVersion
		config.Build = clonedBuild
	} else if config.ServerVersion == "" {
		config.ServerVersion = defaultServerVersion
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if config.MaxInFlightBatches == 0 {
		config.MaxInFlightBatches = defaults.MaxInFlightBatches
	}
	if config.MaxStreamsPerSubject == 0 {
		config.MaxStreamsPerSubject = defaults.MaxStreamsPerSubject
	}
	if config.DefaultRetryAfter == 0 {
		config.DefaultRetryAfter = defaults.DefaultRetryAfter
	}
	if config.SessionCleanupTimeout == 0 {
		config.SessionCleanupTimeout = defaults.SessionCleanupTimeout
	}
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.NewStreamID == nil {
		config.NewStreamID = defaults.NewStreamID
	}
	if config.StreamRegistry == nil {
		config.StreamRegistry = NewInMemoryCollectorStreamRegistry()
	}
	if authorizer == nil {
		return nil, errors.New("ingest authorizer is required")
	}
	if config.SessionManager == nil {
		return nil, errors.New("ingest collector session manager is required")
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
	if err := indexpolicy.ValidateRetentionAt(
		config.DefaultIndexRetention,
		config.Clock().UTC(),
		false,
	); err != nil {
		return nil, fmt.Errorf("default index retention: %w", err)
	}
	if config.SessionCleanupTimeout <= 0 {
		return nil, errors.New("collector session cleanup timeout must be positive")
	}
	if config.MaxInFlightBatches > HardMaxInFlightBatches {
		return nil, fmt.Errorf("max in-flight batches cannot exceed hard limit %d", HardMaxInFlightBatches)
	}
	if config.MaxStreamsPerSubject > HardMaxStreamsPerSubject {
		return nil, fmt.Errorf("max streams per subject cannot exceed hard limit %d", HardMaxStreamsPerSubject)
	}
	if !validIdentifier(config.ServerInstanceID, config.Limits.MaxIDBytes) {
		return nil, errors.New("server instance ID has an invalid format")
	}
	validator, err := NewValidator(config.Limits, config.Redaction)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:         config,
		validator:      validator,
		authorizer:     authorizer,
		sessionManager: config.SessionManager,
		streamRegistry: config.StreamRegistry,
		store:          store,
		admissions:     make(map[string]map[CollectorStreamKey]collectorStreamAdmissionEntry),
		finalizers:     make(map[CollectorStreamKey]*collectorSessionFinalizer),
	}, nil
}

func (s *Service) Collect(stream opensplunkv1.CollectorIngestService_CollectServer) error {
	token, err := bearerToken(stream.Context())
	if err != nil {
		return status.Error(codes.Unauthenticated, "valid bearer authentication is required")
	}
	authorization, err := s.authorizer.Authorize(stream.Context(), token)
	if err != nil {
		return authorizationRPCError(err, "collector authentication failed")
	}
	if authorization.CollectorID == "" {
		return status.Error(codes.Unauthenticated, "collector authentication failed")
	}
	if !validTrustedAuthorizationIdentity(authorization.SubjectID) ||
		!validTrustedAuthorizationIdentity(authorization.TenantID) ||
		!validIdentifier(authorization.CollectorID, s.config.Limits.MaxIDBytes) {
		return status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
	if _, err := compileEventAuthorization(authorization); err != nil {
		return status.Error(
			codes.Unavailable,
			"collector authentication service returned invalid event authority",
		)
	}
	authorization = cloneAuthorization(authorization)
	collectorKey := CollectorStreamKey{
		TenantID:    authorization.TenantID,
		CollectorID: authorization.CollectorID,
	}
	subjectKey := authorizationSubjectKey(authorization.SubjectID)
	admission, ok := s.acquireStreamAdmission(subjectKey, collectorKey)
	if !ok {
		return status.Error(codes.ResourceExhausted, "collector credential stream capacity is exhausted")
	}
	defer s.releaseStreamAdmission(admission)
	admissionContext, cancelAdmission := collectorSupersessionContext(
		stream.Context(),
		admission.Superseded,
	)
	defer cancelAdmission()
	received, stopReceiving := receiveCollectRequests(stream)
	defer stopReceiving()
	var firstResult collectRequestResult
	select {
	case <-admission.Superseded:
		return supersededStreamRPCError()
	case result, open := <-received:
		if !open {
			return status.Error(codes.InvalidArgument, "first request must be CollectorHello")
		}
		firstResult = result
	}
	request, err := firstResult.request, firstResult.err
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "first request must be CollectorHello")
	}
	if err != nil {
		return err
	}
	requestBoundaryAt := s.config.Clock().UTC()
	if err := s.validateRequestEnvelope(request, 1, requestBoundaryAt); err != nil {
		return err
	}
	hello := request.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "first request must be CollectorHello")
	}
	if err := s.validateHello(hello, authorization); err != nil {
		return err
	}
	helloSnapshot, err := collectorHelloSnapshot(hello)
	if err != nil {
		return collectorSnapshotRPCError(err)
	}

	streamID := s.config.NewStreamID()
	if !validIdentifier(streamID, s.config.Limits.MaxIDBytes) {
		return status.Error(codes.Internal, "failed to allocate collector stream ID")
	}
	releaseFinalization := s.lockCollectorFinalization(collectorKey)
	finalizationLocked := true
	defer func() {
		if finalizationLocked {
			releaseFinalization()
		}
	}()
	if !s.streamAdmissionIsCurrent(admission) {
		return supersededStreamRPCError()
	}
	acceptedAt := s.config.Clock().UTC()
	session, err := s.sessionManager.Admit(
		admissionContext,
		token,
		CollectorSessionAdmissionRequest{
			CollectorID: hello.GetCollectorId(),
			BootEpoch:   s.config.ServerInstanceID,
			StreamID:    streamID,
			AcceptedAt:  acceptedAt,
			Hello:       helloSnapshot,
		},
	)
	if err != nil {
		select {
		case <-admission.Superseded:
			return supersededStreamRPCError()
		default:
		}
		return sessionAdmissionRPCError(err)
	}
	// Arm exact, detached durable cleanup immediately after the commit. Every
	// later validation, process activation, and Ready failure is covered.
	defer s.disconnectCollectorSession(session.Lease)
	if err := s.validateAdmittedSession(
		authorization,
		session,
		hello.GetCollectorId(),
		streamID,
	); err != nil {
		return err
	}
	eventAuthorization, err := compileEventAuthorization(session.Authorization)
	if err != nil {
		return status.Error(
			codes.Unavailable,
			"collector admission service returned invalid event authority",
		)
	}
	authorization = cloneAuthorization(session.Authorization)
	if err := authorization.TokenRateLimits.Validate(); err != nil {
		return status.Error(
			codes.Unavailable,
			"collector admission service returned invalid token authority",
		)
	}
	if len(authorization.AuthorizedIndexes) == 0 {
		return status.Error(
			codes.Unauthenticated,
			"collector authentication is no longer valid",
		)
	}
	resolvedIndexes, indexesValid := s.resolveAuthorizedIndexPolicies(
		authorization.AuthorizedIndexes,
		acceptedAt,
	)
	if !indexesValid {
		return status.Error(
			codes.Unavailable,
			"collector admission service returned invalid index authority",
		)
	}
	authorization.AuthorizedIndexes = resolvedIndexes.policies

	lease, err := s.promoteStreamAdmission(admission, session.Lease)
	if errors.Is(err, errStreamAdmissionSuperseded) {
		return supersededStreamRPCError()
	}
	if errors.Is(err, ErrCollectorStreamActivationStale) ||
		errors.Is(err, ErrCollectorStreamActivationConflict) {
		return supersededStreamRPCError()
	}
	if err != nil {
		return status.Error(codes.Unavailable, "collector stream admission is unavailable")
	}
	// Process activation is already authoritative at this point. Arm its exact
	// release before installing the same durable lease in the heartbeat
	// runtime, so an activation failure cannot leak either process authority or
	// the durable lease whose cleanup was armed immediately after Admit.
	defer s.streamRegistry.Release(lease)
	if err := s.sessionManager.Activate(session.Lease); err != nil {
		return sessionActivationRPCError(err)
	}
	releaseFinalization()
	finalizationLocked = false
	cancelAdmission()
	authorizationContext, cancelAuthorization := collectorSupersessionContext(
		stream.Context(),
		lease.Superseded,
	)
	defer cancelAuthorization()

	state := streamState{
		collectorID:        hello.GetCollectorId(),
		instanceID:         hello.GetInstanceId(),
		protocolMajor:      hello.GetProtocolMajor(),
		protocolMinor:      hello.GetProtocolMinor(),
		authorization:      authorization,
		indexPolicies:      resolvedIndexes.byName,
		eventAuthorization: eventAuthorization,
	}
	responseSequence := uint64(1)
	if err := stream.Send(&opensplunkv1.CollectResponse{
		StreamSequence: responseSequence,
		SentAt:         timestamppb.New(acceptedAt),
		Payload: &opensplunkv1.CollectResponse_Ready{Ready: &opensplunkv1.CollectorReady{
			StreamId:                 streamID,
			ServerInstanceId:         s.config.ServerInstanceID,
			ServerVersion:            s.config.ServerVersion,
			Build:                    buildmetadata.Clone(s.config.Build),
			ProtocolMajor:            s.config.ProtocolMajor,
			ProtocolMinor:            hello.GetProtocolMinor(),
			ServerTime:               timestamppb.New(acceptedAt),
			HeartbeatInterval:        durationpb.New(s.config.HeartbeatInterval),
			MaxInFlightBatches:       s.config.MaxInFlightBatches,
			MaxBatchEvents:           s.config.Limits.MaxBatchEvents,
			MaxBatchBytes:            s.config.Limits.MaxBatchBytes,
			MaxEventBytes:            s.config.Limits.MaxEventBytes,
			AuthorizedIndexes:        authorizedIndexPolicyNames(resolvedIndexes.policies),
			AcknowledgmentDurability: opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED,
		}},
	}); err != nil {
		return err
	}

	expectedRequestSequence := uint64(2)
	for {
		var result collectRequestResult
		var ok bool
		select {
		case <-lease.Superseded:
			return supersededStreamRPCError()
		case result, ok = <-received:
			if !ok {
				return nil
			}
		}
		request, err = result.request, result.err
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		boundaryAt := s.config.Clock().UTC()
		if err := s.validateRequestEnvelope(request, expectedRequestSequence, boundaryAt); err != nil {
			return err
		}
		if expectedRequestSequence == math.MaxUint64 {
			return status.Error(codes.ResourceExhausted, "collector stream sequence exhausted")
		}
		var heartbeatSnapshot *collectorfleet.Heartbeat
		switch payload := request.GetPayload().(type) {
		case *opensplunkv1.CollectRequest_Heartbeat:
			if err := s.validateHeartbeat(payload.Heartbeat, &state, boundaryAt); err != nil {
				return err
			}
			snapshot, err := collectorHeartbeatSnapshot(
				payload.Heartbeat,
				request.GetStreamSequence(),
				boundaryAt,
			)
			if err != nil {
				return collectorSnapshotRPCError(err)
			}
			heartbeatSnapshot = &snapshot
		}
		var deferredAuthority, authorizationErr error
		switch request.GetPayload().(type) {
		case *opensplunkv1.CollectRequest_Heartbeat, *opensplunkv1.CollectRequest_Batch:
			deferredAuthority, authorizationErr = s.refreshLeaseAuthorization(
				authorizationContext,
				token,
				lease.Lease,
				boundaryAt,
				&state,
			)
		}
		if !s.streamRegistry.IsCurrent(lease) {
			return supersededStreamRPCError()
		}
		if authorizationErr != nil {
			return authorizationErr
		}
		if _, heartbeat := request.GetPayload().(*opensplunkv1.CollectRequest_Heartbeat); heartbeat &&
			deferredAuthority != nil {
			return authorityRPCError(deferredAuthority)
		}
		expectedRequestSequence++

		switch payload := request.GetPayload().(type) {
		case *opensplunkv1.CollectRequest_Hello:
			return status.Error(codes.InvalidArgument, "CollectorHello may only appear as the first request")
		case *opensplunkv1.CollectRequest_Heartbeat:
			applied, err := s.sessionManager.RecordHeartbeat(
				authorizationContext,
				lease.Lease,
				*heartbeatSnapshot,
			)
			if err != nil {
				select {
				case <-lease.Superseded:
					return supersededStreamRPCError()
				default:
				}
				return heartbeatPersistenceRPCError(err)
			}
			if !applied {
				return supersededStreamRPCError()
			}
			continue
		case *opensplunkv1.CollectRequest_Goodbye:
			if payload.Goodbye == nil {
				return status.Error(codes.InvalidArgument, "goodbye payload is required")
			}
			return nil
		case *opensplunkv1.CollectRequest_Batch:
			response, err := s.processBatchWithDeferredAuthority(
				stream.Context(),
				payload.Batch,
				&state,
				boundaryAt,
				deferredAuthority,
			)
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
			if state.pendingThrottle != nil {
				if responseSequence == math.MaxUint64 {
					return status.Error(codes.ResourceExhausted, "server stream sequence exhausted")
				}
				responseSequence++
				throttle := state.pendingThrottle
				state.pendingThrottle = nil
				sentAt := s.config.Clock().UTC()
				throttle.EffectiveUntil = timestamppb.New(
					sentAt.Add(throttle.GetMinimumSendDelay().AsDuration()),
				)
				followup := &opensplunkv1.CollectResponse{
					StreamSequence: responseSequence,
					SentAt:         timestamppb.New(sentAt),
					Payload: &opensplunkv1.CollectResponse_Throttle{
						Throttle: throttle,
					},
				}
				if err := stream.Send(followup); err != nil {
					return err
				}
			}
		default:
			return status.Error(codes.InvalidArgument, "collector request payload is required")
		}
	}
}

type collectRequestResult struct {
	request *opensplunkv1.CollectRequest
	err     error
}

func receiveCollectRequests(
	stream interface {
		Recv() (*opensplunkv1.CollectRequest, error)
	},
) (<-chan collectRequestResult, func()) {
	received := make(chan collectRequestResult)
	stopped := make(chan struct{})
	go func() {
		defer close(received)
		for {
			request, err := stream.Recv()
			select {
			case <-stopped:
				return
			default:
			}
			select {
			case received <- collectRequestResult{request: request, err: err}:
			case <-stopped:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	var stopOnce sync.Once
	return received, func() {
		stopOnce.Do(func() {
			close(stopped)
		})
	}
}

func supersededStreamRPCError() error {
	return status.Error(codes.Aborted, "collector stream was superseded")
}

func collectorSupersessionContext(
	parent context.Context,
	superseded <-chan struct{},
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-superseded:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func authorizationSubjectKey(subjectID string) string {
	return "subject:" + subjectID
}

func validTrustedAuthorizationIdentity(value string) bool {
	return value != "" &&
		len(value) <= maximumTrustedAuthorizationIdentityBytes &&
		utf8.ValidString(value) &&
		strings.IndexByte(value, 0) < 0
}

var errStreamAdmissionSuperseded = errors.New("collector stream admission was superseded")

type collectorStreamAdmission struct {
	SubjectKey   string
	CollectorKey CollectorStreamKey
	Generation   uint64
	Superseded   <-chan struct{}
}

type collectorStreamAdmissionEntry struct {
	admission  collectorStreamAdmission
	superseded chan struct{}
}

type collectorSessionFinalizer struct {
	mu         sync.Mutex
	references uint64
}

func (s *Service) lockCollectorFinalization(
	key CollectorStreamKey,
) func() {
	s.finalizersMu.Lock()
	finalizer := s.finalizers[key]
	if finalizer == nil {
		finalizer = &collectorSessionFinalizer{}
		s.finalizers[key] = finalizer
	}
	finalizer.references++
	s.finalizersMu.Unlock()

	finalizer.mu.Lock()
	return func() {
		finalizer.mu.Unlock()
		s.finalizersMu.Lock()
		defer s.finalizersMu.Unlock()
		finalizer.references--
		if finalizer.references == 0 {
			delete(s.finalizers, key)
		}
	}
}

func (s *Service) streamAdmissionIsCurrent(
	admission collectorStreamAdmission,
) bool {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	byCollector := s.admissions[admission.SubjectKey]
	current, ok := byCollector[admission.CollectorKey]
	return ok && current.admission.Generation == admission.Generation
}

func (s *Service) acquireStreamAdmission(
	subjectKey string,
	collectorKey CollectorStreamKey,
) (collectorStreamAdmission, bool) {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()

	byCollector := s.admissions[subjectKey]
	previous, replacing := byCollector[collectorKey]
	var distinctAdmissions uint32
	for range byCollector {
		distinctAdmissions++
	}
	if !replacing && distinctAdmissions >= s.config.MaxStreamsPerSubject {
		return collectorStreamAdmission{}, false
	}
	if s.nextAdmission == math.MaxUint64 {
		return collectorStreamAdmission{}, false
	}
	s.nextAdmission++
	superseded := make(chan struct{})
	admission := collectorStreamAdmission{
		SubjectKey:   subjectKey,
		CollectorKey: collectorKey,
		Generation:   s.nextAdmission,
		Superseded:   superseded,
	}
	if byCollector == nil {
		byCollector = make(map[CollectorStreamKey]collectorStreamAdmissionEntry)
		s.admissions[subjectKey] = byCollector
	}
	if replacing {
		close(previous.superseded)
	}
	byCollector[collectorKey] = collectorStreamAdmissionEntry{
		admission:  admission,
		superseded: superseded,
	}
	return admission, true
}

func (s *Service) promoteStreamAdmission(
	admission collectorStreamAdmission,
	durableLease collectorfleet.Lease,
) (CollectorStreamLease, error) {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()

	byCollector := s.admissions[admission.SubjectKey]
	current, ok := byCollector[admission.CollectorKey]
	if !ok || current.admission.Generation != admission.Generation {
		return CollectorStreamLease{}, errStreamAdmissionSuperseded
	}
	lease, err := s.streamRegistry.Activate(durableLease)
	if err != nil {
		return CollectorStreamLease{}, err
	}
	delete(byCollector, admission.CollectorKey)
	if len(byCollector) == 0 {
		delete(s.admissions, admission.SubjectKey)
	}
	return lease, nil
}

func (s *Service) releaseStreamAdmission(admission collectorStreamAdmission) {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()

	byCollector := s.admissions[admission.SubjectKey]
	current, ok := byCollector[admission.CollectorKey]
	if !ok || current.admission.Generation != admission.Generation {
		return
	}
	delete(byCollector, admission.CollectorKey)
	if len(byCollector) == 0 {
		delete(s.admissions, admission.SubjectKey)
	}
}

func (s *Service) validateAdmittedSession(
	preliminary Authorization,
	session CollectorSessionAdmission,
	collectorID string,
	streamID string,
) error {
	fresh := session.Authorization
	if !validTrustedAuthorizationIdentity(fresh.SubjectID) ||
		!validTrustedAuthorizationIdentity(fresh.TenantID) ||
		!validIdentifier(fresh.CollectorID, s.config.Limits.MaxIDBytes) {
		return status.Error(
			codes.Unavailable,
			"collector admission service is unavailable",
		)
	}
	if fresh.SubjectID != preliminary.SubjectID ||
		fresh.TenantID != preliminary.TenantID ||
		fresh.CollectorID != preliminary.CollectorID {
		return status.Error(
			codes.Unavailable,
			"collector admission service returned inconsistent authority",
		)
	}
	lease := session.Lease
	if fresh.CollectorID != collectorID ||
		lease.TenantID != fresh.TenantID ||
		lease.CollectorID != collectorID ||
		lease.BootEpoch != s.config.ServerInstanceID ||
		lease.StreamID != streamID ||
		lease.Generation == 0 ||
		lease.Generation > math.MaxInt64 {
		return status.Error(
			codes.Unavailable,
			"collector admission service returned an invalid lease",
		)
	}
	return nil
}

func (s *Service) disconnectCollectorSession(lease collectorfleet.Lease) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		s.config.SessionCleanupTimeout,
	)
	defer cancel()
	disconnectedAt := s.config.Clock().UTC()
	var disconnectErr error
	for attempt := 1; attempt <= collectorSessionCleanupAttempts; attempt++ {
		_, disconnectErr = s.sessionManager.Disconnect(
			ctx,
			lease,
			disconnectedAt,
		)
		if disconnectErr == nil {
			return
		}
		if attempt == collectorSessionCleanupAttempts {
			break
		}
		if !waitForCollectorSessionCleanupRetry(
			ctx,
			time.Duration(attempt)*collectorSessionCleanupRetryDelay,
		) {
			disconnectErr = ctx.Err()
			break
		}
	}
	if disconnectErr != nil && s.config.SessionErrorHandler != nil {
		s.config.SessionErrorHandler(
			fmt.Errorf("disconnect collector session: %w", disconnectErr),
		)
	}
}

func waitForCollectorSessionCleanupRetry(
	ctx context.Context,
	delay time.Duration,
) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Service) refreshLeaseAuthorization(
	ctx context.Context,
	token string,
	lease collectorfleet.Lease,
	checkedAt time.Time,
	state *streamState,
) (deferredAuthority error, fatalErr error) {
	authorization, err := s.sessionManager.AuthorizeLease(
		ctx,
		token,
		lease,
		checkedAt,
	)
	deferredIndex := errors.Is(err, ErrNoActiveIndexAuthority) ||
		errors.Is(err, ErrInvalidIndexAuthority)
	deferredEvent := errors.Is(err, ErrInvalidEventAuthority)
	deferred := deferredIndex || deferredEvent
	if err != nil && !deferred {
		if errors.Is(err, ErrCollectorLeaseNotCurrent) {
			return nil, supersededStreamRPCError()
		}
		return nil, authorizationRPCError(
			err,
			"collector authentication is no longer valid",
		)
	}
	if deferred && authorization.CollectorID == "" {
		return nil, status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
	if err := authorization.TokenRateLimits.Validate(); err != nil {
		return nil, status.Error(
			codes.Unavailable,
			"collector authentication service returned invalid token authority",
		)
	}
	if authorization.CollectorID == "" {
		return nil, status.Error(codes.Unauthenticated, "collector authentication is no longer valid")
	}
	if !validTrustedAuthorizationIdentity(authorization.SubjectID) ||
		!validTrustedAuthorizationIdentity(authorization.TenantID) ||
		!validIdentifier(authorization.CollectorID, s.config.Limits.MaxIDBytes) {
		return nil, status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
	if authorization.CollectorID != state.collectorID {
		return nil, status.Error(codes.PermissionDenied, "token is not authorized for this collector_id")
	}
	if authorization.CollectorID != state.authorization.CollectorID {
		return nil, status.Error(codes.PermissionDenied, "collector identity scope changed during the stream")
	}
	if authorization.TenantID != state.authorization.TenantID {
		return nil, status.Error(codes.PermissionDenied, "collector tenant scope changed during the stream")
	}
	if authorization.SubjectID != state.authorization.SubjectID {
		return nil, status.Error(codes.PermissionDenied, "collector principal changed during the stream")
	}
	if deferredEvent {
		return ErrInvalidEventAuthority, nil
	}
	if deferredIndex {
		if len(authorization.AuthorizedIndexes) != 0 {
			return nil, status.Error(codes.Unavailable, "collector authentication service is unavailable")
		}
		return err, nil
	}
	if len(authorization.AuthorizedIndexes) == 0 {
		return nil, status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
	eventAuthorization, eventAuthorityErr := refreshEventAuthorization(
		state.eventAuthorization,
		state.authorization,
		authorization,
	)
	if eventAuthorityErr != nil {
		return ErrInvalidEventAuthority, nil
	}
	if authorization.TokenRateLimits == state.authorization.TokenRateLimits && slices.Equal(
		authorization.AuthorizedIndexes,
		state.authorization.AuthorizedIndexes,
	) && slices.Equal(
		authorization.AllowedHostRegexes,
		state.authorization.AllowedHostRegexes,
	) && slices.Equal(
		authorization.AllowedSourceRegexes,
		state.authorization.AllowedSourceRegexes,
	) {
		for _, policy := range authorization.AuthorizedIndexes {
			if _, policyErr := policy.ResolveRetentionAt(
				checkedAt,
				s.config.DefaultIndexRetention,
			); policyErr != nil {
				return ErrInvalidIndexAuthority, nil
			}
		}
		return nil, nil
	}
	resolvedIndexes, indexesValid := s.resolveAuthorizedIndexPolicies(
		authorization.AuthorizedIndexes,
		checkedAt,
	)
	if !indexesValid {
		return ErrInvalidIndexAuthority, nil
	}
	authorization = cloneAuthorization(authorization)
	authorization.AuthorizedIndexes = resolvedIndexes.policies
	state.authorization = authorization
	state.indexPolicies = resolvedIndexes.byName
	state.eventAuthorization = eventAuthorization
	return nil, nil
}

func authorityRPCError(err error) error {
	switch {
	case errors.Is(err, ErrNoActiveIndexAuthority):
		return status.Error(codes.Unauthenticated, "collector authentication is no longer valid")
	case errors.Is(err, ErrInvalidIndexAuthority):
		return status.Error(
			codes.Unavailable,
			"collector authentication service returned invalid index authority",
		)
	case errors.Is(err, ErrInvalidEventAuthority):
		return status.Error(
			codes.Unavailable,
			"collector authentication service returned invalid event authority",
		)
	default:
		return status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
}

func authorizationRPCError(err error, unauthorizedMessage string) error {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return status.Error(codes.Unauthenticated, unauthorizedMessage)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Unavailable, "collector authentication service is unavailable")
	}
}

func sessionAdmissionRPCError(err error) error {
	switch {
	case errors.Is(err, ErrUnauthorized):
		return status.Error(codes.Unauthenticated, "collector authentication is no longer valid")
	case errors.Is(err, ErrCollectorDisabled):
		return status.Error(codes.PermissionDenied, "collector is disabled")
	case errors.Is(err, ErrInvalidCollectorSnapshot):
		return status.Error(codes.InvalidArgument, "collector hello is invalid")
	case errors.Is(err, ErrCollectorSessionCapacity):
		return status.Error(codes.ResourceExhausted, "collector capacity is exhausted")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, context.Canceled.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
	default:
		return status.Error(codes.Unavailable, "collector admission service is unavailable")
	}
}

func sessionActivationRPCError(err error) error {
	switch {
	case errors.Is(err, ErrCollectorLeaseNotCurrent):
		return supersededStreamRPCError()
	case errors.Is(err, ErrCollectorSessionCapacity):
		return status.Error(codes.ResourceExhausted, "collector heartbeat capacity is exhausted")
	default:
		return status.Error(codes.Unavailable, "collector heartbeat runtime is unavailable")
	}
}

func collectorSnapshotRPCError(err error) error {
	switch {
	case errors.Is(err, ErrInvalidCollectorSnapshot):
		return status.Error(codes.InvalidArgument, "collector lifecycle snapshot is invalid")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Unavailable, "collector lifecycle validation is unavailable")
	}
}

func heartbeatPersistenceRPCError(err error) error {
	switch {
	case errors.Is(err, ErrCollectorLeaseNotCurrent):
		return supersededStreamRPCError()
	case errors.Is(err, ErrInvalidCollectorSnapshot):
		return status.Error(codes.InvalidArgument, "collector heartbeat is invalid")
	case errors.Is(err, ErrCollectorSessionCapacity):
		return status.Error(codes.ResourceExhausted, "collector telemetry capacity is exhausted")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	default:
		return status.Error(codes.Unavailable, "collector heartbeat persistence is unavailable")
	}
}

func (s *Service) validateRequestEnvelope(
	request *opensplunkv1.CollectRequest,
	expectedSequence uint64,
	reference time.Time,
) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "collector request is required")
	}
	if request.GetStreamSequence() != expectedSequence {
		return status.Errorf(codes.InvalidArgument, "stream_sequence must be %d", expectedSequence)
	}
	if request.GetPayload() == nil {
		return status.Error(codes.InvalidArgument, "collector request payload is required")
	}
	if err := s.validator.validateTimestamp(request.GetSentAt(), reference); err != nil {
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
	if authorization.CollectorID != hello.GetCollectorId() {
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
			!validIndexName(input.GetIndexName()) {
			return status.Error(codes.InvalidArgument, "hello contains an invalid input registration")
		}
	}
	return nil
}

func (s *Service) validateHeartbeat(
	heartbeat *opensplunkv1.CollectorHeartbeat,
	state *streamState,
	reference time.Time,
) error {
	if heartbeat == nil {
		return status.Error(codes.InvalidArgument, "heartbeat payload is required")
	}
	if heartbeat.GetCollectorId() != state.collectorID || heartbeat.GetInstanceId() != state.instanceID {
		return status.Error(codes.InvalidArgument, "heartbeat collector identity does not match hello")
	}
	if err := s.validator.validateTimestamp(heartbeat.GetObservedAt(), reference); err != nil {
		return status.Error(codes.InvalidArgument, "heartbeat observed_at is invalid or outside accepted bounds")
	}
	if math.IsNaN(heartbeat.GetProcessCpuPercent()) || math.IsInf(heartbeat.GetProcessCpuPercent(), 0) || heartbeat.GetProcessCpuPercent() < 0 {
		return status.Error(codes.InvalidArgument, "heartbeat CPU percentage is invalid")
	}
	return nil
}

type streamState struct {
	collectorID        string
	instanceID         string
	protocolMajor      uint32
	protocolMinor      uint32
	authorization      Authorization
	indexPolicies      map[string]resolvedIndexPolicy
	eventAuthorization eventAuthorizationMatcher
	pendingThrottle    *opensplunkv1.Throttle

	hasHighestBatchSequence bool
	highestBatchSequence    uint64
	pendingBatches          map[uint64]pendingBatchIdentity
	pendingSequencesByID    map[string]uint64
}

type batchIdentity struct {
	batchID     string
	contentHash [sha256Size]byte
}

type pendingBatchIdentity struct {
	identity   batchIdentity
	receivedAt time.Time
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

type resolvedIndexAuthority struct {
	policies []IndexPolicy
	byName   map[string]resolvedIndexPolicy
}

type resolvedIndexPolicy struct {
	defaultSourcetype   string
	validator           Validator
	retentionPeriod     time.Duration
	ingestionRateLimits ingestquota.Limits
}

func (s *Service) resolveAuthorizedIndexPolicies(
	policies []IndexPolicy,
	reference time.Time,
) (resolvedIndexAuthority, bool) {
	if len(policies) > maximumAuthorizedCollectorIndexes {
		return resolvedIndexAuthority{}, false
	}
	detached := append([]IndexPolicy(nil), policies...)
	sort.Slice(detached, func(left, right int) bool {
		return detached[left].Name < detached[right].Name
	})
	result := resolvedIndexAuthority{
		policies: detached,
		byName:   make(map[string]resolvedIndexPolicy, len(detached)),
	}
	for _, policy := range detached {
		retention, policyErr := policy.ResolveRetentionAt(
			reference,
			s.config.DefaultIndexRetention,
		)
		if policyErr != nil {
			return resolvedIndexAuthority{}, false
		}
		if _, duplicate := result.byName[policy.Name]; duplicate {
			return resolvedIndexAuthority{}, false
		}
		effectiveLimits := effectiveIndexLimits(s.config.Limits, policy.Limits)
		result.byName[policy.Name] = resolvedIndexPolicy{
			defaultSourcetype:   policy.DefaultSourcetype,
			validator:           s.validator.withLimits(effectiveLimits),
			retentionPeriod:     retention,
			ingestionRateLimits: policy.IngestionRateLimits,
		}
	}
	return result, true
}

func authorizedIndexPolicyNames(policies []IndexPolicy) []string {
	names := make([]string, len(policies))
	for index, policy := range policies {
		names[index] = policy.Name
	}
	return names
}

func effectiveIndexLimits(global Limits, index IndexLimits) Limits {
	global.MaxEventBytes = tightenLimit(global.MaxEventBytes, index.MaxEventBytes)
	global.MaxFields = tightenLimit(global.MaxFields, index.MaxFieldCount)
	global.MaxNestingDepth = tightenLimit(global.MaxNestingDepth, index.MaxNestingDepth)
	global.MaxFutureSkew = tightenLimit(global.MaxFutureSkew, index.MaximumFutureSkew)
	global.MaxEventAge = tightenLimit(global.MaxEventAge, index.MaximumEventAge)
	return global
}

func tightenLimit[T ~uint32 | ~uint64 | ~int64](global, index T) T {
	if index == 0 {
		return global
	}
	return min(global, index)
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
