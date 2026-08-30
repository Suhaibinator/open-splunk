package ingest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync/atomic"
	"time"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/indexpolicy"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

// AdmissionConfig is the transport-neutral normalization and policy boundary
// shared by request-oriented ingestion transports.
type AdmissionConfig struct {
	Limits                Limits
	Redaction             RedactionPolicy
	DefaultIndexRetention time.Duration
}

// AdmissionEvent preserves a source ordinal while carrying the canonical
// event protobuf. UncompressedBytes is a detached decoder observation for
// diagnostics only; policy and quota accounting always recompute the exact
// serialized source-event size before normalization or redaction.
type AdmissionEvent struct {
	Event             *opensplunk.LogEvent
	UncompressedBytes uint64
}

// AdmissionRequest contains a complete, already framed request. Prepare is
// request-atomic: one event failure returns no StoreBatch.
type AdmissionRequest struct {
	Authorization     Authorization
	Source            IngestionSource
	CollectorID       string
	BatchID           string
	BatchSequence     uint64
	SourceBatchSHA256 [32]byte
	ReceivedAt        time.Time
	QuotaEvaluatedAt  time.Time
	Events            []AdmissionEvent
	HECAdmission      *HECStageAdmission
}

// AdmissionFailure identifies the lowest failing source-event ordinal. The
// enclosed EventError uses only closed server-owned classifications.
type AdmissionFailure struct {
	EventIndex uint32
	EventID    string
	Failure    *EventError
}

func (failure *AdmissionFailure) Error() string {
	if failure == nil || failure.Failure == nil {
		return "ingestion event admission failed"
	}
	return failure.Failure.Error()
}

func (failure *AdmissionFailure) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Failure
}

// AdmissionPreparer owns immutable validator configuration and the durable
// staging dependency. It has no transport authentication or framing logic.
type AdmissionPreparer struct {
	limits                Limits
	validator             *Validator
	defaultIndexRetention time.Duration
	store                 StagingEventStore
	authority             atomic.Pointer[compiledEventAuthority]
}

func NewAdmissionPreparer(
	config AdmissionConfig,
	store StagingEventStore,
) (*AdmissionPreparer, error) {
	if config.Limits == (Limits{}) {
		config.Limits = DefaultLimits()
	}
	if config.DefaultIndexRetention == 0 {
		config.DefaultIndexRetention = DefaultIndexRetention
	}
	if store == nil {
		return nil, errors.New("ingestion admission staging store is required")
	}
	validator, err := NewValidator(config.Limits, config.Redaction)
	if err != nil {
		return nil, err
	}
	if err := indexpolicy.ValidateRetentionAt(
		config.DefaultIndexRetention,
		time.Now().UTC(),
		false,
	); err != nil {
		return nil, fmt.Errorf("ingestion admission default retention: %w", err)
	}
	return &AdmissionPreparer{
		limits:                config.Limits,
		validator:             validator,
		defaultIndexRetention: config.DefaultIndexRetention,
		store:                 store,
	}, nil
}

// Prepare validates, authorizes, redacts, and plans quota for one complete
// request without mutating persistence.
func (preparer *AdmissionPreparer) Prepare(request AdmissionRequest) (StoreBatch, error) {
	if preparer == nil || preparer.validator == nil {
		return StoreBatch{}, errors.New("ingestion admission preparer is unavailable")
	}
	source, err := CanonicalIngestionSource(request.Source, request.CollectorID)
	if err != nil {
		return StoreBatch{}, fmt.Errorf("ingestion admission source: %w", err)
	}
	if request.Authorization.SubjectID == "" || request.Authorization.TenantID == "" ||
		request.BatchID == "" || request.BatchSequence == 0 ||
		request.SourceBatchSHA256 == ([32]byte{}) || request.ReceivedAt.IsZero() ||
		request.QuotaEvaluatedAt.IsZero() {
		return StoreBatch{}, errors.New("ingestion admission identity is incomplete")
	}
	if source.Kind == IngestionSourceKindNativeCollector &&
		request.Authorization.CollectorID != source.CollectorID {
		return StoreBatch{}, errors.New("native ingestion admission authority does not match its source")
	}
	if source.Kind == IngestionSourceKindHEC && request.Authorization.CollectorID != "" {
		return StoreBatch{}, errors.New("HEC ingestion admission cannot carry collector authority")
	}
	if source.Kind == IngestionSourceKindHEC &&
		(request.HECAdmission == nil || request.HECAdmission.TokenID != source.ID ||
			request.HECAdmission.RequestID != request.BatchID) {
		return StoreBatch{}, errors.New("HEC durable admission identity is incomplete")
	}
	if source.Kind != IngestionSourceKindHEC && request.HECAdmission != nil {
		return StoreBatch{}, errors.New("native ingestion admission cannot carry HEC state")
	}
	if len(request.Events) == 0 || len(request.Events) > int(preparer.limits.MaxBatchEvents) ||
		len(request.Events) > int(HardMaxBatchEvents) {
		return StoreBatch{}, errors.New("ingestion admission event count is outside bounds")
	}
	authority, err := preparer.resolveAuthority(request.Authorization, request.ReceivedAt)
	if err != nil {
		return StoreBatch{}, err
	}

	seenEventIDs := make(map[string]struct{}, len(request.Events))
	storedEvents := make([]*StoredEvent, 0, len(request.Events))
	retentionByIndex := make(map[string]time.Duration)
	quotaByIndex := make(map[string]*ingestquota.Charge)
	var quotaUncompressedBytes uint64
	var normalizedUncompressedBytes uint64
	for index, candidate := range request.Events {
		eventID := ""
		if candidate.Event != nil {
			eventID = candidate.Event.GetEventId()
		}
		failure := func(eventErr *EventError) (StoreBatch, error) {

			return StoreBatch{}, &AdmissionFailure{EventIndex: safecast.MustConv[uint32](index), EventID: eventID, Failure: eventErr}
		}
		if candidate.Event == nil {
			return failure(eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_VALUE_INVALID,
				"event is required",
				"event",
				"event_required",
			))
		}
		if !validIdentifier(eventID, preparer.limits.MaxIDBytes) {
			return failure(eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
				"event_id is empty or has an invalid format",
				"event_id",
				"invalid_event_id",
			))
		}
		if _, duplicate := seenEventIDs[eventID]; duplicate {
			return failure(eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_EVENT_ID,
				"event_id is duplicated within the batch",
				"event_id",
				"duplicate_event_id",
			))
		}
		seenEventIDs[eventID] = struct{}{}
		if !validIndexName(candidate.Event.GetIndexName()) {
			return failure(eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_INVALID_INDEX,
				"index_name is empty or has an invalid format",
				"index_name",
				"invalid_index",
			))
		}
		policy, authorized := authority.policies[candidate.Event.GetIndexName()]
		if !authorized {
			return failure(eventFailure(
				opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_INDEX,
				"token is not authorized for the requested index",
				"index_name",
				"unauthorized_index",
			))
		}
		// Match native ingestion quota semantics exactly: charge the
		// server-computed protobuf size as received, before normalization or
		// configured redaction. The detached decoder observation is never an
		// accounting authority.
		sourceSize, sizeOK := protobufSizeUint64(candidate.Event)
		if !sizeOK || sourceSize == 0 {
			return StoreBatch{}, errors.New("ingestion admission source byte charge is outside bounds")
		}
		normalized, eventErr := policy.validator.validateAndNormalizeEventWithSize(
			candidate.Event,
			EventContext{
				ReceivedAt:         request.ReceivedAt,
				TimestampReference: request.ReceivedAt,
				DefaultSourcetype:  policy.defaultSourcetype,
				TenantID:           request.Authorization.TenantID,
				Source:             source,
				CollectorID:        request.CollectorID,
				BatchID:            request.BatchID,
			},
			sourceSize,
		)
		if eventErr != nil {
			return failure(eventErr)
		}
		if eventErr := authority.events.rejection(normalized); eventErr != nil {
			return failure(eventErr)
		}
		normalizedSize, sizeOK := protobufSizeUint64(normalized.Event)
		if !sizeOK || normalizedSize == 0 ||
			normalizedSize > HardMaxBatchBytes-normalizedUncompressedBytes {
			return StoreBatch{}, fmt.Errorf("%w: normalized byte charge is outside bounds", ErrAdmissionRequestTooLarge)
		}
		normalizedUncompressedBytes += normalizedSize
		if normalizedUncompressedBytes > preparer.limits.MaxBatchBytes {
			return StoreBatch{}, ErrAdmissionRequestTooLarge
		}
		if sourceSize > HardMaxBatchBytes-quotaUncompressedBytes {
			return StoreBatch{}, fmt.Errorf("%w: source byte charge is outside bounds", ErrAdmissionRequestTooLarge)
		}
		quotaUncompressedBytes += sourceSize
		storedEvents = append(storedEvents, normalized)
		indexName := normalized.Event.GetIndexName()
		retentionByIndex[indexName] = policy.retentionPeriod
		charge := quotaByIndex[indexName]
		if charge == nil {
			charge = &ingestquota.Charge{
				Scope: ingestquota.ScopeKey{
					Kind:     ingestquota.ScopeKindIndex,
					TenantID: request.Authorization.TenantID,
					Identity: indexName,
				},
				Limits: policy.ingestionRateLimits,
			}
			quotaByIndex[indexName] = charge
		}
		charge.Events++
		charge.UncompressedBytes += sourceSize
	}

	if uint64(len(storedEvents)) > math.MaxUint32 {
		return StoreBatch{}, errors.New("ingestion admission event count exceeds durable range")
	}
	quotaCharges := make([]ingestquota.Charge, 0, len(quotaByIndex)+1)
	quotaCharges = append(quotaCharges, ingestquota.Charge{
		Scope: ingestquota.ScopeKey{
			Kind:     ingestquota.ScopeKindToken,
			TenantID: request.Authorization.TenantID,
			Identity: request.Authorization.SubjectID,
		},
		Limits:            request.Authorization.TokenRateLimits,
		Events:            uint64(len(storedEvents)),
		UncompressedBytes: quotaUncompressedBytes,
	})
	for _, policy := range authority.ordered {
		if charge := quotaByIndex[policy.Name]; charge != nil {
			quotaCharges = append(quotaCharges, *charge)
		}
	}
	var hecAdmission *HECStageAdmission
	if request.HECAdmission != nil {
		hecAdmissionCopy := *request.HECAdmission
		hecAdmissionCopy.AuthorizedIndexes = make([]HECIndexAuthority, 0, len(retentionByIndex))
		for _, policy := range authority.ordered {
			if _, selected := retentionByIndex[policy.Name]; selected {
				hecAdmissionCopy.AuthorizedIndexes = append(hecAdmissionCopy.AuthorizedIndexes, HECIndexAuthority{
					Name: policy.Name, Version: policy.Version,
				})
			}
		}
		hecAdmission = &hecAdmissionCopy
	}
	return StoreBatch{
		TenantID:           request.Authorization.TenantID,
		Source:             source,
		CollectorID:        request.CollectorID,
		BatchID:            request.BatchID,
		BatchSequence:      request.BatchSequence,
		OriginalEventCount: safecast.MustConv[uint32](len(storedEvents)),
		SourceBatchSHA256:  request.SourceBatchSHA256,
		ReceivedAt:         request.ReceivedAt,
		Events:             storedEvents,
		RetentionByIndex:   retentionByIndex,
		RejectedEvents:     []*opensplunk.EventRejection{},
		QuotaAdmission:     &ingestquota.Admission{Charges: quotaCharges},
		QuotaEvaluatedAt:   request.QuotaEvaluatedAt,
		HECAdmission:       hecAdmission,
	}, nil
}

// Stage performs request-atomic preparation and then commits the durable
// staging transaction. No persistence is touched when Prepare fails.
func (preparer *AdmissionPreparer) Stage(
	ctx context.Context,
	request AdmissionRequest,
) (StageResult, error) {
	batch, err := preparer.Prepare(request)
	if err != nil {
		return StageResult{}, err
	}
	result, err := preparer.store.Stage(ctx, batch)
	if err != nil {
		return StageResult{}, err
	}
	result.AcceptedEvents = batch.OriginalEventCount
	if batch.QuotaAdmission != nil && len(batch.QuotaAdmission.Charges) > 0 {
		result.UncompressedBytes = batch.QuotaAdmission.Charges[0].UncompressedBytes
	}
	return result, nil
}

type admissionAuthority struct {
	ordered  []IndexPolicy
	policies map[string]resolvedIndexPolicy
	events   eventAuthorizationMatcher
}

func (preparer *AdmissionPreparer) resolveAuthority(
	authorization Authorization,
	reference time.Time,
) (admissionAuthority, error) {
	if len(authorization.AuthorizedIndexes) == 0 {
		return admissionAuthority{}, ErrNoActiveIndexAuthority
	}
	resolved, ok := resolveIndexPolicies(
		authorization.AuthorizedIndexes,
		reference,
		preparer.validator,
		preparer.limits,
		preparer.defaultIndexRetention,
	)
	if !ok {
		return admissionAuthority{}, ErrInvalidIndexAuthority
	}
	events, err := preparer.eventAuthorization(authorization)
	if err != nil {
		return admissionAuthority{}, ErrInvalidEventAuthority
	}
	return admissionAuthority{ordered: resolved.policies, policies: resolved.byName, events: events}, nil
}

// compiledEventAuthority caches one compiled event-authorization matcher keyed
// by the pattern lists it was compiled from.
type compiledEventAuthority struct {
	hosts   []string
	sources []string
	matcher eventAuthorizationMatcher
}

// eventAuthorization returns the compiled matcher for authorization, reusing
// the previous compilation when the pattern lists are unchanged.
func (preparer *AdmissionPreparer) eventAuthorization(authorization Authorization) (eventAuthorizationMatcher, error) {
	if cached := preparer.authority.Load(); cached != nil &&
		slices.Equal(cached.hosts, authorization.AllowedHostRegexes) &&
		slices.Equal(cached.sources, authorization.AllowedSourceRegexes) {
		return cached.matcher, nil
	}
	matcher, err := compileEventAuthorization(authorization)
	if err != nil {
		return eventAuthorizationMatcher{}, err
	}
	preparer.authority.Store(&compiledEventAuthority{
		hosts:   slices.Clone(authorization.AllowedHostRegexes),
		sources: slices.Clone(authorization.AllowedSourceRegexes),
		matcher: matcher,
	})
	return matcher, nil
}
