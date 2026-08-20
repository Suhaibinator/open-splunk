package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/hechttp"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

type runtimeHECAuthenticator interface {
	AuthenticateHEC(context.Context, string) (auth.Authentication, error)
}

type runtimeHECSequencer interface {
	visibility.HECAcknowledgmentReader
	HECReadiness(context.Context) (visibility.HECReadinessSnapshot, error)
	HECOperationalHealth(context.Context) (visibility.HECOperationalSnapshot, error)
}

type runtimeHECStore interface {
	ingest.StagingEventStore
	HECReconciliationAvailable() bool
	HECReconciliationTelemetry() clickhouse.HECReconciliationSnapshot
}

type runtimeHECOperations struct {
	metrics   *hechttp.Metrics
	sequencer runtimeHECSequencer
	store     runtimeHECStore
	now       func() time.Time
}

func newRuntimeHECOperations(
	metrics *hechttp.Metrics,
	sequencer runtimeHECSequencer,
	store runtimeHECStore,
) (*runtimeHECOperations, error) {
	if metrics == nil || nilcheck.IsNil(sequencer) || nilcheck.IsNil(store) {
		return nil, errors.New("compose HEC operations: complete dependencies are required")
	}
	return &runtimeHECOperations{
		metrics:   metrics,
		sequencer: sequencer,
		store:     store,
		now:       time.Now,
	}, nil
}

func (operations *runtimeHECOperations) HECOperationalSnapshot(
	ctx context.Context,
) (server.HECOperationalSnapshot, error) {
	if operations == nil || operations.metrics == nil ||
		nilcheck.IsNil(operations.sequencer) || nilcheck.IsNil(operations.store) {
		return server.HECOperationalSnapshot{}, errors.New("HEC operations are unavailable")
	}
	durable, err := operations.sequencer.HECOperationalHealth(ctx)
	if err != nil {
		return server.HECOperationalSnapshot{}, err
	}
	requests := operations.metrics.Snapshot()
	reconciliation := operations.store.HECReconciliationTelemetry()
	now := operations.now
	if now == nil {
		now = time.Now
	}
	return server.HECOperationalSnapshot{
		ObservedAt:                now().Round(0).UTC(),
		Requests:                  requests.Requests,
		Events:                    requests.Events,
		UncompressedBytes:         requests.UncompressedBytes,
		AuthenticationFailures:    requests.AuthenticationFailures,
		DecodeFailures:            requests.DecodeFailures,
		EventPolicyFailures:       requests.EventPolicyFailures,
		AcceptedRequests:          requests.AcceptedRequests,
		RateLimitedRequests:       requests.RateLimitedRequests,
		StagingFailures:           requests.StagingFailures,
		StagingDuration:           requests.StagingDuration,
		PendingOutboxReservations: durable.PendingOutboxReservations,
		PendingOutboxBytes:        durable.PendingOutboxBytes,
		OldestPendingOutboxAge:    durable.OldestPendingOutboxAge,
		RequestCapacityAvailable:  durable.RequestCapacityAvailable,
		RetainedRequests:          durable.RetainedRequests,
		QueueAvailable:            durable.QueueAvailable,
		ReconciliationAvailable:   reconciliation.Available,
		ReconciliationSuccesses:   reconciliation.Successes,
		ReconciliationRetries:     reconciliation.Retries,
		ReconciliationAmbiguities: reconciliation.Ambiguities,
		ActiveChannels:            durable.ActiveChannels,
		RetainedChannels:          durable.RetainedChannels,
		PendingAcknowledgments:    durable.PendingAcknowledgments,
		IndexedAcknowledgments:    durable.IndexedAcknowledgments,
		ExpiredAcknowledgments:    durable.ExpiredAcknowledgments,
		TerminalFailedRequests:    durable.TerminalFailedRequests,
		AcknowledgmentAvailable:   durable.AcknowledgmentAvailable,
		AcknowledgmentQueries:     requests.AcknowledgmentQueries,
		AcknowledgmentIDsQueried:  requests.AcknowledgmentIDsQueried,
		AcknowledgmentMisses:      requests.AcknowledgmentMisses,
		ShutdownRejections:        requests.ShutdownRejections,
		ProtocolFailures:          requests.ProtocolFailures,
	}, nil
}

type runtimeHECConfig struct {
	Next                  http.Handler
	Authenticator         runtimeHECAuthenticator
	Store                 runtimeHECStore
	Sequencer             runtimeHECSequencer
	TenantID              string
	DefaultIndexRetention time.Duration
	Metrics               *hechttp.Metrics
}

// newRuntimeHECHandler composes the complete selected HEC surface. It
// returns an error rather than registering JSON without raw, ACK, health, or
// the shared durable admission boundary.
func newRuntimeHECHandler(config runtimeHECConfig) (*hechttp.Handler, error) {
	if nilcheck.IsNil(config.Next) || nilcheck.IsNil(config.Authenticator) ||
		nilcheck.IsNil(config.Store) || nilcheck.IsNil(config.Sequencer) {
		return nil, errors.New("compose HEC runtime: complete dependencies are required")
	}
	admission, err := ingest.NewAdmissionPreparer(ingest.AdmissionConfig{
		Limits:                ingest.DefaultLimits(),
		DefaultIndexRetention: config.DefaultIndexRetention,
	}, config.Store)
	if err != nil {
		return nil, err
	}
	return hechttp.New(hechttp.Config{
		Next:            config.Next,
		Authenticator:   config.Authenticator,
		Admission:       admission,
		Acknowledgments: config.Sequencer,
		TenantID:        config.TenantID,
		Metrics:         config.Metrics,
		Health: hechttp.HealthCheckerFunc(func(ctx context.Context) (hechttp.HealthSnapshot, error) {
			snapshot, healthErr := config.Sequencer.HECReadiness(ctx)
			if healthErr != nil {
				return hechttp.HealthSnapshot{}, healthErr
			}
			return hechttp.HealthSnapshot{
				QueueAvailable: snapshot.QueueAvailable,
				AcknowledgmentAvailable: snapshot.AcknowledgmentAvailable &&
					config.Store.HECReconciliationAvailable(),
			}, nil
		}),
	})
}
