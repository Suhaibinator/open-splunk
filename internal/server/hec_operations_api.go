package server

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	hecOperationsRoute = "/hec/operations/get"
	hecOperationsPath  = apiV1PathPrefix + hecOperationsRoute
)

func (handler *apiHandler) getHECOperationalSnapshot(
	request *http.Request,
	_ *opensplunkv1.GetHECOperationalSnapshotRequest,
) (*opensplunkv1.GetHECOperationalSnapshotResponse, error) {
	snapshot, err := handler.hecOperations.HECOperationalSnapshot(request.Context())
	if contextErr := requestContextFailure(request.Context(), err); contextErr != nil {
		return nil, router.NewHTTPError(
			http.StatusRequestTimeout,
			"HEC operational snapshot request was canceled",
		)
	}
	if err != nil {
		return nil, unavailableError("HEC operational telemetry is unavailable")
	}
	return hecOperationalSnapshotToProto(snapshot)
}

func hecOperationalSnapshotToProto(
	snapshot HECOperationalSnapshot,
) (*opensplunkv1.GetHECOperationalSnapshotResponse, error) {
	observedAt := timestamppb.New(snapshot.ObservedAt.Round(0).UTC())
	stagingDuration := durationpb.New(snapshot.StagingDuration)
	oldestPendingAge := durationpb.New(snapshot.OldestPendingOutboxAge)
	if snapshot.ObservedAt.IsZero() || observedAt.CheckValid() != nil || stagingDuration.CheckValid() != nil ||
		oldestPendingAge.CheckValid() != nil || snapshot.StagingDuration < 0 ||
		snapshot.OldestPendingOutboxAge < 0 {
		return nil, internalError()
	}
	protocolFailures := make([]*opensplunkv1.HECProtocolFailureMetric, 0, len(snapshot.ProtocolFailures)-1)
	for code := 1; code < len(snapshot.ProtocolFailures); code++ {
		protocolFailures = append(protocolFailures, &opensplunkv1.HECProtocolFailureMetric{
			Code:  uint32(code), // #nosec G115 -- fixed array index is at most 31.
			Count: snapshot.ProtocolFailures[code],
		})
	}
	return &opensplunkv1.GetHECOperationalSnapshotResponse{
		ObservedAt: observedAt,
		Request: &opensplunkv1.HECRequestOperationalMetrics{
			Requests:               snapshot.Requests,
			Events:                 snapshot.Events,
			UncompressedBytes:      snapshot.UncompressedBytes,
			AuthenticationFailures: snapshot.AuthenticationFailures,
			DecodeFailures:         snapshot.DecodeFailures,
			EventPolicyFailures:    snapshot.EventPolicyFailures,
			AcceptedRequests:       snapshot.AcceptedRequests,
			RateLimitedRequests:    snapshot.RateLimitedRequests,
			StagingFailures:        snapshot.StagingFailures,
			StagingDuration:        stagingDuration,
			ShutdownRejections:     snapshot.ShutdownRejections,
		},
		Durable: &opensplunkv1.HECDurableOperationalMetrics{
			QueueAvailable:            snapshot.QueueAvailable,
			PendingOutboxReservations: snapshot.PendingOutboxReservations,
			PendingOutboxBytes:        snapshot.PendingOutboxBytes,
			OldestPendingOutboxAge:    oldestPendingAge,
			RequestCapacityAvailable:  snapshot.RequestCapacityAvailable,
			RetainedRequests:          snapshot.RetainedRequests,
		},
		Reconciliation: &opensplunkv1.HECReconciliationOperationalMetrics{
			Available:   snapshot.ReconciliationAvailable,
			Successes:   snapshot.ReconciliationSuccesses,
			Retries:     snapshot.ReconciliationRetries,
			Ambiguities: snapshot.ReconciliationAmbiguities,
		},
		Acknowledgments: &opensplunkv1.HECAcknowledgmentOperationalMetrics{
			Available:              snapshot.AcknowledgmentAvailable,
			ActiveChannels:         snapshot.ActiveChannels,
			RetainedChannels:       snapshot.RetainedChannels,
			PendingRows:            snapshot.PendingAcknowledgments,
			IndexedRows:            snapshot.IndexedAcknowledgments,
			ExpiredRows:            snapshot.ExpiredAcknowledgments,
			TerminalFailedRequests: snapshot.TerminalFailedRequests,
			Queries:                snapshot.AcknowledgmentQueries,
			IdsQueried:             snapshot.AcknowledgmentIDsQueried,
			Misses:                 snapshot.AcknowledgmentMisses,
		},
		ProtocolFailures: protocolFailures,
	}, nil
}

func (handler *apiHandler) hecOperationalRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute[
			*opensplunkv1.GetHECOperationalSnapshotRequest,
			*opensplunkv1.GetHECOperationalSnapshotResponse,
		](router.RouteConfig[
			*opensplunkv1.GetHECOperationalSnapshotRequest,
			*opensplunkv1.GetHECOperationalSnapshotResponse,
		]{
			Path:       hecOperationsRoute,
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      codec.NewProtoCodec[*opensplunkv1.GetHECOperationalSnapshotRequest, *opensplunkv1.GetHECOperationalSnapshotResponse](),
			Handler:    handler.getHECOperationalSnapshot,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}
