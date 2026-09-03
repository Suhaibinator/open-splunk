package server

import (
	"net/http"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

const (
	hecOperationsRoute = "/hec/operations/get"
	hecOperationsPath  = apiPathPrefix + hecOperationsRoute
)

func (handler *apiHandler) getHECOperationalSnapshot(
	request *http.Request,
	_ *opensplunk.GetHECOperationalSnapshotRequest,
) (*opensplunk.GetHECOperationalSnapshotResponse, error) {
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
) (*opensplunk.GetHECOperationalSnapshotResponse, error) {
	observedAt := timestamppb.New(snapshot.ObservedAt.Round(0).UTC())
	stagingDuration := durationpb.New(snapshot.StagingDuration)
	oldestPendingAge := durationpb.New(snapshot.OldestPendingOutboxAge)
	coalescingOldestPendingAge := durationpb.New(snapshot.InsertCoalescing.Queue.OldestPendingAge)
	if snapshot.ObservedAt.IsZero() || observedAt.CheckValid() != nil || stagingDuration.CheckValid() != nil ||
		oldestPendingAge.CheckValid() != nil || coalescingOldestPendingAge.CheckValid() != nil ||
		snapshot.StagingDuration < 0 || snapshot.OldestPendingOutboxAge < 0 ||
		snapshot.InsertCoalescing.Queue.OldestPendingAge < 0 {
		return nil, internalError()
	}
	protocolFailures := make([]*opensplunk.HECProtocolFailureMetric, 0, len(snapshot.ProtocolFailures)-1)
	for code := 1; code < len(snapshot.ProtocolFailures); code++ {
		protocolFailures = append(protocolFailures, &opensplunk.HECProtocolFailureMetric{
			Code:  safecast.MustConv[uint32](code),
			Count: snapshot.ProtocolFailures[code],
		})
	}
	return &opensplunk.GetHECOperationalSnapshotResponse{
		ObservedAt: observedAt,
		Request: &opensplunk.HECRequestOperationalMetrics{
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
		Durable: &opensplunk.HECDurableOperationalMetrics{
			QueueAvailable:            snapshot.QueueAvailable,
			PendingOutboxReservations: snapshot.PendingOutboxReservations,
			PendingOutboxBytes:        snapshot.PendingOutboxBytes,
			OldestPendingOutboxAge:    oldestPendingAge,
			RequestCapacityAvailable:  snapshot.RequestCapacityAvailable,
			RetainedRequests:          snapshot.RetainedRequests,
		},
		Reconciliation: &opensplunk.HECReconciliationOperationalMetrics{
			Available:   snapshot.ReconciliationAvailable,
			Successes:   snapshot.ReconciliationSuccesses,
			Retries:     snapshot.ReconciliationRetries,
			Ambiguities: snapshot.ReconciliationAmbiguities,
		},
		Acknowledgments: &opensplunk.HECAcknowledgmentOperationalMetrics{
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
		InsertCoalescing: hecInsertCoalescingSnapshotToProto(
			snapshot.InsertCoalescing,
			coalescingOldestPendingAge,
		),
	}, nil
}

func hecInsertCoalescingSnapshotToProto(
	snapshot HECInsertCoalescingSnapshot,
	oldestPendingAge *durationpb.Duration,
) *opensplunk.HECInsertCoalescingOperationalMetrics {
	return &opensplunk.HECInsertCoalescingOperationalMetrics{
		StagedLogicalBatches:      snapshot.StagedLogicalBatches,
		StagedLogicalRows:         snapshot.StagedLogicalRows,
		FormedGroups:              snapshot.FormedGroups,
		PhysicalSends:             snapshot.PhysicalSends,
		SuccessfulGroups:          snapshot.SuccessfulGroups,
		Retries:                   snapshot.Retries,
		Ambiguities:               snapshot.Ambiguities,
		GroupsByFillReason:        snapshot.GroupsByFillReason[:],
		MemberBatchesPerGroup:     hecCoalescingHistogramToProto(snapshot.MemberBatchesPerGroup),
		RowsPerGroup:              hecCoalescingHistogramToProto(snapshot.RowsPerGroup),
		DecodedBytesPerGroup:      hecCoalescingHistogramToProto(snapshot.DecodedBytesPerGroup),
		MonthlyPartitionsPerGroup: hecCoalescingHistogramToProto(snapshot.MonthlyPartitionsPerGroup),
		RowsPerPhysicalInsert:     hecCoalescingHistogramToProto(snapshot.RowsPerPhysicalInsert),
		CreationToSeal:            hecCoalescingLatencyHistogramToProto(snapshot.CreationToSeal),
		CreationToSend:            hecCoalescingLatencyHistogramToProto(snapshot.CreationToSend),
		CreationToCommit:          hecCoalescingLatencyHistogramToProto(snapshot.CreationToCommit),
		NativeWaiters:             snapshot.NativeWaiters,
		PeakNativeWaiters:         snapshot.PeakNativeWaiters,
		NativeWaiterWakeups:       snapshot.NativeWaiterWakeups,
		NativeWaiterCancels:       snapshot.NativeWaiterCancels,
		NativeTerminalLookups:     snapshot.NativeTerminalLookups,
		Queue: &opensplunk.HECInsertCoalescingQueueMetrics{
			PendingReservations:   snapshot.Queue.PendingReservations,
			UngroupedReservations: snapshot.Queue.UngroupedReservations,
			ReadyGroups:           snapshot.Queue.ReadyGroups,
			AmbiguousGroups:       snapshot.Queue.AmbiguousGroups,
			LeasedGroups:          snapshot.Queue.LeasedGroups,
			PendingOutboxBytes:    snapshot.Queue.PendingOutboxBytes,
			PendingMetadataBytes:  snapshot.Queue.PendingMetadataBytes,
			OldestPendingAge:      oldestPendingAge,
		},
	}
}

func hecCoalescingHistogramToProto(
	snapshot HECCoalescingHistogramSnapshot,
) *opensplunk.FixedHistogram {
	return &opensplunk.FixedHistogram{
		UpperBounds:  snapshot.Bounds[:],
		BucketCounts: snapshot.Counts[:],
		Count:        snapshot.Count,
		Sum:          snapshot.Sum,
		Max:          snapshot.Max,
	}
}

func hecCoalescingLatencyHistogramToProto(
	snapshot HECCoalescingLatencyHistogramSnapshot,
) *opensplunk.FixedLatencyHistogram {
	return &opensplunk.FixedLatencyHistogram{
		UpperBoundsMicroseconds: snapshot.BoundsMicroseconds[:],
		BucketCounts:            snapshot.Counts[:],
		Count:                   snapshot.Count,
		SumMicroseconds:         snapshot.SumMicroseconds,
		MaxMicroseconds:         snapshot.MaxMicroseconds,
	}
}

func (handler *apiHandler) hecOperationalRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[
			*opensplunk.GetHECOperationalSnapshotRequest,
			*opensplunk.GetHECOperationalSnapshotResponse,
		]{
			Path:       hecOperationsRoute,
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      codec.NewProtoCodec[*opensplunk.GetHECOperationalSnapshotRequest, *opensplunk.GetHECOperationalSnapshotResponse](),
			Handler:    handler.getHECOperationalSnapshot,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer:  sanitizeGetHECOperationalSnapshotRequest,
		},
	}
}
