package ingest

import (
	"context"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

const maximumStreamPendingBytes = 32 << 20

func (state *streamState) lockHistory() (*streamState, func()) {
	if state.historyOwner != nil {
		state = state.historyOwner
	}
	state.historyMu.Lock()
	return state, state.historyMu.Unlock
}

func (state *streamState) releaseAdmission() {
	if state.admissionReady != nil {
		state.admissionReady()
		state.admissionReady = nil
	}
}

type collectBatchResult struct {
	response *opensplunk.CollectResponse
	throttle *opensplunk.Throttle
	err      error
	bytes    int
}

// The stream goroutine owns admission order, authority refresh and all Sends.
// Workers overlap only the durable store wait, with immutable authority views.
// The count and encoded-byte budgets bound retained wire/normalized batches.
type collectBatchPipeline struct {
	service          *Service
	stream           opensplunk.CollectorIngestService_CollectServer
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	completed        chan collectBatchResult
	inflight         int
	bytes            int
	responseSequence uint64
}

func newCollectBatchPipeline(s *Service, stream opensplunk.CollectorIngestService_CollectServer) *collectBatchPipeline {
	ctx, cancel := context.WithCancel(stream.Context())
	return &collectBatchPipeline{service: s, stream: stream, ctx: ctx, cancel: cancel,
		completed: make(chan collectBatchResult, s.config.MaxInFlightBatches), responseSequence: 1}
}

func (p *collectBatchPipeline) close() { p.cancel(); p.wg.Wait() }

func (p *collectBatchPipeline) start(batch *opensplunk.EventBatch, state *streamState, boundary time.Time, authority error, repack bool) error {
	size := proto.Size(batch)
	if err := p.waitForCapacity(batch); err != nil {
		return err
	}
	admitted := make(chan struct{})
	worker := &streamState{collectorID: state.collectorID, instanceID: state.instanceID,
		supportsRepacking: state.supportsRepacking, repackRequest: repack,
		authorization: state.authorization, indexPolicies: state.indexPolicies, eventAuthorization: state.eventAuthorization,
		historyOwner: state, admissionReady: func() { close(admitted) }}
	p.inflight++
	p.bytes += size
	p.wg.Go(func() {
		response, err := p.service.processBatchWithDeferredAuthority(p.ctx, batch, worker, boundary, authority)
		p.completed <- collectBatchResult{response: response, throttle: worker.pendingThrottle, err: err, bytes: size}
	})
	// Preserve wire sequence admission order without waiting for ClickHouse.
	select {
	case <-admitted:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// Capacity must be acquired before refreshing authority: a queued request must
// not start fresh work using a lease superseded while it waited for room.
func (p *collectBatchPipeline) waitForCapacity(batch *opensplunk.EventBatch) error {
	size := proto.Size(batch)
	for p.inflight >= int(p.service.config.MaxInFlightBatches) || (p.inflight > 0 && size > maximumStreamPendingBytes-p.bytes) {
		if err := p.finishOne(); err != nil {
			return err
		}
	}
	return nil
}

func (p *collectBatchPipeline) finishOne() error {
	select {
	case result := <-p.completed:
		return p.finish(result)
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

func (p *collectBatchPipeline) drain() error {
	for p.inflight > 0 {
		if err := p.finishOne(); err != nil {
			return err
		}
	}
	return nil
}

func (p *collectBatchPipeline) send(response *opensplunk.CollectResponse) error {
	if p.responseSequence == math.MaxUint64 {
		return status.Error(codes.ResourceExhausted, "server stream sequence exhausted")
	}
	p.responseSequence++
	response.StreamSequence = p.responseSequence
	if response.SentAt == nil {
		response.SentAt = timestamppb.New(p.service.config.Clock().UTC())
	}
	return p.stream.Send(response)
}

func (p *collectBatchPipeline) finish(result collectBatchResult) error {
	p.inflight--
	p.bytes -= result.bytes
	if result.err != nil {
		return result.err
	}
	// A store's cumulative hint does not prove earlier per-event rejection
	// outcomes have reached this stream. Exact acknowledgments are sufficient.
	if ack := result.response.GetBatchAck(); ack != nil && p.service.config.MaxInFlightBatches > 1 {
		ack.AcknowledgedThroughBatchSequence = nil
	}
	if err := p.send(result.response); err != nil {
		return err
	}
	if result.throttle != nil {
		sentAt := p.service.config.Clock().UTC()
		result.throttle.EffectiveUntil = timestamppb.New(sentAt.Add(result.throttle.GetMinimumSendDelay().AsDuration()))
		return p.send(&opensplunk.CollectResponse{SentAt: timestamppb.New(sentAt), Payload: &opensplunk.CollectResponse_Throttle{Throttle: result.throttle}})
	}
	return nil
}
