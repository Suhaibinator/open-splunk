package sender

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

// withBearer attaches the bearer token to the outgoing gRPC metadata exactly as
// internal/ingest expects ("authorization: Bearer <token>"). The token is only
// placed in call metadata and is never logged.
func withBearer(ctx context.Context, token string) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token))
}

// conn holds the per-connection state for one Collect stream. A new conn is
// created for every (re)connection; stream_sequence starts at 1 and increases
// by one for every request sent on this stream.
type conn struct {
	s      *Sender
	stream opensplunk.CollectorIngestService_CollectClient
	ready  *opensplunk.CollectorReady
	// ctx governs the pump, heartbeat, and retry goroutines; it is derived from
	// the parent Run context. streamCancel tears down the underlying gRPC stream
	// and is kept separate so Goodbye can still be sent after ctx is canceled.
	ctx          context.Context
	cancel       context.CancelFunc
	streamCancel context.CancelFunc

	// sendMu serializes all stream.Send calls (batch, heartbeat, retry, goodbye)
	// and guards the outgoing stream sequence. gRPC permits one concurrent Send.
	//
	// Lock ordering: code that needs both locks must acquire sendMu before mu.
	// No code may acquire sendMu while holding mu, and mu is always released
	// before the potentially blocking stream.Send call. This lets terminal
	// release linearize with a retry's final eligibility check without holding
	// the general connection-state lock across gRPC flow control.
	sendMu    sync.Mutex
	streamSeq uint64
	recvSeq   uint64

	mu   sync.Mutex
	cond *sync.Cond
	// inflight holds sent-but-not-terminally-acked batches keyed by batch
	// sequence, so a RetryBatch resends the exact retained bytes and a partial
	// ack can map event indices back to events.
	inflight  map[uint64]*opensplunk.EventBatch
	inflightN int
	// highestSentBatchSequence bounds cumulative acknowledgments to data this
	// connection has actually dispatched. The WAL may already contain newer
	// batches, but a buggy or compromised server must not be able to burn them.
	highestSentBatchSequence uint64
	// pendingRetry tracks batch sequences with a resend already scheduled, so a
	// flood of RetryBatch messages for one sequence coalesces to a single resend
	// goroutine instead of spawning one per message. Each entry is independently
	// cancellable so a terminal disposition promptly releases its goroutine and
	// retained EventBatch instead of waiting out a server-controlled delay.
	pendingRetry map[uint64]*scheduledRetry
	retryWG      sync.WaitGroup
	failure      error

	maxInFlight    int
	maxBatchEvents uint32
	maxBatchBytes  uint64

	// throttle state (server Throttle message).
	throttled          bool
	throttleUntil      time.Time
	minSendDelay       time.Duration
	throttleMaxInFlt   int
	throttleMaxEvents  uint32
	throttleMaxBytes   uint64
	throttleGeneration uint64
	lastBatchSendAt    time.Time
	nextBatchSendAt    time.Time
	draining           bool
	serverShutdown     bool
	serverReconnectDur time.Duration
	serverDrainTimer   *time.Timer
}

type scheduledRetry struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Sender) newConn(ctx context.Context, cancel, streamCancel context.CancelFunc, stream opensplunk.CollectorIngestService_CollectClient) *conn {
	c := &conn{
		s:            s,
		stream:       stream,
		ctx:          ctx,
		cancel:       cancel,
		streamCancel: streamCancel,
		inflight:     make(map[uint64]*opensplunk.EventBatch),
		pendingRetry: make(map[uint64]*scheduledRetry),
	}
	c.cond = sync.NewCond(&c.mu)
	return c
}

// runConnection performs one full connection lifecycle: dial a stream, send
// Hello, await Ready, then pump batches and heartbeats until the connection
// ends. It returns whether Ready was reached, an optional server-requested
// reconnect delay, and the terminating error (nil for a clean server shutdown).
func (s *Sender) runConnection(parent context.Context) (connected bool, reconnectAfter time.Duration, err error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	// The stream lives on an independent context so a Goodbye can be transmitted
	// after the parent context is canceled during graceful shutdown.
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	token, err := s.opts.Token()
	if err != nil {
		return false, 0, fmt.Errorf("collector/sender: read token: %w", err)
	}
	stream, err := s.client.Collect(withBearer(streamCtx, token), s.collectCallOptions()...)
	if err != nil {
		return false, 0, classifyPreReadyError(err)
	}

	c := s.newConn(ctx, cancel, streamCancel, stream)
	handshakeDone := make(chan error, 1)
	go func() {
		if err := c.sendHello(); err != nil {
			handshakeDone <- err
			return
		}
		handshakeDone <- c.receiveReady()
	}()
	var readyTimer <-chan time.Time
	var timer *time.Timer
	if s.opts.DialTimeout > 0 {
		timer = time.NewTimer(s.opts.DialTimeout)
		readyTimer = timer.C
		defer timer.Stop()
	}
	select {
	case err := <-handshakeDone:
		if err != nil {
			return false, 0, classifyPreReadyError(err)
		}
	case <-parent.Done():
		c.gracefulPreReadyShutdown(handshakeDone)
		return false, 0, parent.Err()
	case <-readyTimer:
		streamCancel()
		<-handshakeDone
		return false, 0, fmt.Errorf("collector/sender: ready negotiation timed out after %s", s.opts.DialTimeout)
	}
	c.observeReadyResume()

	// Batches handed out on a previous connection but never terminally
	// acknowledged are still unacked in the queue, behind its delivery cursor.
	// Rewind so this stream resends them. A Ready resume hint cannot replace the
	// terminal BatchAck/BatchReject outcome: a committed batch may still contain
	// per-event rejections that must be written to the local dead-letter sink.
	// The server deduplicates identical retries by batch ID and replays the
	// original terminal outcome.
	s.queue.Rewind()

	s.logger.Info("collector stream ready",
		zap.String("address", s.opts.Address),
		zap.String("stream_id", c.ready.GetStreamId()),
		zap.Uint32("max_in_flight", c.ready.GetMaxInFlightBatches()))
	s.setConnected(true)
	defer s.setConnected(false)

	stopContextWake := context.AfterFunc(ctx, c.broadcastWaiters)
	defer stopContextWake()

	recvDone := make(chan error, 1)
	go func() { recvDone <- c.receiveLoop() }()
	pumpDone := make(chan struct{})
	go func() { defer close(pumpDone); c.pumpLoop() }()
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); c.heartbeatLoop() }()

	select {
	case <-parent.Done():
		c.gracefulShutdown(recvDone)
		c.stopServerDrainTimer()
		<-pumpDone
		<-hbDone
		c.retryWG.Wait()
		return true, 0, parent.Err()
	case recvErr := <-recvDone:
		c.stopServerDrainTimer()
		streamCancel()
		cancel()
		<-pumpDone
		<-hbDone
		c.retryWG.Wait()
		c.mu.Lock()
		shutdown := c.serverShutdown
		reconnect := c.serverReconnectDur
		c.mu.Unlock()
		if shutdown {
			return true, reconnect, nil
		}
		if failure := c.connectionFailure(); failure != nil {
			return true, 0, failure
		}
		return true, 0, recvErr
	}
}

// classifyPreReadyError stops reconnect churn only for status codes determined
// entirely by the immutable Hello/protocol shape. Authentication and permission
// failures remain retryable because token files and server policy can rotate
// without restarting the collector.
func classifyPreReadyError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*fatalError](err); ok {
		return err
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.Unimplemented:
		return &fatalError{err: err}
	default:
		return err
	}
}

func (c *conn) sendHello() error {
	stats := c.s.queue.Stats()
	hello := &opensplunk.CollectorHello{
		CollectorId:     c.s.opts.CollectorID,
		InstanceId:      c.s.opts.InstanceID,
		SourceRevision:  c.s.opts.Hello.SourceRevision,
		Hostname:        c.s.opts.Hello.Hostname,
		OperatingSystem: c.s.opts.Hello.OperatingSystem,
		Architecture:    c.s.opts.Hello.Architecture,
		StartedAt:       timestamppb.New(c.s.opts.Hello.StartedAt.UTC()),
		Capabilities:    c.s.opts.Hello.Capabilities,
		Inputs:          c.s.opts.Hello.Inputs,
	}
	if stats.LastAckedBatchSequence > 0 {
		v := collectorlimits.ClampFleetCounter(stats.LastAckedBatchSequence)
		hello.LastAcknowledgedBatchSequence = &v
	}
	return c.send(&opensplunk.CollectRequest{
		Payload: &opensplunk.CollectRequest_Hello{Hello: hello},
	})
}

func (c *conn) receiveReady() error {
	resp, err := c.stream.Recv()
	if err != nil {
		return err
	}
	if err := c.validateResponse(resp); err != nil {
		return err
	}
	ready := resp.GetReady()
	if ready == nil {
		return fmt.Errorf("collector/sender: expected CollectorReady, got %T", resp.GetPayload())
	}
	if ready.GetAcknowledgmentDurability() != opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		return &fatalError{err: fmt.Errorf(
			"collector/sender: server acknowledgment durability %s cannot safely advance source checkpoints",
			ready.GetAcknowledgmentDurability().String(),
		)}
	}
	if ready.GetStreamId() == "" {
		return &fatalError{err: errors.New("collector/sender: server Ready has an empty stream id")}
	}
	heartbeatInterval := ready.GetHeartbeatInterval()
	if heartbeatInterval == nil || heartbeatInterval.CheckValid() != nil || heartbeatInterval.AsDuration() <= 0 {
		return &fatalError{err: errors.New("collector/sender: server Ready has an invalid heartbeat interval")}
	}
	if ready.GetMaxInFlightBatches() == 0 || ready.GetMaxBatchEvents() == 0 ||
		ready.GetMaxBatchBytes() == 0 || ready.GetMaxEventBytes() == 0 {
		return &fatalError{err: errors.New("collector/sender: server Ready has zero-valued hard delivery limits")}
	}
	if uint64(ready.GetMaxInFlightBatches()) > uint64(^uint(0)>>1) {
		return &fatalError{err: errors.New("collector/sender: server Ready max_in_flight_batches exceeds platform int capacity")}
	}
	c.ready = ready

	c.mu.Lock()
	c.maxInFlight = int(ready.GetMaxInFlightBatches())
	c.maxBatchEvents = ready.GetMaxBatchEvents()
	c.maxBatchBytes = ready.GetMaxBatchBytes()
	c.mu.Unlock()

	return nil
}

func (c *conn) observeReadyResume() {
	if c.ready.ResumeAfterBatchSequence == nil {
		return
	}
	// resume_after_batch_sequence proves server-side durability, but says nothing
	// about the per-event accepted/duplicate/rejected outcome. If a previous
	// BatchAck was lost, AckThrough here would silently discard any rejected
	// events before they reached the local dead-letter sink. Keep the hint
	// advisory and replay the exact WAL batch so ingest deduplication can return
	// its original terminal outcome.
	c.s.logger.Info("collector stream advertised resume point; replaying local WAL for explicit outcomes",
		zap.Uint64("resume_sequence", c.ready.GetResumeAfterBatchSequence()))
}

// send stamps the next connection-local stream sequence and sent_at, then
// transmits the request. All senders must go through send under sendMu.
func (c *conn) send(req *opensplunk.CollectRequest) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.sendLocked(req)
}

// sendLocked stamps and transmits req while the caller holds sendMu.
func (c *conn) sendLocked(req *opensplunk.CollectRequest) error {
	c.streamSeq++
	req.StreamSequence = c.streamSeq
	req.SentAt = timestamppb.New(c.s.now().UTC())
	return c.stream.Send(req)
}

// --- pump -------------------------------------------------------------------

func (c *conn) pumpLoop() {
	// pending retains a batch already dequeued by NextBatch but held back because a
	// temporary throttle forbids it right now; it must not be re-appended.
	var pending *opensplunk.EventBatch
	for {
		if !c.waitForInFlightCapacity(c.ctx) {
			return
		}

		batch := pending
		pending = nil
		if batch == nil {
			var err error
			batch, err = c.s.queue.NextBatch(c.ctx)
			if err != nil {
				if c.ctx.Err() == nil {
					c.fail(fmt.Errorf("collector/sender: read next durable batch: %w", err))
				}
				return
			}
		}

		// A batch that exceeds the NEGOTIATED Ready limits can never be accepted on
		// this stream. A single event has an exact dead-letter disposition; a
		// multi-event batch must remain durable until a lossless repacker can split
		// valid events from the offending limit.
		if code, ok := c.batchExceedsReadyLimits(batch); ok {
			if len(batch.GetEvents()) != 1 {
				c.fail(&fatalError{err: fmt.Errorf(
					"collector/sender: durable batch %d exceeds negotiated limits and requires lossless repacking; batch retained",
					batch.GetBatchSequence(),
				)})
				return
			}
			if err := c.deadLetterWholeBatch(batch, code, "batch exceeds negotiated server limits"); err != nil {
				c.fail(err)
				return
			}
			through, err := c.s.commitTerminal(batch.GetBatchSequence(), false)
			if err != nil {
				c.fail(err)
				return
			}
			c.s.markDropped(uint64(len(batch.GetEvents())))
			c.s.markAcked(through, 0)
			continue
		}

		result, wait, throttleGeneration, err := c.trySendBatch(batch)
		if err != nil {
			c.fail(err)
			return
		}
		switch result {
		case batchSendStopped:
			return
		case batchSendWaitPacing:
			pending = batch
			if !c.waitForThrottleUpdateOrDelay(c.ctx, wait, throttleGeneration) {
				return
			}
			continue
		case batchSendWaitThrottle:
			// A batch that exceeds only a TEMPORARY throttle's reduced limits
			// is held rather than dead-lettered. Head-of-line blocking is the
			// correct behavior until the temporary policy is lifted or expires.
			pending = batch
			if !c.waitOutThrottle(c.ctx, throttleGeneration) {
				return
			}
			continue
		case batchSendWaitInFlight:
			// A throttle may reduce max-in-flight while NextBatch is blocked.
			// Keep this already-dequeued batch pending and let the outer capacity
			// loop wait without taking another item from the WAL.
			pending = batch
			continue
		case batchSent:
			c.s.markSent(batch)
		}
	}
}

type batchSendResult uint8

const (
	batchSent batchSendResult = iota
	batchSendStopped
	batchSendWaitPacing
	batchSendWaitThrottle
	batchSendWaitInFlight
)

// trySendBatch performs the final throttle, retry, and in-flight checks after
// NextBatch returns. sendMu makes applying a Throttle and starting a batch send
// a single ordered decision: a throttle that wins the lock is observed here;
// otherwise this send was already in progress before that throttle applied.
func (c *conn) trySendBatch(
	batch *opensplunk.EventBatch,
) (batchSendResult, time.Duration, uint64, error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	throttleGeneration := c.throttleGeneration
	if c.ctx.Err() != nil || c.draining {
		c.mu.Unlock()
		return batchSendStopped, 0, throttleGeneration, nil
	}
	now := c.s.now()
	if c.throttleActiveLocked() {
		if c.batchExceedsThrottleLimitsLocked(batch) {
			c.mu.Unlock()
			return batchSendWaitThrottle, 0, throttleGeneration, nil
		}
		if wait := c.nextBatchSendAt.Sub(now); wait > 0 {
			c.mu.Unlock()
			return batchSendWaitPacing, wait, throttleGeneration, nil
		}
	}
	if wait := c.s.batchRetryWait(batch, now); wait > 0 {
		c.mu.Unlock()
		return batchSendWaitPacing, wait, throttleGeneration, nil
	}
	if c.inflightN >= c.effectiveMaxInFlightLocked() {
		c.mu.Unlock()
		return batchSendWaitInFlight, 0, throttleGeneration, nil
	}

	c.inflightN++
	c.inflight[batch.GetBatchSequence()] = batch
	if batch.GetBatchSequence() > c.highestSentBatchSequence {
		c.highestSentBatchSequence = batch.GetBatchSequence()
	}
	c.recordBatchSendLocked(now)
	c.mu.Unlock()

	err := c.sendLocked(&opensplunk.CollectRequest{
		Payload: &opensplunk.CollectRequest_Batch{Batch: batch},
	})
	return batchSent, 0, throttleGeneration, err
}

func (c *conn) recordBatchSendLocked(sentAt time.Time) {
	c.lastBatchSendAt = sentAt
	if c.throttleActiveLocked() && c.minSendDelay > 0 {
		c.nextBatchSendAt = sentAt.Add(c.minSendDelay)
	}
}

func (c *conn) effectiveMaxInFlightLocked() int {
	if c.throttleActiveLocked() && c.throttleMaxInFlt > 0 &&
		c.throttleMaxInFlt < c.maxInFlight {
		return c.throttleMaxInFlt
	}
	return c.maxInFlight
}

// waitForInFlightCapacity observes finite throttle expiry as well as explicit
// condition broadcasts. Without the expiry timer, a temporary reduced
// max-in-flight limit could strand an already-dequeued batch until an unrelated
// acknowledgment or throttle update happened to wake the condition.
func (c *conn) waitForInFlightCapacity(ctx context.Context) bool {
	stopContextWake := context.AfterFunc(ctx, c.broadcastWaiters)
	defer stopContextWake()

	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if ctx.Err() != nil || c.draining {
			return false
		}
		if c.inflightN < c.effectiveMaxInFlightLocked() {
			return true
		}
		var deadline time.Time
		if c.throttled {
			deadline = c.throttleUntil
		}
		c.waitOnConditionLocked(deadline)
	}
}

// waitForThrottleUpdateOrDelay waits for the computed delay unless a newer
// Throttle arrives first. The generation check closes the window between the
// caller's final state check and entering Cond.Wait.
func (c *conn) waitForThrottleUpdateOrDelay(
	ctx context.Context,
	wait time.Duration,
	throttleGeneration uint64,
) bool {
	if wait <= 0 {
		return ctx.Err() == nil
	}
	c.mu.Lock()
	if ctx.Err() != nil || c.draining {
		c.mu.Unlock()
		return false
	}
	if c.throttleGeneration != throttleGeneration {
		c.mu.Unlock()
		return true
	}
	// Register both wakeups while holding c.mu. If either fires immediately,
	// its callback cannot acquire the mutex until Cond.Wait atomically releases
	// it, which prevents an elapsed timer or cancellation from being lost in
	// the final predicate-check-to-wait window.
	stopContextWake := context.AfterFunc(ctx, c.broadcastWaiters)
	timer := time.AfterFunc(wait, c.broadcastWaiters)
	c.cond.Wait()
	stopped := ctx.Err() != nil || c.draining
	c.mu.Unlock()
	timer.Stop()
	stopContextWake()
	return !stopped
}

// waitOnConditionLocked waits for a state broadcast or an optional deadline.
// The caller holds c.mu, so checking a predicate and entering Cond.Wait cannot
// lose a concurrent throttle update. A timer callback takes the same lock and
// broadcasts when a finite throttle expires.
func (c *conn) waitOnConditionLocked(deadline time.Time) {
	var timer *time.Timer
	if !deadline.IsZero() {
		wait := deadline.Sub(c.s.now())
		if wait <= 0 {
			return
		}
		timer = time.AfterFunc(wait, c.broadcastWaiters)
	}
	c.cond.Wait()
	if timer != nil {
		timer.Stop()
	}
}

func (c *conn) broadcastWaiters() {
	c.mu.Lock()
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *conn) throttleActiveLocked() bool {
	if !c.throttled {
		return false
	}
	if c.throttleUntil.IsZero() {
		return true
	}
	if c.s.now().Before(c.throttleUntil) {
		return true
	}
	c.throttled = false
	return false
}

// batchExceedsReadyLimits reports whether batch exceeds the NEGOTIATED Ready
// limits (fixed for the life of the stream). The caller can terminally
// dead-letter a single-event batch; a multi-event batch is retained because it
// requires lossless repacking to preserve its otherwise-valid events.
func (c *conn) batchExceedsReadyLimits(batch *opensplunk.EventBatch) (string, bool) {
	c.mu.Lock()
	maxEvents := c.maxBatchEvents
	maxBytes := c.maxBatchBytes
	maxEventBytes := c.ready.GetMaxEventBytes()
	c.mu.Unlock()

	for _, event := range batch.GetEvents() {
		eventSize := proto.Size(event)
		if eventSize >= 0 && uint64(eventSize) > maxEventBytes {
			return opensplunk.EventRejectionCode_EVENT_REJECTION_CODE_EVENT_TOO_LARGE.String(), true
		}
	}

	// exactly representable as uint64.
	if maxEvents > 0 && uint64(len(batch.GetEvents())) > uint64(maxEvents) {
		return opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS.String(), true
	}
	if maxBytes > 0 && batch.GetUncompressedSizeBytes() > maxBytes {
		return opensplunk.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE.String(), true
	}
	return "", false
}

func (c *conn) batchExceedsThrottleLimitsLocked(batch *opensplunk.EventBatch) bool {

	// exactly representable as uint64.
	if c.throttleMaxEvents > 0 && uint64(len(batch.GetEvents())) > uint64(c.throttleMaxEvents) {
		return true
	}
	if c.throttleMaxBytes > 0 && batch.GetUncompressedSizeBytes() > c.throttleMaxBytes {
		return true
	}
	return false
}

// waitOutThrottle blocks until the observed throttle expires or is replaced,
// so the held batch can be checked against the current policy again. Finite
// expiry, replacement Throttle messages, terminal retry cancellation, and
// connection teardown all wake the same predicate loop. It returns false when
// ctx is canceled or the stream drains.
func (c *conn) waitOutThrottle(ctx context.Context, throttleGeneration uint64) bool {
	stopContextWake := context.AfterFunc(ctx, c.broadcastWaiters)
	defer stopContextWake()
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if ctx.Err() != nil || c.draining {
			return false
		}
		if c.throttleGeneration != throttleGeneration {
			return true
		}
		if !c.throttleActiveLocked() {
			return true
		}
		c.waitOnConditionLocked(c.throttleUntil)
	}
}

// --- heartbeat --------------------------------------------------------------

func (c *conn) heartbeatLoop() {
	interval := c.ready.GetHeartbeatInterval().AsDuration()
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			draining := c.draining
			c.mu.Unlock()
			if draining {
				continue
			}
			hb := c.s.buildHeartbeat()
			if err := c.sendHeartbeat(&opensplunk.CollectRequest{
				Payload: &opensplunk.CollectRequest_Heartbeat{Heartbeat: hb},
			}); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

func (c *conn) sendHeartbeat(req *opensplunk.CollectRequest) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.mu.Lock()
	stopped := c.draining || c.ctx.Err() != nil
	c.mu.Unlock()
	if stopped {
		return nil
	}
	return c.sendLocked(req)
}

// --- receive dispatch -------------------------------------------------------

func (c *conn) receiveLoop() error {
	for {
		resp, err := c.stream.Recv()
		if err != nil {
			return err
		}
		if err := c.validateResponse(resp); err != nil {
			return err
		}
		switch {
		case resp.GetReady() != nil:
			return errors.New("collector/sender: duplicate CollectorReady")
		case resp.GetBatchAck() != nil:
			if err := c.handleAck(resp.GetBatchAck()); err != nil {
				return err
			}
		case resp.GetBatchReject() != nil:
			if err := c.handleReject(resp.GetBatchReject()); err != nil {
				return err
			}
		case resp.GetRetryBatch() != nil:
			if err := c.handleRetry(resp.GetRetryBatch()); err != nil {
				return err
			}
		case resp.GetThrottle() != nil:
			if err := c.handleThrottle(resp); err != nil {
				return err
			}
		case resp.GetNotice() != nil:
			if err := c.handleNotice(resp.GetNotice()); err != nil {
				return err
			}
		default:
			return errors.New("collector/sender: response has no payload")
		}
	}
}

func (c *conn) validateResponse(resp *opensplunk.CollectResponse) error {
	if resp == nil {
		return errors.New("collector/sender: nil response")
	}
	if sentAt := resp.GetSentAt(); sentAt == nil || sentAt.CheckValid() != nil {
		return errors.New("collector/sender: response has invalid sent_at")
	}
	want := c.recvSeq + 1
	if resp.GetStreamSequence() != want {
		return fmt.Errorf("collector/sender: response stream sequence %d, want %d", resp.GetStreamSequence(), want)
	}
	c.recvSeq = want
	return nil
}

// handleAck is terminal for the batch. Accepted and duplicate events are acked
// off the durable queue; rejected events are dead-lettered but the batch is
// still acked (the ack is terminal).
func (c *conn) handleAck(ack *opensplunk.BatchAck) error {
	seq := ack.GetBatchSequence()
	if ack.GetDurability() != opensplunk.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		return fmt.Errorf(
			"collector/sender: ack for batch sequence %d has unsafe durability %s",
			seq, ack.GetDurability().String(),
		)
	}
	batch := c.lookupInflight(seq)
	if batch == nil {
		return fmt.Errorf("collector/sender: ack for unknown batch sequence %d", seq)
	}
	if ack.GetBatchId() != batch.GetBatchId() {
		return fmt.Errorf("collector/sender: ack batch id %q does not match sequence %d", ack.GetBatchId(), seq)
	}
	target := seq
	cumulative := ack.AcknowledgedThroughBatchSequence != nil
	if cumulative {
		target = ack.GetAcknowledgedThroughBatchSequence()
		if target < seq {
			return fmt.Errorf("collector/sender: acknowledged-through %d precedes batch %d", target, seq)
		}
		c.mu.Lock()
		highestSent := c.highestSentBatchSequence
		var outcomeMissingFor uint64
		for inflightSequence := range c.inflight {
			if inflightSequence <= target && inflightSequence != seq {
				outcomeMissingFor = inflightSequence
				break
			}
		}
		c.mu.Unlock()
		if target > highestSent {
			return fmt.Errorf(
				"collector/sender: acknowledged-through %d exceeds highest batch sequence sent on this connection %d",
				target, highestSent,
			)
		}
		if outcomeMissingFor != 0 {
			return fmt.Errorf(
				"collector/sender: cumulative acknowledgment through %d omits terminal outcome for in-flight batch %d",
				target, outcomeMissingFor,
			)
		}
	}

	accounted := uint64(ack.GetAcceptedEventCount()) + uint64(ack.GetDuplicateEventCount()) + uint64(len(ack.GetRejectedEvents()))
	if accounted != uint64(len(batch.GetEvents())) {
		return fmt.Errorf("collector/sender: ack accounts for %d of %d events in batch %d", accounted, len(batch.GetEvents()), seq)
	}
	// A terminal response supersedes any pending retry. Serialize cancellation
	// with retry Send before performing durable local side effects; if a later
	// validation or persistence step fails, the stream is torn down and the WAL
	// batch is replayed on the next connection.
	if cumulative {
		c.cancelRetriesThrough(target)
	} else {
		c.cancelRetry(seq)
	}
	if rejected := ack.GetRejectedEvents(); len(rejected) > 0 {
		records := make([]DeadLetterRecord, 0, len(rejected))
		now := c.s.now()
		seen := make(map[uint32]struct{}, len(rejected))
		for _, rej := range rejected {
			if rej == nil {
				return fmt.Errorf("collector/sender: nil rejected event for batch %d", seq)
			}
			idx := rej.GetEventIndex()
			// Compare without converting the hostile uint32 to int: on 32-bit
			// platforms a value above MaxInt would wrap negative and bypass the
			// bounds check before the slice access below.
			if uint64(idx) >= uint64(len(batch.GetEvents())) {
				return fmt.Errorf("collector/sender: rejection index %d out of range for batch %d", idx, seq)
			}
			if _, duplicate := seen[idx]; duplicate {
				return fmt.Errorf("collector/sender: duplicate rejection index %d for batch %d", idx, seq)
			}
			seen[idx] = struct{}{}
			event := batch.GetEvents()[idx]
			if rej.GetEventId() != "" && rej.GetEventId() != event.GetEventId() {
				return fmt.Errorf("collector/sender: rejection event id does not match index %d for batch %d", idx, seq)
			}
			records = append(records, DeadLetterRecord{
				Event:         event,
				BatchID:       ack.GetBatchId(),
				BatchSequence: seq,
				Code:          rej.GetCode().String(),
				Reason:        rej.GetMessage(),
				RejectedAt:    now,
			})
		}
		if err := c.s.writeDeadLetter(records); err != nil {
			return err
		}
	}
	through, err := c.s.commitTerminal(target, cumulative)
	if err != nil {
		return fmt.Errorf("collector/sender: queue ack through %d: %w", target, err)
	}
	c.s.markRejected(uint64(len(ack.GetRejectedEvents())))
	c.s.markAcked(through, uint64(ack.GetAcceptedEventCount())+uint64(ack.GetDuplicateEventCount()))
	if cumulative {
		c.releaseInflightThrough(target)
	} else {
		c.releaseInflight(seq)
	}
	return nil
}

// handleReject permanently dead-letters the entire batch, then acks it off the
// durable queue. BatchReject is terminal (documented in doc.go).
func (c *conn) handleReject(reject *opensplunk.BatchReject) error {
	seq := reject.GetBatchSequence()
	batch := c.lookupInflight(seq)
	if batch == nil {
		return fmt.Errorf("collector/sender: reject for unknown batch sequence %d", seq)
	}
	if reject.GetBatchId() != batch.GetBatchId() {
		return fmt.Errorf("collector/sender: reject batch id %q does not match sequence %d", reject.GetBatchId(), seq)
	}
	c.cancelRetry(seq)
	if err := c.deadLetterWholeBatch(batch, reject.GetCode().String(), reject.GetMessage()); err != nil {
		return err
	}
	through, err := c.s.commitTerminal(seq, false)
	if err != nil {
		return fmt.Errorf("collector/sender: queue ack %d: %w", seq, err)
	}
	c.s.markDropped(uint64(len(batch.GetEvents())))
	c.s.markAcked(through, 0)
	c.releaseInflight(seq)
	return nil
}

// handleRetry is non-terminal: the exact same durable batch is retained and
// resent after retry_after. The in-flight slot is kept the whole time.
func (c *conn) handleRetry(retry *opensplunk.RetryBatch) error {
	seq := retry.GetBatchSequence()
	retryDelay := time.Duration(0)
	if retryAfter := retry.GetRetryAfter(); retryAfter != nil {
		if err := retryAfter.CheckValid(); err != nil || retryAfter.AsDuration() < 0 ||
			retryAfter.AsDuration() > ingestquota.MaximumRetryAfter {
			return fmt.Errorf("collector/sender: retry for batch sequence %d has invalid retry_after", seq)
		}
		retryDelay = retryAfter.AsDuration()
	}
	c.mu.Lock()
	batch := c.inflight[seq]
	if batch == nil {
		c.mu.Unlock()
		return fmt.Errorf("collector/sender: retry for unknown batch sequence %d", seq)
	}
	if retry.GetBatchId() != batch.GetBatchId() {
		c.mu.Unlock()
		return fmt.Errorf("collector/sender: retry batch id %q does not match sequence %d", retry.GetBatchId(), seq)
	}
	if c.draining || c.ctx.Err() != nil {
		c.mu.Unlock()
		return nil
	}
	c.s.deferBatchRetry(batch, retryDelay)
	if _, scheduled := c.pendingRetry[seq]; scheduled {
		// A resend is already pending for this sequence; coalesce so a flood of
		// RetryBatch messages cannot spawn thousands of goroutines.
		c.mu.Unlock()
		return nil
	}
	retryCtx, cancelRetry := context.WithCancel(c.ctx)
	scheduled := &scheduledRetry{cancel: cancelRetry, done: make(chan struct{})}
	c.pendingRetry[seq] = scheduled
	c.mu.Unlock()

	c.s.markRetried()
	c.retryWG.Add(1)
	go func(state *scheduledRetry) {
		defer c.retryWG.Done()
		defer close(state.done)
		defer state.cancel()
		defer func() {
			c.mu.Lock()
			if c.pendingRetry[seq] == state {
				delete(c.pendingRetry, seq)
			}
			c.mu.Unlock()
		}()
		for {
			if wait := c.s.batchRetryWait(batch, c.s.now()); wait > 0 {
				if !c.s.sleep(retryCtx, wait) {
					return
				}
			}
			if retryCtx.Err() != nil {
				return
			}
			sent, wait, throttleGeneration, throttleLimited, err := c.sendRetryIfCurrent(seq, batch, state, &opensplunk.CollectRequest{
				Payload: &opensplunk.CollectRequest_Batch{Batch: batch},
			})
			if sent {
				if err != nil {
					c.fail(err)
				}
				return
			}
			if err != nil || retryCtx.Err() != nil {
				return
			}
			if throttleLimited {
				if !c.waitOutThrottle(retryCtx, throttleGeneration) {
					return
				}
				continue
			}
			if wait <= 0 || !c.waitForThrottleUpdateOrDelay(retryCtx, wait, throttleGeneration) {
				return
			}
		}
	}(scheduled)
	return nil
}

// sendRetryIfCurrent serializes with terminal retry cancellation using the
// sendMu -> mu lock order. Acquiring sendMu before the final state check removes
// the old check-then-wait window: once cancellation returns, no not-yet-started
// retry can subsequently reach stream.Send. If this function wins first, the
// retry is linearized before terminal release and remains a safe at-least-once
// duplicate. mu is released before the potentially blocking send.
func (c *conn) sendRetryIfCurrent(
	seq uint64,
	batch *opensplunk.EventBatch,
	state *scheduledRetry,
	req *opensplunk.CollectRequest,
) (sent bool, wait time.Duration, throttleGeneration uint64, throttleLimited bool, err error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	current := c.inflight[seq]
	eligible := current == batch && c.pendingRetry[seq] == state && state != nil &&
		c.ctx.Err() == nil && !c.draining
	throttleGeneration = c.throttleGeneration
	if !eligible {
		c.mu.Unlock()
		return false, 0, throttleGeneration, false, nil
	}
	now := c.s.now()
	if c.throttleActiveLocked() {
		if c.batchExceedsThrottleLimitsLocked(batch) {
			c.mu.Unlock()
			return false, 0, throttleGeneration, true, nil
		}
		if wait := c.nextBatchSendAt.Sub(now); wait > 0 {
			c.mu.Unlock()
			return false, wait, throttleGeneration, false, nil
		}
	}
	if wait := c.s.batchRetryWait(batch, now); wait > 0 {
		c.mu.Unlock()
		return false, wait, throttleGeneration, false, nil
	}
	c.recordBatchSendLocked(now)
	c.mu.Unlock()
	return true, 0, throttleGeneration, false, c.sendLocked(req)
}

// handleThrottle applies pacing and in-flight limits until effective_until (or
// another Throttle). Server absolute timestamps are never compared directly to
// the collector clock: when both timestamps are valid, their server-relative
// duration is applied from local receipt time. All server-requested delays are
// bounded by the ingestion quota contract so a buggy peer cannot suspend
// delivery indefinitely.
func (c *conn) handleThrottle(resp *opensplunk.CollectResponse) error {
	throttle := resp.GetThrottle()
	if throttle == nil {
		return nil
	}
	minimumDelay := time.Duration(0)
	if raw := throttle.GetMinimumSendDelay(); raw != nil {
		if err := raw.CheckValid(); err != nil || raw.AsDuration() < 0 ||
			raw.AsDuration() > ingestquota.MaximumRetryAfter {
			return errors.New("collector/sender: throttle has invalid minimum_send_delay")
		}
		minimumDelay = raw.AsDuration()
	}
	if effectiveUntil := throttle.GetEffectiveUntil(); effectiveUntil != nil {
		sentAt := resp.GetSentAt()
		if effectiveUntil.CheckValid() != nil || sentAt == nil || sentAt.CheckValid() != nil ||
			effectiveUntil.AsTime().Sub(sentAt.AsTime()) > ingestquota.MaximumRetryAfter {
			return errors.New("collector/sender: throttle has invalid effective_until")
		}
	}
	if uint64(throttle.GetMaxInFlightBatches()) > uint64(^uint(0)>>1) {
		return errors.New("collector/sender: throttle max_in_flight_batches exceeds platform int capacity")
	}
	receivedAt := c.s.now()
	until := localThrottleUntil(receivedAt, resp.GetSentAt(), throttle.GetEffectiveUntil(), minimumDelay)

	// Serialize applying the response with every outbound Send. This establishes
	// whether a send started before or after the new throttle without holding mu
	// across gRPC flow control.
	c.sendMu.Lock()
	c.mu.Lock()
	c.throttled = true
	c.throttleGeneration++
	c.minSendDelay = minimumDelay
	c.throttleUntil = until
	c.nextBatchSendAt = receivedAt.Add(minimumDelay)
	if !c.lastBatchSendAt.IsZero() {
		fromLastSend := c.lastBatchSendAt.Add(minimumDelay)
		if fromLastSend.After(c.nextBatchSendAt) {
			c.nextBatchSendAt = fromLastSend
		}
	}
	c.throttleMaxInFlt = int(throttle.GetMaxInFlightBatches())
	c.throttleMaxEvents = throttle.GetMaxBatchEvents()
	c.throttleMaxBytes = throttle.GetMaxBatchBytes()
	c.cond.Broadcast()
	c.mu.Unlock()
	c.sendMu.Unlock()
	return nil
}

func localThrottleUntil(
	receivedAt time.Time,
	serverSentAt *timestamppb.Timestamp,
	effectiveUntil *timestamppb.Timestamp,
	minimumDelay time.Duration,
) time.Time {
	if effectiveUntil == nil {
		return time.Time{}
	}
	activeFor := minimumDelay
	if serverSentAt != nil && serverSentAt.CheckValid() == nil && effectiveUntil.CheckValid() == nil {
		if relative := effectiveUntil.AsTime().Sub(serverSentAt.AsTime()); relative > activeFor {
			activeFor = relative
		}
	}
	if activeFor < 0 {
		activeFor = 0
	}
	return receivedAt.Add(activeFor)
}

// handleNotice reacts to server notices. A shutting-down notice drains the
// current in-flight work and asks Run to reconnect after reconnect_after.
func (c *conn) handleNotice(notice *opensplunk.ServerNotice) error {
	if notice.GetType() != opensplunk.ServerNoticeType_SERVER_NOTICE_TYPE_SHUTTING_DOWN {
		c.s.logger.Info("server notice",
			zap.String("type", notice.GetType().String()),
			zap.String("code", notice.GetCode()))
		return nil
	}
	reconnectAfter := time.Duration(0)
	if raw := notice.GetReconnectAfter(); raw != nil {
		if err := raw.CheckValid(); err != nil || raw.AsDuration() < 0 {
			return errors.New("collector/sender: server shutdown notice has invalid reconnect_after")
		}
		reconnectAfter = raw.AsDuration()
		maximum := c.s.opts.Backoff.Max
		if maximum <= 0 {
			maximum = defaultBackoffMax
		}
		if reconnectAfter > maximum {
			reconnectAfter = maximum
		}
	}
	c.mu.Lock()
	c.draining = true
	c.serverShutdown = true
	c.serverReconnectDur = reconnectAfter
	if c.serverDrainTimer == nil {
		c.serverDrainTimer = time.AfterFunc(c.s.drainTimeout, c.streamCancel)
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	c.cancel()
	// Half-close so the server can flush any remaining acks and then EOF; the
	// receive loop drains those before returning.
	c.closeSendSerialized()
	return nil
}

// --- in-flight bookkeeping --------------------------------------------------

func (c *conn) lookupInflight(seq uint64) *opensplunk.EventBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight[seq]
}

// cancelRetry serializes terminal cancellation with the retry send path. The
// sendMu -> mu order is shared with sendRetryIfCurrent; cancellation never
// waits for gRPC while holding mu.
func (c *conn) cancelRetry(seq uint64) {
	c.sendMu.Lock()
	c.mu.Lock()
	c.cancelRetryLocked(seq)
	c.mu.Unlock()
	c.sendMu.Unlock()
}

func (c *conn) cancelRetriesThrough(seq uint64) {
	c.sendMu.Lock()
	c.mu.Lock()
	for key := range c.pendingRetry {
		if key <= seq {
			c.cancelRetryLocked(key)
		}
	}
	c.mu.Unlock()
	c.sendMu.Unlock()
}

// cancelRetryLocked removes and wakes one scheduled retry. The caller holds
// both sendMu and mu, in that order.
func (c *conn) cancelRetryLocked(seq uint64) {
	if retry := c.pendingRetry[seq]; retry != nil {
		delete(c.pendingRetry, seq)
		retry.cancel()
	}
}

func (c *conn) releaseInflight(seq uint64) {
	c.sendMu.Lock()
	c.mu.Lock()
	if _, ok := c.inflight[seq]; ok {
		c.cancelRetryLocked(seq)
		delete(c.inflight, seq)
		c.inflightN--
		c.cond.Broadcast()
	}
	c.mu.Unlock()
	c.sendMu.Unlock()
}

func (c *conn) releaseInflightThrough(seq uint64) {
	c.sendMu.Lock()
	c.mu.Lock()
	for key := range c.inflight {
		if key <= seq {
			c.cancelRetryLocked(key)
			delete(c.inflight, key)
			c.inflightN--
		}
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	c.sendMu.Unlock()
}

func (c *conn) deadLetterWholeBatch(batch *opensplunk.EventBatch, code, reason string) error {
	records := make([]DeadLetterRecord, 0, len(batch.GetEvents()))
	now := c.s.now()
	for _, event := range batch.GetEvents() {
		records = append(records, DeadLetterRecord{
			Event:         event,
			BatchID:       batch.GetBatchId(),
			BatchSequence: batch.GetBatchSequence(),
			Code:          code,
			Reason:        reason,
			RejectedAt:    now,
		})
	}
	return c.s.writeDeadLetter(records)
}

func (c *conn) fail(err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		c.s.setLastError(err)
		c.mu.Lock()
		if c.failure == nil {
			c.failure = err
		}
		c.mu.Unlock()
	}
	// Break the stream so a blocked receive unblocks, then stop the workers.
	c.streamCancel()
	c.cancel()
}

func (c *conn) connectionFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

func (c *conn) stopServerDrainTimer() {
	c.mu.Lock()
	timer := c.serverDrainTimer
	c.serverDrainTimer = nil
	c.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
}

// gracefulPreReadyShutdown covers cancellation after Hello was sent but while
// Ready is still racing with the caller. The stream context is intentionally
// independent of parent, so this best-effort Goodbye remains transmissible.
func (c *conn) gracefulPreReadyShutdown(readyDone <-chan error) {
	c.beginDraining()
	deadline := time.Now().Add(c.s.drainTimeout)
	sendDone := make(chan struct{})
	go func() {
		c.sendGoodbyeAndClose()
		close(sendDone)
	}()
	if waitForSignalUntil(sendDone, deadline) {
		remaining := time.Until(deadline)
		if remaining > 0 {
			timer := time.NewTimer(remaining)
			select {
			case <-readyDone:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	} else {
		c.streamCancel()
		<-sendDone
	}
	// Closing the stream is what guarantees the outstanding Recv exits. Join it
	// before returning so no pre-Ready worker survives into another connection.
	c.streamCancel()
	<-readyDone
}

// gracefulShutdown sends Goodbye(SHUTDOWN) best-effort, half-closes the send
// direction, and briefly drains inbound acks before the caller returns.
func (c *conn) gracefulShutdown(recvDone <-chan error) {
	c.beginDraining()
	deadline := time.Now().Add(c.s.drainTimeout)
	sendDone := make(chan struct{})
	go func() {
		c.sendGoodbyeAndClose()
		close(sendDone)
	}()
	if waitForSignalUntil(sendDone, deadline) {
		remaining := time.Until(deadline)
		if remaining > 0 {
			timer := time.NewTimer(remaining)
			select {
			case <-recvDone:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}
	// The global network-drain deadline covers both a flow-controlled Goodbye
	// and inbound ack draining. Cancel the independent stream at the deadline,
	// then join both network workers before resource owners can close beneath them.
	c.streamCancel()
	<-sendDone
	<-recvDone
}

func (c *conn) beginDraining() {
	c.mu.Lock()
	c.draining = true
	c.cond.Broadcast()
	c.mu.Unlock()
	c.cancel()
}

func (c *conn) sendGoodbyeAndClose() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_ = c.sendLocked(&opensplunk.CollectRequest{
		Payload: &opensplunk.CollectRequest_Goodbye{Goodbye: &opensplunk.CollectorGoodbye{
			Reason: opensplunk.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	})
	_ = c.stream.CloseSend()
}

func (c *conn) closeSendSerialized() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_ = c.stream.CloseSend()
}

func waitForSignalUntil(done <-chan struct{}, deadline time.Time) bool {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
