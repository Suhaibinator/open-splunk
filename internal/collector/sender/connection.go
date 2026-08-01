package sender

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	stream opensplunkv1.CollectorIngestService_CollectClient
	ready  *opensplunkv1.CollectorReady
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
	inflight  map[uint64]*opensplunkv1.EventBatch
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
}

type scheduledRetry struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Sender) newConn(ctx context.Context, cancel, streamCancel context.CancelFunc, stream opensplunkv1.CollectorIngestService_CollectClient) *conn {
	c := &conn{
		s:            s,
		stream:       stream,
		ctx:          ctx,
		cancel:       cancel,
		streamCancel: streamCancel,
		inflight:     make(map[uint64]*opensplunkv1.EventBatch),
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
		return false, 0, err
	}

	c := s.newConn(ctx, cancel, streamCancel, stream)
	if err := c.sendHello(); err != nil {
		return false, 0, err
	}
	readyDone := make(chan error, 1)
	go func() { readyDone <- c.awaitReady() }()
	var readyTimer <-chan time.Time
	var timer *time.Timer
	if s.opts.DialTimeout > 0 {
		timer = time.NewTimer(s.opts.DialTimeout)
		readyTimer = timer.C
		defer timer.Stop()
	}
	select {
	case err := <-readyDone:
		if err != nil {
			return false, 0, err
		}
	case <-parent.Done():
		c.gracefulPreReadyShutdown(readyDone)
		streamCancel()
		return false, 0, parent.Err()
	case <-readyTimer:
		streamCancel()
		return false, 0, fmt.Errorf("collector/sender: ready negotiation timed out after %s", s.opts.DialTimeout)
	}

	// Batches handed out on a previous connection but never terminally
	// acknowledged are still unacked in the queue, behind its delivery cursor.
	// Rewind (after the resume trim in awaitReady) so this stream resends them;
	// the server deduplicates identical retries by batch ID.
	s.queue.Rewind()

	s.logger.Info("collector stream ready",
		"address", s.opts.Address,
		"stream_id", c.ready.GetStreamId(),
		"max_in_flight", c.ready.GetMaxInFlightBatches())
	s.setConnected(true)
	defer s.setConnected(false)

	// Wake blocked pump goroutine when the connection is torn down.
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	}()

	recvDone := make(chan error, 1)
	go func() { recvDone <- c.receiveLoop() }()
	pumpDone := make(chan struct{})
	go func() { defer close(pumpDone); c.pumpLoop() }()
	hbDone := make(chan struct{})
	go func() { defer close(hbDone); c.heartbeatLoop() }()

	select {
	case <-parent.Done():
		c.gracefulShutdown(recvDone)
		cancel()
		<-pumpDone
		<-hbDone
		return true, 0, parent.Err()
	case recvErr := <-recvDone:
		cancel()
		<-pumpDone
		<-hbDone
		c.mu.Lock()
		shutdown := c.serverShutdown
		reconnect := c.serverReconnectDur
		c.mu.Unlock()
		if shutdown {
			return true, reconnect, nil
		}
		return true, 0, recvErr
	}
}

func (c *conn) sendHello() error {
	stats := c.s.queue.Stats()
	hello := &opensplunkv1.CollectorHello{
		CollectorId:      c.s.opts.CollectorID,
		InstanceId:       c.s.opts.InstanceID,
		ProtocolMajor:    c.s.opts.ProtocolMajor,
		ProtocolMinor:    c.s.opts.ProtocolMinor,
		CollectorVersion: c.s.opts.Hello.CollectorVersion,
		Hostname:         c.s.opts.Hello.Hostname,
		OperatingSystem:  c.s.opts.Hello.OperatingSystem,
		Architecture:     c.s.opts.Hello.Architecture,
		StartedAt:        timestamppb.New(c.s.opts.Hello.StartedAt.UTC()),
		Capabilities:     c.s.opts.Hello.Capabilities,
		Inputs:           c.s.opts.Hello.Inputs,
	}
	if stats.LastAckedBatchSequence > 0 {
		v := stats.LastAckedBatchSequence
		hello.LastAcknowledgedBatchSequence = &v
	}
	return c.send(&opensplunkv1.CollectRequest{
		Payload: &opensplunkv1.CollectRequest_Hello{Hello: hello},
	})
}

func (c *conn) awaitReady() error {
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
	if ready.GetProtocolMajor() != c.s.opts.ProtocolMajor {
		return &fatalError{err: fmt.Errorf(
			"collector/sender: server protocol major %d is incompatible with %d",
			ready.GetProtocolMajor(), c.s.opts.ProtocolMajor)}
	}
	if ready.GetAcknowledgmentDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
		return &fatalError{err: fmt.Errorf(
			"collector/sender: server acknowledgment durability %s cannot safely advance source checkpoints",
			ready.GetAcknowledgmentDurability().String(),
		)}
	}
	c.ready = ready

	c.mu.Lock()
	c.maxInFlight = int(ready.GetMaxInFlightBatches())
	if c.maxInFlight < 1 {
		c.maxInFlight = 1
	}
	c.maxBatchEvents = ready.GetMaxBatchEvents()
	c.maxBatchBytes = ready.GetMaxBatchBytes()
	c.mu.Unlock()

	// Honor resume_after_batch_sequence: everything through it is durably held by
	// the server, so ack it off the queue. NextBatch then yields the first higher
	// unacked batch.
	if ready.ResumeAfterBatchSequence != nil {
		resume := ready.GetResumeAfterBatchSequence()
		through, err := c.s.commitTerminal(resume, true)
		switch {
		case err == nil:
			c.s.markAcked(through, 0)
		case errors.Is(err, wal.ErrInvalidAck):
			// The server remembers a durability point this queue does not know —
			// typically a fresh or quarantined state directory behind an older
			// collector identity. Failing the connection here would crash-loop
			// forever and deliver nothing. Proceed without acking: everything
			// local is (re)sent and the server deduplicates or rejects with
			// explicit, operator-visible responses.
			c.s.logger.Warn("collector stream resume point unknown to local queue; continuing without ack",
				"resume_sequence", resume, "error", err.Error())
		default:
			return fmt.Errorf("collector/sender: resume sequence %d: %w", resume, err)
		}
	}
	return nil
}

// send stamps the next connection-local stream sequence and sent_at, then
// transmits the request. All senders must go through send under sendMu.
func (c *conn) send(req *opensplunkv1.CollectRequest) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	return c.sendLocked(req)
}

// sendLocked stamps and transmits req while the caller holds sendMu.
func (c *conn) sendLocked(req *opensplunkv1.CollectRequest) error {
	c.streamSeq++
	req.StreamSequence = c.streamSeq
	req.SentAt = timestamppb.New(c.s.now().UTC())
	return c.stream.Send(req)
}

// --- pump -------------------------------------------------------------------

func (c *conn) pumpLoop() {
	// pending retains a batch already dequeued by NextBatch but held back because a
	// temporary throttle forbids it right now; it must not be re-appended.
	var pending *opensplunkv1.EventBatch
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
				return // context canceled
			}
		}

		// A batch that exceeds the NEGOTIATED Ready limits can never be accepted on
		// this stream, so it is a permanent local dead-letter case: dead-letter it
		// and ack it off the queue so delivery makes progress instead of looping.
		if code, ok := c.batchExceedsReadyLimits(batch); ok {
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
	batch *opensplunkv1.EventBatch,
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

	err := c.sendLocked(&opensplunkv1.CollectRequest{
		Payload: &opensplunkv1.CollectRequest_Batch{Batch: batch},
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

func (c *conn) throttleWaitDuration() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.throttleActiveLocked() || c.minSendDelay <= 0 {
		return 0
	}
	d := c.nextBatchSendAt.Sub(c.s.now())
	if d < 0 {
		return 0
	}
	return d
}

// batchExceedsReadyLimits reports whether batch exceeds the NEGOTIATED Ready
// limits (fixed for the life of the stream). Such a batch can never be accepted
// and is permanently dead-lettered; the returned string is the rejection code.
func (c *conn) batchExceedsReadyLimits(batch *opensplunkv1.EventBatch) (string, bool) {
	c.mu.Lock()
	maxEvents := c.maxBatchEvents
	maxBytes := c.maxBatchBytes
	c.mu.Unlock()

	// #nosec G115 -- len is non-negative and every supported Go int value is
	// exactly representable as uint64.
	if maxEvents > 0 && uint64(len(batch.GetEvents())) > uint64(maxEvents) {
		return opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS.String(), true
	}
	if maxBytes > 0 && batch.GetUncompressedSizeBytes() > maxBytes {
		return opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE.String(), true
	}
	return "", false
}

func (c *conn) batchExceedsThrottleLimitsLocked(batch *opensplunkv1.EventBatch) bool {
	// #nosec G115 -- len is non-negative and every supported Go int value is
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
			if err := c.send(&opensplunkv1.CollectRequest{
				Payload: &opensplunkv1.CollectRequest_Heartbeat{Heartbeat: hb},
			}); err != nil {
				c.fail(err)
				return
			}
		}
	}
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
			c.handleThrottle(resp)
		case resp.GetNotice() != nil:
			c.handleNotice(resp.GetNotice())
		default:
			return errors.New("collector/sender: response has no payload")
		}
	}
}

func (c *conn) validateResponse(resp *opensplunkv1.CollectResponse) error {
	if resp == nil {
		return errors.New("collector/sender: nil response")
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
func (c *conn) handleAck(ack *opensplunkv1.BatchAck) error {
	seq := ack.GetBatchSequence()
	if ack.GetDurability() != opensplunkv1.AckDurability_ACK_DURABILITY_CLICKHOUSE_COMMITTED {
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
		c.mu.Unlock()
		if target > highestSent {
			return fmt.Errorf(
				"collector/sender: acknowledged-through %d exceeds highest batch sequence sent on this connection %d",
				target, highestSent,
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
			idx := rej.GetEventIndex()
			if int(idx) >= len(batch.GetEvents()) {
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
func (c *conn) handleReject(reject *opensplunkv1.BatchReject) error {
	seq := reject.GetBatchSequence()
	batch := c.lookupInflight(seq)
	if batch == nil {
		return fmt.Errorf("collector/sender: reject for unknown batch sequence %d", seq)
	}
	if reject.GetBatchId() != batch.GetBatchId() {
		return fmt.Errorf("collector/sender: reject batch id %q does not match sequence %d", reject.GetBatchId(), seq)
	}
	c.cancelRetry(seq)
	records := make([]DeadLetterRecord, 0, len(batch.GetEvents()))
	now := c.s.now()
	for _, event := range batch.GetEvents() {
		records = append(records, DeadLetterRecord{
			Event:         event,
			BatchID:       reject.GetBatchId(),
			BatchSequence: seq,
			Code:          reject.GetCode().String(),
			Reason:        reject.GetMessage(),
			RejectedAt:    now,
		})
	}
	if err := c.s.writeDeadLetter(records); err != nil {
		return err
	}
	through, err := c.s.commitTerminal(seq, false)
	if err != nil {
		return fmt.Errorf("collector/sender: queue ack %d: %w", seq, err)
	}
	c.s.markDropped(uint64(len(records)))
	c.s.markAcked(through, 0)
	c.releaseInflight(seq)
	return nil
}

// handleRetry is non-terminal: the exact same durable batch is retained and
// resent after retry_after. The in-flight slot is kept the whole time.
func (c *conn) handleRetry(retry *opensplunkv1.RetryBatch) error {
	seq := retry.GetBatchSequence()
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
	c.s.deferBatchRetry(batch, retry.GetRetryAfter().AsDuration())
	if _, scheduled := c.pendingRetry[seq]; scheduled {
		// A resend is already pending for this sequence; coalesce so a flood of
		// RetryBatch messages cannot spawn thousands of goroutines.
		c.mu.Unlock()
		return nil
	}
	// #nosec G118 -- cancelRetry is retained in pendingRetry and invoked on
	// acknowledgement, connection shutdown, or superseding retry state.
	retryCtx, cancelRetry := context.WithCancel(c.ctx)
	scheduled := &scheduledRetry{cancel: cancelRetry, done: make(chan struct{})}
	c.pendingRetry[seq] = scheduled
	c.mu.Unlock()

	c.s.markRetried()
	go func(state *scheduledRetry) {
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
			sent, wait, throttleGeneration, throttleLimited, err := c.sendRetryIfCurrent(seq, batch, state, &opensplunkv1.CollectRequest{
				Payload: &opensplunkv1.CollectRequest_Batch{Batch: batch},
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
	batch *opensplunkv1.EventBatch,
	state *scheduledRetry,
	req *opensplunkv1.CollectRequest,
) (sent bool, wait time.Duration, throttleGeneration uint64, throttleLimited bool, err error) {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()

	c.mu.Lock()
	current := c.inflight[seq]
	eligible := current == batch && c.pendingRetry[seq] == state && state != nil && c.ctx.Err() == nil
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
// duration is applied from local receipt time. A malformed/missing sent_at
// falls back to at least minimum_send_delay.
func (c *conn) handleThrottle(resp *opensplunkv1.CollectResponse) {
	throttle := resp.GetThrottle()
	if throttle == nil {
		return
	}
	receivedAt := c.s.now()
	minimumDelay := throttle.GetMinimumSendDelay().AsDuration()
	if minimumDelay < 0 {
		minimumDelay = 0
	}
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
func (c *conn) handleNotice(notice *opensplunkv1.ServerNotice) {
	if notice.GetType() != opensplunkv1.ServerNoticeType_SERVER_NOTICE_TYPE_SHUTTING_DOWN {
		c.s.logger.Info("server notice", "type", notice.GetType().String(), "code", notice.GetCode())
		return
	}
	c.mu.Lock()
	c.draining = true
	c.serverShutdown = true
	if notice.ReconnectAfter != nil {
		c.serverReconnectDur = notice.GetReconnectAfter().AsDuration()
	}
	c.cond.Broadcast()
	c.mu.Unlock()
	// Half-close so the server can flush any remaining acks and then EOF; the
	// receive loop drains those before returning.
	_ = c.stream.CloseSend()
}

// --- in-flight bookkeeping --------------------------------------------------

func (c *conn) lookupInflight(seq uint64) *opensplunkv1.EventBatch {
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

func (c *conn) deadLetterWholeBatch(batch *opensplunkv1.EventBatch, code, reason string) error {
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
	}
	// Break the stream so a blocked receive unblocks, then stop the workers.
	c.streamCancel()
	c.cancel()
}

// gracefulPreReadyShutdown covers cancellation after Hello was sent but while
// Ready is still racing with the caller. The stream context is intentionally
// independent of parent, so this best-effort Goodbye remains transmissible.
func (c *conn) gracefulPreReadyShutdown(readyDone <-chan error) {
	if err := c.send(&opensplunkv1.CollectRequest{
		Payload: &opensplunkv1.CollectRequest_Goodbye{Goodbye: &opensplunkv1.CollectorGoodbye{
			Reason: opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	}); err != nil {
		return
	}
	_ = c.stream.CloseSend()

	// Do not immediately cancel the independent stream context: Send returning
	// only queues bytes locally. Wait for the outstanding Ready receive, then an
	// EOF, so the server has a chance to consume Goodbye. Both waits are bounded.
	timer := time.NewTimer(c.s.drainTimeout)
	defer timer.Stop()
	select {
	case err := <-readyDone:
		if err != nil {
			return
		}
	case <-timer.C:
		return
	}
	recvDone := make(chan struct{}, 1)
	go func() {
		_, _ = c.stream.Recv()
		recvDone <- struct{}{}
	}()
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(c.s.drainTimeout)
	select {
	case <-recvDone:
	case <-timer.C:
	}
}

// gracefulShutdown sends Goodbye(SHUTDOWN) best-effort, half-closes the send
// direction, and briefly drains inbound acks before the caller returns.
func (c *conn) gracefulShutdown(recvDone <-chan error) {
	c.mu.Lock()
	c.draining = true
	c.cond.Broadcast()
	c.mu.Unlock()

	_ = c.send(&opensplunkv1.CollectRequest{
		Payload: &opensplunkv1.CollectRequest_Goodbye{Goodbye: &opensplunkv1.CollectorGoodbye{
			Reason: opensplunkv1.CollectorGoodbyeReason_COLLECTOR_GOODBYE_REASON_SHUTDOWN,
		}},
	})
	_ = c.stream.CloseSend()

	timer := time.NewTimer(c.s.drainTimeout)
	defer timer.Stop()
	select {
	case <-recvDone:
	case <-timer.C:
	}
}
