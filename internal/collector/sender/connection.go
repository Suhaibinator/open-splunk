package sender

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
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
	// and is kept separate so Goodbye can still be sent after ctx is cancelled.
	ctx          context.Context
	cancel       context.CancelFunc
	streamCancel context.CancelFunc

	// sendMu serializes all stream.Send calls (batch, heartbeat, retry, goodbye)
	// and guards the outgoing stream sequence. gRPC permits one concurrent Send.
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
	lastBatchSendAt    time.Time
	draining           bool
	serverShutdown     bool
	serverReconnectDur time.Duration
}

func (s *Sender) newConn(ctx context.Context, cancel, streamCancel context.CancelFunc, stream opensplunkv1.CollectorIngestService_CollectClient) *conn {
	c := &conn{
		s:            s,
		stream:       stream,
		ctx:          ctx,
		cancel:       cancel,
		streamCancel: streamCancel,
		inflight:     make(map[uint64]*opensplunkv1.EventBatch),
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
	// after the parent context is cancelled during graceful shutdown.
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	token, err := s.opts.Token()
	if err != nil {
		return false, 0, fmt.Errorf("collector/sender: read token: %w", err)
	}
	stream, err := s.client.Collect(withBearer(streamCtx, token), s.collectCallOptions()...)
	token = "" // never retain the secret
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
		if err := c.s.queue.AckThrough(resume); err != nil {
			return fmt.Errorf("collector/sender: invalid resume sequence %d: %w", resume, err)
		}
		c.s.markAcked(resume, 0)
	}
	return nil
}

// send stamps the next connection-local stream sequence and sent_at, then
// transmits the request. All senders must go through send under sendMu.
func (c *conn) send(req *opensplunkv1.CollectRequest) error {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.streamSeq++
	req.StreamSequence = c.streamSeq
	req.SentAt = timestamppb.New(c.s.now().UTC())
	return c.stream.Send(req)
}

// --- pump -------------------------------------------------------------------

func (c *conn) pumpLoop() {
	for {
		c.mu.Lock()
		for {
			if c.ctx.Err() != nil || c.draining {
				c.mu.Unlock()
				return
			}
			if c.inflightN < c.effectiveMaxInFlightLocked() {
				break
			}
			c.cond.Wait()
		}
		c.mu.Unlock()

		if d := c.throttleWaitDuration(); d > 0 {
			if !c.s.sleep(c.ctx, d) {
				return
			}
		}

		batch, err := c.s.queue.NextBatch(c.ctx)
		if err != nil {
			return // context cancelled
		}

		// Send-time guards. A pre-sealed batch that exceeds the negotiated (or
		// throttled) server limits can never be accepted; it is a permanent local
		// dead-letter case. Dead-letter it and ack it off the queue so delivery
		// makes progress rather than looping forever.
		if code, ok := c.batchExceedsLimits(batch); ok {
			if err := c.deadLetterWholeBatch(batch, code, "batch exceeds negotiated server limits"); err != nil {
				c.fail(err)
				return
			}
			if err := c.s.queue.Ack(batch.GetBatchSequence()); err != nil {
				c.fail(err)
				return
			}
			c.s.markDropped(uint64(len(batch.GetEvents())))
			continue
		}

		c.mu.Lock()
		c.inflightN++
		c.inflight[batch.GetBatchSequence()] = batch
		c.mu.Unlock()

		if err := c.send(&opensplunkv1.CollectRequest{
			Payload: &opensplunkv1.CollectRequest_Batch{Batch: batch},
		}); err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		c.lastBatchSendAt = c.s.now()
		c.mu.Unlock()
		c.s.markSent(batch)
	}
}

func (c *conn) effectiveMaxInFlightLocked() int {
	if c.throttleActiveLocked() && c.throttleMaxInFlt > 0 {
		return c.throttleMaxInFlt
	}
	return c.maxInFlight
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
	earliest := c.lastBatchSendAt.Add(c.minSendDelay)
	d := earliest.Sub(c.s.now())
	if d < 0 {
		return 0
	}
	return d
}

func (c *conn) batchExceedsLimits(batch *opensplunkv1.EventBatch) (string, bool) {
	c.mu.Lock()
	maxEvents := c.maxBatchEvents
	maxBytes := c.maxBatchBytes
	if c.throttleActiveLocked() {
		if c.throttleMaxEvents > 0 {
			maxEvents = c.throttleMaxEvents
		}
		if c.throttleMaxBytes > 0 {
			maxBytes = c.throttleMaxBytes
		}
	}
	c.mu.Unlock()

	if maxEvents > 0 && uint32(len(batch.GetEvents())) > maxEvents {
		return opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_TOO_MANY_EVENTS.String(), true
	}
	if maxBytes > 0 && batch.GetUncompressedSizeBytes() > maxBytes {
		return opensplunkv1.BatchRejectionCode_BATCH_REJECTION_CODE_BATCH_TOO_LARGE.String(), true
	}
	return "", false
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
			c.handleThrottle(resp.GetThrottle())
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
	batch := c.lookupInflight(seq)
	if batch == nil {
		return fmt.Errorf("collector/sender: ack for unknown batch sequence %d", seq)
	}
	if ack.GetBatchId() != batch.GetBatchId() {
		return fmt.Errorf("collector/sender: ack batch id %q does not match sequence %d", ack.GetBatchId(), seq)
	}
	if ack.AcknowledgedThroughBatchSequence != nil && ack.GetAcknowledgedThroughBatchSequence() < seq {
		return fmt.Errorf("collector/sender: acknowledged-through %d precedes batch %d", ack.GetAcknowledgedThroughBatchSequence(), seq)
	}

	accounted := uint64(ack.GetAcceptedEventCount()) + uint64(ack.GetDuplicateEventCount()) + uint64(len(ack.GetRejectedEvents()))
	if accounted != uint64(len(batch.GetEvents())) {
		return fmt.Errorf("collector/sender: ack accounts for %d of %d events in batch %d", accounted, len(batch.GetEvents()), seq)
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
		c.s.markRejected(uint64(len(records)))
	}
	if err := c.s.queue.Ack(seq); err != nil {
		return fmt.Errorf("collector/sender: queue ack %d: %w", seq, err)
	}
	c.s.markAcked(c.s.queue.Stats().LastAckedBatchSequence, uint64(ack.GetAcceptedEventCount())+uint64(ack.GetDuplicateEventCount()))
	c.releaseInflight(seq)
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
	c.s.markDropped(uint64(len(records)))
	if err := c.s.queue.Ack(seq); err != nil {
		return fmt.Errorf("collector/sender: queue ack %d: %w", seq, err)
	}
	c.s.markAcked(c.s.queue.Stats().LastAckedBatchSequence, 0)
	c.releaseInflight(seq)
	return nil
}

// handleRetry is non-terminal: the exact same durable batch is retained and
// resent after retry_after. The in-flight slot is kept the whole time.
func (c *conn) handleRetry(retry *opensplunkv1.RetryBatch) error {
	seq := retry.GetBatchSequence()
	batch := c.lookupInflight(seq)
	if batch == nil {
		return fmt.Errorf("collector/sender: retry for unknown batch sequence %d", seq)
	}
	if retry.GetBatchId() != batch.GetBatchId() {
		return fmt.Errorf("collector/sender: retry batch id %q does not match sequence %d", retry.GetBatchId(), seq)
	}
	c.s.markRetried()
	delay := retry.GetRetryAfter().AsDuration()
	go func() {
		if delay > 0 {
			if !c.s.sleep(c.ctx, delay) {
				return
			}
		}
		if c.ctx.Err() != nil {
			return
		}
		if err := c.send(&opensplunkv1.CollectRequest{
			Payload: &opensplunkv1.CollectRequest_Batch{Batch: batch},
		}); err != nil {
			c.fail(err)
		}
	}()
	return nil
}

// handleThrottle applies the pacing and in-flight limits until effective_until
// (or another Throttle). Zero limit fields leave the negotiated limit in place.
func (c *conn) handleThrottle(throttle *opensplunkv1.Throttle) {
	c.mu.Lock()
	c.throttled = true
	c.minSendDelay = throttle.GetMinimumSendDelay().AsDuration()
	if throttle.GetEffectiveUntil() != nil {
		c.throttleUntil = throttle.GetEffectiveUntil().AsTime()
	} else {
		c.throttleUntil = time.Time{}
	}
	c.throttleMaxInFlt = int(throttle.GetMaxInFlightBatches())
	c.throttleMaxEvents = throttle.GetMaxBatchEvents()
	c.throttleMaxBytes = throttle.GetMaxBatchBytes()
	c.cond.Broadcast()
	c.mu.Unlock()
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

func (c *conn) releaseInflight(seq uint64) {
	c.mu.Lock()
	if _, ok := c.inflight[seq]; ok {
		delete(c.inflight, seq)
		c.inflightN--
		c.cond.Broadcast()
	}
	c.mu.Unlock()
}

func (c *conn) releaseInflightThrough(seq uint64) {
	c.mu.Lock()
	for key := range c.inflight {
		if key <= seq {
			delete(c.inflight, key)
			c.inflightN--
		}
	}
	c.cond.Broadcast()
	c.mu.Unlock()
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
