package searchws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type connection struct {
	service *Service
	socket  *websocket.Conn
	ctx     context.Context
	cancel  context.CancelFunc

	queueMu         sync.Mutex
	queue           [][]byte
	queueHead       int
	queuedBytes     uint64
	inFlightFrames  int
	inFlightBytes   uint64
	wake            chan struct{}
	hardClosed      bool
	gracefulClosing bool
	closeCode       int
	closeReason     string
	hardCloseOnce   sync.Once
	writerDone      chan struct{}

	// Only the reader goroutine mutates this map. Target publishers synchronize
	// delivery through each subscription and never inspect the map.
	subscriptions map[string]*subscription
}

type requestedSubscription struct {
	id               string
	key              targetKey
	afterSequence    uint64
	target           *targetState
	subscription     *subscription
	initialFrames    [][]byte
	replayFollows    bool
	earliest, latest uint64
}

type commandFailure struct {
	code       opensplunkv1.SearchWebSocketProtocolErrorCode
	message    string
	violations []*opensplunkv1.FieldViolation
}

type queueResult uint8

const (
	queueAccepted queueResult = iota
	queueIntrinsicLimit
	queuePressure
	queueClosed
)

func (failure *commandFailure) Error() string { return failure.message }

// ServeHTTP admits and owns an upgraded connection until both its reader and
// writer have exited. Admission is counted before Upgrade so Close cannot miss
// a handler racing through HTTP hijacking.
func (service *Service) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if service == nil {
		http.Error(response, "search websocket is unavailable", http.StatusServiceUnavailable)
		return
	}
	if request.Method != http.MethodGet {
		response.Header().Set("Allow", http.MethodGet)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !service.reserveConnection() {
		service.unavailable(response)
		return
	}
	released := false
	defer func() {
		if !released {
			service.releaseConnection(nil)
		}
	}()

	upgrader := websocket.Upgrader{
		CheckOrigin:       service.config.checkOrigin,
		EnableCompression: false,
	}
	socket, err := upgrader.Upgrade(response, request, nil)
	if err != nil {
		return
	}
	// net/http's read/write deadlines apply to the pre-hijack request. Clear
	// both before installing the WebSocket's own liveness deadlines.
	if err := socket.UnderlyingConn().SetDeadline(time.Time{}); err != nil {
		_ = socket.UnderlyingConn().Close()
		return
	}
	connection := newConnection(service, socket)
	if !service.registerConnection(connection) {
		connection.hardClose()
		return
	}
	connection.run()
	service.releaseConnection(connection)
	released = true
}

func newConnection(service *Service, socket *websocket.Conn) *connection {
	ctx, cancel := context.WithCancel(service.ctx)
	return &connection{
		service: service, socket: socket, ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), writerDone: make(chan struct{}),
		subscriptions: make(map[string]*subscription),
	}
}

func (connection *connection) run() {
	connection.socket.SetReadLimit(int64(connection.service.config.maximumFrameBytes) + 1)
	_ = connection.socket.SetReadDeadline(time.Now().Add(connection.service.config.pongTimeout))
	connection.socket.SetPongHandler(func(string) error {
		return connection.socket.SetReadDeadline(time.Now().Add(connection.service.config.pongTimeout))
	})
	go connection.writeLoop()
	graceful := connection.readLoop()
	if !graceful {
		connection.hardClose()
	}
	<-connection.writerDone
	connection.removeAllSubscriptions()
	connection.hardClose()
}

func (connection *connection) readLoop() bool {
	for {
		messageType, data, err := connection.socket.ReadMessage()
		if err != nil {
			var closeError *websocket.CloseError
			if errors.Is(err, websocket.ErrReadLimit) ||
				(errors.As(err, &closeError) && closeError.Code == websocket.CloseMessageTooBig) {
				return connection.fatalProtocolError("", opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_FRAME_TOO_LARGE, "binary command frame exceeds the configured limit")
			}
			return false
		}
		if messageType != websocket.BinaryMessage {
			return connection.fatalProtocolError("", opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, "application messages must be binary protobuf frames")
		}
		if uint64(len(data)) > connection.service.config.maximumFrameBytes {
			return connection.fatalProtocolError("", opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_FRAME_TOO_LARGE, "binary command frame exceeds the configured limit")
		}
		var command opensplunkv1.SearchWebSocketCommand
		if err := proto.Unmarshal(data, &command); err != nil {
			if !connection.sendProtocolError("", opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, "command is not valid protobuf", nil, false) {
				return false
			}
			continue
		}
		requestID := command.GetRequestId()
		if !validProtocolString(requestID, maximumRequestIDBytes, true) {
			violation := fieldViolation("request_id", "INVALID", "request_id must be non-empty, valid UTF-8, and within the byte limit")
			if !connection.sendProtocolError("", opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, "invalid request_id", []*opensplunkv1.FieldViolation{violation}, false) {
				return false
			}
			continue
		}
		if failure := connection.handleCommand(requestID, &command); failure != nil {
			if !connection.sendProtocolError(requestID, failure.code, failure.message, failure.violations, false) {
				return false
			}
		}
	}
}

func (connection *connection) handleCommand(requestID string, command *opensplunkv1.SearchWebSocketCommand) *commandFailure {
	switch payload := command.GetPayload().(type) {
	case *opensplunkv1.SearchWebSocketCommand_Subscribe:
		if payload.Subscribe == nil {
			return invalidCommand("subscribe payload is required", fieldViolation("subscribe", "REQUIRED", "subscribe payload is required"))
		}
		return connection.subscribe(requestID, payload.Subscribe)
	case *opensplunkv1.SearchWebSocketCommand_Unsubscribe:
		if payload.Unsubscribe == nil {
			return invalidCommand("unsubscribe payload is required", fieldViolation("unsubscribe", "REQUIRED", "unsubscribe payload is required"))
		}
		return connection.unsubscribe(requestID, payload.Unsubscribe)
	case *opensplunkv1.SearchWebSocketCommand_Ping:
		if payload.Ping == nil {
			return invalidCommand("ping payload is required", fieldViolation("ping", "REQUIRED", "ping payload is required"))
		}
		if !validProtocolString(payload.Ping.GetNonce(), maximumPingNonceBytes, false) {
			return invalidCommand("invalid ping nonce", fieldViolation("ping.nonce", "INVALID", "nonce must be valid UTF-8 and within the byte limit"))
		}
		if !connection.enqueueEvent(pongEvent(payload.Ping.GetNonce(), connection.service.config.now())) {
			connection.hardClose()
		}
		return nil
	default:
		return invalidCommand("command payload is required", fieldViolation("payload", "REQUIRED", "one command payload is required"))
	}
}

func (connection *connection) subscribe(requestID string, command *opensplunkv1.SubscribeSearchJobsCommand) *commandFailure {
	pinned := make(map[*targetState]struct{})
	defer func() {
		for target := range pinned {
			connection.service.releaseResolvedTarget(target)
		}
	}()
	inputs := command.GetSubscriptions()
	if len(inputs) == 0 {
		return invalidCommand("at least one subscription is required", fieldViolation("subscribe.subscriptions", "REQUIRED", "at least one subscription is required"))
	}
	if len(connection.subscriptions)+len(inputs) > int(connection.service.config.maximumSubscriptions) {
		return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS, message: "subscription limit exceeded"}
	}
	requests := make([]requestedSubscription, 0, len(inputs))
	seenIDs := make(map[string]struct{}, len(inputs))
	resolved := make(map[targetKey]*targetState, len(inputs))
	for index, input := range inputs {
		path := fmt.Sprintf("subscribe.subscriptions[%d]", index)
		if input == nil {
			return invalidCommand("invalid subscription", fieldViolation(path, "REQUIRED", "subscription is required"))
		}
		id := input.GetSubscriptionId()
		if !validProtocolString(id, maximumSubscriptionIDBytes, true) {
			return invalidCommand("invalid subscription_id", fieldViolation(path+".subscription_id", "INVALID", "subscription_id is invalid"))
		}
		if _, duplicate := seenIDs[id]; duplicate {
			return invalidCommand("duplicate subscription_id", fieldViolation(path+".subscription_id", "DUPLICATE", "subscription_id is duplicated in this command"))
		}
		if _, exists := connection.subscriptions[id]; exists {
			return invalidCommand("subscription_id already exists", fieldViolation(path+".subscription_id", "ALREADY_EXISTS", "subscription_id is already active"))
		}
		seenIDs[id] = struct{}{}
		key, failure := parseTarget(input.GetTarget(), path+".target")
		if failure != nil {
			return failure
		}
		if input.GetIncludePreviews() || input.PreviewRowLimit != nil {
			return &commandFailure{
				code:       opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_UNSUPPORTED_COMMAND,
				message:    "preview streaming is not implemented",
				violations: []*opensplunkv1.FieldViolation{fieldViolation(path+".include_previews", "UNSUPPORTED", "preview streaming is not implemented")},
			}
		}
		requests = append(requests, requestedSubscription{id: id, key: key, afterSequence: input.GetAfterSequence()})
	}

	for index := range requests {
		target := resolved[requests[index].key]
		if target == nil {
			var err error
			target, err = connection.service.resolveTarget(connection.ctx, requests[index].key)
			if err != nil {
				code := opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_JOB_NOT_FOUND
				message := "job was not found"
				if errors.Is(err, errTargetCapacity) {
					code = opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS
					message = "target capacity is exhausted"
				}
				return &commandFailure{code: code, message: message}
			}
			resolved[requests[index].key] = target
			pinned[target] = struct{}{}
		}
		requests[index].target = target
		requests[index].subscription = &subscription{id: requests[index].id, target: target, connection: connection}
	}

	targets := uniqueSortedTargets(requests)
	for _, target := range targets {
		target.publishMu.Lock()
	}
	defer func() {
		for index := len(targets) - 1; index >= 0; index-- {
			targets[index].publishMu.Unlock()
		}
	}()

	frames := make([][]byte, 0, len(requests)*2)
	for index := range requests {
		request := &requests[index]
		request.target.mu.Lock()
		if request.target.retired {
			request.target.mu.Unlock()
			return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_JOB_NOT_FOUND, message: "job was not found"}
		}
		request.earliest, request.latest = request.target.replayBoundsLocked()
		var reason opensplunkv1.ResynchronizationReason
		now := canonicalTime(connection.service.config.now())
		overdueTerminal := request.target.terminal && !request.target.refreshAt.IsZero() && !request.target.refreshAt.After(now)
		if request.afterSequence == 0 {
			if request.target.currentIncomplete || overdueTerminal {
				reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
			} else {
				request.initialFrames = request.target.currentEventsLocked()
			}
		} else if !request.target.epochEstablished {
			reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SERVER_RESTARTED
		} else if request.target.currentIncomplete || overdueTerminal {
			reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
		} else if request.afterSequence > request.latest ||
			(request.target.epochStart > 1 && request.afterSequence < request.target.epochStart) {
			reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_STATE_DIVERGED
		} else if request.afterSequence < request.latest {
			var continuous bool
			request.initialFrames, continuous = request.target.replayAfterLocked(request.afterSequence)
			if !continuous {
				reason = opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED
				request.initialFrames = nil
			} else {
				request.replayFollows = len(request.initialFrames) != 0
			}
		}
		request.target.mu.Unlock()

		ack, err := marshalEvent(subscriptionAcknowledgedEvent(requestID, *request, connection.service.config.now()), connection.service.config.maximumFrameBytes)
		if err != nil {
			return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, message: "could not encode subscription acknowledgment"}
		}
		frames = append(frames, ack)
		if reason != opensplunkv1.ResynchronizationReason_RESYNCHRONIZATION_REASON_UNSPECIFIED {
			resync, err := marshalEvent(resynchronizationEvent(request.id, request.key, reason, request.earliest, request.latest, connection.service.config.now()), connection.service.config.maximumFrameBytes)
			if err != nil {
				return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, message: "could not encode resynchronization event"}
			}
			frames = append(frames, resync)
			continue
		}
		for _, canonical := range request.initialFrames {
			stamped, err := stampSubscriptionID(canonical, request.id, connection.service.config.maximumFrameBytes)
			if err != nil {
				return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, message: "could not encode initial target state"}
			}
			frames = append(frames, stamped)
		}
	}
	switch connection.enqueueBatchResult(frames) {
	case queueIntrinsicLimit:
		return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS, message: "subscription response exceeds the configured queue capacity"}
	case queuePressure, queueClosed:
		connection.hardClose()
		return nil
	}
	for index := range requests {
		request := &requests[index]
		subscription := request.subscription
		subscription.mu.Lock()
		subscription.active = true
		subscription.mu.Unlock()
		request.target.mu.Lock()
		request.target.subscriptions[subscription] = struct{}{}
		request.target.subscriberCount.Add(1)
		if request.afterSequence == 0 {
			request.target.epochEstablished = true
		}
		request.target.startPollingLocked()
		request.target.mu.Unlock()
		connection.subscriptions[request.id] = subscription
	}
	return nil
}

func (connection *connection) unsubscribe(requestID string, command *opensplunkv1.UnsubscribeSearchJobsCommand) *commandFailure {
	ids := command.GetSubscriptionIds()
	if len(ids) == 0 {
		return invalidCommand("at least one subscription_id is required", fieldViolation("unsubscribe.subscription_ids", "REQUIRED", "at least one subscription_id is required"))
	}
	seen := make(map[string]struct{}, len(ids))
	targetSet := make(map[*targetState]struct{})
	frames := make([][]byte, 0, len(ids))
	for index, id := range ids {
		if !validProtocolString(id, maximumSubscriptionIDBytes, true) {
			return invalidCommand("invalid subscription_id", fieldViolation(fmt.Sprintf("unsubscribe.subscription_ids[%d]", index), "INVALID", "subscription_id is invalid"))
		}
		if _, duplicate := seen[id]; duplicate {
			return invalidCommand("duplicate subscription_id", fieldViolation(fmt.Sprintf("unsubscribe.subscription_ids[%d]", index), "DUPLICATE", "subscription_id is duplicated in this command"))
		}
		seen[id] = struct{}{}
		if subscription := connection.subscriptions[id]; subscription != nil {
			targetSet[subscription.target] = struct{}{}
		}
		frame, err := marshalEvent(subscriptionRemovedEvent(requestID, id, connection.service.config.now()), connection.service.config.maximumFrameBytes)
		if err != nil {
			connection.hardClose()
			return nil
		}
		frames = append(frames, frame)
	}
	if connection.preflightBatch(frames) == queueIntrinsicLimit {
		return invalidCommand("unsubscribe response exceeds the configured queue capacity", fieldViolation("unsubscribe.subscription_ids", "RESOURCE_LIMIT", "command contains too many subscription_ids for one response batch"))
	}
	targets := sortedTargetSet(targetSet)
	for _, target := range targets {
		target.publishMu.Lock()
	}
	defer func() {
		for index := len(targets) - 1; index >= 0; index-- {
			targets[index].publishMu.Unlock()
		}
	}()

	for _, id := range ids {
		if subscription := connection.subscriptions[id]; subscription != nil {
			subscription.mu.Lock()
			subscription.active = false
			subscription.mu.Unlock()
			target := subscription.target
			target.mu.Lock()
			if _, exists := target.subscriptions[subscription]; exists {
				delete(target.subscriptions, subscription)
				target.subscriberCount.Add(-1)
			}
			if len(target.subscriptions) == 0 {
				target.stopPollingLocked()
			}
			target.mu.Unlock()
			delete(connection.subscriptions, id)
		}
	}
	if result := connection.enqueueBatchResult(frames); result != queueAccepted {
		connection.hardClose()
	}
	return nil
}

func (connection *connection) removeAllSubscriptions() {
	targetSet := make(map[*targetState]struct{}, len(connection.subscriptions))
	for _, subscription := range connection.subscriptions {
		targetSet[subscription.target] = struct{}{}
	}
	targets := sortedTargetSet(targetSet)
	for _, target := range targets {
		target.publishMu.Lock()
	}
	for _, subscription := range connection.subscriptions {
		subscription.mu.Lock()
		subscription.active = false
		subscription.mu.Unlock()
		target := subscription.target
		target.mu.Lock()
		if _, exists := target.subscriptions[subscription]; exists {
			delete(target.subscriptions, subscription)
			target.subscriberCount.Add(-1)
		}
		if len(target.subscriptions) == 0 {
			target.stopPollingLocked()
		}
		target.mu.Unlock()
	}
	clear(connection.subscriptions)
	for index := len(targets) - 1; index >= 0; index-- {
		targets[index].publishMu.Unlock()
	}
}

func (connection *connection) enqueue(data []byte) bool {
	return connection.enqueueBatchResult([][]byte{data}) == queueAccepted
}

func (connection *connection) enqueueBatch(frames [][]byte) bool {
	return connection.enqueueBatchResult(frames) == queueAccepted
}

func (connection *connection) preflightBatch(frames [][]byte) queueResult {
	if len(frames) > connection.service.config.maximumQueuedFrames {
		return queueIntrinsicLimit
	}
	var bytes uint64
	for _, frame := range frames {
		if uint64(len(frame)) > connection.service.config.maximumFrameBytes ||
			uint64(len(frame)) > ^uint64(0)-bytes {
			return queueIntrinsicLimit
		}
		bytes += uint64(len(frame))
	}
	if bytes > connection.service.config.maximumQueuedBytes {
		return queueIntrinsicLimit
	}
	return queueAccepted
}

func (connection *connection) enqueueBatchResult(frames [][]byte) queueResult {
	if len(frames) == 0 {
		return queueAccepted
	}
	if result := connection.preflightBatch(frames); result != queueAccepted {
		return result
	}
	var bytes uint64
	for _, frame := range frames {
		bytes += uint64(len(frame))
	}
	connection.queueMu.Lock()
	defer connection.queueMu.Unlock()
	if connection.hardClosed || connection.gracefulClosing {
		return queueClosed
	}
	usedFrames := len(connection.queue) - connection.queueHead + connection.inFlightFrames
	usedBytes := connection.queuedBytes + connection.inFlightBytes
	if usedFrames > connection.service.config.maximumQueuedFrames ||
		len(frames) > connection.service.config.maximumQueuedFrames-usedFrames ||
		usedBytes > connection.service.config.maximumQueuedBytes ||
		bytes > connection.service.config.maximumQueuedBytes-usedBytes ||
		!connection.service.reserveQueuedBytes(bytes) {
		return queuePressure
	}
	connection.compactQueueLocked(len(frames))
	connection.queue = append(connection.queue, frames...)
	connection.queuedBytes += bytes
	connection.signalWriterLocked()
	return queueAccepted
}

func (connection *connection) writeLoop() {
	defer close(connection.writerDone)
	ticker := time.NewTicker(connection.service.config.pingInterval)
	defer ticker.Stop()
	for {
		frame, closeCode, closeReason, state := connection.nextFrame()
		switch state {
		case writerHardClosed:
			return
		case writerFrame:
			err := connection.socket.SetWriteDeadline(time.Now().Add(connection.service.config.writeTimeout))
			if err == nil {
				err = connection.socket.WriteMessage(websocket.BinaryMessage, frame)
			}
			connection.completeFrame(uint64(len(frame)))
			if err != nil {
				connection.hardClose()
				return
			}
			select {
			case <-ticker.C:
				if !connection.writeTransportPing() {
					return
				}
			default:
			}
			continue
		case writerGracefulClose:
			deadline := time.Now().Add(connection.service.config.writeTimeout)
			_ = connection.socket.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(closeCode, closeReason), deadline)
			_ = connection.socket.UnderlyingConn().Close()
			return
		}
		select {
		case <-connection.ctx.Done():
			return
		case <-connection.wake:
		case <-ticker.C:
			if !connection.writeTransportPing() {
				return
			}
		}
	}
}

type writerState uint8

const (
	writerIdle writerState = iota
	writerFrame
	writerGracefulClose
	writerHardClosed
)

func (connection *connection) nextFrame() ([]byte, int, string, writerState) {
	connection.queueMu.Lock()
	defer connection.queueMu.Unlock()
	if connection.hardClosed {
		return nil, 0, "", writerHardClosed
	}
	if connection.queueHead < len(connection.queue) {
		frame := connection.queue[connection.queueHead]
		connection.queue[connection.queueHead] = nil
		connection.queueHead++
		if connection.queueHead == len(connection.queue) {
			connection.queue = nil
			connection.queueHead = 0
		}
		connection.queuedBytes -= uint64(len(frame))
		connection.inFlightFrames++
		connection.inFlightBytes += uint64(len(frame))
		return frame, 0, "", writerFrame
	}
	if connection.gracefulClosing {
		return nil, connection.closeCode, connection.closeReason, writerGracefulClose
	}
	return nil, 0, "", writerIdle
}

func (connection *connection) compactQueueLocked(incoming int) {
	if connection.queueHead == 0 {
		return
	}
	active := len(connection.queue) - connection.queueHead
	if active == 0 {
		connection.queue = nil
		connection.queueHead = 0
		return
	}
	if len(connection.queue)+incoming <= cap(connection.queue) && connection.queueHead < len(connection.queue)/2 {
		return
	}
	copy(connection.queue[:active], connection.queue[connection.queueHead:])
	clear(connection.queue[active:])
	connection.queue = connection.queue[:active]
	connection.queueHead = 0
}

func (connection *connection) completeFrame(bytes uint64) {
	connection.queueMu.Lock()
	if connection.inFlightFrames > 0 {
		connection.inFlightFrames--
	}
	if bytes >= connection.inFlightBytes {
		connection.inFlightBytes = 0
	} else {
		connection.inFlightBytes -= bytes
	}
	connection.queueMu.Unlock()
	connection.service.releaseQueuedBytes(bytes)
}

func (connection *connection) writeTransportPing() bool {
	deadline := time.Now().Add(connection.service.config.writeTimeout)
	if err := connection.socket.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
		connection.hardClose()
		return false
	}
	return true
}

func (connection *connection) initiateClose(code int, reason string) {
	connection.queueMu.Lock()
	if !connection.hardClosed && !connection.gracefulClosing {
		connection.gracefulClosing = true
		connection.closeCode = code
		connection.closeReason = reason
		connection.signalWriterLocked()
	}
	connection.queueMu.Unlock()
}

func (connection *connection) hardClose() {
	connection.hardCloseOnce.Do(func() {
		connection.cancel()
		connection.queueMu.Lock()
		connection.hardClosed = true
		queuedBytes := connection.queuedBytes
		for index := range connection.queue {
			connection.queue[index] = nil
		}
		connection.queue = nil
		connection.queueHead = 0
		connection.queuedBytes = 0
		connection.signalWriterLocked()
		connection.queueMu.Unlock()
		connection.service.releaseQueuedBytes(queuedBytes)
		_ = connection.socket.UnderlyingConn().Close()
	})
}

func (connection *connection) signalWriterLocked() {
	select {
	case connection.wake <- struct{}{}:
	default:
	}
}

func (connection *connection) fatalProtocolError(requestID string, code opensplunkv1.SearchWebSocketProtocolErrorCode, message string) bool {
	if !connection.sendProtocolError(requestID, code, message, nil, true) {
		connection.hardClose()
		return false
	}
	connection.initiateClose(websocket.ClosePolicyViolation, message)
	return true
}

func (connection *connection) sendProtocolError(requestID string, code opensplunkv1.SearchWebSocketProtocolErrorCode, message string, violations []*opensplunkv1.FieldViolation, willClose bool) bool {
	event := protocolErrorEvent(requestID, code, message, violations, willClose, connection.service.config.now())
	return connection.enqueueEvent(event)
}

func (connection *connection) enqueueEvent(event *opensplunkv1.SearchWebSocketEvent) bool {
	data, err := marshalEvent(event, connection.service.config.maximumFrameBytes)
	return err == nil && connection.enqueue(data)
}

func marshalEvent(event *opensplunkv1.SearchWebSocketEvent, maximumFrameBytes uint64) ([]byte, error) {
	if event == nil || event.GetOccurredAt() == nil || event.GetOccurredAt().CheckValid() != nil {
		return nil, errors.New("search websocket control event has an invalid timestamp")
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(event)
	if err != nil {
		return nil, err
	}
	if uint64(len(data)) > maximumFrameBytes {
		return nil, errors.New("search websocket control event exceeds frame limit")
	}
	return data, nil
}

func subscriptionAcknowledgedEvent(requestID string, request requestedSubscription, now time.Time) *opensplunkv1.SearchWebSocketEvent {
	timestamp, _ := timestampToProto(now)
	id := request.id
	target := request.key.protobuf()
	return &opensplunkv1.SearchWebSocketEvent{
		OccurredAt: timestamp, SubscriptionId: &id, Target: proto.Clone(target).(*opensplunkv1.JobTarget),
		Payload: &opensplunkv1.SearchWebSocketEvent_SubscriptionAcknowledged{SubscriptionAcknowledged: &opensplunkv1.SubscriptionAcknowledged{
			RequestId: requestID, SubscriptionId: id, Target: target,
			EarliestAvailableSequence: request.earliest, LatestSequence: request.latest, ReplayWillFollow: request.replayFollows,
		}},
	}
}

func subscriptionRemovedEvent(requestID, subscriptionID string, now time.Time) *opensplunkv1.SearchWebSocketEvent {
	timestamp, _ := timestampToProto(now)
	return &opensplunkv1.SearchWebSocketEvent{
		OccurredAt: timestamp, SubscriptionId: &subscriptionID,
		Payload: &opensplunkv1.SearchWebSocketEvent_SubscriptionRemoved{SubscriptionRemoved: &opensplunkv1.SubscriptionRemoved{
			RequestId: requestID, SubscriptionId: subscriptionID,
		}},
	}
}

func pongEvent(nonce string, now time.Time) *opensplunkv1.SearchWebSocketEvent {
	timestamp, _ := timestampToProto(now)
	return &opensplunkv1.SearchWebSocketEvent{
		OccurredAt: timestamp,
		Payload:    &opensplunkv1.SearchWebSocketEvent_Pong{Pong: &opensplunkv1.SearchWebSocketPong{Nonce: nonce, ServerTime: timestamp}},
	}
}

func protocolErrorEvent(requestID string, code opensplunkv1.SearchWebSocketProtocolErrorCode, message string, violations []*opensplunkv1.FieldViolation, willClose bool, now time.Time) *opensplunkv1.SearchWebSocketEvent {
	timestamp, _ := timestampToProto(now)
	return &opensplunkv1.SearchWebSocketEvent{
		OccurredAt: timestamp,
		Payload: &opensplunkv1.SearchWebSocketEvent_ProtocolError{ProtocolError: &opensplunkv1.SearchWebSocketProtocolError{
			RequestId: requestID, Code: code, Message: message, Violations: violations, ConnectionWillClose: willClose,
		}},
	}
}

func parseTarget(target *opensplunkv1.JobTarget, path string) (targetKey, *commandFailure) {
	if target == nil {
		return targetKey{}, &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_TARGET, message: "target is required", violations: []*opensplunkv1.FieldViolation{fieldViolation(path, "REQUIRED", "target is required")}}
	}
	var key targetKey
	switch value := target.GetTarget().(type) {
	case *opensplunkv1.JobTarget_SearchJobId:
		if value == nil {
			return targetKey{}, &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_TARGET, message: "target kind is invalid", violations: []*opensplunkv1.FieldViolation{fieldViolation(path, "INVALID", "target kind is invalid")}}
		}
		key = targetKey{kind: targetKindSearch, id: value.SearchJobId}
	case *opensplunkv1.JobTarget_ExportJobId:
		if value == nil {
			return targetKey{}, &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_TARGET, message: "target kind is invalid", violations: []*opensplunkv1.FieldViolation{fieldViolation(path, "INVALID", "target kind is invalid")}}
		}
		key = targetKey{kind: targetKindExport, id: value.ExportJobId}
	default:
		return targetKey{}, &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_TARGET, message: "target kind is required", violations: []*opensplunkv1.FieldViolation{fieldViolation(path, "REQUIRED", "one target kind is required")}}
	}
	if !validProtocolString(key.id, maximumTargetIDBytes, true) {
		return targetKey{}, &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_TARGET, message: "target identifier is invalid", violations: []*opensplunkv1.FieldViolation{fieldViolation(path, "INVALID", "target identifier is invalid")}}
	}
	return key, nil
}

func validProtocolString(value string, maximumBytes int, required bool) bool {
	if (required && value == "") || len(value) > maximumBytes || !utf8.ValidString(value) {
		return false
	}
	return !strings.ContainsAny(value, "\x00\r\n")
}

func fieldViolation(path, code, message string) *opensplunkv1.FieldViolation {
	return &opensplunkv1.FieldViolation{FieldPath: path, Code: code, Message: message}
}

func invalidCommand(message string, violation *opensplunkv1.FieldViolation) *commandFailure {
	return &commandFailure{code: opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND, message: message, violations: []*opensplunkv1.FieldViolation{violation}}
}

func uniqueSortedTargets(requests []requestedSubscription) []*targetState {
	set := make(map[*targetState]struct{}, len(requests))
	for index := range requests {
		set[requests[index].target] = struct{}{}
	}
	return sortedTargetSet(set)
}

func sortedTargetSet(set map[*targetState]struct{}) []*targetState {
	targets := make([]*targetState, 0, len(set))
	for target := range set {
		targets = append(targets, target)
	}
	sort.Slice(targets, func(left, right int) bool {
		if targets[left].key.kind != targets[right].key.kind {
			return targets[left].key.kind < targets[right].key.kind
		}
		return targets[left].key.id < targets[right].key.id
	})
	return targets
}
