package server

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchws"
	"google.golang.org/protobuf/proto"
)

const slowConsumerPollInterval = 10 * time.Millisecond

type slowConsumerWriteGate struct {
	armed atomic.Bool

	blocked     chan struct{}
	closed      chan struct{}
	released    chan struct{}
	blockedOnce sync.Once
	closedOnce  sync.Once
	releaseOnce sync.Once
}

func newSlowConsumerWriteGate() *slowConsumerWriteGate {
	return &slowConsumerWriteGate{
		blocked:  make(chan struct{}),
		closed:   make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (gate *slowConsumerWriteGate) arm() {
	gate.armed.Store(true)
}

func (gate *slowConsumerWriteGate) release() {
	gate.releaseOnce.Do(func() { close(gate.released) })
}

func (gate *slowConsumerWriteGate) waitBlocked(t *testing.T) {
	t.Helper()
	waitForSlowConsumerSignal(t, gate.blocked, "gated server write")
}

func (gate *slowConsumerWriteGate) waitClosed(t *testing.T) {
	t.Helper()
	waitForSlowConsumerSignal(t, gate.closed, "pressured server connection close")
}

func (gate *slowConsumerWriteGate) requireOpen(t *testing.T) {
	t.Helper()
	select {
	case <-gate.closed:
		t.Fatal("slow-consumer connection closed before its exact frame bound")
	default:
	}
}

func waitForSlowConsumerSignal(t *testing.T, signal <-chan struct{}, label string) {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", label)
	}
}

type slowConsumerGatedConn struct {
	net.Conn
	gate *slowConsumerWriteGate
}

func (connection *slowConsumerGatedConn) Write(data []byte) (int, error) {
	if !connection.gate.armed.Load() {
		return connection.Conn.Write(data)
	}
	connection.gate.blockedOnce.Do(func() { close(connection.gate.blocked) })
	select {
	case <-connection.gate.closed:
		return 0, net.ErrClosed
	case <-connection.gate.released:
		return connection.Conn.Write(data)
	}
}

func (connection *slowConsumerGatedConn) Close() error {
	err := connection.Conn.Close()
	connection.gate.closedOnce.Do(func() { close(connection.gate.closed) })
	return err
}

type slowConsumerListener struct {
	net.Listener
	gate     *slowConsumerWriteGate
	gateNext atomic.Bool
}

func newSlowConsumerListener(gate *slowConsumerWriteGate) *slowConsumerListener {
	return &slowConsumerListener{gate: gate}
}

func (listener *slowConsumerListener) wrap(inner net.Listener) net.Listener {
	listener.Listener = inner
	return listener
}

func (listener *slowConsumerListener) gateNextConnection(t *testing.T) {
	t.Helper()
	if !listener.gateNext.CompareAndSwap(false, true) {
		t.Fatal("a server connection is already waiting for the write gate")
	}
}

func (listener *slowConsumerListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if listener.gateNext.CompareAndSwap(true, false) {
		return &slowConsumerGatedConn{Conn: connection, gate: listener.gate}, nil
	}
	return connection, nil
}

func advanceSlowConsumerPoll(t *testing.T, clock *expirationTestClock) {
	t.Helper()
	waitForExpirationClockWaiters(t, clock, 1)
	clock.Set(clock.Now().Add(slowConsumerPollInterval))
	waitForExpirationClockWaiters(t, clock, 1)
}

func waitForRealSearchWebSocketState(
	t *testing.T,
	fixture *realSearchWebSocketFixture,
	state opensplunk.SearchJobState,
) *opensplunk.GetSearchJobResponse {
	t.Helper()
	return waitForIntegrationState(
		t,
		"search "+fixture.jobID,
		state.String(),
		func() (*opensplunk.GetSearchJobResponse, string, error) {
			response := new(opensplunk.GetSearchJobResponse)
			fixture.post(
				"/api/search/jobs/get",
				&opensplunk.GetSearchJobRequest{SearchJobId: fixture.jobID},
				response,
			)
			return response, response.GetSearchJob().GetState().String(), nil
		},
	)
}

func TestSearchWebSocketSlowConsumerIsBoundedAndRecoversWithoutBlockingSearch(t *testing.T) {
	const (
		jobID          = "search-ws-slow-consumer"
		subscriptionID = "slow-consumer-subscription"
		queueFrames    = 7
		pressureStep   = queueFrames + 1
	)
	anchor := time.Date(2026, time.July, 25, 12, 0, 0, 0, time.UTC)
	clock := newExpirationTestClock(anchor)
	writeGate := newSlowConsumerWriteGate()
	t.Cleanup(writeGate.release)
	listener := newSlowConsumerListener(writeGate)
	fixture := newRealSearchWebSocketFixtureWithOptions(
		t,
		jobID,
		6,
		realSearchWebSocketFixtureOptions{
			now:  clock.Now,
			wait: clock.Wait,
			configureWebSocket: func(config *searchws.Config) {
				config.MaximumQueuedFrames = queueFrames
				config.MaximumQueuedBytes = 64 << 10
				config.MaximumTotalQueuedBytes = 256 << 10
				config.PollInterval = slowConsumerPollInterval
			},
			wrapListener: listener.wrap,
		},
	)
	fixture.createRunningSearch()

	listener.gateNextConnection(t)
	slow := fixture.dial()
	earliest, checkpoint := establishRealSearchWebSocketEpoch(
		t,
		slow,
		"slow-initial",
		subscriptionID,
		jobID,
	)
	waitForExpirationClockWaiters(t, clock, 1)
	writeGate.arm()

	healthy := fixture.dial()
	healthyEarliest, healthyCheckpoint := establishRealSearchWebSocketEpoch(
		t,
		healthy,
		"healthy-initial",
		"healthy-subscription",
		jobID,
	)
	if healthyEarliest != earliest || healthyCheckpoint != checkpoint {
		t.Fatalf(
			"healthy initial bounds = [%d,%d], want slow bounds [%d,%d]",
			healthyEarliest,
			healthyCheckpoint,
			earliest,
			checkpoint,
		)
	}

	for step := 1; step <= pressureStep; step++ {
		fixture.executor.step(t, searchjobs.ExecutionProgressDelta{
			ScannedRows:  1,
			ScannedBytes: 10,
		})
		advanceSlowConsumerPoll(t, clock)
		if step == 1 {
			writeGate.waitBlocked(t)
		}
		event := readSearchWebSocketEvent(t, healthy)
		progress := event.GetSearchProgress()
		if progress == nil ||
			progress.GetScannedRows() != uint64(step) ||
			progress.GetScannedBytes() != uint64(step*10) ||
			event.GetSequence() != checkpoint+uint64(step) {
			t.Fatalf("healthy progress[%d] = %+v", step, event)
		}
		if step < pressureStep {
			writeGate.requireOpen(t)
		}
		if step == queueFrames {
			var authoritative opensplunk.GetSearchJobResponse
			fixture.post(
				"/api/search/jobs/get",
				&opensplunk.GetSearchJobRequest{SearchJobId: jobID},
				&authoritative,
			)
			if authoritative.GetSearchJob().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
				authoritative.GetSearchJob().GetProgress().GetScannedRows() != queueFrames ||
				authoritative.GetSearchJob().GetProgress().GetScannedBytes() != queueFrames*10 ||
				fixture.executor.calls.Load() != 1 {
				t.Fatalf(
					"authoritative search while writer blocked = %+v calls=%d",
					authoritative.GetSearchJob(),
					fixture.executor.calls.Load(),
				)
			}
		}
	}
	writeGate.waitClosed(t)

	expired := fixture.dial()
	writeSearchWebSocketCommand(
		t,
		expired,
		searchWebSocketSubscribeCommand("slow-expired", subscriptionID, jobID, checkpoint),
	)
	acknowledgmentEvent := readSearchWebSocketEvent(t, expired)
	acknowledgment := acknowledgmentEvent.GetSubscriptionAcknowledged()
	if acknowledgment == nil ||
		acknowledgment.GetRequestId() != "slow-expired" ||
		acknowledgment.GetSubscriptionId() != subscriptionID ||
		acknowledgment.GetReplayWillFollow() ||
		acknowledgment.GetEarliestAvailableSequence() <= checkpoint ||
		acknowledgment.GetLatestSequence() != checkpoint+pressureStep {
		t.Fatalf("slow-consumer expired acknowledgment = %+v", acknowledgmentEvent)
	}
	resynchronization := readSearchWebSocketEvent(t, expired).GetResynchronizationRequired()
	if resynchronization == nil ||
		resynchronization.GetReason() != opensplunk.ResynchronizationReason_RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED ||
		resynchronization.GetEarliestAvailableSequence() != acknowledgment.GetEarliestAvailableSequence() ||
		resynchronization.GetLatestSequence() != acknowledgment.GetLatestSequence() ||
		resynchronization.GetRecoveryPath() != "/api/search/jobs/get" {
		t.Fatalf("slow-consumer resynchronization = %+v", resynchronization)
	}
	var recoveredSnapshot opensplunk.GetSearchJobResponse
	fixture.post(
		"/api/search/jobs/get",
		&opensplunk.GetSearchJobRequest{SearchJobId: jobID},
		&recoveredSnapshot,
	)
	if recoveredSnapshot.GetSearchJob().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_RUNNING ||
		recoveredSnapshot.GetSearchJob().GetProgress().GetScannedRows() != pressureStep ||
		recoveredSnapshot.GetSearchJob().GetProgress().GetScannedBytes() != pressureStep*10 ||
		fixture.executor.calls.Load() != 1 {
		t.Fatalf(
			"authoritative slow-consumer recovery = %+v calls=%d",
			recoveredSnapshot.GetSearchJob(),
			fixture.executor.calls.Load(),
		)
	}
	if err := expired.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close expired replay WebSocket: %v", err)
	}

	recovered := fixture.dial()
	writeSearchWebSocketCommand(
		t,
		recovered,
		searchWebSocketSubscribeCommand(
			"slow-recovered",
			subscriptionID,
			jobID,
			resynchronization.GetLatestSequence(),
		),
	)
	recoveredAcknowledgmentEvent := readSearchWebSocketEvent(t, recovered)
	recoveredAcknowledgment := recoveredAcknowledgmentEvent.GetSubscriptionAcknowledged()
	if recoveredAcknowledgment == nil ||
		recoveredAcknowledgment.GetRequestId() != "slow-recovered" ||
		recoveredAcknowledgment.GetSubscriptionId() != subscriptionID ||
		recoveredAcknowledgment.GetReplayWillFollow() ||
		recoveredAcknowledgment.GetEarliestAvailableSequence() != resynchronization.GetEarliestAvailableSequence() ||
		recoveredAcknowledgment.GetLatestSequence() != resynchronization.GetLatestSequence() {
		t.Fatalf("recovered slow-consumer acknowledgment = %+v", recoveredAcknowledgmentEvent)
	}

	fixture.executor.step(t, searchjobs.ExecutionProgressDelta{
		ScannedRows:  1,
		ScannedBytes: 10,
	})
	advanceSlowConsumerPoll(t, clock)
	healthyLive := readSearchWebSocketEvent(t, healthy)
	recoveredLive := readSearchWebSocketEvent(t, recovered)
	if healthyLive.GetSequence() != checkpoint+pressureStep+1 ||
		recoveredLive.GetSequence() != healthyLive.GetSequence() ||
		healthyLive.GetSearchProgress().GetScannedRows() != pressureStep+1 ||
		healthyLive.GetSearchProgress().GetScannedBytes() != (pressureStep+1)*10 ||
		!proto.Equal(recoveredLive.GetSearchProgress(), healthyLive.GetSearchProgress()) {
		t.Fatalf("post-recovery live progress = healthy:%+v recovered:%+v", healthyLive, recoveredLive)
	}

	fixture.executor.complete(t)
	select {
	case err := <-fixture.executor.exited:
		if err != nil {
			t.Fatalf("completed executor exit = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for completed executor")
	}
	completed := waitForRealSearchWebSocketState(
		t,
		fixture,
		opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
	)
	if completed.GetSearchJob().GetProgress().GetScannedRows() != pressureStep+1 ||
		completed.GetSearchJob().GetProgress().GetScannedBytes() != (pressureStep+1)*10 ||
		fixture.executor.calls.Load() != 1 ||
		fixture.executor.exits.Load() != 1 {
		t.Fatalf(
			"completed search = %+v calls=%d exits=%d",
			completed.GetSearchJob(),
			fixture.executor.calls.Load(),
			fixture.executor.exits.Load(),
		)
	}

	advanceSlowConsumerPoll(t, clock)
	var healthyTerminal, recoveredTerminal []*opensplunk.SearchWebSocketEvent
	for range 3 {
		healthyTerminal = append(healthyTerminal, readSearchWebSocketEvent(t, healthy))
		recoveredTerminal = append(recoveredTerminal, readSearchWebSocketEvent(t, recovered))
	}
	for index := range healthyTerminal {
		var samePayload bool
		switch index {
		case 0:
			samePayload = proto.Equal(
				healthyTerminal[index].GetSearchStateChanged(),
				recoveredTerminal[index].GetSearchStateChanged(),
			)
		case 1:
			samePayload = proto.Equal(
				healthyTerminal[index].GetSearchProgress(),
				recoveredTerminal[index].GetSearchProgress(),
			)
		case 2:
			samePayload = proto.Equal(
				healthyTerminal[index].GetSearchTerminal(),
				recoveredTerminal[index].GetSearchTerminal(),
			)
		}
		if healthyTerminal[index].GetSequence() != healthyLive.GetSequence()+uint64(index+1) ||
			recoveredTerminal[index].GetSequence() != healthyTerminal[index].GetSequence() ||
			!samePayload {
			t.Fatalf(
				"terminal projection[%d] = healthy:%+v recovered:%+v",
				index,
				healthyTerminal[index],
				recoveredTerminal[index],
			)
		}
	}
	if healthyTerminal[0].GetSearchStateChanged().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED ||
		healthyTerminal[1].GetSearchProgress().GetPhase() != opensplunk.SearchExecutionPhase_SEARCH_EXECUTION_PHASE_COMPLETE ||
		healthyTerminal[2].GetSearchTerminal().GetState() != opensplunk.SearchJobState_SEARCH_JOB_STATE_COMPLETED {
		t.Fatalf("completed terminal projection = %+v", healthyTerminal)
	}

	if err := healthy.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close healthy WebSocket: %v", err)
	}
	if err := recovered.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("close recovered WebSocket: %v", err)
	}
	_ = slow.Close()
	waitForExpirationClockWaiters(t, clock, 0)
}
