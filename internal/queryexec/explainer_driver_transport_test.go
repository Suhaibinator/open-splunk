package queryexec

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
)

// This regression exercises clickhouse-go itself rather than a queryConnection
// fake. The pinned driver sends a native Query synchronously before it installs
// its own context deadline. The injected connection completes the real
// ClickHouse handshake, then simulates a zero-window peer by holding only the
// large query write until our per-lane deadline overlay expires it.
func TestExplainerBoundsInitialNativeQueryWriteAndReusesLanes(t *testing.T) {
	server := newExplainHandshakeServer(t, false)
	defer server.Close(t)

	var dialCount, blockedWrites, closedConnections atomic.Int32
	explainer := newStalledWriteExplainer(
		t,
		[]string{server.Address()},
		&dialCount,
		&blockedWrites,
		&closedConnections,
	)
	defer func() {
		if err := explainer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	query := stalledNativeExplainQuery(t)

	for attempt := 1; attempt <= maximumConcurrentExplains+1; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 125*time.Millisecond)
		started := time.Now()
		got, err := explainer.Explain(ctx, query)
		elapsed := time.Since(started)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) ||
			got != (ExplainResult{}) {
			t.Fatalf(
				"attempt %d Explain() = (%#v, %v)",
				attempt,
				got,
				err,
			)
		}
		if elapsed < 75*time.Millisecond || elapsed > time.Second {
			t.Fatalf(
				"attempt %d initial write returned after %v, want bounded deadline",
				attempt,
				elapsed,
			)
		}
	}
	if got, want := dialCount.Load(), int32(maximumConcurrentExplains+1); got != want {
		t.Fatalf("physical dials = %d, want %d after lane redial", got, want)
	}
	if got, want := blockedWrites.Load(), int32(maximumConcurrentExplains+1); got != want {
		t.Fatalf("blocked initial writes = %d, want %d", got, want)
	}
	if got, want := closedConnections.Load(), int32(maximumConcurrentExplains+1); got != want {
		t.Fatalf("closed failed transports = %d, want %d", got, want)
	}
}

func TestExplainerClosesFailedHandshakeAndRetriesAnotherAddress(t *testing.T) {
	server := newExplainHandshakeServer(t, true)
	defer server.Close(t)

	var dialCount, blockedWrites, closedConnections atomic.Int32
	address := server.Address()
	explainer := newStalledWriteExplainer(
		t,
		[]string{address, address},
		&dialCount,
		&blockedWrites,
		&closedConnections,
	)
	defer func() {
		if err := explainer.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	got, err := explainer.Explain(ctx, stalledNativeExplainQuery(t))
	if !errors.Is(err, context.DeadlineExceeded) ||
		got != (ExplainResult{}) {
		t.Fatalf("Explain() = (%#v, %v)", got, err)
	}
	if got, want := server.accepted.Load(), int32(2); got != want {
		t.Fatalf("accepted connections = %d, want %d", got, want)
	}
	if got, want := dialCount.Load(), int32(2); got != want {
		t.Fatalf("physical dials = %d, want %d", got, want)
	}
	if got := blockedWrites.Load(); got != 1 {
		t.Fatalf("blocked query writes = %d, want 1 after handshake retry", got)
	}
	if got, want := closedConnections.Load(), int32(2); got != want {
		t.Fatalf(
			"closed failed/expired transports = %d, want %d",
			got,
			want,
		)
	}
}

func stalledNativeExplainQuery(t *testing.T) clickhouse.CompiledQuery {
	t.Helper()

	query := sealedExplainQuery(t)
	replaced := false
	for index, argument := range query.Args {
		if argument == "needle" {
			// Backslashes are doubled by the pinned driver's positional
			// binder. The conservative admission estimate remains below the
			// fixed 1 MiB max_query_size while the actual native packet is
			// large enough to enter the forced stalled-write path.
			query.Args[index] = strings.Repeat(`\`, 400<<10)
			replaced = true
			break
		}
	}
	if !replaced {
		t.Fatalf("Compiler arguments have no search literal: %#v", query.Args)
	}
	return query
}

func newStalledWriteExplainer(
	t *testing.T,
	addresses []string,
	dialCount *atomic.Int32,
	blockedWrites *atomic.Int32,
	closedConnections *atomic.Int32,
) *Explainer {
	t.Helper()

	options := &clickhousedriver.Options{
		Addr:        addresses,
		Auth:        clickhousedriver.Auth{Database: "default"},
		DialTimeout: time.Second,
		ReadTimeout: time.Second,
	}
	baseSettings, err := querySettings(Config{})
	if err != nil {
		t.Fatal(err)
	}
	settings, err := settingsForExplain(baseSettings)
	if err != nil {
		t.Fatal(err)
	}
	lanes := make(chan *explainLane, maximumConcurrentExplains)
	allLanes := make([]*explainLane, 0, maximumConcurrentExplains)
	for range maximumConcurrentExplains {
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = func(
			ctx context.Context,
			network string,
			address string,
			timeout time.Duration,
			tlsConfig *tls.Config,
		) (net.Conn, error) {
			if tlsConfig != nil {
				return nil, errors.New("test dial unexpectedly requested TLS")
			}
			connection, dialErr := dialExplainConnection(
				ctx,
				network,
				address,
				timeout,
				nil,
			)
			if dialErr != nil {
				return nil, dialErr
			}
			dialCount.Add(1)
			return newDeadlineBlockingWriteConn(
				connection,
				blockedWrites,
				closedConnections,
			), nil
		}
		connection, openErr := clickhousedriver.Open(
			cloneExplainerLaneOptions(options, coordinator),
		)
		if openErr != nil {
			t.Fatal(openErr)
		}
		lane := &explainLane{
			connection:      connection,
			activateContext: coordinator.ActivateContext,
			discard:         coordinator.DiscardConnection,
			close: func() error {
				return errors.Join(connection.Close(), coordinator.Close())
			},
		}
		lanes <- lane
		allLanes = append(allLanes, lane)
	}
	return &Explainer{
		settings:         settings,
		executionTimeout: maximumExplainExecutionTime,
		lanes:            lanes,
		allLanes:         allLanes,
		newQueryID:       randomExplainQueryID,
	}
}

type deadlineBlockingWriteConn struct {
	net.Conn

	mu             sync.Mutex
	writeDeadline  time.Time
	deadlineChange chan struct{}
	closed         chan struct{}
	closeOnce      sync.Once
	closeErr       error
	blockedWrites  *atomic.Int32
	closedCount    *atomic.Int32
}

func newDeadlineBlockingWriteConn(
	connection net.Conn,
	blockedWrites *atomic.Int32,
	closedCount *atomic.Int32,
) *deadlineBlockingWriteConn {
	return &deadlineBlockingWriteConn{
		Conn:           connection,
		deadlineChange: make(chan struct{}),
		closed:         make(chan struct{}),
		blockedWrites:  blockedWrites,
		closedCount:    closedCount,
	}
}

func (connection *deadlineBlockingWriteConn) Write(buffer []byte) (int, error) {
	const forcedStallThreshold = 64 << 10
	if len(buffer) < forcedStallThreshold {
		return connection.Conn.Write(buffer)
	}
	connection.blockedWrites.Add(1)

	for {
		connection.mu.Lock()
		deadline := connection.writeDeadline
		changed := connection.deadlineChange
		connection.mu.Unlock()

		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		if deadline.IsZero() {
			select {
			case <-changed:
			case <-connection.closed:
				return 0, net.ErrClosed
			}
			continue
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-timer.C:
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-connection.closed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return 0, net.ErrClosed
		}
	}
}

func (connection *deadlineBlockingWriteConn) SetDeadline(
	deadline time.Time,
) error {
	connection.mu.Lock()
	connection.writeDeadline = deadline
	connection.notifyDeadlineChangeLocked()
	connection.mu.Unlock()
	return connection.Conn.SetDeadline(deadline)
}

func (connection *deadlineBlockingWriteConn) SetWriteDeadline(
	deadline time.Time,
) error {
	connection.mu.Lock()
	connection.writeDeadline = deadline
	connection.notifyDeadlineChangeLocked()
	connection.mu.Unlock()
	return connection.Conn.SetWriteDeadline(deadline)
}

func (connection *deadlineBlockingWriteConn) Close() error {
	connection.closeOnce.Do(func() {
		close(connection.closed)
		connection.closeErr = connection.Conn.Close()
		if connection.closedCount != nil {
			connection.closedCount.Add(1)
		}
	})
	return connection.closeErr
}

func (connection *deadlineBlockingWriteConn) notifyDeadlineChangeLocked() {
	close(connection.deadlineChange)
	connection.deadlineChange = make(chan struct{})
}

type explainHandshakeServer struct {
	listener  net.Listener
	stop      chan struct{}
	stopOnce  sync.Once
	group     sync.WaitGroup
	errors    chan error
	failFirst bool
	accepted  atomic.Int32
}

func newExplainHandshakeServer(
	t *testing.T,
	failFirst bool,
) *explainHandshakeServer {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(
		context.Background(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatal(err)
	}
	server := &explainHandshakeServer{
		listener:  listener,
		stop:      make(chan struct{}),
		errors:    make(chan error, 16),
		failFirst: failFirst,
	}
	server.group.Add(1)
	go server.accept()
	return server
}

func (server *explainHandshakeServer) Address() string {
	return server.listener.Addr().String()
}

func (server *explainHandshakeServer) accept() {
	defer server.group.Done()
	for {
		connection, err := server.listener.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				server.report(err)
			}
			return
		}
		ordinal := server.accepted.Add(1)
		server.group.Add(1)
		go server.handshake(connection, ordinal)
	}
}

func (server *explainHandshakeServer) handshake(
	connection net.Conn,
	ordinal int32,
) {
	defer server.group.Done()
	defer func() { _ = connection.Close() }()

	reader := chproto.NewReader(connection)
	packet, err := reader.UVarInt()
	if err != nil {
		server.report(fmt.Errorf("read client hello packet: %w", err))
		return
	}
	if chproto.ClientCode(packet) != chproto.ClientCodeHello {
		server.report(fmt.Errorf("client packet = %d, want hello", packet))
		return
	}
	var clientHello chproto.ClientHello
	if err := clientHello.Decode(reader); err != nil {
		server.report(fmt.Errorf("decode client hello: %w", err))
		return
	}
	if server.failFirst && ordinal == 1 {
		if _, err := connection.Write([]byte{0xff}); err != nil {
			server.report(fmt.Errorf("write invalid server hello: %w", err))
		}
		return
	}
	buffer := new(chproto.Buffer)
	buffer.EncodeAware(&chproto.ServerHello{
		Name:        "open-splunk-stalled-peer",
		Major:       26,
		Minor:       3,
		Revision:    clientHello.ProtocolVersion,
		Timezone:    "UTC",
		DisplayName: "stalled-peer",
		Patch:       17,
	}, clientHello.ProtocolVersion)
	if _, err := io.Copy(connection, bytes.NewReader(buffer.Buf)); err != nil {
		server.report(fmt.Errorf("write server hello: %w", err))
		return
	}

	// Do not read the addendum or query. The client-side test transport
	// deterministically simulates the resulting blocked large write.
	<-server.stop
}

func (server *explainHandshakeServer) report(err error) {
	select {
	case server.errors <- err:
	default:
	}
}

func (server *explainHandshakeServer) Close(t *testing.T) {
	t.Helper()
	server.stopOnce.Do(func() {
		close(server.stop)
		_ = server.listener.Close()
	})
	server.group.Wait()
	close(server.errors)
	for err := range server.errors {
		t.Errorf("stalled ClickHouse peer: %v", err)
	}
}
