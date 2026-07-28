package queryexec

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestExplainDeadlineCoordinatorOverlaysAndRestoresDriverDeadlines(
	t *testing.T,
) {
	t.Parallel()

	underlying := newRecordingDeadlineConn()
	coordinator := newExplainDeadlineCoordinator(3*time.Second, nil)
	coordinator.dialConnection = staticExplainDialer(underlying)
	rawConnection, err := coordinator.DialContext(context.Background(), "clickhouse.test:9000")
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	connection := rawConnection.(*explainDeadlineConn)
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	now := time.Now()
	driverReadDeadline := now.Add(2 * time.Minute)
	driverWriteDeadline := now.Add(30 * time.Minute)
	if err := connection.SetReadDeadline(driverReadDeadline); err != nil {
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	if err := connection.SetWriteDeadline(driverWriteDeadline); err != nil {
		t.Fatalf("SetWriteDeadline() error = %v", err)
	}

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(10*time.Minute))
	defer cancel()
	contextDeadline, _ := ctx.Deadline()
	release, err := coordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("ActivateContext() error = %v", err)
	}
	assertRecordedDeadlines(t, underlying, driverReadDeadline, contextDeadline)

	laterDriverDeadline := now.Add(45 * time.Minute)
	if err := connection.SetDeadline(laterDriverDeadline); err != nil {
		t.Fatalf("SetDeadline() error = %v", err)
	}
	assertRecordedDeadlines(t, underlying, contextDeadline, contextDeadline)

	earlierDriverDeadline := now.Add(time.Minute)
	if err := connection.SetReadDeadline(earlierDriverDeadline); err != nil {
		t.Fatalf("SetReadDeadline() with earlier deadline error = %v", err)
	}
	assertRecordedDeadlines(t, underlying, earlierDriverDeadline, contextDeadline)

	if err := release(); err != nil {
		t.Fatalf("release() error = %v", err)
	}
	assertRecordedDeadlines(t, underlying, earlierDriverDeadline, laterDriverDeadline)
	if err := release(); err != nil {
		t.Fatalf("second release() error = %v", err)
	}
}

func TestExplainDeadlineCoordinatorRejectsInvalidOrOverlappingRegistrations(
	t *testing.T,
) {
	t.Parallel()

	var nilCoordinator *explainDeadlineCoordinator
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := nilCoordinator.ActivateContext(ctx); err == nil ||
		!strings.Contains(err.Error(), "transport is nil") {
		t.Fatalf("nil coordinator error = %v", err)
	}

	coordinator := newExplainDeadlineCoordinator(time.Second, nil)
	if _, err := coordinator.ActivateContext(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "context deadline is required") {
		t.Fatalf("deadline-free context error = %v", err)
	}
	release, err := coordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("ActivateContext() error = %v", err)
	}
	if _, err := coordinator.ActivateContext(ctx); err == nil ||
		!strings.Contains(err.Error(), "registration already active") {
		t.Fatalf("overlapping registration error = %v", err)
	}

	// A second lane has independent state and can be active concurrently.
	otherCoordinator := newExplainDeadlineCoordinator(time.Second, nil)
	otherRelease, err := otherCoordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("other ActivateContext() error = %v", err)
	}
	if err := otherRelease(); err != nil {
		t.Fatalf("other release() error = %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release() error = %v", err)
	}

	nextRelease, err := coordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("ActivateContext() after release error = %v", err)
	}
	if err := nextRelease(); err != nil {
		t.Fatalf("next release() error = %v", err)
	}
}

func TestExplainDeadlineCoordinatorCancellationBeforeAndAfterDial(t *testing.T) {
	t.Parallel()

	t.Run("before dial", func(t *testing.T) {
		t.Parallel()

		underlying := newRecordingDeadlineConn()
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = staticExplainDialer(underlying)
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		release, err := coordinator.ActivateContext(ctx)
		if err != nil {
			t.Fatalf("ActivateContext() error = %v", err)
		}
		cancel()
		<-ctx.Done()

		connection, err := coordinator.DialContext(
			context.Background(),
			"clickhouse.test:9000",
		)
		if err != nil {
			t.Fatalf("DialContext() error = %v", err)
		}
		t.Cleanup(func() {
			if closeErr := connection.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		})
		assertRecordedDeadlinesExpired(t, underlying)
		if err := release(); err != nil {
			t.Fatalf("release() error = %v", err)
		}
		assertRecordedDeadlines(t, underlying, time.Time{}, time.Time{})
	})

	t.Run("after dial", func(t *testing.T) {
		t.Parallel()

		underlying := newRecordingDeadlineConn()
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = staticExplainDialer(underlying)
		connection, err := coordinator.DialContext(
			context.Background(),
			"clickhouse.test:9000",
		)
		if err != nil {
			t.Fatalf("DialContext() error = %v", err)
		}
		t.Cleanup(func() {
			if closeErr := connection.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		release, err := coordinator.ActivateContext(ctx)
		if err != nil {
			t.Fatalf("ActivateContext() error = %v", err)
		}
		cancel()
		waitForRecordedDeadlinesExpired(t, underlying)
		if err := release(); err != nil {
			t.Fatalf("release() error = %v", err)
		}
		assertRecordedDeadlines(t, underlying, time.Time{}, time.Time{})
	})

	t.Run("while dial completes", func(t *testing.T) {
		t.Parallel()

		underlying := newRecordingDeadlineConn()
		dialStarted := make(chan struct{})
		completeDial := make(chan struct{})
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = func(
			context.Context,
			string,
			string,
			time.Duration,
			*tls.Config,
		) (net.Conn, error) {
			close(dialStarted)
			<-completeDial
			return underlying, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		release, err := coordinator.ActivateContext(ctx)
		if err != nil {
			t.Fatalf("ActivateContext() error = %v", err)
		}
		dialResult := make(chan explainDialResult, 1)
		go func() {
			connection, dialErr := coordinator.DialContext(
				context.Background(),
				"clickhouse.test:9000",
			)
			dialResult <- explainDialResult{connection: connection, err: dialErr}
		}()
		<-dialStarted
		cancel()
		close(completeDial)
		result := <-dialResult
		if result.err != nil {
			t.Fatalf("DialContext() error = %v", result.err)
		}
		t.Cleanup(func() {
			if closeErr := result.connection.Close(); closeErr != nil {
				t.Errorf("Close() error = %v", closeErr)
			}
		})
		assertRecordedDeadlinesExpired(t, underlying)
		if err := release(); err != nil {
			t.Fatalf("release() error = %v", err)
		}
	})
}

func TestExplainDeadlineCoordinatorReleaseJoinsCancellationCallback(
	t *testing.T,
) {
	t.Parallel()

	underlying := newRecordingDeadlineConn()
	coordinator := newExplainDeadlineCoordinator(time.Second, nil)
	coordinator.dialConnection = staticExplainDialer(underlying)
	connection, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := connection.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	release, err := coordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("ActivateContext() error = %v", err)
	}
	underlying.blockExpiredDeadline = make(chan struct{})
	cancel()
	select {
	case <-underlying.expiredDeadlineEntered:
	case <-time.After(time.Second):
		t.Fatal("cancellation callback did not begin applying the expired deadline")
	}

	released := make(chan error, 1)
	go func() {
		released <- release()
	}()
	select {
	case err := <-released:
		t.Fatalf("release returned before cancellation callback completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(underlying.blockExpiredDeadline)
	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("release() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("release did not return after cancellation callback completed")
	}
	assertRecordedDeadlines(t, underlying, time.Time{}, time.Time{})
}

func TestExplainDeadlineCoordinatorClosesConnectionAfterDeadlineFailure(
	t *testing.T,
) {
	t.Parallel()

	t.Run("activation", func(t *testing.T) {
		t.Parallel()

		underlying := &deadlineFailingConn{
			recordingDeadlineConn: newRecordingDeadlineConn(),
			failNonzero:           true,
		}
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = staticExplainDialer(underlying)
		wrapped, err := coordinator.DialContext(
			context.Background(),
			"clickhouse.test:9000",
		)
		if err != nil {
			t.Fatalf("DialContext() error = %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		release, activateErr := coordinator.ActivateContext(ctx)
		if activateErr == nil {
			t.Fatal("ActivateContext() unexpectedly succeeded")
		}
		if release != nil {
			t.Fatal("ActivateContext() returned a release after failure")
		}
		assertRecordingConnectionClosed(t, underlying.recordingDeadlineConn)
		if coordinator.connection != nil || coordinator.registration != nil {
			t.Fatal("failed activation retained poisoned transport state")
		}
		if err := wrapped.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}
	})

	t.Run("release", func(t *testing.T) {
		t.Parallel()

		underlying := &deadlineFailingConn{
			recordingDeadlineConn: newRecordingDeadlineConn(),
		}
		coordinator := newExplainDeadlineCoordinator(time.Second, nil)
		coordinator.dialConnection = staticExplainDialer(underlying)
		wrapped, err := coordinator.DialContext(
			context.Background(),
			"clickhouse.test:9000",
		)
		if err != nil {
			t.Fatalf("DialContext() error = %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		release, err := coordinator.ActivateContext(ctx)
		if err != nil {
			t.Fatalf("ActivateContext() error = %v", err)
		}
		underlying.failZero = true
		if err := release(); err == nil {
			t.Fatal("release() unexpectedly succeeded")
		}
		assertRecordingConnectionClosed(t, underlying.recordingDeadlineConn)
		if coordinator.connection != nil || coordinator.registration != nil {
			t.Fatal("failed release retained poisoned transport state")
		}
		if err := wrapped.Close(); err != nil {
			t.Fatalf("second Close() error = %v", err)
		}
	})
}

func TestExplainDeadlineCoordinatorReconnectCloseAndConcurrentRelease(
	t *testing.T,
) {
	t.Parallel()

	first := newRecordingDeadlineConn()
	rejected := newRecordingDeadlineConn()
	second := newRecordingDeadlineConn()
	third := newRecordingDeadlineConn()
	connections := make(chan net.Conn, 4)
	connections <- first
	connections <- rejected
	connections <- second
	connections <- third
	coordinator := newExplainDeadlineCoordinator(time.Second, nil)
	coordinator.dialConnection = func(
		context.Context,
		string,
		string,
		time.Duration,
		*tls.Config,
	) (net.Conn, error) {
		return <-connections, nil
	}

	firstWrapped, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	)
	if err != nil {
		t.Fatalf("first DialContext() error = %v", err)
	}
	replacementWrapped, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	)
	if err != nil {
		t.Fatalf("replacement DialContext() error = %v", err)
	}
	first.mu.Lock()
	firstClosed := first.closed
	first.mu.Unlock()
	if !firstClosed {
		t.Fatal("abandoned first connection was not closed before replacement")
	}
	if err := firstWrapped.Close(); err != nil {
		t.Fatalf("second close of abandoned connection error = %v", err)
	}
	secondWrapped, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	)
	if err != nil {
		t.Fatalf("second DialContext() after close error = %v", err)
	}
	rejected.mu.Lock()
	replacementClosed := rejected.closed
	rejected.mu.Unlock()
	if !replacementClosed {
		t.Fatal("second abandoned connection was not closed before replacement")
	}
	if err := replacementWrapped.Close(); err != nil {
		t.Fatalf("second close of replacement connection error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	release, err := coordinator.ActivateContext(ctx)
	if err != nil {
		t.Fatalf("ActivateContext() error = %v", err)
	}
	contextDeadline, _ := ctx.Deadline()
	assertRecordedDeadlines(t, second, contextDeadline, contextDeadline)
	assertRecordedDeadlines(t, first, time.Time{}, time.Time{})
	assertRecordedDeadlines(t, rejected, time.Time{}, time.Time{})

	if err := secondWrapped.Close(); err != nil {
		t.Fatalf("closing current connection error = %v", err)
	}
	cancel()
	thirdWrapped, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	)
	if err != nil {
		t.Fatalf("third DialContext() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := thirdWrapped.Close(); closeErr != nil {
			t.Errorf("third Close() error = %v", closeErr)
		}
	})
	assertRecordedDeadlinesExpired(t, third)

	const releaseCallers = 8
	releaseErrors := make(chan error, releaseCallers)
	var releaseGroup sync.WaitGroup
	releaseGroup.Add(releaseCallers)
	for range releaseCallers {
		go func() {
			defer releaseGroup.Done()
			releaseErrors <- release()
		}()
	}
	releaseGroup.Wait()
	close(releaseErrors)
	for releaseErr := range releaseErrors {
		if releaseErr != nil {
			t.Errorf("concurrent release() error = %v", releaseErr)
		}
	}
	assertRecordedDeadlines(t, third, time.Time{}, time.Time{})
}

func TestExplainDeadlineCoordinatorCloseOwnsAbandonedConnection(t *testing.T) {
	t.Parallel()

	underlying := newRecordingDeadlineConn()
	coordinator := newExplainDeadlineCoordinator(time.Second, nil)
	coordinator.dialConnection = staticExplainDialer(underlying)
	if _, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	); err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := coordinator.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	underlying.mu.Lock()
	closed := underlying.closed
	underlying.mu.Unlock()
	if !closed {
		t.Fatal("coordinator Close did not close its abandoned connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := coordinator.ActivateContext(ctx); err == nil ||
		!strings.Contains(err.Error(), "transport is closed") {
		t.Fatalf("ActivateContext() after Close error = %v", err)
	}
	if _, err := coordinator.DialContext(
		context.Background(),
		"clickhouse.test:9000",
	); err == nil || !strings.Contains(err.Error(), "transport is closed") {
		t.Fatalf("DialContext() after Close error = %v", err)
	}

	var nilCoordinator *explainDeadlineCoordinator
	if err := nilCoordinator.Close(); err == nil {
		t.Fatal("nil coordinator Close() unexpectedly succeeded")
	}
}

func TestExplainDeadlineCoordinatorDialContextPreservesConfiguration(
	t *testing.T,
) {
	t.Parallel()

	type contextKey struct{}
	const contextValue = "dial-context"
	const address = "clickhouse.test:9000"
	const dialTimeout = 137 * time.Millisecond
	originalTLSConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "clickhouse.test",
		NextProtos: []string{"clickhouse"},
	}
	coordinator := newExplainDeadlineCoordinator(dialTimeout, originalTLSConfig)
	originalTLSConfig.ServerName = "mutated.test"
	originalTLSConfig.NextProtos[0] = "mutated"

	var (
		dialCount      int
		ownedTLSConfig *tls.Config
	)
	coordinator.dialConnection = func(
		ctx context.Context,
		network string,
		gotAddress string,
		gotTimeout time.Duration,
		tlsConfig *tls.Config,
	) (net.Conn, error) {
		if network != "tcp" {
			t.Errorf("network = %q, want tcp", network)
		}
		if gotAddress != address {
			t.Errorf("address = %q, want %q", gotAddress, address)
		}
		if gotTimeout != dialTimeout {
			t.Errorf("dial timeout = %s, want %s", gotTimeout, dialTimeout)
		}
		if ctx.Value(contextKey{}) != contextValue {
			t.Errorf("dial context value = %v, want %q", ctx.Value(contextKey{}), contextValue)
		}
		if tlsConfig == nil {
			t.Fatal("TLS config is nil")
		}
		if tlsConfig == originalTLSConfig {
			t.Fatal("TLS config was not cloned")
		}
		if tlsConfig.ServerName != "clickhouse.test" {
			t.Errorf("TLS ServerName = %q, want clickhouse.test", tlsConfig.ServerName)
		}
		if len(tlsConfig.NextProtos) != 1 || tlsConfig.NextProtos[0] != "clickhouse" {
			t.Errorf("TLS NextProtos = %v, want [clickhouse]", tlsConfig.NextProtos)
		}
		if dialCount == 0 {
			ownedTLSConfig = tlsConfig
		} else if tlsConfig != ownedTLSConfig {
			t.Error("immutable coordinator TLS config was redundantly cloned")
		}
		dialCount++
		return newRecordingDeadlineConn(), nil
	}
	ctx := context.WithValue(context.Background(), contextKey{}, contextValue)
	firstConnection, err := coordinator.DialContext(ctx, address)
	if err != nil {
		t.Fatalf("first DialContext() error = %v", err)
	}
	if err := firstConnection.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	secondConnection, err := coordinator.DialContext(ctx, address)
	if err != nil {
		t.Fatalf("second DialContext() error = %v", err)
	}
	if err := secondConnection.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestExplainDeadlineCoordinatorDialsPlainTCP(t *testing.T) {
	t.Parallel()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(
		context.Background(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := listener.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener Close() error = %v", closeErr)
		}
	})
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	coordinator := newExplainDeadlineCoordinator(time.Second, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := coordinator.DialContext(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	if _, ok := connection.(*explainDeadlineConn); !ok {
		t.Fatalf("connection type = %T, want *explainDeadlineConn", connection)
	}
	assertExplainSyscallConn(t, connection)
	if err := connection.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case peer := <-accepted:
		if err := peer.Close(); err != nil {
			t.Errorf("accepted connection Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not accept plain TCP connection")
	}
}

func TestExplainDeadlineCoordinatorDialsTLSWithDetachedConfig(t *testing.T) {
	t.Parallel()

	certificate, roots := newExplainTestCertificate(t)
	var listenConfig net.ListenConfig
	baseListener, err := listenConfig.Listen(
		context.Background(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	listener := tls.NewListener(baseListener, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS12,
	})
	t.Cleanup(func() {
		if closeErr := listener.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener Close() error = %v", closeErr)
		}
	})
	serverResult := make(chan error, 1)
	serverContext, cancelServer := context.WithTimeout(context.Background(), time.Second)
	defer cancelServer()
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverResult <- acceptErr
			return
		}
		tlsConnection := connection.(*tls.Conn)
		handshakeErr := tlsConnection.HandshakeContext(serverContext)
		closeErr := tlsConnection.Close()
		serverResult <- errors.Join(handshakeErr, closeErr)
	}()

	originalTLSConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
		ServerName: "clickhouse.test",
	}
	coordinator := newExplainDeadlineCoordinator(time.Second, originalTLSConfig)
	originalTLSConfig.ServerName = "mutated.test"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := coordinator.DialContext(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	wrapped := connection.(*explainDeadlineConn)
	if _, ok := wrapped.Conn.(*tls.Conn); !ok {
		t.Fatalf("underlying connection type = %T, want *tls.Conn", wrapped.Conn)
	}
	assertExplainSyscallConn(t, connection)
	if err := connection.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-serverResult:
		if err != nil {
			t.Fatalf("TLS server error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS server did not finish")
	}
}

func TestExplainDeadlineCoordinatorTLSHandshakeHonorsContext(t *testing.T) {
	t.Parallel()

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(
		context.Background(),
		"tcp",
		"127.0.0.1:0",
	)
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := listener.Close(); closeErr != nil &&
			!errors.Is(closeErr, net.ErrClosed) {
			t.Errorf("listener Close() error = %v", closeErr)
		}
	})
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	coordinator := newExplainDeadlineCoordinator(time.Minute, &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "clickhouse.test",
	})
	ctx, cancel := context.WithCancel(context.Background())
	dialResult := make(chan error, 1)
	go func() {
		_, dialErr := coordinator.DialContext(ctx, listener.Addr().String())
		dialResult <- dialErr
	}()
	var peer net.Conn
	select {
	case peer = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("server did not accept TLS connection")
	}
	cancel()
	select {
	case err := <-dialResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DialContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("TLS handshake did not stop after context cancellation")
	}
	if err := peer.Close(); err != nil {
		t.Errorf("accepted connection Close() error = %v", err)
	}
}

type explainDialResult struct {
	connection net.Conn
	err        error
}

type recordingDeadlineConn struct {
	mu                     sync.Mutex
	readDeadline           time.Time
	writeDeadline          time.Time
	closed                 bool
	blockExpiredDeadline   chan struct{}
	expiredDeadlineEntered chan struct{}
	expiredEnteredOnce     sync.Once
}

type deadlineFailingConn struct {
	*recordingDeadlineConn

	failNonzero bool
	failZero    bool
}

func newRecordingDeadlineConn() *recordingDeadlineConn {
	return &recordingDeadlineConn{
		expiredDeadlineEntered: make(chan struct{}),
	}
}

func staticExplainDialer(connection net.Conn) explainConnectionDialer {
	return func(
		context.Context,
		string,
		string,
		time.Duration,
		*tls.Config,
	) (net.Conn, error) {
		return connection, nil
	}
}

func (connection *recordingDeadlineConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (connection *recordingDeadlineConn) Write(buffer []byte) (int, error) {
	return len(buffer), nil
}

func (connection *recordingDeadlineConn) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.mu.Unlock()
	return nil
}

func (connection *recordingDeadlineConn) LocalAddr() net.Addr {
	return explainTestAddr("local")
}

func (connection *recordingDeadlineConn) RemoteAddr() net.Addr {
	return explainTestAddr("remote")
}

func (connection *recordingDeadlineConn) SetDeadline(deadline time.Time) error {
	connection.mu.Lock()
	connection.readDeadline = deadline
	connection.writeDeadline = deadline
	block := connection.blockExpiredDeadline
	expired := !deadline.IsZero() && deadline.Before(time.Now())
	connection.mu.Unlock()
	if expired && block != nil {
		connection.expiredEnteredOnce.Do(func() {
			close(connection.expiredDeadlineEntered)
		})
		<-block
	}
	return nil
}

func (connection *deadlineFailingConn) SetDeadline(deadline time.Time) error {
	if err := connection.recordingDeadlineConn.SetDeadline(deadline); err != nil {
		return err
	}
	if deadline.IsZero() && connection.failZero {
		return errors.New("injected zero-deadline failure")
	}
	if !deadline.IsZero() && connection.failNonzero {
		return errors.New("injected nonzero-deadline failure")
	}
	return nil
}

func (connection *recordingDeadlineConn) SetReadDeadline(deadline time.Time) error {
	connection.mu.Lock()
	connection.readDeadline = deadline
	connection.mu.Unlock()
	return nil
}

func (connection *recordingDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	connection.mu.Lock()
	connection.writeDeadline = deadline
	connection.mu.Unlock()
	return nil
}

func (connection *recordingDeadlineConn) deadlines() (time.Time, time.Time) {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	return connection.readDeadline, connection.writeDeadline
}

type explainTestAddr string

func (address explainTestAddr) Network() string {
	return "test"
}

func (address explainTestAddr) String() string {
	return string(address)
}

func assertRecordedDeadlines(
	t *testing.T,
	connection *recordingDeadlineConn,
	wantRead time.Time,
	wantWrite time.Time,
) {
	t.Helper()
	gotRead, gotWrite := connection.deadlines()
	if !gotRead.Equal(wantRead) {
		t.Errorf("read deadline = %s, want %s", gotRead, wantRead)
	}
	if !gotWrite.Equal(wantWrite) {
		t.Errorf("write deadline = %s, want %s", gotWrite, wantWrite)
	}
}

func assertRecordingConnectionClosed(
	t *testing.T,
	connection *recordingDeadlineConn,
) {
	t.Helper()
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if !connection.closed {
		t.Fatal("underlying connection is not closed")
	}
}

func assertExplainSyscallConn(t *testing.T, connection net.Conn) {
	t.Helper()
	syscallConnection, ok := connection.(syscall.Conn)
	if !ok {
		t.Fatalf("connection type = %T, want syscall.Conn", connection)
	}
	rawConnection, err := syscallConnection.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn() error = %v", err)
	}
	if rawConnection == nil {
		t.Fatal("SyscallConn() returned nil")
	}
}

func assertRecordedDeadlinesExpired(
	t *testing.T,
	connection *recordingDeadlineConn,
) {
	t.Helper()
	readDeadline, writeDeadline := connection.deadlines()
	now := time.Now()
	if readDeadline.IsZero() || !readDeadline.Before(now) {
		t.Errorf("read deadline = %s, want an expired deadline", readDeadline)
	}
	if writeDeadline.IsZero() || !writeDeadline.Before(now) {
		t.Errorf("write deadline = %s, want an expired deadline", writeDeadline)
	}
}

func waitForRecordedDeadlinesExpired(
	t *testing.T,
	connection *recordingDeadlineConn,
) {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		readDeadline, writeDeadline := connection.deadlines()
		now := time.Now()
		if !readDeadline.IsZero() && readDeadline.Before(now) &&
			!writeDeadline.IsZero() && writeDeadline.Before(now) {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf(
				"deadlines did not expire: read=%s write=%s",
				readDeadline,
				writeDeadline,
			)
		}
	}
}

func newExplainTestCertificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey() error = %v", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"clickhouse.test"},
	}
	der, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		publicKey,
		privateKey,
	)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate() error = %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  privateKey,
	}, roots
}
