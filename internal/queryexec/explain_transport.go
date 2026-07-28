package queryexec

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"syscall"
	"time"
)

// explainDeadlineCoordinator owns the deadline state for one dedicated native
// ClickHouse connection lane. A lane admits exactly one active EXPLAIN
// registration; separate lanes deliberately share no deadline or connection
// state.
type explainDeadlineCoordinator struct {
	mu             sync.Mutex
	dialTimeout    time.Duration
	tlsConfig      *tls.Config
	dialConnection explainConnectionDialer
	connection     *explainDeadlineConn
	registration   *explainDeadlineRegistration
	closed         bool
	closeOnce      sync.Once
	closeErr       error
}

type explainDeadlineRegistration struct {
	ctx      context.Context
	deadline time.Time
	canceled bool
	applyErr error
}

type explainDeadlineConn struct {
	net.Conn

	coordinator   *explainDeadlineCoordinator
	readDeadline  time.Time
	writeDeadline time.Time
	closed        bool
	closeOnce     sync.Once
	closeErr      error
}

type explainConnectionDialer func(
	context.Context,
	string,
	string,
	time.Duration,
	*tls.Config,
) (net.Conn, error)

// newExplainDeadlineCoordinator constructs the custom DialContext used by one
// native ClickHouse EXPLAIN lane. clickhouse-go bypasses its built-in TLS path
// whenever DialContext is set, so the transport retains its own immutable TLS
// configuration and performs the TLS handshake itself.
func newExplainDeadlineCoordinator(
	dialTimeout time.Duration,
	tlsConfig *tls.Config,
) *explainDeadlineCoordinator {
	var detachedTLSConfig *tls.Config
	if tlsConfig != nil {
		detachedTLSConfig = cloneExplainTLSConfig(tlsConfig)
	}
	return &explainDeadlineCoordinator{
		dialTimeout:    dialTimeout,
		tlsConfig:      detachedTLSConfig,
		dialConnection: dialExplainConnection,
	}
}

// DialContext is suitable for clickhouse.Options.DialContext. It attaches the
// newly established connection before returning it so an already-canceled
// registration cannot race past the initial native-protocol write without an
// expired socket deadline.
func (coordinator *explainDeadlineCoordinator) DialContext(
	ctx context.Context,
	address string,
) (net.Conn, error) {
	if coordinator == nil {
		return nil, errors.New("dial ClickHouse EXPLAIN connection: transport is nil")
	}
	if ctx == nil {
		return nil, errors.New("dial ClickHouse EXPLAIN connection: context is nil")
	}
	if coordinator.dialTimeout < 0 {
		return nil, errors.New(
			"dial ClickHouse EXPLAIN connection: timeout cannot be negative",
		)
	}

	// clickhouse-go does not close a custom-dialed net.Conn when its native
	// handshake or addendum fails. With MaxOpenConns=1, a later DialContext
	// call on this lane proves the retained connection was abandoned before
	// entering the driver pool. Close and detach it before retrying another
	// configured address or a later request.
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, errors.New(
			"dial ClickHouse EXPLAIN connection: transport is closed",
		)
	}
	abandoned := coordinator.connection
	coordinator.connection = nil
	coordinator.mu.Unlock()
	if abandoned != nil {
		if err := abandoned.Close(); err != nil {
			return nil, errors.New(
				"dial ClickHouse EXPLAIN connection: close abandoned connection failed",
			)
		}
	}

	connection, err := coordinator.dialConnection(
		ctx,
		"tcp",
		address,
		coordinator.dialTimeout,
		coordinator.tlsConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("dial ClickHouse EXPLAIN connection: %w", err)
	}
	if connection == nil {
		return nil, errors.New(
			"dial ClickHouse EXPLAIN connection: dialer returned a nil connection",
		)
	}

	wrapped := &explainDeadlineConn{
		Conn:        connection,
		coordinator: coordinator,
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		closeErr := wrapped.Close()
		return nil, fmt.Errorf(
			"dial ClickHouse EXPLAIN connection: transport closed during dial: %w",
			errors.Join(net.ErrClosed, closeErr),
		)
	}
	if current := coordinator.connection; current != nil && !current.closed {
		coordinator.mu.Unlock()
		closeErr := wrapped.Close()
		return nil, fmt.Errorf(
			"dial ClickHouse EXPLAIN connection: concurrent dial is not allowed: %w",
			errors.Join(net.ErrClosed, closeErr),
		)
	}
	coordinator.connection = wrapped
	if registration := coordinator.registration; registration != nil &&
		registration.ctx.Err() != nil {
		registration.canceled = true
	}
	applyErr := coordinator.applyDeadlinesLocked(wrapped)
	if applyErr != nil && coordinator.connection == wrapped {
		coordinator.connection = nil
	}
	coordinator.mu.Unlock()
	if applyErr != nil {
		closeErr := wrapped.Close()
		return nil, fmt.Errorf(
			"dial ClickHouse EXPLAIN connection: apply active deadline: %w",
			errors.Join(applyErr, closeErr),
		)
	}
	return wrapped, nil
}

// ActivateContext overlays the exact context deadline on the lane,
// expires the socket immediately when the context is canceled, and returns an
// idempotent release function. Release stops and joins the cancellation
func (coordinator *explainDeadlineCoordinator) ActivateContext(
	ctx context.Context,
) (func() error, error) {
	if coordinator == nil {
		return nil, errors.New(
			"register ClickHouse EXPLAIN transport deadline: transport is nil",
		)
	}
	if ctx == nil {
		return nil, errors.New(
			"register ClickHouse EXPLAIN transport deadline: context is nil",
		)
	}
	deadline, ok := ctx.Deadline()
	if !ok || deadline.IsZero() {
		return nil, errors.New(
			"register ClickHouse EXPLAIN transport deadline: context deadline is required",
		)
	}

	registration := &explainDeadlineRegistration{
		ctx:      ctx,
		deadline: deadline,
		canceled: ctx.Err() != nil,
	}
	coordinator.mu.Lock()
	if coordinator.closed {
		coordinator.mu.Unlock()
		return nil, errors.New(
			"register ClickHouse EXPLAIN transport deadline: transport is closed",
		)
	}
	if coordinator.registration != nil {
		coordinator.mu.Unlock()
		return nil, errors.New(
			"register ClickHouse EXPLAIN transport deadline: registration already active",
		)
	}
	coordinator.registration = registration
	applyErr := coordinator.applyDeadlinesLocked(coordinator.connection)
	if applyErr != nil {
		coordinator.registration = nil
		restoreErr := coordinator.applyDeadlinesLocked(coordinator.connection)
		connection := coordinator.connection
		coordinator.connection = nil
		coordinator.mu.Unlock()
		var closeErr error
		if connection != nil {
			closeErr = connection.Close()
		}
		return nil, fmt.Errorf(
			"register ClickHouse EXPLAIN transport deadline: apply deadline: %w",
			errors.Join(applyErr, restoreErr, closeErr),
		)
	}
	coordinator.mu.Unlock()

	callbackDone := make(chan struct{})
	stopCallback := context.AfterFunc(ctx, func() {
		coordinator.cancelRegistration(registration)
		close(callbackDone)
	})
	var (
		releaseOnce sync.Once
		releaseErr  error
	)
	release := func() error {
		releaseOnce.Do(func() {
			if stopCallback() {
				close(callbackDone)
			}
			<-callbackDone
			releaseErr = coordinator.releaseRegistration(registration)
		})
		return releaseErr
	}
	return release, nil
}

func (coordinator *explainDeadlineCoordinator) cancelRegistration(
	registration *explainDeadlineRegistration,
) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.registration != registration || registration.canceled {
		return
	}
	registration.canceled = true
	if err := coordinator.applyDeadlinesLocked(coordinator.connection); err != nil {
		registration.applyErr = errors.Join(registration.applyErr, err)
	}
}

func (coordinator *explainDeadlineCoordinator) releaseRegistration(
	registration *explainDeadlineRegistration,
) error {
	coordinator.mu.Lock()
	if coordinator.registration != registration {
		coordinator.mu.Unlock()
		return errors.New(
			"release ClickHouse EXPLAIN transport deadline: registration is not active",
		)
	}
	coordinator.registration = nil
	restoreErr := coordinator.applyDeadlinesLocked(coordinator.connection)
	releaseErr := errors.Join(registration.applyErr, restoreErr)
	var connection *explainDeadlineConn
	if releaseErr != nil {
		connection = coordinator.connection
		coordinator.connection = nil
	}
	coordinator.mu.Unlock()
	if releaseErr != nil {
		var closeErr error
		if connection != nil {
			closeErr = connection.Close()
		}
		return fmt.Errorf(
			"release ClickHouse EXPLAIN transport deadline: restore deadline: %w",
			errors.Join(releaseErr, closeErr),
		)
	}
	return nil
}

func (coordinator *explainDeadlineCoordinator) setReadDeadline(
	connection *explainDeadlineConn,
	deadline time.Time,
) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if connection.closed {
		return net.ErrClosed
	}
	previous := connection.readDeadline
	connection.readDeadline = deadline
	if err := connection.Conn.SetReadDeadline(
		coordinator.effectiveDeadlineLocked(deadline),
	); err != nil {
		connection.readDeadline = previous
		restoreErr := connection.Conn.SetReadDeadline(
			coordinator.effectiveDeadlineLocked(previous),
		)
		return errors.Join(err, restoreErr)
	}
	return nil
}

func (coordinator *explainDeadlineCoordinator) setWriteDeadline(
	connection *explainDeadlineConn,
	deadline time.Time,
) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if connection.closed {
		return net.ErrClosed
	}
	previous := connection.writeDeadline
	connection.writeDeadline = deadline
	if err := connection.Conn.SetWriteDeadline(
		coordinator.effectiveDeadlineLocked(deadline),
	); err != nil {
		connection.writeDeadline = previous
		restoreErr := connection.Conn.SetWriteDeadline(
			coordinator.effectiveDeadlineLocked(previous),
		)
		return errors.Join(err, restoreErr)
	}
	return nil
}

func (coordinator *explainDeadlineCoordinator) setDeadline(
	connection *explainDeadlineConn,
	deadline time.Time,
) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if connection.closed {
		return net.ErrClosed
	}
	previousRead := connection.readDeadline
	previousWrite := connection.writeDeadline
	connection.readDeadline = deadline
	connection.writeDeadline = deadline
	if err := connection.Conn.SetDeadline(
		coordinator.effectiveDeadlineLocked(deadline),
	); err != nil {
		connection.readDeadline = previousRead
		connection.writeDeadline = previousWrite
		restoreErr := coordinator.applyDeadlinesLocked(connection)
		return errors.Join(err, restoreErr)
	}
	return nil
}

func (coordinator *explainDeadlineCoordinator) applyDeadlinesLocked(
	connection *explainDeadlineConn,
) error {
	if connection == nil || connection.closed {
		return nil
	}
	readDeadline := coordinator.effectiveDeadlineLocked(connection.readDeadline)
	writeDeadline := coordinator.effectiveDeadlineLocked(connection.writeDeadline)
	if readDeadline.Equal(writeDeadline) {
		return connection.Conn.SetDeadline(readDeadline)
	}
	return errors.Join(
		connection.Conn.SetReadDeadline(readDeadline),
		connection.Conn.SetWriteDeadline(writeDeadline),
	)
}

func (coordinator *explainDeadlineCoordinator) effectiveDeadlineLocked(
	driverDeadline time.Time,
) time.Time {
	registration := coordinator.registration
	if registration == nil {
		return driverDeadline
	}
	explainDeadline := registration.deadline
	if registration.canceled {
		explainDeadline = time.Unix(1, 0)
	}
	return earlierNonzeroDeadline(driverDeadline, explainDeadline)
}

func earlierNonzeroDeadline(left, right time.Time) time.Time {
	switch {
	case left.IsZero():
		return right
	case right.IsZero():
		return left
	case left.Before(right):
		return left
	default:
		return right
	}
}

func dialExplainConnection(
	ctx context.Context,
	network string,
	address string,
	dialTimeout time.Duration,
	tlsConfig *tls.Config,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: dialTimeout}
	if tlsConfig == nil {
		return dialer.DialContext(ctx, network, address)
	}
	tlsDialer := &tls.Dialer{
		NetDialer: dialer,
		Config:    tlsConfig,
	}
	return tlsDialer.DialContext(ctx, network, address)
}

func cloneExplainTLSConfig(config *tls.Config) *tls.Config {
	if config == nil {
		return nil
	}
	cloned := config.Clone()
	cloned.NextProtos = slices.Clone(config.NextProtos)
	cloned.CipherSuites = slices.Clone(config.CipherSuites)
	cloned.CurvePreferences = slices.Clone(config.CurvePreferences)
	cloned.EncryptedClientHelloConfigList = slices.Clone(
		config.EncryptedClientHelloConfigList,
	)
	cloned.Certificates = make([]tls.Certificate, len(config.Certificates))
	for index := range config.Certificates {
		cloned.Certificates[index] = cloneExplainTLSCertificate(
			config.Certificates[index],
		)
	}
	if config.RootCAs != nil {
		cloned.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCAs != nil {
		cloned.ClientCAs = config.ClientCAs.Clone()
	}
	cloned.EncryptedClientHelloKeys = make(
		[]tls.EncryptedClientHelloKey,
		len(config.EncryptedClientHelloKeys),
	)
	for index, key := range config.EncryptedClientHelloKeys {
		key.Config = slices.Clone(key.Config)
		key.PrivateKey = slices.Clone(key.PrivateKey)
		cloned.EncryptedClientHelloKeys[index] = key
	}
	return cloned
}

func cloneExplainTLSCertificate(certificate tls.Certificate) tls.Certificate {
	cloned := certificate
	cloned.Certificate = cloneExplainByteSlices(certificate.Certificate)
	cloned.SupportedSignatureAlgorithms = slices.Clone(
		certificate.SupportedSignatureAlgorithms,
	)
	cloned.OCSPStaple = slices.Clone(certificate.OCSPStaple)
	cloned.SignedCertificateTimestamps = cloneExplainByteSlices(
		certificate.SignedCertificateTimestamps,
	)
	return cloned
}

func cloneExplainByteSlices(values [][]byte) [][]byte {
	if values == nil {
		return nil
	}
	cloned := make([][]byte, len(values))
	for index, value := range values {
		cloned[index] = slices.Clone(value)
	}
	return cloned
}

// Close releases a custom-dialed connection that clickhouse-go abandoned
// before adding it to its pool. Driver-owned idle connections normally detach
// themselves first, making this a no-op.
func (coordinator *explainDeadlineCoordinator) Close() error {
	if coordinator == nil {
		return errors.New("close ClickHouse EXPLAIN transport: transport is nil")
	}
	coordinator.closeOnce.Do(func() {
		coordinator.mu.Lock()
		coordinator.closed = true
		connection := coordinator.connection
		coordinator.connection = nil
		coordinator.mu.Unlock()
		if connection != nil {
			if err := connection.Close(); err != nil {
				coordinator.closeErr = errors.New(
					"close ClickHouse EXPLAIN transport connection failed",
				)
			}
		}
	})
	return coordinator.closeErr
}

func (connection *explainDeadlineConn) SetDeadline(deadline time.Time) error {
	return connection.coordinator.setDeadline(connection, deadline)
}

func (connection *explainDeadlineConn) SetReadDeadline(deadline time.Time) error {
	return connection.coordinator.setReadDeadline(connection, deadline)
}

func (connection *explainDeadlineConn) SetWriteDeadline(deadline time.Time) error {
	return connection.coordinator.setWriteDeadline(connection, deadline)
}

// SyscallConn preserves clickhouse-go's stale-socket probe. The driver cannot
// see through this deadline wrapper, and tls.Conn deliberately exposes the
// raw transport through NetConn rather than implementing syscall.Conn itself.
func (connection *explainDeadlineConn) SyscallConn() (syscall.RawConn, error) {
	if connection == nil || connection.Conn == nil {
		return nil, errors.New(
			"access ClickHouse EXPLAIN raw connection: connection is nil",
		)
	}
	rawConnection := connection.Conn
	if tlsConnection, ok := rawConnection.(*tls.Conn); ok {
		rawConnection = tlsConnection.NetConn()
	}
	syscallConnection, ok := rawConnection.(syscall.Conn)
	if !ok {
		return nil, errors.New(
			"access ClickHouse EXPLAIN raw connection: syscall connection is unavailable",
		)
	}
	rawConn, err := syscallConnection.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf(
			"access ClickHouse EXPLAIN raw connection: %w",
			err,
		)
	}
	return rawConn, nil
}

func (connection *explainDeadlineConn) Close() error {
	connection.closeOnce.Do(func() {
		connection.coordinator.mu.Lock()
		connection.closed = true
		if connection.coordinator.connection == connection {
			connection.coordinator.connection = nil
		}
		connection.coordinator.mu.Unlock()
		connection.closeErr = connection.Conn.Close()
	})
	return connection.closeErr
}
