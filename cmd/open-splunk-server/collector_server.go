package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Register the negotiated collector compressor.
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

const (
	collectorMaxConcurrentStreams = 8
	collectorMaxConnections       = 64
	collectorMaxActiveStreams     = collectorfleet.MaximumActiveCollectors
	collectorMaxHeaderBytes       = 16 << 10
	collectorConnectionTimeout    = 10 * time.Second
	collectorReadyTimeout         = 30 * time.Second
)

type collectorServerConfig struct {
	Address     string
	Insecure    bool
	TLSCertFile string
	TLSKeyFile  string
}

// openCollectorServer returns nil values when the collector listener is not
// configured. A configured listener is TLS-only unless Insecure is explicitly
// selected for a loopback address.
func openCollectorServer(
	ctx context.Context,
	config collectorServerConfig,
	service opensplunk.CollectorIngestServiceServer,
) (*grpc.Server, net.Listener, error) {
	if err := validateCollectorServerConfig(config); err != nil {
		return nil, nil, err
	}
	address := strings.TrimSpace(config.Address)
	if address == "" {
		return nil, nil, nil
	}
	if service == nil {
		return nil, nil, errors.New("collector gRPC service is required")
	}

	serverOptions, err := collectorGRPCServerOptions(config)
	if err != nil {
		return nil, nil, err
	}
	rawListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", address)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for collector gRPC: %w", err)
	}
	listener := newConnectionLimitedListener(rawListener, collectorMaxConnections)
	serverOptions = append(serverOptions, grpc.StatsHandler(listener))
	server := grpc.NewServer(serverOptions...)
	opensplunk.RegisterCollectorIngestServiceServer(server, service)
	return server, listener, nil
}

// optionalRuntimeGRPCServer prevents a nil *grpc.Server from becoming a
// non-nil interface when collector ingestion is disabled.
func optionalRuntimeGRPCServer(server *grpc.Server) runtimeGRPCServer {
	if server == nil {
		return nil
	}
	return server
}

// validateCollectorServerConfig is pure so run can reject an invalid disabled
// or configured transport before opening or mutating either persistence plane.
func validateCollectorServerConfig(config collectorServerConfig) error {
	address := strings.TrimSpace(config.Address)
	certFile := strings.TrimSpace(config.TLSCertFile)
	keyFile := strings.TrimSpace(config.TLSKeyFile)
	if address == "" {
		if config.Insecure || certFile != "" || keyFile != "" {
			return errors.New(
				"collector gRPC address is required when transport options are configured",
			)
		}
		return nil
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("collector gRPC address must be host:port: %w", err)
	}
	if config.Insecure {
		if certFile != "" || keyFile != "" {
			return errors.New(
				"collector gRPC cannot combine plaintext mode with TLS certificate options",
			)
		}
		if !loopbackAddress(address) {
			return errors.New(
				"collector gRPC plaintext is allowed only for a loopback address",
			)
		}
		return nil
	}
	if certFile == "" || keyFile == "" {
		return errors.New(
			"collector gRPC TLS certificate and key are required; use -collector-grpc-plaintext-enabled only for loopback development",
		)
	}
	return nil
}

func collectorGRPCServerOptions(config collectorServerConfig) ([]grpc.ServerOption, error) {
	if err := validateCollectorServerConfig(config); err != nil {
		return nil, err
	}
	certFile := strings.TrimSpace(config.TLSCertFile)
	keyFile := strings.TrimSpace(config.TLSKeyFile)
	if !config.Insecure {
		tlsConfig, err := loadServerTLSConfig(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load collector gRPC TLS certificate: %w", err)
		}
		return append(
			collectorResourceServerOptions(),
			grpc.Creds(credentials.NewTLS(tlsConfig)),
		), nil
	}
	return collectorResourceServerOptions(), nil
}

func collectorResourceServerOptions() []grpc.ServerOption {
	// The wire event payload is capped at HardMaxBatchBytes. A separate bounded
	// allowance covers its request/batch envelopes and repeated-field framing;
	// server-owned normalized outbox expansion must not inflate the untrusted
	// gRPC allocation ceiling.
	maxReceiveBytes := int(ingest.HardMaxBatchBytes + ingest.HardMaxDurableMetadataBytes)
	maxSendBytes := int(ingest.HardMaxCollectResponseBytes)
	return []grpc.ServerOption{
		grpc.ConnectionTimeout(collectorConnectionTimeout),
		grpc.MaxConcurrentStreams(collectorMaxConcurrentStreams),
		grpc.MaxHeaderListSize(collectorMaxHeaderBytes),
		grpc.MaxRecvMsgSize(maxReceiveBytes),
		grpc.MaxSendMsgSize(maxSendBytes),
		grpc.ChainStreamInterceptor(concurrentStreamLimit(collectorMaxActiveStreams)),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute,
			MaxConnectionAge:      24 * time.Hour,
			MaxConnectionAgeGrace: time.Minute,
			Time:                  2 * time.Minute,
			Timeout:               20 * time.Second,
		}),
	}
}

func concurrentStreamLimit(limit int) grpc.StreamServerInterceptor {
	slots := make(chan struct{}, limit)
	return func(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			return handler(server, stream)
		default:
			return status.Error(codes.ResourceExhausted, "collector stream capacity is exhausted")
		}
	}
}

type connectionLimitedListener struct {
	net.Listener
	slots        chan struct{}
	readyTimeout time.Duration
	mu           sync.Mutex
	connections  map[net.Addr]*limitedConnection
}

func newConnectionLimitedListener(listener net.Listener, limit int) *connectionLimitedListener {
	return &connectionLimitedListener{
		Listener: listener, slots: make(chan struct{}, limit),
		readyTimeout: collectorReadyTimeout, connections: make(map[net.Addr]*limitedConnection),
	}
}

func (listener *connectionLimitedListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case listener.slots <- struct{}{}:
			// TCP connections return a stable *net.TCPAddr, which TLS preserves.
			// Keep its identity, not its string: a later connection can reuse the
			// same endpoint without inheriting an earlier connection's Ready.
			address := connection.RemoteAddr()
			limited := &limitedConnection{Conn: connection}
			limited.release = func() {
				listener.mu.Lock()
				if listener.connections[address] == limited {
					delete(listener.connections, address)
				}
				listener.mu.Unlock()
				<-listener.slots
			}
			listener.mu.Lock()
			listener.connections[address] = limited
			listener.mu.Unlock()
			limited.mu.Lock()
			// gRPC clears socket deadlines after transport setup. This separate
			// timer also bounds idle HTTP/2 and unsuccessful RPC activity.
			limited.timer = time.AfterFunc(listener.readyTimeout, limited.expireBeforeReady)
			limited.mu.Unlock()
			return limited, nil
		default:
			_ = connection.Close()
		}
	}
}

type limitedConnection struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
	mu          sync.Mutex
	timer       *time.Timer
	ready       bool
	closed      bool
}

func (connection *limitedConnection) expireBeforeReady() {
	connection.mu.Lock()
	if connection.ready || connection.closed {
		connection.mu.Unlock()
		return
	}
	connection.closed = true
	connection.mu.Unlock()
	_ = connection.Close()
}

func (connection *limitedConnection) markReady() {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if !connection.closed {
		connection.ready = true
		connection.timer.Stop()
	}
}

func (connection *limitedConnection) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.timer.Stop()
	connection.mu.Unlock()
	err := connection.Conn.Close()
	connection.releaseOnce.Do(connection.release)
	return err
}

type collectorConnectionContextKey struct{}

func (listener *connectionLimitedListener) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	listener.mu.Lock()
	connection := listener.connections[info.RemoteAddr]
	listener.mu.Unlock()
	return context.WithValue(ctx, collectorConnectionContextKey{}, connection)
}

func (*connectionLimitedListener) HandleConn(context.Context, stats.ConnStats) {}

func (*connectionLimitedListener) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	if info.FullMethodName != opensplunk.CollectorIngestService_Collect_FullMethodName {
		return context.WithValue(ctx, collectorConnectionContextKey{}, (*limitedConnection)(nil))
	}
	return ctx
}

func (*connectionLimitedListener) HandleRPC(ctx context.Context, event stats.RPCStats) {
	payload, ok := event.(*stats.OutPayload)
	if !ok || payload.Client {
		return
	}
	response, ok := payload.Payload.(*opensplunk.CollectResponse)
	if !ok || response.GetReady() == nil {
		return
	}
	// Only the server's successful Ready proves bearer authentication and
	// collector session admission. Incoming traffic cannot extend this budget.
	connection, _ := ctx.Value(collectorConnectionContextKey{}).(*limitedConnection)
	if connection != nil {
		connection.markReady()
	}
}

type gracefulGRPCServer interface {
	GracefulStop()
	Stop()
}

// shutdownGRPCServer allows active collector RPCs to finish, then forcibly
// cancels them at the deadline. It always waits for GracefulStop to return so
// the caller may safely close the ingestion store afterward.
func shutdownGRPCServer(server gracefulGRPCServer, timeout time.Duration) error {
	if server == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		server.Stop()
		<-done
		return errors.New("graceful collector gRPC shutdown timed out")
	}
}
