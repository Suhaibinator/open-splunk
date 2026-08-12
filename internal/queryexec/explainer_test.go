package queryexec

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestNewExplainerOwnsTwoOneConnectionNativeLanes(t *testing.T) {
	t.Parallel()

	options := &clickhousedriver.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "open_splunk",
			Password: "not-used-without-a-query",
		},
		DialTimeout:     4 * time.Second,
		ReadTimeout:     6 * time.Second,
		MaxOpenConns:    99,
		MaxIdleConns:    88,
		ConnMaxLifetime: 7 * time.Minute,
		ClientInfo: clickhousedriver.ClientInfo{
			Comment: []string{"administrative-plan"},
		},
	}
	explainer, err := NewExplainer(options, Config{
		MaxExecutionTime: 3 * time.Second,
		MaxThreads:       7,
	})
	if err != nil {
		t.Fatalf("NewExplainer() error = %v", err)
	}
	t.Cleanup(func() {
		if closeErr := explainer.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	})

	if cap(explainer.lanes) != maximumConcurrentExplains ||
		len(explainer.lanes) != maximumConcurrentExplains ||
		len(explainer.allLanes) != maximumConcurrentExplains ||
		!explainer.requireExecutionSeal {
		t.Fatalf(
			"lanes = len %d cap %d all %d",
			len(explainer.lanes),
			cap(explainer.lanes),
			len(explainer.allLanes),
		)
	}
	seen := make(map[*explainLane]struct{}, maximumConcurrentExplains)
	for _, lane := range explainer.allLanes {
		if lane == nil ||
			lane.activateContext == nil ||
			lane.discard == nil ||
			lane.close == nil {
			t.Fatalf("invalid lane %#v", lane)
		}
		if _, duplicate := seen[lane]; duplicate {
			t.Fatal("Explainer reused one lane object")
		}
		seen[lane] = struct{}{}
		connection, ok := lane.connection.(driver.Conn)
		if !ok {
			t.Fatalf("lane connection type = %T", lane.connection)
		}
		stats := connection.Stats()
		if stats.MaxOpenConns != 1 || stats.MaxIdleConns != 1 {
			t.Fatalf("lane pool stats = %#v, want one physical connection", stats)
		}
	}

	gotSettings, err := settingsForExplain(explainer.settings)
	if err != nil {
		t.Fatal(err)
	}
	if gotSettings["max_execution_time"] != uint64(3) ||
		gotSettings["max_threads"] != maximumExplainThreads ||
		gotSettings["readonly"] != uint8(2) ||
		explainer.executionTimeout != 3*time.Second {
		t.Fatalf("EXPLAIN settings = %#v", gotSettings)
	}
	if options.MaxOpenConns != 99 || options.MaxIdleConns != 88 ||
		options.DialContext != nil || len(options.Settings) != 0 {
		t.Fatal("NewExplainer mutated caller options")
	}
}

func TestNewExplainerRejectsUnsafeTransportOptionsWithoutDialing(t *testing.T) {
	t.Parallel()

	valid := clickhousedriver.Options{Addr: []string{"127.0.0.1:9000"}}
	tests := []struct {
		name   string
		mutate func(*clickhousedriver.Options)
	}{
		{
			name: "HTTP protocol",
			mutate: func(options *clickhousedriver.Options) {
				options.Protocol = clickhousedriver.HTTP
			},
		},
		{
			name: "no address",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = nil
			},
		},
		{
			name: "blank address",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{" "}
			},
		},
		{
			name: "padded address",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{" 127.0.0.1:9000"}
			},
		},
		{
			name: "missing port",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{"127.0.0.1"}
			},
		},
		{
			name: "zero port",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{"127.0.0.1:0"}
			},
		},
		{
			name: "plaintext remote IP",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{"192.0.2.1:9000"}
			},
		},
		{
			name: "plaintext remote name",
			mutate: func(options *clickhousedriver.Options) {
				options.Addr = []string{"clickhouse.example:9000"}
			},
		},
		{
			name: "negative dial timeout",
			mutate: func(options *clickhousedriver.Options) {
				options.DialTimeout = -time.Second
			},
		},
		{
			name: "negative read timeout",
			mutate: func(options *clickhousedriver.Options) {
				options.ReadTimeout = -time.Second
			},
		},
		{
			name: "negative connection lifetime",
			mutate: func(options *clickhousedriver.Options) {
				options.ConnMaxLifetime = -time.Second
			},
		},
		{
			name: "custom dial context",
			mutate: func(options *clickhousedriver.Options) {
				options.DialContext = func(
					ctx context.Context,
					address string,
				) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", address)
				}
			},
		},
		{
			name: "custom dial strategy",
			mutate: func(options *clickhousedriver.Options) {
				options.DialStrategy = func(
					context.Context,
					int,
					*clickhousedriver.Options,
					clickhousedriver.Dial,
				) (clickhousedriver.DialResult, error) {
					return clickhousedriver.DialResult{}, errors.New("must not run")
				}
			},
		},
		{
			name: "invalid connection strategy",
			mutate: func(options *clickhousedriver.Options) {
				options.ConnOpenStrategy = clickhousedriver.ConnOpenStrategy(255)
			},
		},
		{
			name: "connection settings",
			mutate: func(options *clickhousedriver.Options) {
				options.Settings = clickhousedriver.Settings{"readonly": uint8(0)}
			},
		},
		{
			name: "JWT callback",
			mutate: func(options *clickhousedriver.Options) {
				options.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
				options.GetJWT = func(context.Context) (string, error) {
					return "must-not-run", nil
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			options := valid
			options.Addr = append([]string(nil), valid.Addr...)
			test.mutate(&options)
			explainer, err := NewExplainer(&options, Config{})
			if err == nil || explainer != nil {
				t.Fatalf("NewExplainer() = (%#v, %v), want nil and error", explainer, err)
			}
		})
	}

	if explainer, err := NewExplainer(nil, Config{}); err == nil || explainer != nil {
		t.Fatalf("NewExplainer(nil) = (%#v, %v)", explainer, err)
	}
	invalidConfig := Config{MaxExecutionTime: -time.Second}
	if explainer, err := NewExplainer(&valid, invalidConfig); err == nil ||
		explainer != nil {
		t.Fatalf("NewExplainer(invalid config) = (%#v, %v)", explainer, err)
	}
}

func TestNewExplainerPreservesFractionalExecutionTimeout(t *testing.T) {
	t.Parallel()

	explainer, err := NewExplainer(
		&clickhousedriver.Options{Addr: []string{"127.0.0.1:9000"}},
		Config{MaxExecutionTime: 1500 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("NewExplainer() error = %v", err)
	}
	defer func() {
		if closeErr := explainer.Close(); closeErr != nil {
			t.Errorf("Close() error = %v", closeErr)
		}
	}()
	if explainer.executionTimeout != 1500*time.Millisecond ||
		explainer.settings["max_execution_time"] != uint64(2) {
		t.Fatalf(
			"execution limits = (%v, %#v), want exact 1.5s and server 2s",
			explainer.executionTimeout,
			explainer.settings["max_execution_time"],
		)
	}
}

func TestNewExplainerAcceptsLoopbackPlaintextAndRemoteTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		address string
		tls     *tls.Config
	}{
		{name: "IPv4 loopback", address: "127.0.0.1:9000"},
		{name: "IPv6 loopback", address: "[::1]:9000"},
		{name: "localhost", address: "localhost:9000"},
		{
			name:    "remote TLS",
			address: "clickhouse.example:9440",
			tls: &tls.Config{
				MinVersion: tls.VersionTLS12,
				ServerName: "clickhouse.example",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			explainer, err := NewExplainer(&clickhousedriver.Options{
				Addr: []string{test.address},
				TLS:  test.tls,
			}, Config{})
			if err != nil {
				t.Fatalf("NewExplainer() error = %v", err)
			}
			if err := explainer.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func TestValidateExplainerTLSRejectsUnboundedOrUnsafeFacilities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*tls.Config)
	}{
		{
			name: "legacy version",
			mutate: func(config *tls.Config) {
				config.MinVersion = tls.VersionTLS11
			},
		},
		{
			name: "verification disabled",
			mutate: func(config *tls.Config) {
				config.InsecureSkipVerify = true
			},
		},
		{
			name: "custom entropy",
			mutate: func(config *tls.Config) {
				config.Rand = strings.NewReader("entropy")
			},
		},
		{
			name: "custom clock",
			mutate: func(config *tls.Config) {
				config.Time = time.Now
			},
		},
		{
			name: "client certificate",
			mutate: func(config *tls.Config) {
				config.Certificates = []tls.Certificate{{}}
			},
		},
		{
			name: "verification callback",
			mutate: func(config *tls.Config) {
				config.VerifyConnection = func(tls.ConnectionState) error {
					return nil
				}
			},
		},
		{
			name: "renegotiation",
			mutate: func(config *tls.Config) {
				config.Renegotiation = tls.RenegotiateOnceAsClient
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			config := &tls.Config{MinVersion: tls.VersionTLS12}
			test.mutate(config)
			if err := validateExplainerTLS(config); err == nil {
				t.Fatal("validateExplainerTLS() unexpectedly succeeded")
			}
		})
	}
	for _, config := range []*tls.Config{
		nil,
		{},
		{MinVersion: tls.VersionTLS12, ServerName: "clickhouse.internal"},
		{MinVersion: tls.VersionTLS13, NextProtos: []string{"native"}},
	} {
		if err := validateExplainerTLS(config); err != nil {
			t.Fatalf("validateExplainerTLS(valid) error = %v", err)
		}
	}
}

func TestCloneExplainerLaneOptionsDetachesMutableConfiguration(t *testing.T) {
	t.Parallel()

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: "clickhouse.internal",
		NextProtos: []string{"native"},
	}
	options := &clickhousedriver.Options{
		Addr: []string{"clickhouse.internal:9440"},
		Auth: clickhousedriver.Auth{
			Database: "open_splunk",
			Username: "reader",
			Password: "secret",
		},
		TLS:         tlsConfig,
		DialTimeout: 20 * time.Second,
		ReadTimeout: 30 * time.Second,
		ClientInfo: clickhousedriver.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{{Name: "open-splunk", Version: "test"}},
			Comment: []string{"before"},
		},
	}
	coordinator := newExplainDeadlineCoordinator(
		explainTransportTimeout(options.DialTimeout),
		options.TLS,
	)
	cloned := cloneExplainerLaneOptions(options, coordinator)
	options.Addr[0] = "mutated:9000"
	options.ClientInfo.Products[0].Name = "mutated"
	options.ClientInfo.Comment[0] = "mutated"
	tlsConfig.ServerName = "mutated"
	tlsConfig.NextProtos[0] = "mutated"

	if cloned.Addr[0] != "clickhouse.internal:9440" ||
		cloned.ClientInfo.Products[0].Name != "open-splunk" ||
		cloned.ClientInfo.Comment[0] != "before" ||
		cloned.TLS != nil {
		t.Fatal("lane options retained caller-owned state")
	}
	if cloned.DialTimeout != maximumExplainExecutionTime ||
		cloned.ReadTimeout != maximumExplainExecutionTime ||
		cloned.MaxOpenConns != 1 ||
		cloned.MaxIdleConns != 1 ||
		cloned.BlockBufferSize != 1 ||
		!cloned.FreeBufOnConnRelease {
		t.Fatal("lane limits are invalid")
	}
	if cloned.DialContext == nil || cloned.Protocol != clickhousedriver.Native ||
		len(cloned.Settings) != 0 || cloned.Compression != nil ||
		cloned.Logger != nil {
		t.Fatal("lane options retained an unsafe facility")
	}
}

func TestExplainerTimeoutHelpersRespectStricterValuesAndFixedCaps(t *testing.T) {
	t.Parallel()

	if got := explainTransportTimeout(0); got != maximumExplainExecutionTime {
		t.Fatalf("default transport timeout = %v", got)
	}
	if got := explainTransportTimeout(2 * time.Second); got != 2*time.Second {
		t.Fatalf("stricter transport timeout = %v", got)
	}
	if got := explainTransportTimeout(time.Hour); got != maximumExplainExecutionTime {
		t.Fatalf("capped transport timeout = %v", got)
	}
	if got := explainExecutionTimeout(1500 * time.Millisecond); got !=
		1500*time.Millisecond {
		t.Fatalf("exact execution timeout = %v", got)
	}
}

func TestExplainerConstructorErrorsDoNotContainCredentials(t *testing.T) {
	t.Parallel()

	const secret = "super-secret-password"
	options := &clickhousedriver.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhousedriver.Auth{Password: secret},
		Settings: clickhousedriver.Settings{
			"custom": "invalid",
		},
	}
	_, err := NewExplainer(options, Config{})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("NewExplainer() error = %v", err)
	}
}
