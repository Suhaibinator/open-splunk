package queryexec

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

// NewExplainer constructs two isolated native ClickHouse lanes for bounded
// administrative plans. It copies address, password authentication, a
// callback-free TLS configuration, client-info, timeout, and
// connection-strategy fields from options. Logging, compression, pool sizing,
// HTTP fields, JWT callbacks, and other facilities are deliberately not
// inherited. Per-query settings are rebuilt from config and tightened by
// Explain. The caller must not supply a custom dialer, strategy, or
// connection-wide settings because the Explainer owns those safety boundaries.
//
// The returned Explainer owns both connections and must be closed after all
// inspection callers have stopped.
func NewExplainer(
	options *clickhousedriver.Options,
	config Config,
) (*Explainer, error) {
	if err := validateExplainerOptions(options); err != nil {
		return nil, err
	}
	baseSettings, err := validatedQuerySettings(config)
	if err != nil {
		return nil, fmt.Errorf("create ClickHouse EXPLAIN: %w", err)
	}
	settings, err := settingsForExplain(baseSettings)
	if err != nil {
		return nil, fmt.Errorf("create ClickHouse EXPLAIN: %w", err)
	}
	executionTimeout := explainExecutionTimeout(config.MaxExecutionTime)
	if executionTimeout <= 0 ||
		executionTimeout > maximumExplainExecutionTime {
		return nil, errors.New(
			"create ClickHouse EXPLAIN: execution timeout is invalid",
		)
	}

	lanes := make(chan *explainLane, maximumConcurrentExplains)
	allLanes := make([]*explainLane, 0, maximumConcurrentExplains)
	for range maximumConcurrentExplains {
		coordinator := newExplainDeadlineCoordinator(
			explainTransportTimeout(options.DialTimeout),
			options.TLS,
		)
		laneOptions := cloneExplainerLaneOptions(options, coordinator)
		connection, openErr := clickhousedriver.Open(laneOptions)
		if openErr != nil {
			for _, lane := range allLanes {
				if lane.close != nil {
					_ = lane.close()
				}
			}
			return nil, errors.New(
				"create ClickHouse EXPLAIN: open native transport failed",
			)
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
		settings:             settings,
		executionTimeout:     executionTimeout,
		requireExecutionSeal: true,
		lanes:                lanes,
		allLanes:             allLanes,
		newQueryID:           randomExplainQueryID,
	}, nil
}

func validateExplainerOptions(options *clickhousedriver.Options) error {
	if options == nil {
		return errors.New("create ClickHouse EXPLAIN: options are required")
	}
	if options.Protocol != clickhousedriver.Native {
		return errors.New(
			"create ClickHouse EXPLAIN: native protocol is required",
		)
	}
	if len(options.Addr) == 0 {
		return errors.New(
			"create ClickHouse EXPLAIN: at least one address is required",
		)
	}
	for index, address := range options.Addr {
		if strings.TrimSpace(address) == "" ||
			address != strings.TrimSpace(address) {
			return errors.New(
				"create ClickHouse EXPLAIN: address is invalid",
			)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil || host == "" {
			return fmt.Errorf(
				"create ClickHouse EXPLAIN: address at position %d is invalid",
				index,
			)
		}
		portNumber, err := strconv.ParseUint(port, 10, 16)
		if err != nil || portNumber == 0 {
			return fmt.Errorf(
				"create ClickHouse EXPLAIN: address at position %d is invalid",
				index,
			)
		}
		if options.TLS == nil && !isExplainLoopbackHost(host) {
			return fmt.Errorf(
				"create ClickHouse EXPLAIN: TLS is required for "+
					"non-loopback address at position %d",
				index,
			)
		}
	}
	if options.DialTimeout < 0 {
		return errors.New(
			"create ClickHouse EXPLAIN: dial timeout cannot be negative",
		)
	}
	if options.ReadTimeout < 0 {
		return errors.New(
			"create ClickHouse EXPLAIN: read timeout cannot be negative",
		)
	}
	if options.ConnMaxLifetime < 0 {
		return errors.New(
			"create ClickHouse EXPLAIN: connection lifetime cannot be negative",
		)
	}
	if options.DialContext != nil {
		return errors.New(
			"create ClickHouse EXPLAIN: custom dial context is not allowed",
		)
	}
	if options.DialStrategy != nil {
		return errors.New(
			"create ClickHouse EXPLAIN: custom dial strategy is not allowed",
		)
	}
	switch options.ConnOpenStrategy {
	case clickhousedriver.ConnOpenInOrder,
		clickhousedriver.ConnOpenRoundRobin,
		clickhousedriver.ConnOpenRandom:
	default:
		return errors.New(
			"create ClickHouse EXPLAIN: connection strategy is invalid",
		)
	}
	if len(options.Settings) != 0 {
		return errors.New(
			"create ClickHouse EXPLAIN: connection settings are not allowed",
		)
	}
	if options.GetJWT != nil {
		return errors.New(
			"create ClickHouse EXPLAIN: JWT callbacks are not allowed",
		)
	}
	if err := validateExplainerTLS(options.TLS); err != nil {
		return err
	}
	return nil
}

func isExplainLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validateExplainerTLS(config *tls.Config) error {
	if config == nil {
		return nil
	}
	if config.MinVersion != 0 && config.MinVersion < tls.VersionTLS12 {
		return errors.New(
			"create ClickHouse EXPLAIN: TLS 1.2 or newer is required",
		)
	}
	if config.InsecureSkipVerify {
		return errors.New(
			"create ClickHouse EXPLAIN: TLS certificate verification is required",
		)
	}
	if config.Rand != nil ||
		config.Time != nil ||
		len(config.Certificates) != 0 ||
		len(config.NameToCertificate) != 0 || //nolint:staticcheck // reject the deprecated certificate map
		config.GetCertificate != nil ||
		config.GetClientCertificate != nil ||
		config.GetConfigForClient != nil ||
		config.VerifyPeerCertificate != nil ||
		config.VerifyConnection != nil ||
		config.ClientSessionCache != nil ||
		config.UnwrapSession != nil ||
		config.WrapSession != nil ||
		config.KeyLogWriter != nil ||
		config.EncryptedClientHelloRejectionVerify != nil ||
		config.GetEncryptedClientHelloKeys != nil {
		return errors.New(
			"create ClickHouse EXPLAIN: TLS callbacks, client certificates, " +
				"custom entropy, sessions, and key logging are not allowed",
		)
	}
	if config.Renegotiation != tls.RenegotiateNever {
		return errors.New(
			"create ClickHouse EXPLAIN: TLS renegotiation is not allowed",
		)
	}
	return nil
}

func cloneExplainerLaneOptions(
	options *clickhousedriver.Options,
	coordinator *explainDeadlineCoordinator,
) *clickhousedriver.Options {
	clientInfo := options.ClientInfo
	clientInfo.Products = slices.Clone(clientInfo.Products)
	clientInfo.Comment = slices.Clone(clientInfo.Comment)

	return &clickhousedriver.Options{
		Protocol:             clickhousedriver.Native,
		ClientInfo:           clientInfo,
		Addr:                 slices.Clone(options.Addr),
		Auth:                 options.Auth,
		DialContext:          coordinator.DialContext,
		DialTimeout:          explainTransportTimeout(options.DialTimeout),
		ReadTimeout:          explainTransportTimeout(options.ReadTimeout),
		MaxOpenConns:         1,
		MaxIdleConns:         1,
		ConnMaxLifetime:      options.ConnMaxLifetime,
		ConnOpenStrategy:     options.ConnOpenStrategy,
		FreeBufOnConnRelease: true,
		BlockBufferSize:      1,
	}
}

func explainTransportTimeout(configured time.Duration) time.Duration {
	if configured == 0 {
		return maximumExplainExecutionTime
	}
	return min(configured, maximumExplainExecutionTime)
}

func explainExecutionTimeout(configured time.Duration) time.Duration {
	if configured == 0 {
		configured = defaultMaxExecutionTime
	}
	return min(configured, maximumExplainExecutionTime)
}
