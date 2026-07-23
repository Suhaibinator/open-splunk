// Package searchws serves the binary protobuf search and export progress
// WebSocket. It polls each unique scoped job target once, coalesces identical
// snapshots, and retains bounded per-target replay state.
package searchws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	defaultMaximumConnections      = 128
	defaultMaximumSubscriptions    = uint32(64)
	defaultMaximumPreviewRows      = uint32(100)
	defaultMaximumFrameBytes       = uint64(1 << 20)
	defaultMaximumQueuedFrames     = 512
	defaultMaximumQueuedBytes      = uint64(2 << 20)
	defaultMaximumTotalQueuedBytes = uint64(64 << 20)
	defaultMaximumTargets          = 1_024
	defaultMaximumProjections      = 16
	defaultMaximumReplayEvents     = 256
	defaultMaximumReplayBytes      = uint64(4 << 20)
	defaultMaximumTotalReplayBytes = uint64(64 << 20)
	defaultPollInterval            = 250 * time.Millisecond
	defaultWriteTimeout            = 10 * time.Second
	defaultPongTimeout             = 60 * time.Second
	defaultPingInterval            = 20 * time.Second

	minimumFrameBytes              = uint64(1 << 10)
	minimumQueuedFrames            = 7
	maximumConnectionsCeiling      = 4_096
	maximumSubscriptionsCeiling    = uint32(256)
	maximumPreviewRowsCeiling      = uint32(1_000)
	maximumFrameBytesCeiling       = uint64(1 << 20)
	maximumQueuedFramesCeiling     = 4_096
	maximumQueuedBytesCeiling      = uint64(64 << 20)
	maximumTotalQueuedBytesCeiling = uint64(4 << 30)
	maximumTargetsCeiling          = 100_000
	maximumProjectionsCeiling      = 256
	maximumReplayEventsCeiling     = 10_000
	maximumReplayBytesCeiling      = uint64(64 << 20)
	maximumTotalReplayBytesCeiling = uint64(4 << 30)
	maximumIdentityBytes           = 1 << 10
	maximumPollInterval            = time.Minute
	maximumTransportTimeout        = 10 * time.Minute
	minimumPollInterval            = 5 * time.Millisecond
	minimumWriteTimeout            = 50 * time.Millisecond
	minimumPongTimeout             = 100 * time.Millisecond
	minimumPingInterval            = 25 * time.Millisecond
	maximumRequestIDBytes          = 256
	maximumSubscriptionIDBytes     = 256
	maximumTargetIDBytes           = maximumIdentityBytes
	maximumPingNonceBytes          = 256
)

// SearchSnapshots is the only search-job capability used by Service. Manager
// satisfies it directly, including cross-scope non-disclosure through
// searchjobs.ErrNotFound.
type SearchSnapshots interface {
	GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error)
	PreviewForBytes(searchjobs.AccessScope, string, int, uint64) (searchjobs.PreviewSnapshot, error)
	MaximumPreviewRows() uint32
}

// ExportSnapshots is the only export-job capability used by Service. Manager
// satisfies it directly and observes the polling context during shutdown.
type ExportSnapshots interface {
	Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error)
}

// Limits is the drift-free subset advertised by the browser bootstrap API.
type Limits struct {
	MaximumSubscriptions uint32
	MaximumPreviewRows   uint32
	MaximumFrameBytes    uint64
}

// Config defines every resource and liveness bound owned by Service. Zero
// values select conservative defaults; values above hard ceilings are rejected.
type Config struct {
	Searches SearchSnapshots
	Exports  ExportSnapshots
	Access   searchjobs.AccessScope

	// CheckOrigin is passed to Gorilla's upgrader. Nil permits requests without
	// Origin and otherwise requires Origin.Host to equal request.Host.
	CheckOrigin func(*http.Request) bool

	MaximumConnections           int
	MaximumSubscriptions         uint32
	MaximumPreviewRows           uint32
	MaximumFrameBytes            uint64
	MaximumQueuedFrames          int
	MaximumQueuedBytes           uint64
	MaximumTotalQueuedBytes      uint64
	MaximumTargets               int
	MaximumConcurrentProjections int
	MaximumReplayEvents          int
	MaximumReplayBytes           uint64
	MaximumTotalReplayBytes      uint64

	PollInterval time.Duration
	WriteTimeout time.Duration
	PongTimeout  time.Duration
	PingInterval time.Duration
	Now          func() time.Time
}

type normalizedConfig struct {
	searches SearchSnapshots
	exports  ExportSnapshots
	access   searchjobs.AccessScope

	checkOrigin func(*http.Request) bool

	maximumConnections           int
	maximumSubscriptions         uint32
	maximumPreviewRows           uint32
	maximumFrameBytes            uint64
	maximumQueuedFrames          int
	maximumQueuedBytes           uint64
	maximumTotalQueuedBytes      uint64
	maximumTargets               int
	maximumConcurrentProjections int
	maximumReplayEvents          int
	maximumReplayBytes           uint64
	maximumTotalReplayBytes      uint64

	pollInterval time.Duration
	writeTimeout time.Duration
	pongTimeout  time.Duration
	pingInterval time.Duration
	now          func() time.Time
}

func normalizeConfig(config Config) (normalizedConfig, error) {
	if isNil(config.Searches) {
		return normalizedConfig{}, errors.New("create search websocket: search snapshot reader is required")
	}
	if isNil(config.Exports) {
		return normalizedConfig{}, errors.New("create search websocket: export snapshot reader is required")
	}
	if err := validateIdentity(config.Access.TenantID, "tenant"); err != nil {
		return normalizedConfig{}, err
	}
	if err := validateIdentity(config.Access.OwnerID, "owner"); err != nil {
		return normalizedConfig{}, err
	}

	connections, err := boundedInt(config.MaximumConnections, defaultMaximumConnections, maximumConnectionsCeiling, "connection")
	if err != nil {
		return normalizedConfig{}, err
	}
	subscriptions, err := boundedUint32(config.MaximumSubscriptions, defaultMaximumSubscriptions, maximumSubscriptionsCeiling, "subscription")
	if err != nil {
		return normalizedConfig{}, err
	}
	backingMaximum := config.Searches.MaximumPreviewRows()
	if backingMaximum == 0 {
		return normalizedConfig{}, errors.New("create search websocket: search snapshot reader has no preview row capacity")
	}
	previewRowsCeiling := min(maximumPreviewRowsCeiling, backingMaximum)
	previewRowsDefault := min(defaultMaximumPreviewRows, previewRowsCeiling)
	previewRows, err := boundedUint32(config.MaximumPreviewRows, previewRowsDefault, previewRowsCeiling, "preview row")
	if err != nil {
		return normalizedConfig{}, err
	}
	frameBytes, err := boundedBytes(config.MaximumFrameBytes, defaultMaximumFrameBytes, minimumFrameBytes, maximumFrameBytesCeiling, "frame")
	if err != nil {
		return normalizedConfig{}, err
	}
	queuedFrames, err := boundedInt(config.MaximumQueuedFrames, defaultMaximumQueuedFrames, maximumQueuedFramesCeiling, "queued frame")
	if err != nil {
		return normalizedConfig{}, err
	}
	if queuedFrames < minimumQueuedFrames {
		return normalizedConfig{}, errors.New("create search websocket: queued frame count cannot hold one subscription snapshot")
	}
	queuedBytes, err := boundedBytes(config.MaximumQueuedBytes, defaultMaximumQueuedBytes, frameBytes, maximumQueuedBytesCeiling, "queued byte")
	if err != nil {
		return normalizedConfig{}, err
	}
	totalQueuedBytes, err := boundedBytes(config.MaximumTotalQueuedBytes, defaultMaximumTotalQueuedBytes, queuedBytes, maximumTotalQueuedBytesCeiling, "total queued byte")
	if err != nil {
		return normalizedConfig{}, err
	}
	targets, err := boundedInt(config.MaximumTargets, defaultMaximumTargets, maximumTargetsCeiling, "target")
	if err != nil {
		return normalizedConfig{}, err
	}
	projections, err := boundedInt(
		config.MaximumConcurrentProjections,
		defaultMaximumProjections,
		maximumProjectionsCeiling,
		"concurrent projection",
	)
	if err != nil {
		return normalizedConfig{}, err
	}
	projections = min(projections, targets)
	replayEvents, err := boundedInt(config.MaximumReplayEvents, defaultMaximumReplayEvents, maximumReplayEventsCeiling, "replay event")
	if err != nil {
		return normalizedConfig{}, err
	}
	replayBytes, err := boundedBytes(config.MaximumReplayBytes, defaultMaximumReplayBytes, minimumFrameBytes, maximumReplayBytesCeiling, "per-target replay byte")
	if err != nil {
		return normalizedConfig{}, err
	}
	totalReplayBytes, err := boundedBytes(config.MaximumTotalReplayBytes, defaultMaximumTotalReplayBytes, minimumFrameBytes, maximumTotalReplayBytesCeiling, "total replay byte")
	if err != nil {
		return normalizedConfig{}, err
	}
	if replayBytes > totalReplayBytes {
		return normalizedConfig{}, errors.New("create search websocket: per-target replay byte limit exceeds total replay byte limit")
	}

	pollInterval, err := boundedDuration(config.PollInterval, defaultPollInterval, minimumPollInterval, maximumPollInterval, "poll interval")
	if err != nil {
		return normalizedConfig{}, err
	}
	writeTimeout, err := boundedDuration(config.WriteTimeout, defaultWriteTimeout, minimumWriteTimeout, maximumTransportTimeout, "write timeout")
	if err != nil {
		return normalizedConfig{}, err
	}
	pongTimeout, err := boundedDuration(config.PongTimeout, defaultPongTimeout, minimumPongTimeout, maximumTransportTimeout, "pong timeout")
	if err != nil {
		return normalizedConfig{}, err
	}
	pingInterval, err := boundedDuration(config.PingInterval, defaultPingInterval, minimumPingInterval, maximumTransportTimeout, "ping interval")
	if err != nil {
		return normalizedConfig{}, err
	}
	if pingInterval >= pongTimeout {
		return normalizedConfig{}, errors.New("create search websocket: ping interval must be shorter than pong timeout")
	}

	now := config.Now
	if now == nil {
		now = time.Now
	}
	checkOrigin := config.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = sameOrigin
	}
	return normalizedConfig{
		searches: config.Searches,
		exports:  config.Exports,
		access: searchjobs.AccessScope{
			TenantID: strings.Clone(config.Access.TenantID),
			OwnerID:  strings.Clone(config.Access.OwnerID),
		},
		checkOrigin:        checkOrigin,
		maximumConnections: connections, maximumSubscriptions: subscriptions, maximumPreviewRows: previewRows,
		maximumFrameBytes: frameBytes, maximumQueuedFrames: queuedFrames, maximumQueuedBytes: queuedBytes,
		maximumTotalQueuedBytes: totalQueuedBytes,
		maximumTargets:          targets, maximumConcurrentProjections: projections,
		maximumReplayEvents: replayEvents, maximumReplayBytes: replayBytes,
		maximumTotalReplayBytes: totalReplayBytes,
		pollInterval:            pollInterval, writeTimeout: writeTimeout, pongTimeout: pongTimeout, pingInterval: pingInterval,
		now: now,
	}, nil
}

func boundedInt(value, fallback, ceiling int, name string) (int, error) {
	if value < 0 || value > ceiling {
		return 0, fmt.Errorf("create search websocket: invalid maximum %s count", name)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

func boundedUint32(value, fallback, ceiling uint32, name string) (uint32, error) {
	if value > ceiling {
		return 0, fmt.Errorf("create search websocket: invalid maximum %s count", name)
	}
	if value == 0 {
		return fallback, nil
	}
	return value, nil
}

func boundedBytes(value, fallback, minimum, ceiling uint64, name string) (uint64, error) {
	if value == 0 {
		value = fallback
	}
	if value < minimum || value > ceiling {
		return 0, fmt.Errorf("create search websocket: invalid maximum %s limit", name)
	}
	return value, nil
}

func boundedDuration(value, fallback, minimum, maximum time.Duration, name string) (time.Duration, error) {
	if value == 0 {
		value = fallback
	}
	if value < minimum || value > maximum {
		return 0, fmt.Errorf("create search websocket: invalid %s", name)
	}
	return value, nil
}

func validateIdentity(value, name string) error {
	if value == "" || len(value) > maximumIdentityBytes || !utf8.ValidString(value) {
		return fmt.Errorf("create search websocket: %s identity is invalid", name)
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func sameOrigin(request *http.Request) bool {
	origins := request.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin := origins[0]
	if origin == "" || strings.TrimSpace(origin) != origin || strings.ContainsAny(origin, "\x00,\r\n?#") {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || !validAuthority(parsed.Host) || parsed.User != nil || parsed.Path != "" || parsed.RawPath != "" ||
		parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return false
	}
	wantScheme := "http"
	if request.TLS != nil {
		wantScheme = "https"
	}
	if !strings.EqualFold(parsed.Scheme, wantScheme) || !validAuthority(request.Host) {
		return false
	}
	return strings.EqualFold(parsed.Host, request.Host)
}

func validAuthority(authority string) bool {
	if authority == "" || strings.TrimSpace(authority) != authority ||
		strings.ContainsAny(authority, "\x00,\r\n/?#@") || strings.HasSuffix(authority, ":") {
		return false
	}
	parsed, err := url.Parse("http://" + authority)
	if err != nil || parsed.Host != authority || parsed.Hostname() == "" || parsed.Path != "" {
		return false
	}
	// URL.Port validates port syntax but intentionally permits values outside
	// the TCP range, so impose the transport bound explicitly.
	port := parsed.Port()
	if port != "" {
		value, err := strconv.ParseUint(port, 10, 16)
		if err != nil || value > 65_535 {
			return false
		}
	}
	// A colon in the decoded hostname is IPv6 and must use bracket notation in
	// an HTTP authority. This also rejects ambiguous unbracketed IPv6 hosts.
	if strings.Contains(parsed.Hostname(), ":") && !strings.HasPrefix(authority, "[") {
		return false
	}
	return true
}
