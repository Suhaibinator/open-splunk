package searchws

import (
	"context"
	"crypto/tls"
	"net/http"
	"strings"
	"testing"
	"time"
	"unsafe"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type configTestSearchSnapshots struct{}

func (configTestSearchSnapshots) GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error) {
	return searchjobs.Job{}, searchjobs.ErrNotFound
}

type configTestExportSnapshots struct{}

func (configTestExportSnapshots) Get(context.Context, searchjobs.AccessScope, string) (exportjobs.Job, error) {
	return exportjobs.Job{}, exportjobs.ErrNotFound
}

func TestNormalizeConfigDefaultsAndInjectedFunctions(t *testing.T) {
	searches := &configTestSearchSnapshots{}
	exports := &configTestExportSnapshots{}
	config := Config{
		Searches: searches,
		Exports:  exports,
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	}
	before := time.Now()
	normalized := mustNormalizeSearchWebSocketConfig(t, config)
	observedNow := normalized.now()
	after := time.Now()

	if normalized.searches != searches || normalized.exports != exports {
		t.Fatalf("normalized dependencies = (%T, %T), want original instances", normalized.searches, normalized.exports)
	}
	if normalized.access != config.Access {
		t.Fatalf("normalized access = %+v, want %+v", normalized.access, config.Access)
	}
	if observedNow.Before(before) || observedNow.After(after) {
		t.Fatalf("nil Now fallback returned %s, want within [%s, %s]", observedNow, before, after)
	}
	if normalized.maximumConnections != defaultMaximumConnections ||
		normalized.maximumSubscriptions != defaultMaximumSubscriptions ||
		normalized.maximumFrameBytes != defaultMaximumFrameBytes ||
		normalized.maximumQueuedFrames != defaultMaximumQueuedFrames ||
		normalized.maximumQueuedBytes != defaultMaximumQueuedBytes ||
		normalized.maximumTotalQueuedBytes != defaultMaximumTotalQueuedBytes ||
		normalized.maximumTargets != defaultMaximumTargets ||
		normalized.maximumReplayEvents != defaultMaximumReplayEvents ||
		normalized.maximumReplayBytes != defaultMaximumReplayBytes ||
		normalized.maximumTotalReplayBytes != defaultMaximumTotalReplayBytes ||
		normalized.pollInterval != defaultPollInterval ||
		normalized.writeTimeout != defaultWriteTimeout ||
		normalized.pongTimeout != defaultPongTimeout ||
		normalized.pingInterval != defaultPingInterval {
		t.Fatalf("normalized defaults = %+v", normalized)
	}
	if !normalized.checkOrigin(originRequest("example.test", false, "http://example.test")) ||
		normalized.checkOrigin(originRequest("example.test", false, "https://example.test")) {
		t.Fatal("nil CheckOrigin did not install the same-origin policy")
	}

	fixedNow := time.Date(2026, 7, 22, 12, 34, 56, 0, time.UTC)
	originCalls := 0
	config.Now = func() time.Time { return fixedNow }
	config.CheckOrigin = func(request *http.Request) bool {
		originCalls++
		return request.Host == "injected.example"
	}
	normalized = mustNormalizeSearchWebSocketConfig(t, config)
	if got := normalized.now(); !got.Equal(fixedNow) {
		t.Fatalf("injected Now returned %s, want %s", got, fixedNow)
	}
	if !normalized.checkOrigin(originRequest("injected.example", false)) || originCalls != 1 {
		t.Fatalf("injected CheckOrigin result/calls = (false, %d)", originCalls)
	}
}

func TestNormalizeConfigRejectsNilDependenciesAndInvalidIdentities(t *testing.T) {
	var typedNilSearches *configTestSearchSnapshots
	var typedNilExports *configTestExportSnapshots
	tests := []struct {
		name        string
		mutate      func(*Config)
		wantMessage string
	}{
		{name: "nil searches", mutate: func(config *Config) { config.Searches = nil }, wantMessage: "search snapshot reader"},
		{name: "typed nil searches", mutate: func(config *Config) { config.Searches = typedNilSearches }, wantMessage: "search snapshot reader"},
		{name: "nil exports", mutate: func(config *Config) { config.Exports = nil }, wantMessage: "export snapshot reader"},
		{name: "typed nil exports", mutate: func(config *Config) { config.Exports = typedNilExports }, wantMessage: "export snapshot reader"},
		{name: "empty tenant", mutate: func(config *Config) { config.Access.TenantID = "" }, wantMessage: "tenant identity"},
		{name: "invalid UTF-8 tenant", mutate: func(config *Config) { config.Access.TenantID = string([]byte{0xff}) }, wantMessage: "tenant identity"},
		{name: "oversized tenant", mutate: func(config *Config) { config.Access.TenantID = strings.Repeat("t", maximumIdentityBytes+1) }, wantMessage: "tenant identity"},
		{name: "empty owner", mutate: func(config *Config) { config.Access.OwnerID = "" }, wantMessage: "owner identity"},
		{name: "invalid UTF-8 owner", mutate: func(config *Config) { config.Access.OwnerID = string([]byte{0xc3, 0x28}) }, wantMessage: "owner identity"},
		{name: "oversized owner", mutate: func(config *Config) { config.Access.OwnerID = strings.Repeat("o", maximumIdentityBytes+1) }, wantMessage: "owner identity"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validSearchWebSocketConfig()
			test.mutate(&config)
			_, err := normalizeConfig(config)
			if err == nil || !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf("normalizeConfig() error = %v, want containing %q", err, test.wantMessage)
			}
		})
	}

	config := validSearchWebSocketConfig()
	config.Access.TenantID = strings.Repeat("t", maximumIdentityBytes)
	config.Access.OwnerID = strings.Repeat("o", maximumIdentityBytes)
	normalized := mustNormalizeSearchWebSocketConfig(t, config)
	if len(normalized.access.TenantID) != maximumIdentityBytes || len(normalized.access.OwnerID) != maximumIdentityBytes {
		t.Fatalf("maximum-length identities were not retained: tenant=%d owner=%d",
			len(normalized.access.TenantID), len(normalized.access.OwnerID))
	}
}

func TestNormalizeConfigCountBounds(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		read    func(normalizedConfig) int64
		want    int64
		wantErr bool
	}{
		{name: "connections/zero uses default", mutate: func(config *Config) { config.MaximumConnections = 0 }, read: func(config normalizedConfig) int64 { return int64(config.maximumConnections) }, want: defaultMaximumConnections},
		{name: "connections/explicit default", mutate: func(config *Config) { config.MaximumConnections = defaultMaximumConnections }, read: func(config normalizedConfig) int64 { return int64(config.maximumConnections) }, want: defaultMaximumConnections},
		{name: "connections/minimum", mutate: func(config *Config) { config.MaximumConnections = 1 }, read: func(config normalizedConfig) int64 { return int64(config.maximumConnections) }, want: 1},
		{name: "connections/ceiling", mutate: func(config *Config) { config.MaximumConnections = maximumConnectionsCeiling }, read: func(config normalizedConfig) int64 { return int64(config.maximumConnections) }, want: maximumConnectionsCeiling},
		{name: "connections/negative", mutate: func(config *Config) { config.MaximumConnections = -1 }, wantErr: true},
		{name: "connections/over ceiling", mutate: func(config *Config) { config.MaximumConnections = maximumConnectionsCeiling + 1 }, wantErr: true},

		{name: "subscriptions/zero uses default", mutate: func(config *Config) { config.MaximumSubscriptions = 0 }, read: func(config normalizedConfig) int64 { return int64(config.maximumSubscriptions) }, want: int64(defaultMaximumSubscriptions)},
		{name: "subscriptions/explicit default", mutate: func(config *Config) { config.MaximumSubscriptions = defaultMaximumSubscriptions }, read: func(config normalizedConfig) int64 { return int64(config.maximumSubscriptions) }, want: int64(defaultMaximumSubscriptions)},
		{name: "subscriptions/minimum", mutate: func(config *Config) { config.MaximumSubscriptions = 1 }, read: func(config normalizedConfig) int64 { return int64(config.maximumSubscriptions) }, want: 1},
		{name: "subscriptions/ceiling", mutate: func(config *Config) { config.MaximumSubscriptions = maximumSubscriptionsCeiling }, read: func(config normalizedConfig) int64 { return int64(config.maximumSubscriptions) }, want: int64(maximumSubscriptionsCeiling)},
		{name: "subscriptions/over ceiling", mutate: func(config *Config) { config.MaximumSubscriptions = maximumSubscriptionsCeiling + 1 }, wantErr: true},

		{name: "queued frames/zero uses default", mutate: func(config *Config) { config.MaximumQueuedFrames = 0 }, read: func(config normalizedConfig) int64 { return int64(config.maximumQueuedFrames) }, want: defaultMaximumQueuedFrames},
		{name: "queued frames/explicit default", mutate: func(config *Config) { config.MaximumQueuedFrames = defaultMaximumQueuedFrames }, read: func(config normalizedConfig) int64 { return int64(config.maximumQueuedFrames) }, want: defaultMaximumQueuedFrames},
		{name: "queued frames/minimum", mutate: func(config *Config) { config.MaximumQueuedFrames = minimumQueuedFrames }, read: func(config normalizedConfig) int64 { return int64(config.maximumQueuedFrames) }, want: minimumQueuedFrames},
		{name: "queued frames/below minimum", mutate: func(config *Config) { config.MaximumQueuedFrames = minimumQueuedFrames - 1 }, wantErr: true},
		{name: "queued frames/ceiling", mutate: func(config *Config) { config.MaximumQueuedFrames = maximumQueuedFramesCeiling }, read: func(config normalizedConfig) int64 { return int64(config.maximumQueuedFrames) }, want: maximumQueuedFramesCeiling},
		{name: "queued frames/negative", mutate: func(config *Config) { config.MaximumQueuedFrames = -1 }, wantErr: true},
		{name: "queued frames/over ceiling", mutate: func(config *Config) { config.MaximumQueuedFrames = maximumQueuedFramesCeiling + 1 }, wantErr: true},

		{name: "targets/zero uses default", mutate: func(config *Config) { config.MaximumTargets = 0 }, read: func(config normalizedConfig) int64 { return int64(config.maximumTargets) }, want: defaultMaximumTargets},
		{name: "targets/explicit default", mutate: func(config *Config) { config.MaximumTargets = defaultMaximumTargets }, read: func(config normalizedConfig) int64 { return int64(config.maximumTargets) }, want: defaultMaximumTargets},
		{name: "targets/minimum", mutate: func(config *Config) { config.MaximumTargets = 1 }, read: func(config normalizedConfig) int64 { return int64(config.maximumTargets) }, want: 1},
		{name: "targets/ceiling", mutate: func(config *Config) { config.MaximumTargets = maximumTargetsCeiling }, read: func(config normalizedConfig) int64 { return int64(config.maximumTargets) }, want: maximumTargetsCeiling},
		{name: "targets/negative", mutate: func(config *Config) { config.MaximumTargets = -1 }, wantErr: true},
		{name: "targets/over ceiling", mutate: func(config *Config) { config.MaximumTargets = maximumTargetsCeiling + 1 }, wantErr: true},

		{name: "replay events/zero uses default", mutate: func(config *Config) { config.MaximumReplayEvents = 0 }, read: func(config normalizedConfig) int64 { return int64(config.maximumReplayEvents) }, want: defaultMaximumReplayEvents},
		{name: "replay events/explicit default", mutate: func(config *Config) { config.MaximumReplayEvents = defaultMaximumReplayEvents }, read: func(config normalizedConfig) int64 { return int64(config.maximumReplayEvents) }, want: defaultMaximumReplayEvents},
		{name: "replay events/minimum", mutate: func(config *Config) { config.MaximumReplayEvents = 1 }, read: func(config normalizedConfig) int64 { return int64(config.maximumReplayEvents) }, want: 1},
		{name: "replay events/ceiling", mutate: func(config *Config) { config.MaximumReplayEvents = maximumReplayEventsCeiling }, read: func(config normalizedConfig) int64 { return int64(config.maximumReplayEvents) }, want: maximumReplayEventsCeiling},
		{name: "replay events/negative", mutate: func(config *Config) { config.MaximumReplayEvents = -1 }, wantErr: true},
		{name: "replay events/over ceiling", mutate: func(config *Config) { config.MaximumReplayEvents = maximumReplayEventsCeiling + 1 }, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validSearchWebSocketConfig()
			test.mutate(&config)
			normalized, err := normalizeConfig(config)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeConfig() succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeConfig() error = %v", err)
			}
			if got := test.read(normalized); got != test.want {
				t.Fatalf("normalized value = %d, want %d", got, test.want)
			}
		})
	}
}

func TestNormalizeConfigByteBoundsAndRelationships(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		read    func(normalizedConfig) uint64
		want    uint64
		wantErr bool
	}{
		{name: "frame/zero uses default", mutate: func(config *Config) { config.MaximumFrameBytes = 0 }, read: func(config normalizedConfig) uint64 { return config.maximumFrameBytes }, want: defaultMaximumFrameBytes},
		{name: "frame/explicit default", mutate: func(config *Config) { config.MaximumFrameBytes = defaultMaximumFrameBytes }, read: func(config normalizedConfig) uint64 { return config.maximumFrameBytes }, want: defaultMaximumFrameBytes},
		{name: "frame/minimum", mutate: func(config *Config) { config.MaximumFrameBytes = minimumFrameBytes }, read: func(config normalizedConfig) uint64 { return config.maximumFrameBytes }, want: minimumFrameBytes},
		{name: "frame/ceiling", mutate: func(config *Config) { config.MaximumFrameBytes = maximumFrameBytesCeiling }, read: func(config normalizedConfig) uint64 { return config.maximumFrameBytes }, want: maximumFrameBytesCeiling},
		{name: "frame/below minimum", mutate: func(config *Config) { config.MaximumFrameBytes = minimumFrameBytes - 1 }, wantErr: true},
		{name: "frame/over ceiling", mutate: func(config *Config) { config.MaximumFrameBytes = maximumFrameBytesCeiling + 1 }, wantErr: true},

		{name: "per-connection queue/zero uses default", mutate: func(config *Config) { config.MaximumQueuedBytes = 0 }, read: func(config normalizedConfig) uint64 { return config.maximumQueuedBytes }, want: defaultMaximumQueuedBytes},
		{name: "per-connection queue/explicit default", mutate: func(config *Config) { config.MaximumQueuedBytes = defaultMaximumQueuedBytes }, read: func(config normalizedConfig) uint64 { return config.maximumQueuedBytes }, want: defaultMaximumQueuedBytes},
		{name: "per-connection queue/equal to frame minimum", mutate: func(config *Config) {
			config.MaximumFrameBytes = minimumFrameBytes
			config.MaximumQueuedBytes = minimumFrameBytes
		}, read: func(config normalizedConfig) uint64 { return config.maximumQueuedBytes }, want: minimumFrameBytes},
		{name: "per-connection queue/ceiling", mutate: func(config *Config) { config.MaximumQueuedBytes = maximumQueuedBytesCeiling }, read: func(config normalizedConfig) uint64 { return config.maximumQueuedBytes }, want: maximumQueuedBytesCeiling},
		{name: "per-connection queue/smaller than frame", mutate: func(config *Config) {
			config.MaximumFrameBytes = 2 * minimumFrameBytes
			config.MaximumQueuedBytes = minimumFrameBytes
		}, wantErr: true},
		{name: "per-connection queue/over ceiling", mutate: func(config *Config) { config.MaximumQueuedBytes = maximumQueuedBytesCeiling + 1 }, wantErr: true},

		{name: "global queue/zero uses default", mutate: func(config *Config) { config.MaximumTotalQueuedBytes = 0 }, read: func(config normalizedConfig) uint64 { return config.maximumTotalQueuedBytes }, want: defaultMaximumTotalQueuedBytes},
		{name: "global queue/explicit default", mutate: func(config *Config) { config.MaximumTotalQueuedBytes = defaultMaximumTotalQueuedBytes }, read: func(config normalizedConfig) uint64 { return config.maximumTotalQueuedBytes }, want: defaultMaximumTotalQueuedBytes},
		{name: "global queue/equal to per-connection minimum", mutate: func(config *Config) {
			config.MaximumFrameBytes = minimumFrameBytes
			config.MaximumQueuedBytes = minimumFrameBytes
			config.MaximumTotalQueuedBytes = minimumFrameBytes
		}, read: func(config normalizedConfig) uint64 { return config.maximumTotalQueuedBytes }, want: minimumFrameBytes},
		{name: "global queue/ceiling", mutate: func(config *Config) { config.MaximumTotalQueuedBytes = maximumTotalQueuedBytesCeiling }, read: func(config normalizedConfig) uint64 { return config.maximumTotalQueuedBytes }, want: maximumTotalQueuedBytesCeiling},
		{name: "global queue/smaller than per-connection queue", mutate: func(config *Config) {
			config.MaximumFrameBytes = minimumFrameBytes
			config.MaximumQueuedBytes = 2 * minimumFrameBytes
			config.MaximumTotalQueuedBytes = minimumFrameBytes
		}, wantErr: true},
		{name: "global queue/over ceiling", mutate: func(config *Config) { config.MaximumTotalQueuedBytes = maximumTotalQueuedBytesCeiling + 1 }, wantErr: true},

		{name: "per-target replay/zero uses default", mutate: func(config *Config) { config.MaximumReplayBytes = 0 }, read: func(config normalizedConfig) uint64 { return config.maximumReplayBytes }, want: defaultMaximumReplayBytes},
		{name: "per-target replay/explicit default", mutate: func(config *Config) { config.MaximumReplayBytes = defaultMaximumReplayBytes }, read: func(config normalizedConfig) uint64 { return config.maximumReplayBytes }, want: defaultMaximumReplayBytes},
		{name: "per-target replay/minimum", mutate: func(config *Config) { config.MaximumReplayBytes = minimumFrameBytes }, read: func(config normalizedConfig) uint64 { return config.maximumReplayBytes }, want: minimumFrameBytes},
		{name: "per-target replay/ceiling", mutate: func(config *Config) { config.MaximumReplayBytes = maximumReplayBytesCeiling }, read: func(config normalizedConfig) uint64 { return config.maximumReplayBytes }, want: maximumReplayBytesCeiling},
		{name: "per-target replay/below minimum", mutate: func(config *Config) { config.MaximumReplayBytes = minimumFrameBytes - 1 }, wantErr: true},
		{name: "per-target replay/over ceiling", mutate: func(config *Config) { config.MaximumReplayBytes = maximumReplayBytesCeiling + 1 }, wantErr: true},

		{name: "global replay/zero uses default", mutate: func(config *Config) { config.MaximumTotalReplayBytes = 0 }, read: func(config normalizedConfig) uint64 { return config.maximumTotalReplayBytes }, want: defaultMaximumTotalReplayBytes},
		{name: "global replay/explicit default", mutate: func(config *Config) { config.MaximumTotalReplayBytes = defaultMaximumTotalReplayBytes }, read: func(config normalizedConfig) uint64 { return config.maximumTotalReplayBytes }, want: defaultMaximumTotalReplayBytes},
		{name: "global replay/equal to per-target minimum", mutate: func(config *Config) {
			config.MaximumReplayBytes = minimumFrameBytes
			config.MaximumTotalReplayBytes = minimumFrameBytes
		}, read: func(config normalizedConfig) uint64 { return config.maximumTotalReplayBytes }, want: minimumFrameBytes},
		{name: "global replay/ceiling", mutate: func(config *Config) { config.MaximumTotalReplayBytes = maximumTotalReplayBytesCeiling }, read: func(config normalizedConfig) uint64 { return config.maximumTotalReplayBytes }, want: maximumTotalReplayBytesCeiling},
		{name: "global replay/smaller than per-target replay", mutate: func(config *Config) {
			config.MaximumReplayBytes = 2 * minimumFrameBytes
			config.MaximumTotalReplayBytes = minimumFrameBytes
		}, wantErr: true},
		{name: "global replay/default per-target exceeds explicit global", mutate: func(config *Config) {
			config.MaximumReplayBytes = 0
			config.MaximumTotalReplayBytes = minimumFrameBytes
		}, wantErr: true},
		{name: "global replay/over ceiling", mutate: func(config *Config) { config.MaximumTotalReplayBytes = maximumTotalReplayBytesCeiling + 1 }, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validSearchWebSocketConfig()
			test.mutate(&config)
			normalized, err := normalizeConfig(config)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeConfig() succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeConfig() error = %v", err)
			}
			if got := test.read(normalized); got != test.want {
				t.Fatalf("normalized value = %d, want %d", got, test.want)
			}
		})
	}
}

func TestNormalizeConfigDurationBoundsAndPingPongRelationship(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		read    func(normalizedConfig) time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "poll/zero uses default", mutate: func(config *Config) { config.PollInterval = 0 }, read: func(config normalizedConfig) time.Duration { return config.pollInterval }, want: defaultPollInterval},
		{name: "poll/explicit default", mutate: func(config *Config) { config.PollInterval = defaultPollInterval }, read: func(config normalizedConfig) time.Duration { return config.pollInterval }, want: defaultPollInterval},
		{name: "poll/minimum", mutate: func(config *Config) { config.PollInterval = minimumPollInterval }, read: func(config normalizedConfig) time.Duration { return config.pollInterval }, want: minimumPollInterval},
		{name: "poll/maximum", mutate: func(config *Config) { config.PollInterval = maximumPollInterval }, read: func(config normalizedConfig) time.Duration { return config.pollInterval }, want: maximumPollInterval},
		{name: "poll/below minimum", mutate: func(config *Config) { config.PollInterval = minimumPollInterval - time.Nanosecond }, wantErr: true},
		{name: "poll/over maximum", mutate: func(config *Config) { config.PollInterval = maximumPollInterval + time.Nanosecond }, wantErr: true},

		{name: "write/zero uses default", mutate: func(config *Config) { config.WriteTimeout = 0 }, read: func(config normalizedConfig) time.Duration { return config.writeTimeout }, want: defaultWriteTimeout},
		{name: "write/explicit default", mutate: func(config *Config) { config.WriteTimeout = defaultWriteTimeout }, read: func(config normalizedConfig) time.Duration { return config.writeTimeout }, want: defaultWriteTimeout},
		{name: "write/minimum", mutate: func(config *Config) { config.WriteTimeout = minimumWriteTimeout }, read: func(config normalizedConfig) time.Duration { return config.writeTimeout }, want: minimumWriteTimeout},
		{name: "write/maximum", mutate: func(config *Config) { config.WriteTimeout = maximumTransportTimeout }, read: func(config normalizedConfig) time.Duration { return config.writeTimeout }, want: maximumTransportTimeout},
		{name: "write/below minimum", mutate: func(config *Config) { config.WriteTimeout = minimumWriteTimeout - time.Nanosecond }, wantErr: true},
		{name: "write/over maximum", mutate: func(config *Config) { config.WriteTimeout = maximumTransportTimeout + time.Nanosecond }, wantErr: true},

		{name: "pong/zero uses default", mutate: func(config *Config) { config.PongTimeout = 0 }, read: func(config normalizedConfig) time.Duration { return config.pongTimeout }, want: defaultPongTimeout},
		{name: "pong/explicit default", mutate: func(config *Config) { config.PongTimeout = defaultPongTimeout }, read: func(config normalizedConfig) time.Duration { return config.pongTimeout }, want: defaultPongTimeout},
		{name: "pong/minimum", mutate: func(config *Config) {
			config.PongTimeout = minimumPongTimeout
			config.PingInterval = minimumPingInterval
		}, read: func(config normalizedConfig) time.Duration { return config.pongTimeout }, want: minimumPongTimeout},
		{name: "pong/maximum", mutate: func(config *Config) { config.PongTimeout = maximumTransportTimeout }, read: func(config normalizedConfig) time.Duration { return config.pongTimeout }, want: maximumTransportTimeout},
		{name: "pong/below minimum", mutate: func(config *Config) {
			config.PongTimeout = minimumPongTimeout - time.Nanosecond
			config.PingInterval = minimumPingInterval
		}, wantErr: true},
		{name: "pong/over maximum", mutate: func(config *Config) { config.PongTimeout = maximumTransportTimeout + time.Nanosecond }, wantErr: true},

		{name: "ping/zero uses default", mutate: func(config *Config) { config.PingInterval = 0 }, read: func(config normalizedConfig) time.Duration { return config.pingInterval }, want: defaultPingInterval},
		{name: "ping/explicit default", mutate: func(config *Config) { config.PingInterval = defaultPingInterval }, read: func(config normalizedConfig) time.Duration { return config.pingInterval }, want: defaultPingInterval},
		{name: "ping/minimum", mutate: func(config *Config) { config.PingInterval = minimumPingInterval }, read: func(config normalizedConfig) time.Duration { return config.pingInterval }, want: minimumPingInterval},
		{name: "ping/highest value below maximum pong", mutate: func(config *Config) {
			config.PingInterval = maximumTransportTimeout - time.Nanosecond
			config.PongTimeout = maximumTransportTimeout
		}, read: func(config normalizedConfig) time.Duration { return config.pingInterval }, want: maximumTransportTimeout - time.Nanosecond},
		{name: "ping/below minimum", mutate: func(config *Config) { config.PingInterval = minimumPingInterval - time.Nanosecond }, wantErr: true},
		{name: "ping/over maximum", mutate: func(config *Config) {
			config.PingInterval = maximumTransportTimeout + time.Nanosecond
			config.PongTimeout = maximumTransportTimeout
		}, wantErr: true},
		{name: "ping/equal to maximum pong", mutate: func(config *Config) {
			config.PingInterval = maximumTransportTimeout
			config.PongTimeout = maximumTransportTimeout
		}, wantErr: true},
		{name: "ping/just below pong", mutate: func(config *Config) {
			config.PingInterval = minimumPongTimeout - time.Nanosecond
			config.PongTimeout = minimumPongTimeout
		}, read: func(config normalizedConfig) time.Duration { return config.pingInterval }, want: minimumPongTimeout - time.Nanosecond},
		{name: "ping/equal to pong", mutate: func(config *Config) { config.PingInterval = time.Second; config.PongTimeout = time.Second }, wantErr: true},
		{name: "ping/greater than pong", mutate: func(config *Config) {
			config.PingInterval = time.Second + time.Nanosecond
			config.PongTimeout = time.Second
		}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validSearchWebSocketConfig()
			test.mutate(&config)
			normalized, err := normalizeConfig(config)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeConfig() succeeded")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeConfig() error = %v", err)
			}
			if got := test.read(normalized); got != test.want {
				t.Fatalf("normalized value = %s, want %s", got, test.want)
			}
		})
	}
}

func TestNormalizeConfigDefensivelyClonesAccessIdentities(t *testing.T) {
	config := validSearchWebSocketConfig()
	normalized := mustNormalizeSearchWebSocketConfig(t, config)
	if normalized.access != config.Access {
		t.Fatalf("normalized access = %+v, want %+v", normalized.access, config.Access)
	}
	if unsafe.StringData(normalized.access.TenantID) == unsafe.StringData(config.Access.TenantID) ||
		unsafe.StringData(normalized.access.OwnerID) == unsafe.StringData(config.Access.OwnerID) {
		t.Fatal("normalized access identities share caller-owned backing storage")
	}
}

func TestSameOriginAcceptsOnlyExactOriginShapes(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		secure  bool
		origins []string
	}{
		{name: "absent origin", host: "example.test"},
		{name: "absent origin does not require host", host: ""},
		{name: "HTTP authority", host: "example.test", origins: []string{"http://example.test"}},
		{name: "HTTPS authority", host: "example.test", secure: true, origins: []string{"https://example.test"}},
		{name: "scheme and host are case insensitive", host: "example.test:8042", origins: []string{"HTTP://EXAMPLE.TEST:8042"}},
		{name: "IPv6 authority", host: "[::1]:8042", origins: []string{"http://[::1]:8042"}},
		{name: "IPv6 HTTPS authority", host: "[2001:db8::1]", secure: true, origins: []string{"https://[2001:DB8::1]"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !sameOrigin(originRequest(test.host, test.secure, test.origins...)) {
				t.Fatalf("sameOrigin(host=%q, secure=%t, origins=%q) = false", test.host, test.secure, test.origins)
			}
		})
	}
}

func TestSameOriginRejectsHostileOriginShapes(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		secure  bool
		origins []string
	}{
		{name: "empty origin", host: "example.test", origins: []string{""}},
		{name: "duplicate headers", host: "example.test", origins: []string{"http://example.test", "http://example.test"}},
		{name: "comma-folded duplicates", host: "example.test", origins: []string{"http://example.test, http://example.test"}},
		{name: "comma without whitespace", host: "example.test", origins: []string{"http://example.test,http://example.test"}},
		{name: "null serialization", host: "example.test", origins: []string{"null"}},
		{name: "leading space", host: "example.test", origins: []string{" http://example.test"}},
		{name: "trailing space", host: "example.test", origins: []string{"http://example.test "}},
		{name: "leading tab", host: "example.test", origins: []string{"\thttp://example.test"}},
		{name: "embedded tab", host: "example.test", origins: []string{"http://exam\tple.test"}},
		{name: "carriage return", host: "example.test", origins: []string{"http://example.test\r"}},
		{name: "line feed", host: "example.test", origins: []string{"http://example.test\n"}},
		{name: "NUL", host: "example.test", origins: []string{"http://example.test\x00"}},
		{name: "userinfo", host: "example.test", origins: []string{"http://user@example.test"}},
		{name: "userinfo password", host: "example.test", origins: []string{"http://user:password@example.test"}},
		{name: "missing scheme", host: "example.test", origins: []string{"//example.test"}},
		{name: "relative authority text", host: "example.test", origins: []string{"example.test"}},
		{name: "opaque URL", host: "example.test", origins: []string{"http:example.test"}},
		{name: "WebSocket scheme", host: "example.test", origins: []string{"ws://example.test"}},
		{name: "secure WebSocket scheme", host: "example.test", secure: true, origins: []string{"wss://example.test"}},
		{name: "FTP scheme", host: "example.test", origins: []string{"ftp://example.test"}},
		{name: "HTTPS origin on HTTP request", host: "example.test", origins: []string{"https://example.test"}},
		{name: "HTTP origin on TLS request", host: "example.test", secure: true, origins: []string{"http://example.test"}},
		{name: "root path", host: "example.test", origins: []string{"http://example.test/"}},
		{name: "nonempty path", host: "example.test", origins: []string{"http://example.test/path"}},
		{name: "escaped path", host: "example.test", origins: []string{"http://example.test/%2e"}},
		{name: "empty query delimiter", host: "example.test", origins: []string{"http://example.test?"}},
		{name: "query", host: "example.test", origins: []string{"http://example.test?key=value"}},
		{name: "empty fragment delimiter", host: "example.test", origins: []string{"http://example.test#"}},
		{name: "fragment", host: "example.test", origins: []string{"http://example.test#fragment"}},
		{name: "different host", host: "example.test", origins: []string{"http://evil.test"}},
		{name: "missing requested port", host: "example.test:8042", origins: []string{"http://example.test"}},
		{name: "unexpected origin port", host: "example.test", origins: []string{"http://example.test:8042"}},
		{name: "different port", host: "example.test:8042", origins: []string{"http://example.test:8043"}},
		{name: "explicit default port differs", host: "example.test", origins: []string{"http://example.test:80"}},
		{name: "empty port", host: "example.test:", origins: []string{"http://example.test:"}},
		{name: "nonnumeric port", host: "example.test:port", origins: []string{"http://example.test:port"}},
		{name: "out-of-range port", host: "example.test:65536", origins: []string{"http://example.test:65536"}},
		{name: "IPv6 host mismatch", host: "[::1]:8042", origins: []string{"http://[::2]:8042"}},
		{name: "IPv6 port mismatch", host: "[::1]:8042", origins: []string{"http://[::1]:8043"}},
		{name: "IPv6 missing port", host: "[::1]:8042", origins: []string{"http://[::1]"}},
		{name: "origin present with missing request host", host: "", origins: []string{"http://example.test"}},
		{name: "request host leading space", host: " example.test", origins: []string{"http://example.test"}},
		{name: "request host trailing space", host: "example.test ", origins: []string{"http://example.test"}},
		{name: "request host comma", host: "example.test,evil.test", origins: []string{"http://example.test,evil.test"}},
		{name: "request host NUL", host: "example.test\x00", origins: []string{"http://example.test\x00"}},
		{name: "request host carriage return", host: "example.test\r", origins: []string{"http://example.test\r"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if sameOrigin(originRequest(test.host, test.secure, test.origins...)) {
				t.Fatalf("sameOrigin(host=%q, secure=%t, origins=%q) = true", test.host, test.secure, test.origins)
			}
		})
	}
}

func FuzzSameOriginRejectsAuthoritySuffix(f *testing.F) {
	for _, suffix := range []string{"", "/", "?", "#", ",http://evil.test", "\x00", ":80", "@evil.test"} {
		f.Add(suffix)
	}
	f.Fuzz(func(t *testing.T, suffix string) {
		if len(suffix) > 256 {
			t.Skip()
		}
		accepted := sameOrigin(originRequest("example.test", false, "http://example.test"+suffix))
		if accepted != (suffix == "") {
			t.Fatalf("sameOrigin accepted appended authority suffix %q", suffix)
		}
	})
}

func validSearchWebSocketConfig() Config {
	return Config{
		Searches: configTestSearchSnapshots{},
		Exports:  configTestExportSnapshots{},
		Access:   searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
	}
}

func mustNormalizeSearchWebSocketConfig(t *testing.T, config Config) normalizedConfig {
	t.Helper()
	normalized, err := normalizeConfig(config)
	if err != nil {
		t.Fatalf("normalizeConfig() error = %v", err)
	}
	return normalized
}

func originRequest(host string, secure bool, origins ...string) *http.Request {
	request := &http.Request{Host: host, Header: make(http.Header)}
	if secure {
		request.TLS = &tls.ConnectionState{}
	}
	for _, origin := range origins {
		request.Header.Add("Origin", origin)
	}
	return request
}
