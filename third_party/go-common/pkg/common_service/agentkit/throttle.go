package agentkit

import (
	"sync"
	"time"
)

// DefaultStartThrottleMaxEntries bounds the per-key window map by default so a burst
// of unique keys cannot grow it without bound. Expired entries are pruned before the
// cap is consulted; if the map is still at the cap (every window is live) the window
// nearest to opening is evicted.
const DefaultStartThrottleMaxEntries = 4096

// StartThrottle is a mutex-guarded, per-key fixed-window rate limiter, typically
// used to bound how often a user may start an agent run. It is process-local (not
// shared across replicas). A window of Limit starts is allowed per key per Window;
// the Limit+1th start in the same window is rejected until the window rolls over. A
// non-positive Limit or Window disables the throttle entirely.
//
// K is the throttle key type (e.g. a user ID).
type StartThrottle[K comparable] struct {
	mu         sync.Mutex
	config     StartThrottleConfigProvider
	maxEntries int
	entries    map[K]*startWindow
	now        func() time.Time
}

// StartThrottleConfig is the coherent per-operation configuration for a
// StartThrottle. A non-positive Window or Limit disables the throttle.
type StartThrottleConfig struct {
	Window time.Duration
	Limit  int
}

// StartThrottleConfigProvider captures one coherent StartThrottleConfig for an
// Allow operation. Providers may return different configurations over time; the
// throttle retains its existing per-key windows so a configuration refresh does
// not reset callers' accumulated counts.
type StartThrottleConfigProvider func() StartThrottleConfig

type startWindow struct {
	windowStart time.Time
	count       int
}

// NewStartThrottle builds a StartThrottle with the given window and per-window
// limit. A non-positive window or limit yields a disabled throttle that allows
// every call. The entry cap defaults to DefaultStartThrottleMaxEntries.
func NewStartThrottle[K comparable](window time.Duration, limit int) *StartThrottle[K] {
	config := StartThrottleConfig{Window: window, Limit: limit}
	return NewStartThrottleWithConfigProvider[K](func() StartThrottleConfig { return config })
}

// NewStartThrottleWithConfigProvider builds a StartThrottle whose Window and
// Limit are captured once per Allow call. A nil provider yields a disabled
// throttle. Existing windows are evaluated against the newly captured values:
// shortening a window may expire one immediately, lengthening it extends the
// current window, and changing the limit applies to the current count.
func NewStartThrottleWithConfigProvider[K comparable](provider StartThrottleConfigProvider) *StartThrottle[K] {
	if provider == nil {
		provider = func() StartThrottleConfig { return StartThrottleConfig{} }
	}
	return &StartThrottle[K]{
		config:     provider,
		maxEntries: DefaultStartThrottleMaxEntries,
		entries:    map[K]*startWindow{},
		now:        time.Now,
	}
}

// SetClock overrides the time source. Intended for tests; a nil clock is ignored.
func (t *StartThrottle[K]) SetClock(now func() time.Time) {
	if t == nil || now == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.now = now
}

// SetMaxEntries overrides the per-key window map cap. A non-positive value restores
// the default.
func (t *StartThrottle[K]) SetMaxEntries(maxEntries int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if maxEntries <= 0 {
		maxEntries = DefaultStartThrottleMaxEntries
	}
	t.maxEntries = maxEntries
}

// Enabled reports whether the throttle actually limits (positive window and limit).
// A nil throttle is disabled.
func (t *StartThrottle[K]) Enabled() bool {
	config := t.currentConfig()
	return config.Limit > 0 && config.Window > 0
}

// Allow records an attempt for key and reports whether it is permitted. When
// rejected, it returns the duration until the current window rolls over so callers
// can surface a retry-after hint. A disabled (or nil) throttle always allows.
func (t *StartThrottle[K]) Allow(key K) (bool, time.Duration) {
	config := t.currentConfig()
	if config.Limit <= 0 || config.Window <= 0 {
		return true, 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()

	if t.entries == nil {
		t.entries = map[K]*startWindow{}
	}

	window := t.entries[key]
	if window == nil || now.Sub(window.windowStart) >= config.Window {
		t.pruneLocked(now, config.Window)
		t.entries[key] = &startWindow{windowStart: now, count: 1}
		return true, 0
	}
	if window.count >= config.Limit {
		return false, config.Window - now.Sub(window.windowStart)
	}
	window.count++
	return true, 0
}

func (t *StartThrottle[K]) currentConfig() StartThrottleConfig {
	if t == nil {
		return StartThrottleConfig{}
	}
	return t.config()
}

func (t *StartThrottle[K]) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// pruneLocked drops fully-expired windows. It runs whenever a new window opens, and
// is the only reclaim path, so the map cannot leak entries for idle keys. If the map
// is still at the cap after pruning (every entry is live), the window nearest to
// opening is evicted to bound memory.
func (t *StartThrottle[K]) pruneLocked(now time.Time, windowDuration time.Duration) {
	for key, window := range t.entries {
		if window == nil || now.Sub(window.windowStart) >= windowDuration {
			delete(t.entries, key)
		}
	}
	maxEntries := t.maxEntries
	if maxEntries <= 0 {
		maxEntries = DefaultStartThrottleMaxEntries
	}
	for len(t.entries) >= maxEntries {
		var oldestKey K
		var oldestStart time.Time
		found := false
		for key, window := range t.entries {
			if !found || window.windowStart.Before(oldestStart) {
				oldestKey = key
				oldestStart = window.windowStart
				found = true
			}
		}
		if !found {
			break
		}
		delete(t.entries, oldestKey)
	}
}

// RetryAfterSeconds converts a retry-after duration to a whole number of seconds,
// rounding up and clamping to at least one so a sub-second remainder still yields a
// usable Retry-After hint. Apps use it to fill a rate-limit error's retry field.
func RetryAfterSeconds(d time.Duration) uint32 {
	if d <= 0 {
		return 1
	}
	seconds := (d + time.Second - 1) / time.Second
	if seconds < 1 {
		return 1
	}
	return uint32(seconds)
}
