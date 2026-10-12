package config

import (
	"sync"
	"sync/atomic"
	"time"
)

// Provider returns the current immutable configuration generation. It is the
// single configuration argument applications pass to go-common facades; each
// facade narrows it to its own aggregate view via the methods below, and hot
// boundaries sample it once per operation. Construct a RootView once per
// published generation and reuse it. A RootView identity must never be
// reused for changed contents; providers own this immutability contract.
// Providers must return valid, non-nil views with non-panicking getters.
// Constructors may read getters outside GenerationMemo's recovery boundary;
// a typed-nil custom view or panicking getter can therefore panic at startup.
// Narrowed providers may return custom view implementations, but generation
// memos cache only the exact RootView dynamic type, not wrappers embedding it.
// FromSnapshots and the narrowings below preserve that type and cache identity.
type Provider func() RootView

// Static pins one immutable generation of cfg for callers that have no
// configuration store to sample: tests and applications that load a bare
// *Config once at startup. The resulting Provider serves the same values
// forever, so nothing downstream ever observes a new generation. Production
// applications with a managed store should instead pass a Provider that
// samples the store's current snapshot — that is what makes every hot
// boundary actually hot. cfg must be non-nil.
func Static(cfg *Config) Provider {
	view := NewRootView(cfg)
	return func() RootView { return view }
}

// FromSnapshots builds a live provider that reuses its RootView while the
// current immutable snapshot compares equal. sample runs on every call; build
// runs only on a cache miss, so config extraction or cloning belongs in build.
// Snapshots must compare safely with == and must not reuse identities for changed
// contents. Only the latest snapshot is cached; revisiting a displaced one rebuilds it.
// It panics immediately when sample or build is nil.
func FromSnapshots[S comparable](sample func() S, build func(S) RootView) Provider {
	if sample == nil || build == nil {
		panic("config invariant snapshot_provider_present violated: FromSnapshots requires non-nil sample and build")
	}
	type entry struct {
		snapshot S
		view     RootView
	}
	var (
		mu   sync.Mutex // serializes build only
		last atomic.Pointer[entry]
	)
	// The locked miss path stays out of the warm closure.
	miss := func(snapshot S) RootView {
		mu.Lock()
		defer mu.Unlock()
		if e := last.Load(); e != nil && e.snapshot == snapshot {
			return e.view // built while we waited
		}
		e := &entry{snapshot: snapshot, view: build(snapshot)}
		last.Store(e)
		return e.view
	}
	return func() RootView {
		snapshot := sample()
		if e := last.Load(); e != nil && e.snapshot == snapshot {
			return e.view
		}
		return miss(snapshot)
	}
}

// ServiceView is the configuration visible to the common_service facade.
type ServiceView interface {
	AttachmentsView
	AuthView
	BotView
	CareersView
	ChatView
	NotificationsView
	EmailView
	FeaturesView
	OauthView
	ObjectStorageView
	OrganizationsView
	PaginationView
	PermissionsView
	RateLimitsView
	SanitizerView
	ServerView
	SmsView
	UserSearchView
}

// ServiceProvider captures one immutable generation per call for the
// common_service facade.
type ServiceProvider func() ServiceView

// Service narrows the root provider to the common_service facade.
func (p Provider) Service() ServiceProvider {
	return func() ServiceView { return p() }
}

// APIView is the configuration visible to the common_api facade.
type APIView interface {
	AttachmentsView
	CareersView
	ChatView
	NotificationsView
	FeaturesView
	OauthView
	OrganizationsView
	RealtimeView
	ServerView
	SmsView
	TelemetryView
	UserSearchView
}

// APIProvider captures one immutable generation per call for the common_api
// facade.
type APIProvider func() APIView

// API narrows the root provider to the common_api facade.
func (p Provider) API() APIProvider {
	return func() APIView { return p() }
}

// PersistenceView is the configuration visible to the common_persistence
// facade.
type PersistenceView interface {
	DatabaseView
	ObjectStorageView
	// PostCommitHookTimeout bounds the post-commit hooks the persistence layer
	// drains itself when no request or ambient transaction owns them.
	PostCommitHookTimeout() time.Duration
}

// PersistenceProvider captures one immutable generation per call for the
// common_persistence facade.
type PersistenceProvider func() PersistenceView

// Persistence narrows the root provider to the common_persistence facade.
func (p Provider) Persistence() PersistenceProvider {
	return func() PersistenceView { return p() }
}

// ConverterView is the configuration visible to the common_converter facade.
type ConverterView interface {
	ObjectStorageView
	PermissionsView
}

// ConverterProvider captures one immutable generation per call for the
// common_converter facade.
type ConverterProvider func() ConverterView

// Converter narrows the root provider to the common_converter facade.
func (p Provider) Converter() ConverterProvider {
	return func() ConverterView { return p() }
}

// SanitizationView is the configuration visible to the common_sanitizer
// facade. It spans every domain the request policy validates against.
type SanitizationView interface {
	AuthView
	ChatView
	NotificationsView
	OrganizationsView
	PaginationView
	SanitizerView
	SmsView
	UserSearchView
}

// SanitizationProvider captures one immutable generation per call for the
// common_sanitizer facade.
type SanitizationProvider func() SanitizationView

// Sanitization narrows the root provider to the common_sanitizer facade.
func (p Provider) Sanitization() SanitizationProvider {
	return func() SanitizationView { return p() }
}

// MetricsProvider captures one immutable generation per call for the
// common_metrics facade.
type MetricsProvider func() MetricsView

// Metrics narrows the root provider to the common_metrics facade.
func (p Provider) Metrics() MetricsProvider {
	return func() MetricsView { return p() }
}

// LoggingProvider captures one immutable generation per call for the logger
// facade; the log level is sampled per entry.
type LoggingProvider func() LoggingView

// Logging narrows the root provider to the logger facade.
func (p Provider) Logging() LoggingProvider {
	return func() LoggingView { return p() }
}

// GenaiProvider captures one immutable generation per call for the
// genairuntime facade.
type GenaiProvider func() GenaiView

// Genai narrows the root provider to the genairuntime facade.
func (p Provider) Genai() GenaiProvider {
	return func() GenaiView { return p() }
}

// RealtimeProvider captures one immutable generation per call for WebSocket
// handshake and connection policy. The realtime hub samples it once per
// handshake so its hot keys apply to connections accepted afterwards.
type RealtimeProvider func() RealtimeView

// Realtime narrows the root provider to the realtime transport view.
func (p Provider) Realtime() RealtimeProvider {
	return func() RealtimeView { return p() }
}

var (
	_ ServiceView      = RootView{}
	_ APIView          = RootView{}
	_ PersistenceView  = RootView{}
	_ ConverterView    = RootView{}
	_ SanitizationView = RootView{}
)
