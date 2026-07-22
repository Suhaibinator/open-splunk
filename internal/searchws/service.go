package searchws

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"net/http"
	"sync"
)

// Service owns all upgraded connections, unique-target pollers, and bounded
// replay state for the search progress WebSocket.
type Service struct {
	config normalizedConfig

	ctx    context.Context
	cancel context.CancelFunc

	mu                sync.Mutex
	closed            bool
	activeConnections int
	connections       map[*connection]struct{}
	targets           map[targetKey]*targetState
	loads             map[targetKey]*targetLoad
	accessSequence    uint64
	closeOnce         sync.Once
	done              chan struct{}
	connectionWG      sync.WaitGroup
	targetWG          sync.WaitGroup
	loadWG            sync.WaitGroup

	replayBudgetMu sync.Mutex
	replayBytes    uint64
	queueBudgetMu  sync.Mutex
	queuedBytes    uint64
}

// New validates all hard limits and constructs an idle Service.
func New(config Config) (*Service, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{
		config: normalized, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		connections: make(map[*connection]struct{}), targets: make(map[targetKey]*targetState), loads: make(map[targetKey]*targetLoad),
	}, nil
}

func randomSequenceBase() (uint64, error) {
	var encoded [8]byte
	if _, err := rand.Read(encoded[:]); err != nil {
		return 0, err
	}
	// A per-journal high-entropy base prevents a pre-restart or evicted journal
	// sequence from numerically colliding with a new in-memory incarnation after
	// another client establishes it with after_sequence=0. Keep ample headroom.
	return (binary.BigEndian.Uint64(encoded[:]) & ((uint64(1) << 62) - 1)) | (uint64(1) << 61), nil
}

// MaximumSubscriptions returns the per-connection subscription bound advertised
// by the browser bootstrap response.
func (service *Service) MaximumSubscriptions() uint32 {
	if service == nil {
		return 0
	}
	return service.config.maximumSubscriptions
}

// MaximumFrameBytes returns the binary application-frame bound advertised by
// the browser bootstrap response.
func (service *Service) MaximumFrameBytes() uint64 {
	if service == nil {
		return 0
	}
	return service.config.maximumFrameBytes
}

// Limits returns the drift-free bootstrap limits exposed by this service.
func (service *Service) Limits() Limits {
	if service == nil {
		return Limits{}
	}
	return Limits{MaximumSubscriptions: service.config.maximumSubscriptions, MaximumFrameBytes: service.config.maximumFrameBytes}
}

// ServeHTTP is implemented in connection.go.

// Close stops admission, cancels pollers, hard-closes every hijacked socket,
// and waits for their handlers. Repeated calls wait on the same completion.
func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("close search websocket: context is required")
	}
	service.closeOnce.Do(func() {
		service.mu.Lock()
		service.closed = true
		connections := make([]*connection, 0, len(service.connections))
		for connection := range service.connections {
			connections = append(connections, connection)
		}
		targets := make([]*targetState, 0, len(service.targets))
		for _, target := range service.targets {
			targets = append(targets, target)
		}
		service.mu.Unlock()

		service.cancel()
		for _, target := range targets {
			target.cancel()
		}
		for _, connection := range connections {
			connection.hardClose()
		}
		go func() {
			service.connectionWG.Wait()
			service.targetWG.Wait()
			service.loadWG.Wait()
			service.mu.Lock()
			remainingTargets := make([]*targetState, 0, len(service.targets))
			for _, target := range service.targets {
				remainingTargets = append(remainingTargets, target)
			}
			clear(service.targets)
			clear(service.loads)
			service.mu.Unlock()
			for _, target := range remainingTargets {
				target.retire()
			}
			close(service.done)
		}()
	})
	// Hijacked connections are not owned by http.Server.Shutdown. Returning
	// before these waits finish would let runtime teardown close shared managers
	// beneath a still-running handler. The caller context therefore controls the
	// returned status, not whether this ownership barrier is honored.
	<-service.done
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (service *Service) reserveConnection() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed || service.activeConnections >= service.config.maximumConnections {
		return false
	}
	service.activeConnections++
	service.connectionWG.Add(1)
	return true
}

func (service *Service) registerConnection(connection *connection) bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return false
	}
	service.connections[connection] = struct{}{}
	return true
}

func (service *Service) releaseConnection(connection *connection) {
	service.mu.Lock()
	delete(service.connections, connection)
	service.activeConnections--
	service.mu.Unlock()
	service.connectionWG.Done()
}

func (service *Service) reserveReplayBytes(amount uint64) bool {
	service.replayBudgetMu.Lock()
	defer service.replayBudgetMu.Unlock()
	if amount > service.config.maximumTotalReplayBytes-service.replayBytes {
		return false
	}
	service.replayBytes += amount
	return true
}

func (service *Service) releaseReplayBytes(amount uint64) {
	service.replayBudgetMu.Lock()
	if amount >= service.replayBytes {
		service.replayBytes = 0
	} else {
		service.replayBytes -= amount
	}
	service.replayBudgetMu.Unlock()
}

func (service *Service) reserveQueuedBytes(amount uint64) bool {
	service.queueBudgetMu.Lock()
	defer service.queueBudgetMu.Unlock()
	if amount > service.config.maximumTotalQueuedBytes-service.queuedBytes {
		return false
	}
	service.queuedBytes += amount
	return true
}

func (service *Service) releaseQueuedBytes(amount uint64) {
	service.queueBudgetMu.Lock()
	if amount >= service.queuedBytes {
		service.queuedBytes = 0
	} else {
		service.queuedBytes -= amount
	}
	service.queueBudgetMu.Unlock()
}

// evictInactiveTarget releases a complete idle journal under global retained
// byte pressure. Resolver pins close the lookup-to-attach race, while deleting
// under service.mu prevents new resolvers from observing a half-retired target.
func (service *Service) evictInactiveTarget(except *targetState) bool {
	service.mu.Lock()
	var candidate *targetState
	for _, target := range service.targets {
		if target == except || target.subscriberCount.Load() != 0 || target.resolverCount.Load() != 0 {
			continue
		}
		if candidate == nil || target.lastAccess.Load() < candidate.lastAccess.Load() {
			candidate = target
		}
	}
	if candidate != nil {
		delete(service.targets, candidate.key)
	}
	service.mu.Unlock()
	if candidate == nil {
		return false
	}
	candidate.retire()
	return true
}

func (service *Service) releaseResolvedTarget(target *targetState) {
	if target != nil {
		target.resolverCount.Add(-1)
	}
}

func (service *Service) unavailable(response http.ResponseWriter) {
	http.Error(response, "search websocket is unavailable", http.StatusServiceUnavailable)
}
