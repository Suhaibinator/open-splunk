package ingest

import (
	"context"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
)

type testCollectorSessionManager struct {
	source Authorizer

	mu             sync.Mutex
	preliminary    map[string][]Authorization
	nextGeneration uint64
	admitFunc      func(context.Context, string, CollectorSessionAdmissionRequest) (CollectorSessionAdmission, error)
	activateFunc   func(collectorfleet.Lease) error
	authorizeFunc  func(context.Context, string, collectorfleet.Lease, time.Time) (Authorization, error)
	heartbeatFunc  func(context.Context, collectorfleet.Lease, collectorfleet.Heartbeat) (bool, error)
	disconnectFunc func(context.Context, collectorfleet.Lease, time.Time) (bool, error)
}

func newTestCollectorSessionManager(source Authorizer) *testCollectorSessionManager {
	return &testCollectorSessionManager{
		source:      source,
		preliminary: make(map[string][]Authorization),
	}
}

func (manager *testCollectorSessionManager) preliminaryAuthorizer() Authorizer {
	return AuthorizerFunc(func(ctx context.Context, bearer string) (Authorization, error) {
		authorization, err := manager.source.Authorize(ctx, bearer)
		if err != nil {
			return Authorization{}, err
		}
		authorization = cloneAuthorization(authorization)
		manager.mu.Lock()
		manager.preliminary[bearer] = append(
			manager.preliminary[bearer],
			authorization,
		)
		manager.mu.Unlock()
		return cloneAuthorization(authorization), nil
	})
}

func (manager *testCollectorSessionManager) Admit(
	ctx context.Context,
	bearer string,
	request CollectorSessionAdmissionRequest,
) (CollectorSessionAdmission, error) {
	if manager.admitFunc != nil {
		return manager.admitFunc(ctx, bearer, request)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	queued := manager.preliminary[bearer]
	if len(queued) == 0 {
		return CollectorSessionAdmission{}, ErrUnauthorized
	}
	authorization := queued[0]
	if len(queued) == 1 {
		delete(manager.preliminary, bearer)
	} else {
		manager.preliminary[bearer] = queued[1:]
	}
	return manager.admissionLocked(authorization, request), nil
}

func (manager *testCollectorSessionManager) admissionFor(
	authorization Authorization,
	request CollectorSessionAdmissionRequest,
) CollectorSessionAdmission {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return manager.admissionLocked(authorization, request)
}

func (manager *testCollectorSessionManager) admissionLocked(
	authorization Authorization,
	request CollectorSessionAdmissionRequest,
) CollectorSessionAdmission {
	manager.nextGeneration++
	return CollectorSessionAdmission{
		Authorization: cloneAuthorization(authorization),
		Lease: collectorfleet.Lease{
			Scope: collectorfleet.Scope{
				TenantID: authorization.TenantID,
			},
			CollectorID: request.CollectorID,
			BootEpoch:   request.BootEpoch,
			StreamID:    request.StreamID,
			Generation:  manager.nextGeneration,
		},
	}
}

func (manager *testCollectorSessionManager) Activate(
	lease collectorfleet.Lease,
) error {
	if manager.activateFunc != nil {
		return manager.activateFunc(lease)
	}
	return nil
}

func (manager *testCollectorSessionManager) AuthorizeLease(
	ctx context.Context,
	bearer string,
	lease collectorfleet.Lease,
	checkedAt time.Time,
) (Authorization, error) {
	if manager.authorizeFunc != nil {
		return manager.authorizeFunc(ctx, bearer, lease, checkedAt)
	}
	return manager.source.Authorize(ctx, bearer)
}

func (manager *testCollectorSessionManager) RecordHeartbeat(
	ctx context.Context,
	lease collectorfleet.Lease,
	heartbeat collectorfleet.Heartbeat,
) (bool, error) {
	if manager.heartbeatFunc != nil {
		return manager.heartbeatFunc(ctx, lease, heartbeat)
	}
	return true, nil
}

func (manager *testCollectorSessionManager) Disconnect(
	ctx context.Context,
	lease collectorfleet.Lease,
	receivedAt time.Time,
) (bool, error) {
	if manager.disconnectFunc != nil {
		return manager.disconnectFunc(ctx, lease, receivedAt)
	}
	return true, nil
}

var _ CollectorSessionManager = (*testCollectorSessionManager)(nil)
