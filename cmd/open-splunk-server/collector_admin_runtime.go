package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

const collectorCatalogCursorKeyPurpose = "collector-catalog-cursors"

type runtimeCollectorCatalog interface {
	Get(
		context.Context,
		collectorfleet.Scope,
		string,
		[]collectorfleet.CollectorLiveness,
	) (collectorfleet.CatalogEntry, error)
	List(
		context.Context,
		collectorfleet.Scope,
		[]collectorfleet.CollectorLiveness,
		collectorfleet.ListRequest,
	) (collectorfleet.ListResult, error)
}

type runtimeCollectorAdministrationStore interface {
	UpdateDisplayName(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		*string,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
	SetAdministrativeState(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		collectorfleet.AdministrativeState,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
}

type runtimeCollectorLiveness interface {
	SnapshotLiveness(
		collectorfleet.Scope,
	) ([]collectorfleet.CollectorLiveness, error)
}

// runtimeCollectorAdministration is a boundary adapter, not an owner. Its
// catalog and store borrow the process control database, and its optional
// heartbeat runtime remains owned by the collector transport lifecycle.
type runtimeCollectorAdministration struct {
	catalog  runtimeCollectorCatalog
	store    runtimeCollectorAdministrationStore
	liveness runtimeCollectorLiveness
}

var _ server.CollectorAdministration = (*runtimeCollectorAdministration)(nil)

func newRuntimeCollectorAdministration(
	ctx context.Context,
	db *control.DB,
	store *collectorfleet.Store,
	heartbeats *collectorfleet.HeartbeatRuntime,
	masterKeyPath string,
) (*runtimeCollectorAdministration, error) {
	if store == nil {
		return nil, errors.New(
			"create collector administration: collector store is required",
		)
	}
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return nil, fmt.Errorf(
			"open collector-catalog master key: %w",
			err,
		)
	}
	defer clear(masterKey)

	cursorKey, err := deriveCollectorCatalogCursorKey(masterKey)
	if err != nil {
		return nil, err
	}
	defer clear(cursorKey)

	catalog, err := collectorfleet.NewCatalog(
		db,
		collectorfleet.CatalogOptions{CursorKey: cursorKey},
	)
	if err != nil {
		return nil, fmt.Errorf("create collector catalog: %w", err)
	}
	var liveness runtimeCollectorLiveness
	if heartbeats != nil {
		liveness = heartbeats
	}
	return &runtimeCollectorAdministration{
		catalog:  catalog,
		store:    store,
		liveness: liveness,
	}, nil
}

func deriveCollectorCatalogCursorKey(masterKey []byte) ([]byte, error) {
	key, err := deriveServerKey(masterKey, collectorCatalogCursorKeyPurpose)
	if err != nil {
		return nil, fmt.Errorf("derive collector-catalog cursor key: %w", err)
	}
	return key, nil
}

func (adapter *runtimeCollectorAdministration) Get(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
) (collectorfleet.CatalogEntry, error) {
	scope = detachedCollectorScope(scope)
	liveness, err := adapter.snapshotLiveness(scope)
	if err != nil {
		return collectorfleet.CatalogEntry{}, err
	}
	return adapter.catalog.Get(
		ctx,
		scope,
		strings.Clone(collectorID),
		liveness,
	)
}

func (adapter *runtimeCollectorAdministration) List(
	ctx context.Context,
	scope collectorfleet.Scope,
	request collectorfleet.ListRequest,
) (collectorfleet.ListResult, error) {
	scope = detachedCollectorScope(scope)
	liveness, err := adapter.snapshotLiveness(scope)
	if err != nil {
		return collectorfleet.ListResult{}, err
	}
	return adapter.catalog.List(
		ctx,
		scope,
		liveness,
		collectorfleet.CloneListRequest(request),
	)
}

func (adapter *runtimeCollectorAdministration) UpdateDisplayName(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	displayName *string,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	return adapter.store.UpdateDisplayName(
		ctx,
		detachedCollectorScope(scope),
		strings.Clone(collectorID),
		expectedVersion,
		cloneRuntimeString(displayName),
		receivedAt,
	)
}

func (adapter *runtimeCollectorAdministration) SetAdministrativeState(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	state collectorfleet.AdministrativeState,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	return adapter.store.SetAdministrativeState(
		ctx,
		detachedCollectorScope(scope),
		strings.Clone(collectorID),
		expectedVersion,
		state,
		receivedAt,
	)
}

func (adapter *runtimeCollectorAdministration) snapshotLiveness(
	scope collectorfleet.Scope,
) ([]collectorfleet.CollectorLiveness, error) {
	var snapshot []collectorfleet.CollectorLiveness
	if adapter.liveness != nil {
		var err error
		snapshot, err = adapter.liveness.SnapshotLiveness(scope)
		if err != nil {
			// SnapshotLiveness is a trusted-runtime boundary. Do not preserve
			// any storage/control sentinel identity that the HTTP layer could
			// misclassify as a client request error.
			return nil, errors.New(
				"snapshot trusted collector liveness: " + err.Error(),
			)
		}
	}
	if err := collectorfleet.ValidateLivenessSnapshot(
		scope,
		snapshot,
	); err != nil {
		// This snapshot and scope are server-owned. Deliberately do not wrap
		// ErrInvalidArgument: the HTTP boundary reserves that identity for
		// malformed client requests.
		return nil, errors.New(
			"validate trusted collector liveness snapshot: " + err.Error(),
		)
	}
	return snapshot, nil
}

func detachedCollectorScope(
	scope collectorfleet.Scope,
) collectorfleet.Scope {
	return collectorfleet.Scope{TenantID: strings.Clone(scope.TenantID)}
}
