// Package common_persistence: this file implements the narrow AI control-plane store. It is
// intentionally NOT part of CommonPersistenceInterface (which is already large): the control plane
// depends only on this small surface for route resolution, inference routing policy, attempt
// recording, and admin read queries. See the AI control-plane migration plan, "Suggested go-common
// ownership" and "Narrow control-plane store".
//
// The store surface is split across sibling files by concern: operation routes
// (inference_control_plane_registry.go), model lookups and rate-limit policies
// (inference_control_plane_policies.go), attempt-ledger writes (inference_control_plane_recording.go),
// attempt-ledger reads/aggregation (inference_control_plane_analytics.go), routing policy writes
// (inference_routing_store.go), the routing snapshot (inference_routing_snapshot.go), policy
// previews (inference_routing_preview.go), policy session reads and blocks
// (inference_routing_sessions.go), and the policy mutation lock (inference_routing_lock.go).
// This file holds the shared store type, constructor, session helper, and sentinel errors.
//
// Transaction independence (decision 13): route and attempt-ledger methods use the store's base
// *gorm.DB through session and never pull an ambient transaction from the request context. Attempt
// writes therefore survive a product transaction rollback, and a provider call never holds a
// product transaction open. Routing policy methods are the deliberate exception: they join the
// caller's ambient transaction through routingSession so a policy change commits with its product
// action.
package common_persistence

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// ErrControlPlaneRouteNotFound is returned when a registered operation has no persisted route.
var ErrControlPlaneRouteNotFound = errors.New("control plane: no route configured for operation")

// ErrControlPlaneModelNotFound is returned when a route references a missing inference model.
var ErrControlPlaneModelNotFound = errors.New("control plane: inference model not found")

// ErrControlPlaneAttemptNotFound is returned when an attempt-ledger row does not exist.
var ErrControlPlaneAttemptNotFound = errors.New("control plane: inference attempt not found")

// ErrControlPlaneAttemptOrganizationRequired is returned when an attempt-ledger write is missing
// a positive owning organization. Historical unattributed rows remain readable, but every new
// start/finalization write must carry organization ownership.
var ErrControlPlaneAttemptOrganizationRequired = errors.New("control plane: inference attempt organization is required")

// ControlPlaneStore is the persistence-backed control-plane surface. It is safe for concurrent use;
// the embedded *gorm.DB manages its own pool.
type ControlPlaneStore struct {
	db *gorm.DB
}

// NewControlPlaneStore builds a store over the supplied gorm connection. The connection must be the
// base pool, not a request-scoped transaction, so recorder writes stay transaction-independent.
func NewControlPlaneStore(db *gorm.DB) *ControlPlaneStore {
	return &ControlPlaneStore{db: db}
}

// session returns the base connection bound to ctx. It deliberately does not consult the scontext
// transaction: control-plane writes are always their own transaction.
func (s *ControlPlaneStore) session(ctx context.Context) *gorm.DB {
	return s.db.WithContext(ctx)
}
