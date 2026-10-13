package common_persistence

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/lib/pq"
	"gorm.io/gorm/clause"
)

// LockInferenceRoutingPolicy serializes mutation validation and commit against
// every persisted policy dependency. Call it before taking the snapshot used to
// validate a write, in the same ambient transaction as the write. Ordinary
// discovery, generation and dry-run snapshots must remain unlocked.
//
// Routes are locked first in operation order, then the work organization and all
// ancestors in ID order. Organization locks cover absent policy rows as well as
// existing rows. Provider-policy writes pass all registered operations, so all
// policy writers retain this same route-before-organization order. Session
// action scanning remains optimistic; writers use the run-before-session order.
func (s *ControlPlaneStore) LockInferenceRoutingPolicy(ctx context.Context, organizationID int64, operations []int32) error {
	if tx, ok := scontext.GetTransaction(ctx); !ok || tx == nil {
		return ErrInferenceRoutingTransactionRequired
	}
	if organizationID < 0 {
		return ErrInferenceRoutingInvalidPolicy
	}
	if len(operations) > 0 {
		var routes []cpm.InferenceOperationRoute
		if err := s.routingSession(ctx).Select("operation").Where("operation = ANY(?::integer[])", pq.Array(operations)).
			Order("operation ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&routes).Error; err != nil {
			return fmt.Errorf("lock inference routing operations: %w", err)
		}
		found := make(map[int32]struct{}, len(routes))
		for _, route := range routes {
			found[route.Operation] = struct{}{}
		}
		for _, operation := range operations {
			if _, exists := found[operation]; !exists {
				return ErrControlPlaneRouteNotFound
			}
		}
	}
	if organizationID == 0 {
		return nil
	}
	var organizations []cpm.Organization
	if err := s.routingSession(ctx).Select("id").Where(`id IN (
 SELECT unnest(lineage || ARRAY[id]::bigint[]) FROM gocommon_organizations WHERE id = ? AND deleted_at IS NULL
 )`, organizationID).Order("id ASC").Clauses(clause.Locking{Strength: "UPDATE"}).Find(&organizations).Error; err != nil {
		return fmt.Errorf("lock inference routing organizations: %w", err)
	}
	for _, organization := range organizations {
		if organization.ID == organizationID {
			return nil
		}
	}
	return ErrInferenceRoutingOrganizationNotFound
}
