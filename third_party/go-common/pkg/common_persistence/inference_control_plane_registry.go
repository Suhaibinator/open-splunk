// Control-plane route persistence keyed by the application's numeric operation. The shared store
// type, constructor, and sentinel errors live in inference_control_plane_store.go.

package common_persistence

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
)

// GetRoute loads the persisted route for a numeric operation. It returns
// ErrControlPlaneRouteNotFound when none is configured.
func (s *ControlPlaneStore) GetRoute(ctx context.Context, operation int32) (*common_persistence_models.InferenceOperationRoute, error) {
	var route common_persistence_models.InferenceOperationRoute
	err := s.session(ctx).
		Where("operation = ?", operation).
		First(&route).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrControlPlaneRouteNotFound
	}
	if err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("get route: %w", err),
			zap.String(logkeys.Stage, "get_inference_operation_route"),
			zap.Int32(logkeys.Operation, operation))
	}
	return &route, nil
}

// SeedRouteIfMissing inserts a default route only when the operation has none. It returns true when
// it inserted, false when a route already existed. It never overwrites existing admin configuration
// (seed-missing-never-overwrite).
func (s *ControlPlaneStore) SeedRouteIfMissing(ctx context.Context, route *common_persistence_models.InferenceOperationRoute) (bool, error) {
	if route == nil {
		return false, appLogger.InvariantError("inference_operation_route_present",
			errors.New("inference operation route is required"),
			zap.String(logkeys.Stage, "seed_inference_operation_route"),
			zap.String(logkeys.Expected, "non-nil route"),
			zap.String(logkeys.Actual, "nil route"))
	}
	result := s.session(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "operation"}},
			DoNothing: true,
		}).
		Create(route)
	if result.Error != nil {
		return false, appLogger.WithErrorFields(fmt.Errorf("seed route: %w", result.Error),
			zap.String(logkeys.Stage, "seed_inference_operation_route"),
			zap.Int32(logkeys.Operation, route.Operation),
			zap.Uint64(appLogger.KeyInferenceModelID, route.InferenceModelID))
	}
	return result.RowsAffected > 0, nil
}

// ListRoutes returns every persisted route ordered by numeric operation.
func (s *ControlPlaneStore) ListRoutes(ctx context.Context) ([]common_persistence_models.InferenceOperationRoute, error) {
	var routes []common_persistence_models.InferenceOperationRoute
	if err := s.session(ctx).
		Order("operation ASC").
		Find(&routes).Error; err != nil {
		return nil, appLogger.WithErrorFields(fmt.Errorf("list routes: %w", err),
			zap.String(logkeys.Stage, "list_inference_operation_routes"))
	}
	return routes, nil
}

// UpsertRoute writes an admin route edit: it inserts the route when the operation has none and
// otherwise replaces every mutable column (never created_at). This is the explicit-overwrite
// counterpart of SeedRouteIfMissing, which never touches existing configuration. The SQL assigns
// config_version atomically (1 on insert, previous+1 on update) and refreshes route from the written
// row, so route carries this edit's exact version even under concurrent edits.
func (s *ControlPlaneStore) UpsertRoute(ctx context.Context, route *common_persistence_models.InferenceOperationRoute) error {
	if route == nil {
		return appLogger.InvariantError("inference_operation_route_present",
			errors.New("inference operation route is required"),
			zap.String(logkeys.Stage, "upsert_inference_operation_route"),
			zap.String(logkeys.Expected, "non-nil route"),
			zap.String(logkeys.Actual, "nil route"))
	}
	route.ConfigVersion = new(uint64(1))
	updates := clause.AssignmentColumns([]string{
		"inference_model_id", "api", "total_timeout", "max_attempts", "initial_backoff",
		"max_backoff", "temperature", "updater_user_id", "updated_at", "model_list_version",
	})
	updates = append(updates, clause.Assignment{Column: clause.Column{Name: "config_version"}, Value: gorm.Expr("COALESCE(" + common_persistence_models.TableNameInferenceOperationRoutes + ".config_version, 0) + 1")})
	err := s.session(ctx).Transaction(func(tx *gorm.DB) error {
		var existing common_persistence_models.InferenceOperationRoute
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("operation = ?", route.Operation).First(&existing).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			seed := *route
			seed.ConfigVersion = new(uint64(0))
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
				return err
			}
			err = tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("operation = ?", route.Operation).First(&existing).Error
		}
		if err != nil {
			return err
		}
		if existing.ModelListVersion != route.ModelListVersion {
			return ErrInferencePolicyVersionConflict
		}
		var options []common_persistence_models.InferenceOperationModelOption
		if err := tx.Where("operation = ?", route.Operation).Find(&options).Error; err != nil {
			return err
		}
		if !routeDefaultAllowed(options, route.InferenceModelID) {
			return ErrInferenceRoutingInvalidPolicy
		}
		if len(options) > 0 {
			if !validInferencePolicyVersion(existing.ModelListVersion) {
				return ErrInferenceRoutingInvalidPolicy
			}
			route.ModelListVersion++
		}
		return tx.Clauses(clause.Returning{}, clause.OnConflict{Columns: []clause.Column{{Name: "operation"}}, DoUpdates: updates}).Create(route).Error
	})
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("upsert route: %w", err),
			zap.String(logkeys.Stage, "upsert_inference_operation_route"),
			zap.Int32(logkeys.Operation, route.Operation),
			zap.Uint64(appLogger.KeyInferenceModelID, route.InferenceModelID))
	}
	return nil
}
