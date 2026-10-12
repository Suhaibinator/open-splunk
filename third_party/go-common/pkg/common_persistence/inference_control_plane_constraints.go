package common_persistence

import (
	"fmt"
	"strings"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	inferenceOperationRouteModelConstraint = "fk_inference_operation_routes_model"
	inferenceModelPolicyModelConstraint    = "fk_inference_model_rate_limit_policies_model"
)

type orphanedInferenceOperationRoute struct {
	Operation        int32
	InferenceModelID uint64
}

// ensureInferenceControlPlaneModelConstraints installs the model ownership constraints that
// cannot be represented without adding GORM association fields to the narrow control-plane rows.
// PostgreSQL DDL is transactional, so both constraints are installed atomically.
func (h *CommonPersistenceHandler[U]) ensureInferenceControlPlaneModelConstraints(db *gorm.DB) error {
	models := common_persistence_models.TableNameInferenceModels
	routes := common_persistence_models.TableNameInferenceOperationRoutes
	policies := common_persistence_models.TableNameInferenceModelRateLimitPolicies

	return db.Transaction(func(tx *gorm.DB) error {
		var orphanedRoutes []orphanedInferenceOperationRoute
		if err := tx.Table(routes + " AS routes").
			Select("routes.operation, routes.inference_model_id").
			Joins("LEFT JOIN " + models + " AS models ON models.id = routes.inference_model_id").
			Where("models.id IS NULL").
			Order("routes.operation ASC").
			Scan(&orphanedRoutes).Error; err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("find orphaned inference operation routes: %w", err),
				zap.String(logkeys.Stage, "find_orphaned_inference_operation_routes"))
		}
		if len(orphanedRoutes) > 0 {
			refs := make([]string, 0, len(orphanedRoutes))
			for _, route := range orphanedRoutes {
				refs = append(refs, fmt.Sprintf("operation=%d model_id=%d", route.Operation, route.InferenceModelID))
			}
			firstOrphan := orphanedRoutes[0]
			return appLogger.InvariantError(
				"inference_operation_routes_reference_existing_models",
				fmt.Errorf("orphaned operation routes must be repaired: %s", strings.Join(refs, ", ")),
				zap.String(logkeys.Stage, "install_inference_model_constraints"),
				zap.Int32(logkeys.Operation, firstOrphan.Operation),
				zap.Uint64(appLogger.KeyInferenceModelID, firstOrphan.InferenceModelID),
				zap.String(logkeys.Expected, "zero orphaned operation routes"),
				zap.Int(logkeys.Actual, len(orphanedRoutes)),
			)
		}

		var orphanedPolicies int64
		if err := tx.Raw(fmt.Sprintf("SELECT count(*) FROM %s p WHERE NOT EXISTS (SELECT 1 FROM %s m WHERE m.id = p.inference_model_id)", policies, models)).Scan(&orphanedPolicies).Error; err != nil {
			return err
		}
		if orphanedPolicies != 0 {
			return fmt.Errorf("%w: orphaned inference rate-limit policies must be repaired", ErrManualMigrationRequired)
		}

		constraints := []sharedConstraint{
			{routes, inferenceOperationRouteModelConstraint, fmt.Sprintf("FOREIGN KEY (inference_model_id) REFERENCES %s(id) ON DELETE RESTRICT", models)},
			{policies, inferenceModelPolicyModelConstraint, fmt.Sprintf("FOREIGN KEY (inference_model_id) REFERENCES %s(id) ON DELETE CASCADE", models)},
		}
		for _, constraint := range constraints {
			if err := executeSharedConstraint(tx, constraint); err != nil {
				return fmt.Errorf("install inference model constraint: %w", err)
			}
		}
		return nil
	})
}
