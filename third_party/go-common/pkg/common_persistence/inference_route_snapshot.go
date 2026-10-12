package common_persistence

import (
	"context"
	"errors"
	"fmt"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
)

// Joined associations keep all three rows in one PostgreSQL statement snapshot.
// They are read-only: -:migration keeps AutoMigrate from deriving foreign keys
// from these cached association schemas.
type inferenceRouteSnapshot struct {
	cpm.InferenceOperationRoute `gorm:"embedded"`
	Model                       *inferenceSnapshotModel `gorm:"foreignKey:InferenceModelID;references:ID;-:migration"`
}

func (inferenceRouteSnapshot) TableName() string { return cpm.TableNameInferenceOperationRoutes }

type inferenceSnapshotModel struct {
	cpm.InferenceModel `gorm:"embedded"`
	Policy             *cpm.InferenceModelRateLimitPolicy `gorm:"foreignKey:InferenceModelID;references:ID;-:migration"`
}

func (inferenceSnapshotModel) TableName() string { return cpm.TableNameInferenceModels }

// GetRouteSnapshot loads the route, selected model and nullable model policy in
// one query. Every invocation reads fresh persisted settings; there is no cache
// invalidation or stale generation interval.
func (s *ControlPlaneStore) GetRouteSnapshot(ctx context.Context, operation int32) (*cpm.InferenceOperationRoute, *cpm.InferenceModel, *cpm.InferenceModelRateLimitPolicy, error) {
	var snapshot inferenceRouteSnapshot
	err := s.session(ctx).Joins("Model").Joins("Model.Policy").
		Where(cpm.TableNameInferenceOperationRoutes+".operation = ?", operation).First(&snapshot).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil, ErrControlPlaneRouteNotFound
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read inference route snapshot: %w", err)
	}
	if snapshot.Model == nil {
		return nil, nil, nil, ErrControlPlaneModelNotFound
	}
	return &snapshot.InferenceOperationRoute, &snapshot.Model.InferenceModel, snapshot.Model.Policy, nil
}
