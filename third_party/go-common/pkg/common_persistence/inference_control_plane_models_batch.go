package common_persistence

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// GetModels loads catalog rows without filtering inactive models. Missing IDs are
// omitted. Like GetModel, it uses the transaction-independent control-plane session.
// This optional capability does not expand the required control-plane interfaces.
func (s *ControlPlaneStore) GetModels(ctx context.Context, modelIDs []uint64) ([]common_persistence_models.InferenceModel, error) {
	const chunkSize = 1000
	models := make([]common_persistence_models.InferenceModel, 0)
	for start := 0; start < len(modelIDs); start += chunkSize {
		end := min(start+chunkSize, len(modelIDs))
		var chunk []common_persistence_models.InferenceModel
		if err := s.session(ctx).Where("id IN ?", modelIDs[start:end]).Find(&chunk).Error; err != nil {
			return nil, fmt.Errorf("get inference models: %w", err)
		}
		models = append(models, chunk...)
	}
	return models, nil
}
