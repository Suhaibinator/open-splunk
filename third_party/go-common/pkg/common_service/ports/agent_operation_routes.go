package ports

import (
	"context"

	common_persistence_models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// AgentOperationRouteStore is the narrow control-plane read surface the agent runtime resolves
// models through: an agent's operation selects a route row, and the route's InferenceModelID
// selects the exact inference-catalog row to run on. *common_persistence.ControlPlaneStore
// satisfies it, which is the same store an application wires into SetInferenceControlPlane.
type AgentOperationRouteStore interface {
	GetRoute(ctx context.Context, operation int32) (*common_persistence_models.InferenceOperationRoute, error)
	GetModel(ctx context.Context, modelID uint64) (*common_persistence_models.InferenceModel, error)
}
