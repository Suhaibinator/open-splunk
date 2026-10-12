package inference

import (
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

type BlockError struct {
	Reason           proto_models.InferenceBlockReason
	OrganizationID   uint64
	Operation        int32
	InferenceModelID uint64
	Cause            error
}

func (blocked *BlockError) Error() string {
	if blocked.Cause != nil {
		return fmt.Sprintf("inference blocked: %s (operation %d, organization %d, model %d): %v", blocked.Reason, blocked.Operation, blocked.OrganizationID, blocked.InferenceModelID, blocked.Cause)
	}
	return fmt.Sprintf("inference blocked: %s (operation %d, organization %d, model %d)", blocked.Reason, blocked.Operation, blocked.OrganizationID, blocked.InferenceModelID)
}

func (blocked *BlockError) Unwrap() error { return blocked.Cause }
