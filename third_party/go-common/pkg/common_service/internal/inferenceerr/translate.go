// Package inferenceerr translates inference-routing persistence errors into the service error
// contract, so callers above the service layer never match persistence sentinels.
package inferenceerr

import (
	"errors"
	"fmt"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	apperrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
)

// Translate wraps a known inference-routing or session-pin persistence error in its service
// error, keeping the persistence cause in the chain for logs. Other errors are returned unchanged.
func Translate(err error) error {
	var service error
	switch {
	case err == nil:
		return nil
	case errors.Is(err, persistence.ErrInferencePolicyVersionConflict), errors.Is(err, persistence.ErrChatAgentSessionInferencePinVersionConflict):
		service = apperrors.ErrInferencePolicyVersionConflict
	case errors.Is(err, persistence.ErrInferenceRoutingInvalidPolicy), errors.Is(err, persistence.ErrChatAgentSessionInferencePinInvalid):
		service = apperrors.ErrInferencePolicyInvalid
	case errors.Is(err, persistence.ErrInferenceRoutingOrganizationNotFound):
		service = apperrors.ErrOrganizationNotFound
	default:
		return err
	}
	if errors.Is(err, service) {
		return err
	}
	return fmt.Errorf("%w: %w", service, err)
}
