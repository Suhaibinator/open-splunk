package notification

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/config"
)

// sourceDeliveryPreference reuses metadata loaded for authorization at this
// boundary, or performs exactly one combined resource-preference callback.
// Snapshots are never retained across requests, rendering, or later attempts.
func (h *Handler) sourceDeliveryPreference(ctx context.Context, t Type, candidate SourceCandidate, snapshot *DeliveryPreference) (DeliveryPreference, error) {
	var preference DeliveryPreference
	if snapshot != nil {
		preference = *snapshot
	} else if t.DeliveryPreference != nil {
		var err error
		preference, err = t.DeliveryPreference(ctx, candidate.RecipientID, candidate.Payload)
		if err != nil {
			return DeliveryPreference{}, safeCallbackError("delivery_preference", err)
		}
	}
	if len(preference.Tag) > config.MaxNotificationDeliveryTagBytes {
		return DeliveryPreference{}, fmt.Errorf("%w: delivery preference exceeds tag limit", ErrInvalid)
	}
	return preference, nil
}
