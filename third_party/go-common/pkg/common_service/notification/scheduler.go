package notification

import (
	"context"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
)

const notificationSafetyInterval = 5 * time.Minute

func wakeQueue(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (h *Handler) subscribeQueueWakes() {
	h.queueSubscribe.Do(func() {
		if h.store != nil {
			h.unsubscribeQueues = h.store.SubscribeNotificationQueues(func(queue persistence.NotificationQueue) {
				switch queue {
				case persistence.NotificationQueueDelivery:
					wakeQueue(h.deliveryWake)
				case persistence.NotificationQueueFanout:
					wakeQueue(h.fanoutWake)
				case persistence.NotificationQueueRetirement:
					wakeQueue(h.retirementWake)
				}
			})
		}
	})
}

// WakeWorkers reconciles queues after a caller-owned transaction commits when
// that transaction cannot register commit hooks. Never call it before commit.
// Ordinary store mutations register their own bounded, coalescing commit wakes.
func (h *Handler) WakeWorkers() {
	if h == nil {
		return
	}
	wakeQueue(h.deliveryWake)
	wakeQueue(h.fanoutWake)
	wakeQueue(h.retirementWake)
}

func (h *Handler) nextQueueClaim(ctx context.Context, queue persistence.NotificationQueue, timeout, contentionFloor time.Duration, maxAttempts uint32) (time.Time, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	delay, pending, err := h.store.NextNotificationDelay(lookupCtx, queue, h.now(), maxAttempts)
	if err != nil {
		return time.Time{}, err
	}
	// Jitter only recovery scans; actual job deadlines retain their precision.
	safety := pollJitter(notificationSafetyInterval, .1)
	if !pending {
		delay = safety
	} else if delay <= 0 {
		// SKIP LOCKED or recipient lock contention can leave a past deadline
		// after an empty claim. Retrying it immediately would spin.
		delay = contentionFloor
	}
	return time.Now().Add(min(delay, safety)), nil
}
