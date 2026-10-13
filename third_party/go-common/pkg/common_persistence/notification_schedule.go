package common_persistence

import (
	"context"
	"sync"
	"time"

	models "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// NotificationQueue identifies a coordinator, not an individual durable job.
type NotificationQueue uint8

const (
	NotificationQueueDelivery NotificationQueue = iota
	NotificationQueueFanout
	NotificationQueueRetirement
)

// NotificationSchedulingStore supplies process-local commit hints and the next
// eligibility delay. A false pending result means no currently schedulable rows.
// Delay uses the queue's authoritative clock and includes lease recovery.
// now and maxAttempts match delivery claims; other queues use the store clock.
// Subscribers must return immediately; unsubscribe is safe during publication.
type NotificationSchedulingStore interface {
	SubscribeNotificationQueues(func(NotificationQueue)) func()
	NextNotificationDelay(context.Context, NotificationQueue, time.Time, uint32) (delay time.Duration, pending bool, err error)
}

type notificationQueueSubscribers struct {
	mu        sync.Mutex
	next      uint64
	listeners map[uint64]func(NotificationQueue)
}

func (s *GormNotificationStore[U]) SubscribeNotificationQueues(wake func(NotificationQueue)) func() {
	s.queueWakes.mu.Lock()
	if s.queueWakes.listeners == nil {
		s.queueWakes.listeners = make(map[uint64]func(NotificationQueue))
	}
	s.queueWakes.next++
	id := s.queueWakes.next
	s.queueWakes.listeners[id] = wake
	s.queueWakes.mu.Unlock()
	return func() {
		s.queueWakes.mu.Lock()
		delete(s.queueWakes.listeners, id)
		s.queueWakes.mu.Unlock()
	}
}

// queueChanged runs after an autocommit write, or registers with the ambient
// transaction. Hook-less transactions rely on explicit caller wakes or recovery.
// TODO(multi-instance-v2): Producers and workers must share this store in one
// process for prompt wakeups. Add cross-process commit hints and reconnect scans
// before supporting separate worker processes or multiple replicas.
func (s *GormNotificationStore[U]) queueChanged(ctx context.Context, queues ...NotificationQueue) {
	wake := func(context.Context) error {
		s.queueWakes.mu.Lock()
		listeners := make([]func(NotificationQueue), 0, len(s.queueWakes.listeners))
		for _, listener := range s.queueWakes.listeners {
			listeners = append(listeners, listener)
		}
		s.queueWakes.mu.Unlock()
		for _, listener := range listeners {
			for _, queue := range queues {
				listener(queue)
			}
		}
		return nil
	}
	if !RegisterNotificationPostCommitOrSuppress(ctx, wake) {
		_ = wake(ctx)
	}
}

// ORDER BY/LIMIT uses the eligibility expression indexes; MIN over all pending
// leases would repeatedly scan a large delayed backlog. PostgreSQL GREATEST
// ignores NULL, so an unleased row's eligibility is simply due_at.
func (s *GormNotificationStore[U]) NextNotificationDelay(ctx context.Context, queue NotificationQueue, now time.Time, maxAttempts uint32) (time.Duration, bool, error) {
	statement, args, err := notificationDeadlineStatement(queue, now, maxAttempts)
	if err != nil {
		return 0, false, err
	}
	var result struct{ Seconds *float64 }
	if err := s.db.DB(ctx).Raw(statement, args...).Scan(&result).Error; err != nil {
		return 0, false, err
	}
	if result.Seconds == nil {
		return 0, false, nil
	}
	return max(0, time.Duration(*result.Seconds*float64(time.Second))), true, nil
}

func notificationDeadlineStatement(queue NotificationQueue, now time.Time, maxAttempts uint32) (string, []any, error) {
	var statement string
	var args []any
	switch queue {
	case NotificationQueueDelivery:
		statement = `SELECT EXTRACT(EPOCH FROM (deadline - ?::timestamptz)) AS seconds FROM (
		 SELECT GREATEST(due_at, lease_expires_at) AS deadline FROM gocommon_notification_deliveries
		 WHERE state = 'pending' AND (attempts >= ? OR NOT ` + sourceResolutionPendingSQL("gocommon_notification_deliveries.notification_id") + `)
		 ORDER BY GREATEST(due_at, lease_expires_at), id LIMIT 1) q`
		args = []any{now, maxAttempts}
	case NotificationQueueFanout:
		statement = `SELECT EXTRACT(EPOCH FROM (deadline - clock_timestamp())) AS seconds FROM (
		 SELECT GREATEST(due_at, lease_expires_at) AS deadline FROM gocommon_notification_emissions
		 WHERE state = 'pending' AND fingerprint <> ''
		 ORDER BY GREATEST(due_at, lease_expires_at), id LIMIT 1) q`
	case NotificationQueueRetirement:
		// Each arm reads at most one indexed deadline from its queue.
		statement = `SELECT EXTRACT(EPOCH FROM (MIN(deadline) - clock_timestamp())) AS seconds FROM (`
		for i, table := range []string{models.NotificationChatRetirement{}.TableName(), models.NotificationScopeRetirement{}.TableName(), models.NotificationRecipientRetirement{}.TableName(), models.NotificationSourceResolution{}.TableName()} {
			if i > 0 {
				statement += " UNION ALL "
			}
			statement += "(SELECT due_at AS deadline FROM " + table + " ORDER BY due_at LIMIT 1)"
		}
		statement += ") q"
	default:
		return "", nil, ErrNotificationInput
	}
	return statement, args, nil
}
