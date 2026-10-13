package common_service

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	persistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/email_client"
	"github.com/Suhaibinator/go-common/pkg/common_service/notification"
	"github.com/Suhaibinator/go-common/pkg/common_service/rate_limiter"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// WithNotificationType registers an application-owned notification type before workers start.
func WithNotificationType[U common_service_models.UserLike](t notification.Type) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationTypes = append(o.notificationTypes, t) }
}

// WithNotificationPolicyProvider overrides individual type policies at each decision boundary.
func WithNotificationPolicyProvider[U common_service_models.UserLike](provider notification.PolicyProvider) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationPolicyProvider = provider }
}

// WithNotificationWorkerCount sets the startup-bound delivery concurrency (1–64).
// Omission uses four workers; an explicit zero is invalid.
func WithNotificationWorkerCount[U common_service_models.UserLike](count int) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationWorkerCount = &count }
}

// WithNotificationFanoutWorkersEnabled controls whether this process claims
// fan-out work (default true). The hot KMS worker count applies to each
// participating process, so total claim concurrency is the count multiplied by
// the number of participating processes. The hot KMS pause is global: it stops
// claims on every process without stopping enqueue. Prompt discovery requires
// producers and workers to share one NotificationStore; separate processes use
// slow recovery scans until TODO(multi-instance-v2) adds distributed wakeups.
func WithNotificationFanoutWorkersEnabled[U common_service_models.UserLike](enabled bool) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationFanoutWorkersEnabled = &enabled }
}

// WithNotificationProducer registers an application event producer before Start.
func WithNotificationProducer[U common_service_models.UserLike](producer notification.Producer) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationProducers = append(o.notificationProducers, producer) }
}

// WithChatNotificationApprovers adds application-selected candidates to the run's
// requesting user. Every candidate is freshly checked against channel visibility
// and the existing tool resolver policy.
func WithChatNotificationApprovers[U common_service_models.UserLike](callback func(context.Context, uint64) ([]uint64, error)) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationApprovers = callback }
}

// WithChatNotificationApproverPage installs bounded keyset enumeration for large
// tool approver audiences. Return at most limit strictly ascending IDs greater
// than after; fewer than limit finishes the audience. The callback runs with
// the ambient source/materialization transaction. It supersedes
// WithChatNotificationApprovers when present.
func WithChatNotificationApproverPage[U common_service_models.UserLike](callback func(context.Context, uint64, uint64, int) ([]uint64, error)) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationApproverPage = callback }
}

// WithNotificationLinkBuilder installs an application-owned link builder for native
// chat email. Unread payload v2 contains only ChannelId: MessageId zero means
// open the channel without a message anchor. Approvals retain their exact message
// target. It receives IDs only; returning an invalid URL prevents sending.
// Without it, emails link no external target.
func WithNotificationLinkBuilder[U common_service_models.UserLike](builder func(context.Context, uint64, uint32, *proto_models.ChatNotificationPayload) (string, error)) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationLinkBuilder = builder }
}

// WithNotificationStore replaces the durable inbox store, primarily for alternate persistence.
func WithNotificationStore[U common_service_models.UserLike](store persistence.NotificationStore) HandlerOption[U] {
	return func(o *handlerOptions[U]) { o.notificationStore = store }
}
func (h *CommonServiceHandler[U]) GetNotificationHandler() *notification.Handler {
	return h.notificationHandler
}

// CountNotifications returns the caller's bounded unread count after enforcing
// the per-user rate_limits.notification_count_* limit, which the reloading
// limiter samples once per call. Exceeding it returns ErrRateLimitExceeded.
func (h *CommonServiceHandler[U]) CountNotifications(ctx context.Context, userID uint64) (notification.CountResult, error) {
	if err := h.checkRateLimit(ctx, rate_limiter.ActionNotificationCount, strconv.FormatUint(userID, 10)); err != nil {
		return notification.CountResult{}, err
	}
	return h.notificationHandler.Count(ctx, userID)
}
func (h *CommonServiceHandler[U]) RegisterNotificationType(t notification.Type) error {
	return h.notificationHandler.Register(t)
}
func (h *CommonServiceHandler[U]) RegisterNotificationProducer(producer notification.Producer) error {
	return h.notificationHandler.RegisterProducer(producer)
}
func (h *CommonServiceHandler[U]) initializeNotifications(options handlerOptions[U]) error {
	store := options.notificationStore
	if store == nil {
		if db, ok := h.commonPersistenceHandler.(persistence.DatabaseProvider); ok {
			store = persistence.NewNotificationStore[U](db, func() time.Duration { return h.runtimeConfig().PostCommitHookTimeout() })
		} else if h.runtimeConfig().EnableNotifications() {
			return fmt.Errorf("notifications require persistence DatabaseProvider or WithNotificationStore")
		}
	}

	h.notificationHandler = notification.New(store, notification.Dependencies{
		Config: notificationConfigProvider(h.runtimeConfig),
		Logger: h.logger,
		Log: func(ctx context.Context) *zap.Logger {
			return appLogger.For(ctx, h.logger, "common_service.notification")
		},
		Metrics: h.metricsHandler,
		Preferences: func(ctx context.Context, userID uint64) (notification.GlobalPreferences, error) {
			settings, err := h.userSettingsHandler.GetUserSettings(ctx, userID)
			if err != nil {
				return notification.GlobalPreferences{}, err
			}
			return notification.GlobalPreferences{EmailEnabled: settings.GetEmailNotifications(), ChatEmailEnabled: settings.GetChatEmailNotifications()}, nil
		},
		PrepareEmail: func(ctx context.Context, userID uint64, email notification.Email) (func(context.Context) error, error) {
			core, err := h.commonPersistenceHandler.GetUserByID(ctx, userID)
			if errors.Is(err, persistence.ErrUserNotFound) {
				return nil, notification.ErrNotFound
			}
			if err != nil {
				return nil, err
			}
			if core == nil || core.ID != userID || core.Banned || core.DeletedAt.Valid {
				return nil, notification.ErrNotFound
			}
			user, err := h.userCallbacks.UserLookup(ctx, common_service_models.UserIdType(userID))
			if err != nil {
				return nil, err
			}
			if uint64(user.GetID()) != userID || strings.TrimSpace(user.GetEmail()) == "" {
				return nil, notification.ErrNotFound
			}
			address := user.GetEmail()
			return func(sendCtx context.Context) error {
				var err error
				if email.HTML {
					err = h.emailClient.SendHTMLEmail(sendCtx, address, email.Subject, email.Body)
				} else {
					err = h.emailClient.SendEmail(sendCtx, address, email.Subject, email.Body)
				}
				// The reloading client samples email_enabled again per send.
				if errors.Is(err, email_client.ErrEmailDisabled) {
					return notification.ErrEmailDisabled
				}
				return err
			}, nil
		},
		// Same provider the reloading email client samples per send.
		EmailEnabled: func() bool { return h.runtimeConfig().EmailEnabled() },
	})
	if options.notificationWorkerCount != nil {
		if err := h.notificationHandler.SetWorkerCount(*options.notificationWorkerCount); err != nil {
			return err
		}
	}
	if options.notificationFanoutWorkersEnabled != nil {
		if err := h.notificationHandler.SetFanoutWorkersEnabled(*options.notificationFanoutWorkersEnabled); err != nil {
			return err
		}
	}
	h.notificationHandler.SetPolicyProvider(options.notificationPolicyProvider)
	if err := h.registerNativeNotificationTypes(); err != nil {
		return err
	}
	if store != nil {
		h.configureInvitationNotifications()
	}
	for _, t := range options.notificationTypes {
		if err := h.notificationHandler.Register(t); err != nil {
			return err
		}
	}
	for _, producer := range options.notificationProducers {
		if err := h.notificationHandler.RegisterProducer(producer); err != nil {
			return err
		}
	}
	return nil
}

// notificationConfigProvider shares the entire immutable policy, including its
// catalog, rather than rebuilding the scalar config at each operation.
func notificationConfigProvider(sample func() config.ServiceView) func() *notification.Config {
	memo := config.NewGenerationMemo(func(v config.ServiceView) (*notification.Config, error) {
		return notification.ConfigFromView(v), nil
	}, nil)
	return memo.Provider(sample)
}
