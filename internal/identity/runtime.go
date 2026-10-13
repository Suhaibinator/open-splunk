// Package identity adapts GoCommon identities to OpenSplunk resource authorization.
package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	gcapi "github.com/Suhaibinator/go-common/pkg/common_api"
	"github.com/Suhaibinator/go-common/pkg/common_converter"
	gcpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_sanitizer"
	"github.com/Suhaibinator/go-common/pkg/common_service"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	gclogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/oauth"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const Prefix = "/api/identity"

type Options struct {
	Common             *gcconfig.Config
	GoogleClientID     string
	GoogleClientSecret gcconfig.Secret
	GoogleRedirectURLs []string
	Logger             *zap.Logger
	// The supplied DB shares the native organization mutation transaction.
	BeforeOrganizationDelete func(context.Context, *gorm.DB, int64) error
}

type Runtime struct {
	persistence *gcpersistence.CommonPersistenceHandler[sm.User]
	service     *common_service.CommonServiceHandler[sm.User]
	api         *gcapi.CommonAPIHandler[sm.User]
	router      *router.Router[sm.UserIdType, sm.User]
	roles       *organization.OrgRoleProvider
	sanitizer   *common_sanitizer.Handler
	cookieName  string
	closeOnce   sync.Once
	closeErr    error
}

// New borrows the application's PostgreSQL pool. Close stops identity-owned
// workers without closing that pool. Google owns credentials; no bootstrap or
// alternate administrator credential is installed.
func New(ctx context.Context, pool *gorm.DB, options Options) (_ *Runtime, err error) {
	if pool == nil || options.Common == nil || options.GoogleClientID == "" || options.GoogleClientSecret.StringValue() == "" || len(options.GoogleRedirectURLs) == 0 {
		return nil, fmt.Errorf("identity: PostgreSQL, configuration and Google OAuth credentials/redirects are required")
	}
	logger := options.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg := *options.Common
	Configure(&cfg)
	if err := cfg.ValidateConfig(); err != nil {
		return nil, fmt.Errorf("identity configuration: %w", err)
	}
	source := gcconfig.Static(&cfg)
	r := &Runtime{cookieName: source().SessionCookieName()}
	defer func() {
		if err != nil {
			_ = r.Close()
		}
	}()
	r.persistence, err = gcpersistence.Borrow[sm.User](ctx, pool, source.Persistence(), logger, nil)
	if err != nil {
		return nil, err
	}
	converter := common_converter.NewConverterHandler[sm.User](source.Converter(), logger)
	lookup := &userLookup{persistence: r.persistence, converter: converter}
	callbacks := ports.NewUserCallbackConfig[sm.User](lookup).WithSyncCallbacks(&ports.UserSyncCallbacks[sm.User]{OnUserDeleted: r.removeDeletedUserMemberships})
	r.roles, err = organization.NewOrgRoleProvider(r.persistence, func() gcconfig.OrganizationsView { return source() })
	if err != nil {
		return nil, err
	}
	orgCallbacks := ports.NewOrganizationCallbackConfig[sm.User]().WithCallbacks(&ports.OrganizationCallbacks{
		OnOrganizationDeleted: func(ctx context.Context, id sm.OrganizationIdType) error {
			if options.BeforeOrganizationDelete == nil {
				return gerrors.ErrInsufficientPermissions
			}
			return options.BeforeOrganizationDelete(ctx, r.persistence.DB(ctx), id)
		},
	})
	r.service, err = common_service.NewCommonServiceHandler(source.Service(), r.persistence, callbacks, logger, nil, nil,
		common_service.WithOAuthProviders[sm.User](oauth.Static(oauth.Google(options.GoogleClientID, options.GoogleClientSecret, options.GoogleRedirectURLs...))),
		common_service.WithOrganizationCallbacks(orgCallbacks),
		common_service.WithOrganizationOptions[sm.User](organization.WithMembershipRolePolicy(rolePolicy{}), organization.WithInitialMembershipPolicy(r.initialMemberships)),
	)
	if err != nil {
		return nil, err
	}
	r.sanitizer = common_sanitizer.NewHandler(source.Sanitization(), logger)
	r.router = router.NewRouter(router.RouterConfig{Logger: logger, AddUserObjectToCtx: true, GlobalMaxBodySize: 1 << 20}, router.RouterDependencies[sm.UserIdType, sm.User]{
		Authenticate: func(ctx context.Context, token string) (*sm.User, bool) {
			user, err := r.service.ValidateAuthTokenAndGetUser(ctx, token)
			if err != nil || user == nil {
				return nil, false
			}
			if _, err := subjectForUser(*user); err != nil {
				return nil, false
			}
			return user, true
		},
		UserID:      func(user *sm.User) sm.UserIdType { return user.ID },
		UserIDField: gclogger.UserIDField,
	})
	r.api, err = gcapi.NewCommonAPIHandler(source.API(), r.router, r.service, logger, nil, r.sanitizer)
	if err != nil {
		return nil, err
	}
	r.api.RegisterAllRoutes()
	r.registerCreationRoute()
	if err := r.router.Build(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Runtime) Handler() http.Handler { return r.router }

// SessionRefreshMiddleware applies GoCommon's sliding-session policy to the
// application's routes after their authenticated user context is installed.
func (r *Runtime) SessionRefreshMiddleware(next http.Handler) http.Handler {
	return r.api.GetSessionRefreshMiddleware()(next)
}
func (r *Runtime) Persistence() *gcpersistence.CommonPersistenceHandler[sm.User] {
	return r.persistence
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		if r.api != nil {
			r.api.Stop()
		} else if r.service != nil {
			r.service.Stop()
		}
		if r.roles != nil {
			r.roles.Stop()
		}
		if r.persistence != nil {
			r.closeErr = r.persistence.Close()
		}
	})
	return r.closeErr
}

func subjectForUser(user sm.User) (authorization.Subject, error) {
	subject := authorization.Subject{UserID: user.ID, UserType: int32(user.UserType)}
	_, known := pb.UserType_name[int32(user.UserType)]
	if !known || !subject.Valid() || user.Banned || user.Bot {
		return authorization.Subject{}, authorization.ErrUnauthorized
	}
	return subject, nil
}

func (r *Runtime) AuthenticateRequest(request *http.Request) (*http.Request, authorization.Subject, error) {
	cookie, err := request.Cookie(r.cookieName)
	if err != nil || strings.TrimSpace(cookie.Value) == "" {
		return nil, authorization.Subject{}, authorization.ErrUnauthorized
	}
	user, err := r.service.ValidateAuthTokenAndGetUser(request.Context(), cookie.Value)
	if err != nil || user == nil {
		return nil, authorization.Subject{}, authorization.ErrUnauthorized
	}
	subject, err := subjectForUser(*user)
	if err != nil {
		return nil, authorization.Subject{}, err
	}
	ctx := scontext.CopySRouterContext[sm.UserIdType, sm.User](request.Context(), request.Context())
	ctx = scontext.WithUser[sm.UserIdType](ctx, user)
	ctx = scontext.WithUserID[sm.UserIdType, sm.User](ctx, user.ID)
	return request.WithContext(ctx), subject, nil
}

// CurrentSubject reloads persisted user status/type, even on a long-lived job
// context. Existing cookie sessions observe committed SQL promotion/demotion.
func (r *Runtime) CurrentSubject(ctx context.Context, userID uint64) (authorization.Subject, error) {
	user, err := r.persistence.GetUserByID(gcpersistence.WithoutRequestTransaction[sm.User](ctx), userID)
	if errors.Is(err, gcpersistence.ErrUserNotFound) {
		return authorization.Subject{}, authorization.ErrUnauthorized
	}
	if err != nil {
		return authorization.Subject{}, err
	}
	return subjectForUser(sm.User{ID: user.ID, UserType: user.UserType, Banned: user.Banned, Bot: user.IsBot})
}
func (r *Runtime) LookupSubject(ctx context.Context, userID uint64) (authorization.Subject, error) {
	return r.CurrentSubject(ctx, userID)
}

func (r *Runtime) EffectiveRole(ctx context.Context, userID uint64, organizationID int64) (int32, bool, error) {
	organization, err := r.persistence.GetOrganizationByID(ctx, organizationID)
	if errors.Is(err, gcpersistence.ErrOrganizationNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if organization.PersonalOwnerUserID != nil || organization.OrganizationType != TeamOrganizationType {
		return 0, false, nil
	}
	info, member, err := r.roles.GetUserRoleFresh(ctx, userID, organizationID)
	if errors.Is(err, gerrors.ErrUserNotMember) || errors.Is(err, gcpersistence.ErrUserOrganizationNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	role := int32(info.EffectiveRole)
	// Inherited authority can strengthen an existing member's role, but must
	// never substitute for explicit membership when authorizing an index grant.
	return role, member && authorization.ValidRole(int32(info.ExplicitRole)) && authorization.ValidRole(role), nil
}

type userLookup struct {
	persistence *gcpersistence.CommonPersistenceHandler[sm.User]
	converter   *common_converter.ConverterHandler[sm.User]
}

func (l *userLookup) UserLookup(ctx context.Context, id sm.UserIdType) (sm.User, error) {
	u, err := l.persistence.GetUserByID(ctx, id)
	if err != nil {
		return sm.User{}, err
	}
	return *l.converter.ConvertPersistenceToUser(u), nil
}
func (l *userLookup) UserLookupMany(ctx context.Context, ids []sm.UserIdType) (map[sm.UserIdType]sm.User, error) {
	result := make(map[sm.UserIdType]sm.User, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	var users []pm.User
	if err := l.persistence.DB(ctx).Where("id IN ?", ids).Find(&users).Error; err != nil {
		return nil, err
	}
	for i := range users {
		user := l.converter.ConvertPersistenceToUser(&users[i])
		result[user.ID] = *user
	}
	return result, nil
}

// AuthorizationTransactionContext shares a caller-owned PostgreSQL transaction
// with GoCommon reads. The caller owns commit/rollback and transaction lifetime.
func (r *Runtime) AuthorizationTransactionContext(ctx context.Context, tx *sql.Tx) (context.Context, error) {
	if ctx == nil || tx == nil || r == nil || r.persistence == nil {
		return nil, authorization.ErrUnauthorized
	}
	orm := r.persistence.DB(gcpersistence.WithoutRequestTransaction[sm.User](ctx)).Session(&gorm.Session{NewDB: true, Context: ctx})
	// Session clones the statement before pinning it to this SQL transaction.
	orm = orm.WithContext(ctx)
	orm.Statement.ConnPool = tx
	orm.ConnPool = tx
	return scontext.WithTransaction[sm.UserIdType, sm.User](ctx, &gcpersistence.GormTransaction{Tx: orm}), nil
}

// CurrentSubjectInTransaction uses the native ambient GoCommon transaction for
// a locked authorization recheck; it never consults a detached user projection.
func (r *Runtime) CurrentSubjectInTransaction(ctx context.Context, userID uint64) (authorization.Subject, error) {
	user, err := r.persistence.GetUserByID(ctx, userID)
	if errors.Is(err, gcpersistence.ErrUserNotFound) {
		return authorization.Subject{}, authorization.ErrUnauthorized
	}
	if err != nil {
		return authorization.Subject{}, err
	}
	return subjectForUser(sm.User{ID: user.ID, UserType: user.UserType, Banned: user.Banned, Bot: user.IsBot})
}
