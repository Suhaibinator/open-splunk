package identity

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	gcpersistence "github.com/Suhaibinator/go-common/pkg/common_persistence"
	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	gerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_service/organization"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// The S3 fixture answers startup bucket discovery only; identity operations run
// against the real PostgreSQL schema and native GoCommon token/HTTP handlers.
func newPostgresIdentityRuntime(t *testing.T) (*Runtime, *gorm.DB, *gcconfig.Config) {
	t.Helper()
	objectServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Has("location") {
			fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
			return
		}
		if r.Method != http.MethodHead {
			t.Errorf("unexpected identity startup object request %s %s", r.Method, r.URL)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(objectServer.Close)
	// Native GORM schema introspection joins information_schema across a database.
	// A disposable database keeps this acceptance independent of concurrently
	// created control-plane fixture schemas while retaining the production pool.
	dsn := pgtest.EmptyCopy(t, pgtest.DSN(t))
	pool, err := gorm.Open(postgres.Open(dsn), &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := pool.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	cfg := gcconfig.GetDefaults()
	cfg.MinioEndpoint = strings.TrimPrefix(objectServer.URL, "http://")
	cfg.MinioUseSSL = new(false)
	cfg.MinioBucket = "private-results"
	cfg.MinioPublicBucket = ""
	cfg.MinioAccessKey = "test-identity-access"
	cfg.MinioSecretKey = gcconfig.NewSecret("test-identity-secret")
	cfg.SessionTokenSecret = gcconfig.NewSecret("0123456789abcdef0123456789abcdef")
	cfg.ObjectHashSalt = gcconfig.NewSecret("abcdef0123456789abcdef0123456789")
	cfg.FrontendURL = new("http://localhost:8080")
	cfg.OrgRoleInheritanceEnabled = new(true)
	runtime, err := New(context.Background(), pool, Options{Common: cfg, GoogleClientID: "google-test-client", GoogleClientSecret: gcconfig.NewSecret("google-test-secret"), GoogleRedirectURLs: []string{"http://localhost:8080/auth/callback"}, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Error(err)
		}
	})
	return runtime, pool, cfg
}

func runtimeSameGoogleSessionSeesManualPromotionDemotion(t *testing.T, runtime *Runtime, pool *gorm.DB, cfg *gcconfig.Config) {
	user := pm.User{Username: "googleuser", Email: "googleuser@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := pool.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if err := pool.Create(&pm.OAuthLogin{UserID: user.ID, ProviderSlug: "google", Subject: new("google-subject"), Email: user.Email}).Error; err != nil {
		t.Fatal(err)
	}
	// Exercise the same trusted workspace lifecycle used by Google registration.
	if _, err := identityNativeTransaction(runtime, t.Context(), func(ctx context.Context) (*sm.Organization, error) {
		return runtime.service.GetPersonalWorkspaceService().EnsureForUser(ctx, &sm.User{ID: user.ID, Username: user.Username, Email: user.Email, UserType: user.UserType})
	}); err != nil {
		t.Fatal(err)
	}
	tokens, err := token.NewTokenService(runtime.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: cfg.SessionTokenSecret.StringValue()}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	session, err := tokens.GenerateSessionToken(t.Context(), user.ID, token.SessionAuthMethod{Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/search", nil)
	request.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	for _, tier := range []pb.UserType{pb.UserType_USER_TYPE_REGULAR_TIER_0, pb.UserType_USER_TYPE_ADMIN_TIER_10, pb.UserType_USER_TYPE_REGULAR_TIER_0} {
		if err := pool.Exec("UPDATE gocommon_users SET user_type=? WHERE id=?", tier, user.ID).Error; err != nil {
			t.Fatal(err)
		}
		_, subject, err := runtime.AuthenticateRequest(request.Clone(t.Context()))
		if err != nil {
			t.Fatal(err)
		}
		if subject.UserType != int32(tier) || subject.IsSiteAdmin() != (tier == pb.UserType_USER_TYPE_ADMIN_TIER_10) {
			t.Fatalf("same-cookie fresh type got%#v want%d", subject, tier)
		}
		adminReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+"/admin/users", strings.NewReader(""))
		adminReq.Header.Set("Content-Type", "application/x-protobuf")
		adminReq.Header.Set("Accept", "application/json")
		adminReq.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		record := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(record, adminReq)
		expected := http.StatusForbidden
		if tier == pb.UserType_USER_TYPE_ADMIN_TIER_10 {
			expected = http.StatusOK
		}
		if record.Code != expected {
			t.Fatalf("same-session user administration got%d want%d: %s", record.Code, expected, record.Body.String())
		}
		// /user/me goes through GoCommon's GetUserByID facade as well as its
		// router auth context. Both must see the new persisted type even after
		// the token cache was warmed by previous API calls.
		me := httptest.NewRequestWithContext(t.Context(), http.MethodGet, Prefix+"/user/me", nil)
		me.Header.Set("Accept", "application/x-protobuf")
		me.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		profile := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(profile, me)
		if profile.Code != http.StatusOK {
			t.Fatalf("native profile status %d: %s", profile.Code, profile.Body.String())
		}
		var projected pb.GetCurrentUserResponse
		if err := proto.Unmarshal(profile.Body.Bytes(), &projected); err != nil {
			t.Fatal(err)
		}
		if projected.GetUser().GetUserType() != tier {
			t.Fatalf("native profile retained stale type %v want%v", projected.GetUser().GetUserType(), tier)
		}
		for _, route := range []string{"/auth/add-password", "/auth/otp/setup", "/auth/otp/verify-setup", "/auth/otp/disable"} {
			credentialReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+route, strings.NewReader(""))
			credentialReq.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
			credentialReq.Header.Set("Content-Type", "application/x-protobuf")
			disabled := httptest.NewRecorder()
			runtime.Handler().ServeHTTP(disabled, credentialReq)
			if disabled.Code != http.StatusNotFound {
				t.Fatalf("authenticated credential route %s not disabled: %d", route, disabled.Code)
			}
		}
		fresh, err := runtime.CurrentSubject(t.Context(), user.ID)
		if err != nil || fresh.UserType != int32(tier) {
			t.Fatalf("worker fresh user %#v %v", fresh, err)
		}
	}
	// A native token-cache hit must not freeze account-state decisions either.
	for _, banned := range []bool{true, false} {
		if err := pool.Exec("UPDATE gocommon_users SET banned=? WHERE id=?", banned, user.ID).Error; err != nil {
			t.Fatal(err)
		}
		_, _, err := runtime.AuthenticateRequest(request.Clone(t.Context()))
		if banned == (err == nil) {
			t.Fatalf("same-cookie persisted ban=%v authentication error=%v", banned, err)
		}
	}
	var organizations int64
	if err := pool.Table(pm.TableNameOrganizations).Count(&organizations).Error; err != nil {
		t.Fatal(err)
	}
	if organizations != 1 {
		t.Fatal("signup must provision exactly one managed workspace")
	}
	var teams, memberships int64
	if err := pool.Table(pm.TableNameOrganizations).Where("organization_type = ?", TeamOrganizationType).Count(&teams).Error; err != nil {
		t.Fatal(err)
	}
	if err := pool.Table(pm.TableNameUserOrganizations).Count(&memberships).Error; err != nil {
		t.Fatal(err)
	}
	var personal pm.Organization
	if err := pool.Where("personal_owner_user_id=?", user.ID).First(&personal).Error; err != nil {
		t.Fatal(err)
	}
	if role, member, err := runtime.EffectiveRole(t.Context(), user.ID, personal.ID); err != nil || member || role != 0 {
		t.Fatal("managed workspace conferred team authority", role, member, err)
	}
	if teams != 0 || memberships != 1 {
		t.Fatal("signup must grant only managed-workspace membership, no team access")
	}
	if err := runtime.Persistence().RevokeSessionToken(t.Context(), session); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runtime.AuthenticateRequest(request); err == nil {
		t.Fatal("revoked Google session remains authenticated")
	}
}

func runtimeNativeGoogleOnlyRouteRegistration(t *testing.T, runtime *Runtime) {
	for _, route := range []string{"/auth/email/login", "/auth/email/register", "/auth/email/request-reset", "/auth/add-password", "/auth/otp/setup", "/auth/otp/disable"} {
		record := httptest.NewRecorder()
		runtime.Handler().ServeHTTP(record, httptest.NewRequestWithContext(t.Context(), http.MethodPost, Prefix+route, strings.NewReader("{}")))
		if record.Code != http.StatusNotFound && record.Code != http.StatusUnauthorized {
			t.Fatalf("credential route %s registered: %d %s", route, record.Code, record.Body.String())
		}
	}
	record := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, Prefix+"/auth/oauth/providers", nil)
	request.Header.Set("Accept", "application/json")
	runtime.Handler().ServeHTTP(record, request)
	if record.Code != http.StatusOK || !strings.Contains(record.Body.String(), "google") {
		t.Fatalf("native Google provider route: %d %s", record.Code, record.Body.String())
	}
}

func runtimeNativeOrganizationPolicyPreservesAncestorMembership(t *testing.T, runtime *Runtime, pool *gorm.DB) {
	userContexts := make([]context.Context, 5)
	users := make([]pm.User, 5)
	for i := range users {
		tier := pb.UserType_USER_TYPE_REGULAR_TIER_0
		if i == 0 {
			tier = pb.UserType_USER_TYPE_ADMIN_TIER_10
		}
		users[i] = pm.User{Username: fmt.Sprintf("orguser%d", i), Email: fmt.Sprintf("orguser%d@example.com", i), UserType: tier}
		if err := pool.Create(&users[i]).Error; err != nil {
			t.Fatal(err)
		}
		userContexts[i] = scontext.WithUser[sm.UserIdType](t.Context(), &sm.User{ID: users[i].ID, UserType: tier})
		userContexts[i] = scontext.WithUserID[sm.UserIdType, sm.User](userContexts[i], users[i].ID)
	}
	handler := runtime.service.GetOrganizationHandler()
	create := func(ctx context.Context, org *sm.Organization, members []InitialMembership) (*organization.OrganizationResponse, error) {
		return identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (*organization.OrganizationResponse, error) {
			return runtime.CreateOrganizationWithInitialMemberships(txCtx, org, members)
		})
	}
	add := func(ctx context.Context, user uint64, org int64, role pb.OrganizationRole) (*sm.UserOrganizationResponse, error) {
		return identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (*sm.UserOrganizationResponse, error) {
			return handler.AddMember(txCtx, user, org, role)
		})
	}
	update := func(ctx context.Context, user uint64, org int64, role pb.OrganizationRole) (*sm.UserOrganizationResponse, error) {
		return identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (*sm.UserOrganizationResponse, error) {
			return handler.UpdateMemberRole(txCtx, user, org, role)
		})
	}
	remove := func(ctx context.Context, user uint64, org int64) error {
		_, err := identityNativeTransaction(runtime, ctx, func(txCtx context.Context) (struct{}, error) {
			return struct{}{}, handler.RemoveMember(txCtx, user, org)
		})
		return err
	}
	parent, err := create(userContexts[0], &sm.Organization{Name: "Parent", Slug: "parent"}, []InitialMembership{{UserID: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
	if err != nil {
		t.Fatal(err)
	}
	orgID := parent.ID
	if _, err := add(userContexts[0], users[2].ID, orgID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); err != nil {
		t.Fatal(err)
	}
	child, err := create(userContexts[2], &sm.Organization{Name: "Child", Slug: "child", ParentOrganizationID: &orgID}, []InitialMembership{{UserID: users[1].ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10}})
	if err != nil {
		t.Fatalf("parent Admin creating child without global root permission: %v", err)
	}
	if _, err := add(userContexts[0], users[4].ID, orgID, pb.OrganizationRole_ORGANIZATION_ROLE_OWNER_TIER_10); err != nil {
		t.Fatal(err)
	}
	if role, member, err := runtime.EffectiveRole(t.Context(), users[4].ID, child.ID); err != nil || member {
		t.Fatalf("inherited parentOwner is not explicit childmembership: role%d member%t %v", role, member, err)
	}
	if role, member, err := runtime.EffectiveRole(t.Context(), users[2].ID, child.ID); err != nil || !member || role != 23 {
		t.Fatalf("creator elevated or missing: role%d member%t %v", role, member, err)
	}
	if _, err := add(userContexts[2], users[3].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0); !errors.Is(err, gerrors.ErrOrganizationAncestorMembershipRequired) {
		t.Fatalf("child admitted outsider without ancestor: %v", err)
	}
	if _, err := add(userContexts[2], users[3].ID, orgID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); !errors.Is(err, gerrors.ErrInsufficientPermissions) {
		t.Fatalf("Admin peer grant must be rejected: %v", err)
	}
	if _, member, err := runtime.EffectiveRole(t.Context(), users[3].ID, orgID); err != nil || member {
		t.Fatalf("rejected Admin peer grant created parent membership: member%t %v", member, err)
	}
	if _, err := add(userContexts[1], users[3].ID, orgID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); err != nil {
		t.Fatalf("Owner adding parent Admin: %v", err)
	}
	if _, err := add(userContexts[2], users[3].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); !errors.Is(err, gerrors.ErrInsufficientPermissions) {
		t.Fatalf("Admin child peer grant must be rejected: %v", err)
	}
	if _, member, err := runtime.EffectiveRole(t.Context(), users[3].ID, child.ID); err != nil || member {
		t.Fatalf("rejected Admin peer grant created child membership: member%t %v", member, err)
	}
	if _, err := add(userContexts[1], users[3].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); err != nil {
		t.Fatalf("Owner adding ancestor-backed Admin: %v", err)
	}
	if _, err := update(userContexts[2], users[3].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0); !errors.Is(err, gerrors.ErrInsufficientPermissions) {
		t.Fatalf("Admin peer role mutation must be rejected: %v", err)
	}
	if role, member, err := runtime.EffectiveRole(t.Context(), users[3].ID, child.ID); err != nil || !member || role != 23 {
		t.Fatalf("rejected Admin peer role mutation changed membership: role%d member%t %v", role, member, err)
	}
	if _, err := update(userContexts[1], users[3].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0); err != nil {
		t.Fatalf("Owner managing Admin: %v", err)
	}
	if _, err := update(userContexts[2], users[1].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); err == nil {
		t.Fatal("Admin demoted Owner")
	}
	if _, err := update(userContexts[1], users[1].ID, child.ID, pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0); !errors.Is(err, gerrors.ErrCannotDemoteLastOwner) {
		t.Fatalf("last Owner demotion protection: %v", err)
	}
	if err := remove(userContexts[0], users[3].ID, orgID); err != nil {
		t.Fatal(err)
	}
	if _, member, err := runtime.EffectiveRole(t.Context(), users[3].ID, child.ID); err != nil || member {
		t.Fatalf("ancestor removal must cascade native descendant membership: member%t %v", member, err)
	}
}

func runtimeAuthorizationTransactionReadsUncommittedPersistedType(t *testing.T, runtime *Runtime, pool *gorm.DB) {
	user := pm.User{Username: "transactionuser", Email: "transactionuser@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := pool.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	db, err := pool.DB()
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback identity transaction: %v", err)
		}
	}()
	if _, err := tx.ExecContext(t.Context(), "UPDATE gocommon_users SET user_type=44 WHERE id=$1", user.ID); err != nil {
		t.Fatal(err)
	}
	ctx, err := runtime.AuthorizationTransactionContext(t.Context(), tx)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := runtime.CurrentSubjectInTransaction(ctx, user.ID)
	if err != nil || !subject.IsSiteAdmin() {
		t.Fatalf("native locked tx did not see stagedtype %#v %v", subject, err)
	}
	outside, err := runtime.CurrentSubject(t.Context(), user.ID)
	if err != nil || outside.IsSiteAdmin() {
		t.Fatalf("worker accidentally borrowed ambienttx %#v %v", outside, err)
	}
}

func TestRuntimePostgresAcceptance(t *testing.T) {
	runtime, pool, cfg := newPostgresIdentityRuntime(t)
	t.Run("same Google cookie promotion demotion revocation", func(t *testing.T) { runtimeSameGoogleSessionSeesManualPromotionDemotion(t, runtime, pool, cfg) })
	t.Run("reject foreign session methods", func(t *testing.T) { runtimeRejectsForeignSessionMethods(t, runtime, pool, cfg) })
	t.Run("native Google only route registration", func(t *testing.T) { runtimeNativeGoogleOnlyRouteRegistration(t, runtime) })
	t.Run("native Owner-managed Admin and ancestry policy", func(t *testing.T) { runtimeNativeOrganizationPolicyPreservesAncestorMembership(t, runtime, pool) })
	t.Run("native SQL authorization transaction", func(t *testing.T) { runtimeAuthorizationTransactionReadsUncommittedPersistedType(t, runtime, pool) })
	t.Run("native membership grants CAS audit and demotion", func(t *testing.T) { runtimeAuthorizationUsesNativeMembershipAndFreshType(t, runtime, pool) })
	t.Run("identity transactions do not wait for catalog writers", func(t *testing.T) {
		raw, err := pool.DB()
		if err != nil {
			t.Fatal(err)
		}
		blocker, err := raw.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = blocker.Rollback() }()
		if _, err = blocker.ExecContext(t.Context(), "SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || current_schema() || ':10:catalog-publication',0))"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		native, err := runtime.Persistence().BeginTransaction(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer native.Rollback()
		if err = native.Exec("UPDATE gocommon_users SET user_type=user_type WHERE id=(SELECT min(id) FROM gocommon_users)").Error; err != nil {
			t.Fatal(err)
		}
		if err = native.Commit().Error; err != nil {
			t.Fatal(err)
		}
		var count int
		if err = raw.QueryRowContext(ctx, "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND (t.tgname='a000_control_write_lock' OR (c.relname LIKE 'gocommon_%' AND t.tgname LIKE 'a00%'))").Scan(&count); err != nil || count != 0 {
			t.Fatalf("unexpected runtime writer interceptors: %d %v", count, err)
		}
	})
	t.Run("native user deletion preserves ownership and ancestry", func(t *testing.T) { runtimeUserDeletionPreservesNativeOwnership(t, runtime, pool, cfg) })
}

func identityNativeTransaction[T any](runtime *Runtime, ctx context.Context, operation func(context.Context) (T, error)) (result T, returnedErr error) {
	var zero T
	orm, err := runtime.Persistence().BeginTransaction(ctx)
	if err != nil {
		return zero, err
	}
	tx := &gcpersistence.GormTransactionWithHooks{Tx: orm}
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
				returnedErr = errors.Join(returnedErr, err)
			}
		}
	}()
	txCtx := scontext.WithTransaction[sm.UserIdType, sm.User](ctx, tx)
	value, err := operation(txCtx)
	if err != nil {
		return zero, err
	}
	if err := tx.Commit(); err != nil {
		return zero, err
	}
	committed = true
	for _, hook := range tx.DrainPostCommitHooks() {
		if err := hook(ctx); err != nil {
			return zero, err
		}
	}
	return value, nil
}

func runtimeRejectsForeignSessionMethods(t *testing.T, runtime *Runtime, pool *gorm.DB, cfg *gcconfig.Config) {
	user := pm.User{Username: "methoduser", Email: "methoduser@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := pool.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	tokens, err := token.NewTokenService(runtime.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: cfg.SessionTokenSecret.StringValue()}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []token.SessionAuthMethod{{Provider: pb.AuthProvider_AUTH_PROVIDER_EMAIL}, {Provider: pb.AuthProvider_AUTH_PROVIDER_PHONE}, {Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "unknown"}, {Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"}} {
		raw, err := tokens.GenerateSessionToken(t.Context(), user.ID, method)
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/search", nil)
		request.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: raw, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
		_, subject, err := runtime.AuthenticateRequest(request)
		if method.OAuthProviderSlug == "google" {
			if err != nil || subject.UserID != user.ID {
				t.Fatalf("native Google cookie rejected %#v %v", subject, err)
			}
		} else {
			if err == nil {
				t.Fatalf("foreign credential method accepted: %#v", method)
			}
			native := httptest.NewRecorder()
			request = httptest.NewRequestWithContext(t.Context(), http.MethodGet, Prefix+"/auth/methods", nil)
			request.AddCookie(&http.Cookie{Name: runtime.cookieName, Value: raw, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
			runtime.Handler().ServeHTTP(native, request)
			if native.Code != http.StatusUnauthorized {
				t.Fatalf("native route accepted foreign method %#v: %d", method, native.Code)
			}
		}
	}
}
