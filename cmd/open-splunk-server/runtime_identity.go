package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/identity"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// runtimeIdentity borrows the control pool and uses its verified deployment key.
// Google sessions and private objects therefore share the backup generation.
type runtimeIdentity struct {
	users         *identity.Runtime
	authorization *authorization.Service
	blobs         *objectstore.Store
	tenantID      string
	indexes       *control.DB
}

func validateRuntimeIdentityOptions(options options) (string, error) {
	parsed, err := url.Parse(options.publicBaseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("-public-base-url must be an absolute origin")
	}
	if parsed.Scheme != "https" {
		ip := net.ParseIP(parsed.Hostname())
		if parsed.Scheme != "http" || (parsed.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return "", errors.New("-public-base-url requires HTTPS except on loopback")
		}
	}
	for _, field := range []struct{ name, value string }{{"google-client-id", options.googleClientID}, {"google-client-secret", options.googleClientSecret}, {"minio-endpoint", options.minioEndpoint}, {"minio-access-key", options.minioAccessKey}, {"minio-secret-key", options.minioSecretKey}, {"minio-bucket", options.minioBucket}} {
		if strings.TrimSpace(field.value) == "" {
			return "", fmt.Errorf("-%s is required", field.name)
		}
	}
	if strings.Contains(options.minioEndpoint, "://") || strings.ContainsAny(options.minioEndpoint, "/?#@") {
		return "", errors.New("-minio-endpoint must be a host and optional port")
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

func newRuntimeIdentity(ctx context.Context, db *control.DB, options options, logger *zap.Logger) (_ *runtimeIdentity, err error) {
	origin, err := validateRuntimeIdentityOptions(options)
	if err != nil {
		return nil, err
	}
	master, err := loadVerifiedMasterKey(ctx, db, options.masterKeyPath)
	if err != nil {
		return nil, err
	}
	defer clear(master)
	session, err := deriveServerKey(master, "gocommon-session-signing")
	if err != nil {
		return nil, err
	}
	defer clear(session)
	salt, err := deriveServerKey(master, "gocommon-private-object-hashing")
	if err != nil {
		return nil, err
	}
	defer clear(salt)
	common := gcconfig.GetDefaults()
	common.Env = new(gcconfig.EnvironmentProduction)
	common.FrontendURL = new(origin)
	common.AllowedFrontendOrigins = []string{origin}
	common.CorsAllowedOrigins = []string{origin}
	common.CorsAllowCredentials = new(true)
	common.CorsAllowedHeaders = []string{"Content-Type", "Authorization", "X-OpenSplunk-Organization-ID"}
	common.SessionCookieName = new("open_splunk_session")
	common.SessionCookieSameSite = new("Lax")
	common.SessionCookieSecure = new(strings.HasPrefix(origin, "https://"))
	common.SessionTokenSecret = gcconfig.NewSecret(hex.EncodeToString(session))
	common.ObjectHashSalt = gcconfig.NewSecret(hex.EncodeToString(salt))
	common.MinioEndpoint = options.minioEndpoint
	common.MinioAccessKey = options.minioAccessKey
	common.MinioSecretKey = gcconfig.NewSecret(options.minioSecretKey)
	common.MinioUseSSL = new(options.minioUseTLS)
	common.MinioBucket = options.minioBucket
	common.MinioPublicBucket = ""
	common.MinioPublicBaseURL = ""
	runtime := &runtimeIdentity{tenantID: options.tenantID, indexes: db}
	defer func() {
		if err != nil && runtime.users != nil {
			_ = runtime.users.Close()
		}
	}()
	runtime.users, err = identity.New(ctx, db.GORMDB(), identity.Options{Common: common, GoogleClientID: options.googleClientID, GoogleClientSecret: gcconfig.NewSecret(options.googleClientSecret), GoogleRedirectURLs: []string{origin + "/auth/oauth/callback/"}, Logger: logger, BeforeOrganizationDelete: beforeRuntimeOrganizationDelete})
	if err != nil {
		return nil, fmt.Errorf("initialize Google identity: %w", err)
	}
	if err = db.ApplyAdditionalMigration(ctx, "organization-index-authorization-v1", authorization.Schema); err != nil {
		return nil, fmt.Errorf("apply organization authorization schema: %w", err)
	}
	runtime.authorization, err = authorization.New(db.SQLDB(), runtime.users, options.tenantID)
	if err != nil {
		return nil, err
	}
	objectContext, cancelObjects := context.WithTimeout(ctx, startupTimeout)
	defer cancelObjects()
	runtime.blobs, err = objectstore.NewContext(objectContext, runtime.users.Persistence())
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

func beforeRuntimeOrganizationDelete(ctx context.Context, tx *gorm.DB, id int64) error {
	var dependent bool
	// Lineage covers descendants. References are preserved rather than implicitly
	// deleting indexes, queries or retained results when an organization is removed.
	query := `WITH subtree AS (SELECT id FROM gocommon_organizations WHERE id=? OR ?=ANY(lineage))
 SELECT EXISTS(SELECT 1 FROM os_index_organizations WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM app_workspaces WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM saved_searches WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM dashboards WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM search_capture_sessions WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM search_retained_export_reservations WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM durable_search_jobs WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM durable_export_jobs WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM search_history WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM search_history_pending WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM saved_search_schedules WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM saved_search_schedule_runs WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM alerts WHERE organization_id IN(SELECT id FROM subtree)
 UNION ALL SELECT 1 FROM ingestion_tokens WHERE organization_id IN(SELECT id FROM subtree))`
	if err := tx.WithContext(ctx).Raw(query, id, id).Scan(&dependent).Error; err != nil {
		return err
	}
	if dependent {
		return errors.New("organization contains OpenSplunk resources")
	}
	return nil
}

func (runtime *runtimeIdentity) bindCreatedIndex(ctx context.Context, tx *gorm.DB, indexID string) error {
	scope, ok := authorization.FromContext(ctx)
	if !ok || scope.PhysicalTenant != runtime.tenantID || scope.OrganizationID <= 0 {
		return authorization.ErrForbidden
	}
	sqlTransaction, ok := tx.Statement.ConnPool.(*sql.Tx)
	if !ok {
		return errors.New("index organization binding requires the native control transaction")
	}
	ctx, err := runtime.users.AuthorizationTransactionContext(ctx, sqlTransaction)
	if err != nil {
		return err
	}
	subject, err := runtime.users.CurrentSubjectInTransaction(ctx, scope.Subject.UserID)
	if err != nil {
		return err
	}
	if err := runtime.authorization.AuthorizeOrganization(ctx, subject, scope.OrganizationID, authorization.AdminRole); err != nil {
		return err
	}
	var live bool
	if err := tx.WithContext(ctx).Raw("SELECT EXISTS(SELECT 1 FROM gocommon_organizations WHERE id=? AND deleted_at IS NULL)", scope.OrganizationID).Scan(&live).Error; err != nil {
		return err
	}
	if !live {
		return authorization.ErrForbidden
	}
	return tx.WithContext(ctx).Exec("INSERT INTO os_index_organizations(index_id,organization_id) VALUES(?,?)", indexID, scope.OrganizationID).Error
}

func (runtime *runtimeIdentity) freshAccess(ctx context.Context, ownerID, orgID string) (searchjobs.AccessScope, error) {
	access, _, err := runtime.freshAccessAndSubject(ctx, ownerID, orgID)
	return access, err
}
func (runtime *runtimeIdentity) freshAccessAndSubject(ctx context.Context, ownerID, orgID string) (searchjobs.AccessScope, authorization.Subject, error) {
	userID, err := strconv.ParseUint(ownerID, 10, 64)
	if err != nil || userID == 0 || strconv.FormatUint(userID, 10) != ownerID {
		return searchjobs.AccessScope{}, authorization.Subject{}, authorization.ErrUnauthorized
	}
	subject, err := runtime.users.CurrentSubject(ctx, userID)
	if err != nil {
		return searchjobs.AccessScope{}, authorization.Subject{}, err
	}
	if subject.IsSiteAdmin() && (orgID == "" || orgID == "0") {
		return searchjobs.AccessScope{TenantID: runtime.tenantID, OwnerID: subject.OwnerID(), SiteAdministrator: true}, subject, nil
	}
	organizationID, err := strconv.ParseInt(orgID, 10, 64)
	if err != nil || organizationID <= 0 || strconv.FormatInt(organizationID, 10) != orgID {
		return searchjobs.AccessScope{}, authorization.Subject{}, authorization.ErrForbidden
	}
	if err := runtime.authorization.AuthorizeOrganization(ctx, subject, organizationID, authorization.ViewerRole); err != nil {
		return searchjobs.AccessScope{}, authorization.Subject{}, err
	}
	return searchjobs.AccessScope{TenantID: runtime.tenantID, OwnerID: subject.OwnerID(), OrganizationID: orgID, SiteAdministrator: subject.IsSiteAdmin()}, subject, nil
}
func (runtime *runtimeIdentity) accessForRequest(request *http.Request) (searchjobs.AccessScope, error) {
	scope, ok := authorization.FromContext(request.Context())
	if !ok || scope.PhysicalTenant != runtime.tenantID {
		return searchjobs.AccessScope{}, authorization.ErrUnauthorized
	}
	return runtime.freshAccess(request.Context(), scope.Subject.OwnerID(), strconv.FormatInt(scope.OrganizationID, 10))
}
func (runtime *runtimeIdentity) authorizeJob(ctx context.Context, access searchjobs.AccessScope, job searchjobs.Job) error {
	return runtime.authorizeJobSources(ctx, access, job, true)
}
func (runtime *runtimeIdentity) authorizeJobSources(ctx context.Context, access searchjobs.AccessScope, job searchjobs.Job, requireOwner bool) error {
	fresh, subject, err := runtime.freshAccessAndSubject(ctx, access.OwnerID, access.OrganizationID)
	if err != nil {
		return err
	}
	if job.SitePrivilegeUsed && !fresh.SiteAdministrator && !job.State.Terminal() {
		return authorization.ErrForbidden
	}
	if job.TenantID != runtime.tenantID || (!fresh.SiteAdministrator && ((requireOwner && job.OwnerID != fresh.OwnerID) || job.OrganizationID != fresh.OrganizationID)) {
		return authorization.ErrForbidden
	}
	organizationID, err := strconv.ParseInt(job.OrganizationID, 10, 64)
	if err != nil || organizationID <= 0 {
		return authorization.ErrForbidden
	}
	if fresh.SiteAdministrator && (!requireOwner || (job.State.Terminal() && (len(job.EffectiveIndexes) != 0 || safeUnboundJobMetadata(job)))) {
		return nil
	}
	if !fresh.SiteAdministrator && job.AppID != "" {
		var available bool
		if err := runtime.indexes.SQLDB().QueryRowContext(ctx, `SELECT EXISTS (
			SELECT 1 FROM app_workspaces WHERE tenant_id=$1 AND organization_id=$2 AND app_id=$3 AND state='active'
		)`, runtime.tenantID, organizationID, job.AppID).Scan(&available); err != nil {
			return err
		}
		if !available {
			return authorization.ErrForbidden
		}
	}
	if len(job.EffectiveIndexes) == 0 {
		// Direct asynchronous searches have not bound their source indexes at
		// admission. Their user, organization, owner and app checks above remain
		// mandatory; the manager checks the resolved source set again before
		// any inventory or execution reads. Retained consumers never use this
		// preliminary authorization path.
		if requireOwner && safeUnboundJobMetadata(job) {
			return runtime.authorizeKnowledgeSources(ctx, fresh, job)
		}
		return authorization.ErrForbidden
	}
	indexes, err := runtime.indexes.GetIndexesByNames(ctx, job.EffectiveIndexes)
	if err != nil {
		return err
	}
	if len(indexes) != len(job.EffectiveIndexes) {
		return authorization.ErrForbidden
	}
	for i, index := range indexes {
		if index.Definition.Name != job.EffectiveIndexes[i] || index.State != control.IndexStateActive || !index.Definition.SearchEnabled {
			return authorization.ErrForbidden
		}
		if err := runtime.authorization.AuthorizeIndex(ctx, subject, organizationID, index.ID, authorization.Search); err != nil {
			return err
		}
	}
	return runtime.authorizeKnowledgeSources(ctx, fresh, job)
}

// Unbound diagnostic metadata is readable only when no source-derived
// authority or progress exists. The manager records inventory source bindings
// before backend work so a failure after a read cannot enter this path.
func safeUnboundJobMetadata(job searchjobs.Job) bool {
	if len(job.EffectiveIndexes) != 0 || job.KnowledgeSnapshot != nil || len(job.KnowledgeObjectIDs) != 0 ||
		job.Schema != nil || job.NearbyEventProvenance != nil || job.ScannedRows != 0 || job.ScannedBytes != 0 ||
		job.RowCount != 0 || job.ResultBytes != 0 || job.ResultsTruncated || job.PreviewTruncated ||
		job.CaptureComplete || job.CaptureRows != 0 || job.CaptureBytes != 0 {
		return false
	}
	return job.State == searchjobs.StateQueued || job.State == searchjobs.StateParsing ||
		job.State == searchjobs.StatePlanning || job.State == searchjobs.StateCanceled || (job.State == searchjobs.StateFailed && job.Failure != nil)
}

func (runtime *runtimeIdentity) authorizeCapture(ctx context.Context, job searchjobs.Job) error {
	access := searchjobs.AccessScope{TenantID: job.TenantID, OwnerID: job.OwnerID, OrganizationID: job.OrganizationID}
	if scope, ok := authorization.FromContext(ctx); ok {
		access.OwnerID = scope.Subject.OwnerID()
		access.OrganizationID = strconv.FormatInt(scope.OrganizationID, 10)
	}
	// Metadata reads also use this hook. A diagnostic with no source work is
	// owner-private (or site-admin accessible), while actual capture and shared
	// result consumers retain complete source authorization below.
	requireOwner := safeUnboundJobMetadata(job)
	if requireOwner {
		if _, ok := authorization.FromContext(ctx); !ok {
			return authorization.ErrUnauthorized
		}
	}
	return runtime.authorizeJobSources(ctx, access, job, requireOwner)
}
func (runtime *runtimeIdentity) exportAuthorizer(artifacts *searchartifacts.Store) func(context.Context, searchjobs.AccessScope, string) error {
	return func(ctx context.Context, access searchjobs.AccessScope, id string) error {
		fresh, subject, err := runtime.freshAccessAndSubject(ctx, access.OwnerID, access.OrganizationID)
		if err != nil {
			return err
		}
		organizationID, _ := strconv.ParseInt(fresh.OrganizationID, 10, 64)
		ctx = authorization.WithScope(ctx, authorization.Scope{Subject: subject, OrganizationID: organizationID, PhysicalTenant: fresh.TenantID})
		job, err := artifacts.AuthorizationJob(ctx, fresh, id)
		if err != nil {
			return err
		}
		return runtime.authorizeJobSources(ctx, fresh, job, false)
	}
}
func (runtime *runtimeIdentity) websocketExportAuthorizer(artifacts *searchartifacts.Store) func(context.Context, searchjobs.AccessScope, exportjobs.Job) error {
	authorize := runtime.exportAuthorizer(artifacts)
	return func(ctx context.Context, access searchjobs.AccessScope, job exportjobs.Job) error {
		return authorize(ctx, access, job.SearchJobID)
	}
}
