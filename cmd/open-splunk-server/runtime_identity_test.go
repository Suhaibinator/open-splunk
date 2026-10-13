package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"go.uber.org/zap"
)

func TestRuntimeIdentityOptionsRequireGooglePrivateStorageAndTrustedOrigin(t *testing.T) {
	valid := options{publicBaseURL: "https://logs.example.com", googleClientID: "client", googleClientSecret: "secret", minioEndpoint: "minio:9000", minioAccessKey: "access", minioSecretKey: "secret", minioBucket: "private"}
	if origin, err := validateRuntimeIdentityOptions(valid); err != nil || origin != valid.publicBaseURL {
		t.Fatalf("valid origin %q %v", origin, err)
	}
	for _, origin := range []string{"", "http://logs.example.com", "https://user:secret@logs.example.com", "https://logs.example.com/path", "https://logs.example.com/?redirect=x", "https://logs.example.com/#x"} {
		candidate := valid
		candidate.publicBaseURL = origin
		if _, err := validateRuntimeIdentityOptions(candidate); err == nil {
			t.Fatalf("accepted untrusted origin %q", origin)
		}
	}
	candidate := valid
	candidate.publicBaseURL = "http://127.0.0.1:8080"
	if _, err := validateRuntimeIdentityOptions(candidate); err != nil {
		t.Fatal(err)
	}
	candidate = valid
	candidate.googleClientSecret = ""
	if _, err := validateRuntimeIdentityOptions(candidate); err == nil {
		t.Fatal("missing Google secret accepted")
	}
	candidate = valid
	candidate.minioEndpoint = "https://minio.example.com"
	if _, err := validateRuntimeIdentityOptions(candidate); err == nil {
		t.Fatal("MinIO endpoint URL accepted")
	}
}

// Bucket startup is isolated from the identity/domain transaction checks.
// Actual object streaming and version identity are tested with real MinIO by
// the storage integration suite.
func newRuntimeIdentityFixture(t *testing.T) (*runtimeIdentity, *control.DB) {
	t.Helper()
	s3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch {
		case r.URL.Query().Has("location"):
			fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
		case r.URL.Query().Has("lifecycle"):
			switch r.Method {
			case http.MethodGet:
				fmt.Fprint(w, `<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LifecycleConfiguration>`)
			case http.MethodPut:
				w.WriteHeader(http.StatusOK)
			default:
				t.Errorf("unexpected lifecycle verb %s", r.Method)
			}
		case r.URL.Query().Has("versioning"):
			fmt.Fprint(w, `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`)
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected startup storage request %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(s3.Close)
	db, err := control.Open(t.Context(), pgtest.EmptyCopy(t, pgtest.DSN(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	identity, err := newRuntimeIdentity(context.Background(), db, options{tenantID: "physical", masterKeyPath: filepath.Join(t.TempDir(), "master.key"), publicBaseURL: "http://localhost:8080", googleClientID: "test-google-client", googleClientSecret: "test-google-secret", minioEndpoint: strings.TrimPrefix(s3.URL, "http://"), minioAccessKey: "test-access", minioSecretKey: "test-secret", minioBucket: "private-results"}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := identity.users.Close(); err != nil {
			t.Error(err)
		}
	})
	return identity, db
}

func TestRuntimeSharedIdentityBindingAndFreshResultAuthorization(t *testing.T) {
	runtime, db := newRuntimeIdentityFixture(t)
	orm := db.GORMDB()
	user := pm.User{Username: "viewer", Email: "viewer@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := orm.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	org := pm.Organization{Name: "Logs", Slug: "logs"}
	if err := orm.Create(&org).Error; err != nil {
		t.Fatal(err)
	}
	membership := pm.UserOrganization{UserID: user.ID, OrganizationID: org.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_ADMIN_TIER_0, JoinedAt: time.Now()}
	if err := orm.Create(&membership).Error; err != nil {
		t.Fatal(err)
	}
	index, err := db.CreateIndex(t.Context(), control.IndexDefinition{Name: "events", SearchEnabled: true, IngestionEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	actor := authorization.Subject{UserID: user.ID, UserType: int32(user.UserType)}
	scope := authorization.Scope{Subject: actor, OrganizationID: org.ID, PhysicalTenant: "physical"}
	ctx := authorization.WithScope(t.Context(), scope)
	tx := pgmeta.Begin(orm.WithContext(ctx))
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	if err := runtime.bindCreatedIndex(ctx, tx, index.ID); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	bound, err := runtime.authorization.OrganizationForIndex(t.Context(), index.ID)
	if err != nil || bound != org.ID {
		t.Fatalf("bound org %d %v", bound, err)
	}
	organizationID := strconv.FormatInt(org.ID, 10)
	job := searchjobs.Job{ID: "job", TenantID: "physical", OwnerID: actor.OwnerID(), OrganizationID: organizationID, EffectiveIndexes: []string{"events"}}
	access := searchjobs.AccessScope{TenantID: "physical", OwnerID: actor.OwnerID(), OrganizationID: organizationID}
	if err := runtime.authorizeJob(t.Context(), access, job); err != nil {
		t.Fatal(err)
	}
	runtimeIdentityDirectSearchAcceptance(t, runtime, db, membership, access, index.ID)
	privilegedJob := job
	privilegedJob.SitePrivilegeUsed = true
	privilegedJob.State = searchjobs.StateRunning
	if err := runtime.authorizeJob(t.Context(), access, privilegedJob); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("non-site actor retained privileged active work: %v", err)
	}
	// Stored sharing is enforced by the artifact store before the source hook.
	// A member may consume an explicitly shared snapshot without owning its job.
	viewer := pm.User{Username: "sharedviewer", Email: "sharedviewer@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := orm.Create(&viewer).Error; err != nil {
		t.Fatal(err)
	}
	viewerMembership := pm.UserOrganization{UserID: viewer.ID, OrganizationID: org.ID, Role: pb.OrganizationRole_ORGANIZATION_ROLE_MEMBER_TIER_0, JoinedAt: time.Now()}
	if err := orm.Create(&viewerMembership).Error; err != nil {
		t.Fatal(err)
	}
	viewerScope := authorization.Scope{Subject: authorization.Subject{UserID: viewer.ID, UserType: int32(viewer.UserType)}, OrganizationID: org.ID, PhysicalTenant: "physical"}
	viewerContext := authorization.WithScope(t.Context(), viewerScope)
	if err := runtime.authorizeCapture(viewerContext, job); err != nil {
		t.Fatalf("shared snapshot source authority: %v", err)
	}
	catalog, err := control.NewAppCatalog(db, control.AppCatalogOptions{CursorKey: []byte("0123456789abcdef0123456789abcdef")})
	if err != nil {
		t.Fatal(err)
	}
	appScope := control.AppAccessScope{TenantID: "physical", OrganizationID: org.ID}
	app, err := catalog.CreateApp(t.Context(), appScope, control.AppDefinition{Slug: "captured-app", DisplayName: "Captured app"})
	if err != nil {
		t.Fatal(err)
	}
	appJob := job
	appJob.AppID = app.ID
	if err := runtime.authorizeCapture(viewerContext, appJob); err != nil {
		t.Fatalf("active owning app source authority: %v", err)
	}
	if _, err := catalog.SetAppState(t.Context(), appScope, control.AppSelector{AppID: app.ID}, app.Version, control.AppStateArchived); err != nil {
		t.Fatal(err)
	}
	if err := runtime.authorizeCapture(viewerContext, appJob); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("archived app retained source authority: %v", err)
	}
	viewerAccess := searchjobs.AccessScope{TenantID: "physical", OwnerID: viewerScope.Subject.OwnerID(), OrganizationID: organizationID}
	if err := runtime.authorizeJob(t.Context(), viewerAccess, job); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("private live job owner gate: %v", err)
	}
	// Revocation wins over direct resource allows and applies to a retained job.
	if err := orm.Exec("INSERT INTO os_index_permissions(index_id,subject_kind,subject_id,capability,effect) VALUES(?,'user',?,'search','allow')", index.ID, user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := orm.Delete(&membership).Error; err != nil {
		t.Fatal(err)
	}
	if err := runtime.authorizeJob(t.Context(), access, job); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("removed member retained result access: %v", err)
	}
	// A committed manual promotion changes the same actor's authority immediately.
	if err := orm.Exec("UPDATE gocommon_users SET user_type=44 WHERE id=?", user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := orm.Exec("UPDATE os_index_permissions SET effect='deny' WHERE index_id=?", index.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := runtime.authorizeJob(t.Context(), access, job); err != nil {
		t.Fatalf("site administrator deny override: %v", err)
	}
	if err := runtime.authorizeJob(t.Context(), access, privilegedJob); err != nil {
		t.Fatalf("site administrator active privileged work: %v", err)
	}
	if siteAccess, err := runtime.freshAccess(t.Context(), actor.OwnerID(), ""); err != nil || !siteAccess.SiteAdministrator {
		t.Fatalf("site-wide access after promotion %#v %v", siteAccess, err)
	}
	archivedJob := job
	archivedJob.EffectiveIndexes = []string{"removed-index"}
	siteContext := authorization.WithScope(t.Context(), authorization.Scope{Subject: actor, PhysicalTenant: "physical"})
	if err := runtime.authorizeCapture(siteContext, archivedJob); err != nil {
		t.Fatalf("site administrator retained source bypass: %v", err)
	}
	archivedJob.State = searchjobs.StateCompleted
	if err := runtime.authorizeJob(t.Context(), access, archivedJob); err != nil {
		t.Fatalf("site administrator completed metadata source bypass: %v", err)
	}
	archivedIndex, err := db.SetIndexState(t.Context(), index.ID, index.Version, control.IndexStateArchived)
	if err != nil {
		t.Fatal(err)
	}
	completedSiteJob := job
	completedSiteJob.State = searchjobs.StateCompleted
	if err := runtime.authorizeJob(t.Context(), access, completedSiteJob); err != nil {
		t.Fatalf("site administrator archived/denied index metadata: %v", err)
	}
	if _, err := db.SetIndexState(t.Context(), index.ID, archivedIndex.Version, control.IndexStateActive); err != nil {
		t.Fatal(err)
	}
	if err := beforeRuntimeOrganizationDelete(t.Context(), orm, org.ID); err == nil {
		t.Fatal("organization with index deleted")
	}
	if err := orm.Exec("UPDATE gocommon_users SET user_type=? WHERE id=?", pb.UserType_USER_TYPE_REGULAR_TIER_0, user.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := runtime.authorizeJob(t.Context(), access, job); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("demotion did not revoke access: %v", err)
	}
	if err := orm.Create(&membership).Error; err != nil {
		t.Fatal(err)
	}
	if err := orm.Exec("UPDATE os_index_permissions SET effect='allow' WHERE index_id=?", index.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := runtime.authorizeJob(t.Context(), access, job); err != nil {
		t.Fatalf("ordinary current source authority after demotion: %v", err)
	}
	if err := runtime.authorizeJob(t.Context(), access, privilegedJob); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("demotion retained privileged active work despite current ordinary grants: %v", err)
	}
	privilegedJob.State = searchjobs.StateCompleted
	if err := runtime.authorizeCapture(ctx, privilegedJob); err != nil {
		t.Fatalf("terminal snapshot ignored current ordinary source authority: %v", err)
	}
}
