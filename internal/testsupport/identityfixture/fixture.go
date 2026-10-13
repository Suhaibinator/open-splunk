// Package identityfixture provisions disposable Google identities for tests.
// Production server code does not import this package or expose a seed route.
package identityfixture

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/token"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/identity"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

const CookieName = "open_splunk_session"

type Options struct {
	Database                                       *control.DB
	Directory, Origin, PhysicalTenant, PostgresDSN string
}

type Fixture struct {
	Database                                                   *control.DB
	Runtime                                                    *identity.Runtime
	Authorization                                              *authorization.Service
	Blobs                                                      *objectstore.Store
	PostgresDSN, MasterKeyPath, Session, OrganizationID        string
	MinioEndpoint, MinioAccessKey, MinioSecretKey, MinioBucket string
	ownedDatabase                                              bool
	closeOnce                                                  sync.Once
	closeError                                                 error
}

func New(t testing.TB, options Options) *Fixture {
	t.Helper()
	ctx := context.Background()
	fixture := &Fixture{Database: options.Database}
	if options.Directory == "" {
		options.Directory = t.TempDir()
	}
	if options.Origin == "" {
		options.Origin = "http://localhost:8080"
	}
	if options.PhysicalTenant == "" {
		options.PhysicalTenant = "integration-tenant"
	}
	if fixture.Database == nil {
		fixture.PostgresDSN = options.PostgresDSN
		if fixture.PostgresDSN == "" {
			fixture.PostgresDSN = pgtest.DSN(t)
		}
		// A dedicated disposable database keeps GoCommon native schema introspection
		// independent of other concurrently active integration schemas.
		fixture.PostgresDSN = pgtest.EmptyCopy(t, fixture.PostgresDSN)
		db, err := control.Open(ctx, fixture.PostgresDSN)
		if err != nil {
			t.Fatal("initialize native identity fixture database:", err)
		}
		fixture.Database, fixture.ownedDatabase = db, true
	}
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Errorf("close Google identity fixture: %v", err)
		}
	})
	fixture.MinioEndpoint = required(t, "OPEN_SPLUNK_TEST_MINIO_ENDPOINT")
	fixture.MinioAccessKey = required(t, "OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY")
	fixture.MinioSecretKey = required(t, "OPEN_SPLUNK_TEST_MINIO_SECRET_KEY")
	fixture.MinioBucket = fmt.Sprintf("os-integration-%d", time.Now().UnixNano())
	fixture.MasterKeyPath = filepath.Join(options.Directory, "server.key")
	master := make([]byte, auth.ServerMasterKeyBytes)
	if _, err := rand.Read(master); err != nil {
		t.Fatal(err)
	}
	defer clear(master)
	if err := os.WriteFile(fixture.MasterKeyPath, master, 0o600); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.FingerprintServerMasterKey(master)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RegisterServerMasterKeyIdentity(ctx, fixture.Database, fingerprint[:]); err != nil {
		t.Fatal(err)
	}
	derive := func(purpose string) string {
		mac := hmac.New(sha256.New, master)
		_, _ = mac.Write([]byte("open-splunk/server-key/v1\x00" + purpose))
		return hex.EncodeToString(mac.Sum(nil))
	}
	cfg := gcconfig.GetDefaults()
	cfg.Env = new(gcconfig.EnvironmentProduction)
	cfg.FrontendURL = new(options.Origin)
	cfg.AllowedFrontendOrigins, cfg.CorsAllowedOrigins = []string{options.Origin}, []string{options.Origin}
	cfg.CorsAllowCredentials = new(true)
	cfg.SessionCookieName, cfg.SessionCookieSameSite = new(CookieName), new("Lax")
	cfg.SessionCookieSecure = new(strings.HasPrefix(options.Origin, "https://"))
	cfg.SessionTokenSecret = gcconfig.NewSecret(derive("gocommon-session-signing"))
	cfg.ObjectHashSalt = gcconfig.NewSecret(derive("gocommon-private-object-hashing"))
	cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioBucket = fixture.MinioEndpoint, fixture.MinioAccessKey, fixture.MinioBucket
	cfg.MinioSecretKey, cfg.MinioUseSSL, cfg.MinioPublicBucket = gcconfig.NewSecret(fixture.MinioSecretKey), new(false), ""
	fixture.Runtime, err = identity.New(ctx, fixture.Database.GORMDB(), identity.Options{Common: cfg, GoogleClientID: "integration-google-client", GoogleClientSecret: gcconfig.NewSecret("integration-google-secret"), GoogleRedirectURLs: []string{options.Origin + "/auth/oauth/callback/"}, Logger: zap.NewNop()})
	if err != nil {
		t.Fatal("initialize native Google identity fixture:", err)
	}
	if err := fixture.Database.ApplyAdditionalMigration(ctx, "organization-index-authorization-v1", authorization.Schema); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Database.ApplyAdditionalMigration(ctx, "organization-index-authorization-v2-granular", authorization.GranularSchema); err != nil {
		t.Fatal(err)
	}
	fixture.Authorization, err = authorization.New(fixture.Database.SQLDB(), fixture.Runtime, options.PhysicalTenant)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture operator provisions a dedicated versioned artifact bucket;
	// application startup only checks its existing versioning policy.
	client, err := minio.New(fixture.MinioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(fixture.MinioAccessKey, fixture.MinioSecretKey, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetBucketVersioning(ctx, fixture.MinioBucket, minio.BucketVersioningConfiguration{Status: minio.Enabled}); err != nil {
		t.Fatal(err)
	}
	objects, err := objectstore.NewPrivateObjectStorage(ctx, gcconfig.Static(cfg).Persistence(), zap.NewNop())
	if err != nil {
		t.Fatal("initialize dedicated fixture object client:", err)
	}
	fixture.Blobs, err = objectstore.NewContext(ctx, objects)
	if err != nil {
		_ = objects.Close()
		t.Fatal("initialize private versioned fixture objects:", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for upload := range client.ListIncompleteUploads(cleanup, fixture.MinioBucket, "", true) {
			if upload.Err != nil {
				t.Errorf("list disposable incomplete uploads: %v", upload.Err)
				break
			}
			if err := client.RemoveIncompleteUpload(cleanup, fixture.MinioBucket, upload.Key); err != nil {
				t.Errorf("remove disposable incomplete upload: %v", err)
			}
		}
		for object := range client.ListObjects(cleanup, fixture.MinioBucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if object.Err != nil {
				t.Errorf("list disposable private objects: %v", object.Err)
				break
			}
			if err := client.RemoveObject(cleanup, fixture.MinioBucket, object.Key, minio.RemoveObjectOptions{VersionID: object.VersionID}); err != nil {
				t.Errorf("remove disposable private object: %v", err)
			}
		}
		if err := client.RemoveBucket(cleanup, fixture.MinioBucket); err != nil {
			t.Errorf("remove disposable private bucket: %v", err)
		}
	})
	pool := fixture.Database.GORMDB().WithContext(ctx)
	user := pm.User{ID: 42, Username: "integrationgoogle", Email: "integration-google@example.com", UserType: pb.UserType_USER_TYPE_REGULAR_TIER_0}
	if err := pool.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if err := pool.Create(&pm.OAuthLogin{UserID: user.ID, ProviderSlug: "google", Subject: new("integration-verified-google-account"), Email: user.Email}).Error; err != nil {
		t.Fatal(err)
	}
	// The operator action promotes an existing Google account, never a token.
	if err := pool.Exec("UPDATE gocommon_users SET user_type=44 WHERE id=?", user.ID).Error; err != nil {
		t.Fatal(err)
	}
	tokens, err := token.NewTokenService(fixture.Runtime.Persistence(), token.TokenServiceConfig{SessionLifetime: func() time.Duration { return time.Hour }, SecurityLifetime: func() time.Duration { return time.Hour }, SessionTokenSecret: cfg.SessionTokenSecret.StringValue()}, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	fixture.Session, err = tokens.GenerateSessionToken(ctx, user.ID, token.SessionAuthMethod{Provider: pb.AuthProvider_AUTH_PROVIDER_OAUTH, OAuthProviderSlug: "google"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, identity.Prefix+"/organizations/create-with-owner", strings.NewReader(`{"name":"Integration organization","slug":"integration","owner_user_id":42}`))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(&http.Cookie{Name: CookieName, Value: fixture.Session, Secure: true, HttpOnly: true, SameSite: http.SameSiteLaxMode})
	response := httptest.NewRecorder()
	fixture.Runtime.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("create disposable native organization status=%d", response.Code)
	}
	var organization struct {
		ID int64 `json:"organization_id"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &organization); err != nil || organization.ID <= 0 {
		t.Fatalf("decode disposable organization: %v", err)
	}
	fixture.OrganizationID = strconv.FormatInt(organization.ID, 10)
	return fixture
}

func (fixture *Fixture) Close() error {
	fixture.closeOnce.Do(func() {
		if fixture.Blobs != nil {
			fixture.closeError = fixture.Blobs.Close()
		}
		if fixture.Runtime != nil {
			if err := fixture.Runtime.Close(); err != nil {
				fixture.closeError = errors.Join(fixture.closeError, err)
			}
		}
		if fixture.ownedDatabase {
			if err := fixture.Database.Close(); fixture.closeError == nil {
				fixture.closeError = err
			}
		}
	})
	return fixture.closeError
}

// BindIndex is fixture setup corresponding to production index creation's
// ownership transaction. Every browser fixture index belongs to its real org.
func (fixture *Fixture) BindIndex(ctx context.Context, indexID string) error {
	_, err := fixture.Database.SQLDB().ExecContext(ctx, "INSERT INTO os_index_organizations(index_id,organization_id) VALUES($1,$2)", indexID, fixture.OrganizationID)
	return err
}

func (fixture *Fixture) AuthorizeCapture(ctx context.Context, job searchjobs.Job) error {
	userID, err := strconv.ParseUint(job.OwnerID, 10, 64)
	if err != nil {
		return authorization.ErrUnauthorized
	}
	if actor, ok := authorization.FromContext(ctx); ok {
		userID = actor.Subject.UserID
	}
	subject, err := fixture.Runtime.CurrentSubject(ctx, userID)
	if err != nil {
		return err
	}
	organization, err := strconv.ParseInt(job.OrganizationID, 10, 64)
	if err != nil || organization <= 0 {
		return authorization.ErrForbidden
	}
	for _, name := range job.EffectiveIndexes {
		index, err := fixture.Database.GetIndexByName(ctx, name)
		if err != nil {
			return err
		}
		if err := fixture.Authorization.AuthorizeIndex(ctx, subject, organization, index.ID, authorization.Search); err != nil {
			return err
		}
	}
	return nil
}

func required(t testing.TB, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required for the native integration fixture", name)
	}
	return value
}
