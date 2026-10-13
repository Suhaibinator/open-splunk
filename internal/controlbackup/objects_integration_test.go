//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/exportjournal"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func realRecoveryBlobs(t *testing.T) *objectstore.Store {
	t.Helper()
	endpoint := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Fatal("OPEN_SPLUNK_TEST_MINIO_ENDPOINT is required for native recovery object tests")
	}
	access := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY")
	secret := os.Getenv("OPEN_SPLUNK_TEST_MINIO_SECRET_KEY")
	if access == "" || secret == "" {
		t.Fatal("native MinIO test credentials are required")
	}
	bucket := "os-recovery-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config := gcconfig.GetDefaults()
	config.MinioEndpoint = endpoint
	config.MinioAccessKey = access
	config.MinioSecretKey = gcconfig.NewSecret(secret)
	config.MinioBucket = bucket
	config.MinioPublicBucket = ""
	config.MinioUseSSL = new(false)
	objects, err := objectstore.NewPrivateObjectStorage(t.Context(), gcconfig.Static(config).Persistence(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = objects.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: false})
		if err != nil {
			t.Error(err)
			return
		}
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if object.Err != nil {
				t.Error(object.Err)
				continue
			}
			if err := client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{VersionID: object.VersionID}); err != nil {
				t.Error(err)
			}
		}
		if err := client.RemoveBucket(ctx, bucket); err != nil {
			t.Error(err)
		}
	})
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetBucketVersioning(t.Context(), bucket, minio.BucketVersioningConfiguration{Status: minio.Enabled}); err != nil {
		t.Fatal(err)
	}
	store, err := objectstore.NewContext(t.Context(), objects)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
func seedCapturedResults(t *testing.T, database *control.DB, blobs searchartifacts.BlobStore) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().UTC().Truncate(time.Microsecond)
	store, err := searchartifacts.New(ctx, searchartifacts.Config{DB: database.SQLDB(), Blobs: blobs, Directory: privateTestDirectory(t), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rangeValue, err := searchtime.NewAbsoluteRange(now.Add(-time.Hour), now)
	if err != nil {
		t.Fatal(err)
	}
	job := searchjobs.Job{ID: "native-recovery-capture", Version: 1, OwnerID: "1", TenantID: "tenant", OrganizationID: "1", SPL: "index=recovery-index | table message", TimeRange: rangeValue.Intent(), State: searchjobs.StateQueued, CreatedAt: now, Earliest: now.Add(-time.Hour), Latest: now, IndexTimeCutoff: now, EffectiveIndexes: []string{"recovery-index"}}
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	parsed, err := spl.Parse(job.SPL)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := uint64(0)
	logical, err := plan.Build(parsed, plan.Scope{TenantID: job.TenantID, AuthorizedIndexes: job.EffectiveIndexes, RequestedIndexes: job.EffectiveIndexes, Earliest: job.Earliest, Latest: job.Latest, SearchStart: now, SearchTimezone: "UTC", IndexTimeCutoff: now, VisibilityCutoff: &cutoff})
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatal(err)
	}
	capture, err := store.BeginCapture(ctx, job, compiled, 7)
	if err != nil {
		t.Fatal(err)
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	if err := capture.SetSchema(schema); err != nil {
		t.Fatal(err)
	}
	const rows = 4097
	for ordinal := range uint64(rows) {
		if err := capture.Append(ctx, searchjobs.ResultRow{Ordinal: ordinal, Values: []searchjobs.Value{searchjobs.StringValue(fmt.Sprintf("%05d:", ordinal) + strings.Repeat("x", 4096))}}); err != nil {
			t.Fatal(err)
		}
	}
	job.Version = 6
	job.State = searchjobs.StateCompleted
	job.StartedAt = now
	job.FinishedAt = now
	job.ExpiresAt = now.Add(time.Hour)
	job.Schema = &schema
	job.RowCount = rows
	job.CaptureComplete = true
	if err := capture.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
}
func seedExport(t *testing.T, database *control.DB, blobs searchartifacts.BlobStore) (searchartifacts.BlobIdentity, []byte) {
	t.Helper()
	payload := bytes.Repeat([]byte("message\nretained-export\n"), 2048)
	key := "open-splunk/exports/native-recovery-export/11111111111111111111111111111111"
	identity, err := blobs.Put(t.Context(), key, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	events, err := audit.NewStore(database, audit.StoreOptions{CursorKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	journal, err := exportjournal.New(database.GORMDB(), events)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := audit.WithActor(t.Context(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "1", Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "1", OrganizationID: "1"}
	job := exportjobs.Job{OrganizationID: "1", ID: "native-recovery-export", Version: 1, SearchJobID: "native-recovery-capture", Format: exportjobs.FormatCSV, State: exportjobs.StateQueued, CreatedAt: now, Progress: exportjobs.Progress{UpdatedAt: now}, Columns: []string{"message"}, RowLimit: 4097, ByteLimit: 1 << 20}
	if err := journal.Admit(ctx, access, job); err != nil {
		t.Fatal(err)
	}
	job.Version = 2
	job.State = exportjobs.StateCompleted
	job.StartedAt = now
	job.FinishedAt = now
	job.ExpiresAt = now.Add(time.Hour)
	job.Artifact = &exportjobs.Artifact{FileName: "result.csv", MediaType: "text/csv", SizeBytes: uint64(len(payload)), RowCount: 4097, ExpiresAt: job.ExpiresAt}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if err := journal.Update(ctx, exportjobs.DurableJob{Access: access, Job: job, ArtifactName: string(encoded), ArtifactSHA256: digest[:]}); err != nil {
		t.Fatal(err)
	}
	return identity, payload
}
func TestNativePostgresMinIOBackupRestoresResultChunksAndExportVersions(t *testing.T) {
	fixture := newRecoveryFixture(t)
	sourceBlobs := realRecoveryBlobs(t)
	fixture.blobs = sourceBlobs
	seedCapturedResults(t, fixture.database, sourceBlobs)
	exportIdentity, exportPayload := seedExport(t, fixture.database, sourceBlobs)
	oldReferences, err := sourceObjectReferences(t.Context(), fixture.database)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldReferences) < 3 {
		t.Fatalf("expected multiple result chunks and export: %d", len(oldReferences))
	}
	manifest, err := Create(t.Context(), fixture.createOptions())
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Objects.FileCount < 3 {
		t.Fatal("native object archive omitted payloads")
	}
	targetBlobs := realRecoveryBlobs(t)
	options := fixture.restoreOptions()
	options.Blobs = targetBlobs
	collisionBlobs := realRecoveryBlobs(t)
	reference := oldReferences[0]
	if _, err := collisionBlobs.Put(t.Context(), reference.Identity.Key, bytes.NewReader(bytes.Repeat([]byte{0}, int(reference.Identity.Bytes))), reference.Identity.Bytes); err != nil {
		t.Fatal(err)
	}
	collisionOptions := options
	collisionOptions.Blobs = collisionBlobs
	if err := PreflightRestore(t.Context(), collisionOptions); err == nil {
		t.Fatal("recovery preflight accepted different content under existing object key")
	}
	if _, err := os.Stat(options.MasterKeyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("conflicting object preflight published master key")
	}

	injected := errors.New("interrupt after object rebind")
	if err := restoreWithHooks(t.Context(), options, restoreHooks{afterObjectsPublish: func() error { return injected }}); !errors.Is(err, injected) {
		t.Fatalf("interrupted object recovery: %v", err)
	}
	if _, err := os.Stat(options.MasterKeyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("master key published before coordinated object rebind")
	}
	if err := Restore(t.Context(), options); err != nil {
		t.Fatal(err)
	}
	database, err := control.Open(t.Context(), options.PostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	references, err := sourceObjectReferences(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	if len(references) != len(oldReferences) {
		t.Fatal("restored object count changed")
	}
	for index, ref := range references {
		old := oldReferences[index]
		if ref.Identity.Key != old.Identity.Key || ref.SHA256 != old.SHA256 || ref.Identity.Version == old.Identity.Version {
			t.Fatal("SQL object identity did not bind fresh immutable target version")
		}
		reader, err := targetBlobs.Open(t.Context(), ref.Identity)
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, reader)
		readErr = errors.Join(readErr, reader.Close())
		if readErr != nil || !bytes.Equal(hash.Sum(nil), mustDigest(ref.SHA256)) {
			t.Fatalf("restored version content mismatch: %v", readErr)
		}
	}
	targetIdentity, err := targetBlobs.Current(t.Context(), exportIdentity.Key)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := targetBlobs.Open(t.Context(), targetIdentity)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(reader)
	err = errors.Join(err, reader.Close())
	if err != nil || !bytes.Equal(actual, exportPayload) {
		t.Fatalf("restored CSV payload differs: %v", err)
	}
	results, err := searchartifacts.New(t.Context(), searchartifacts.Config{DB: database.SQLDB(), Blobs: targetBlobs, Directory: privateTestDirectory(t), CleanupInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer results.Close()
	lease, err := results.Acquire(t.Context(), searchjobs.AccessScope{TenantID: "tenant", OwnerID: "1", OrganizationID: "1"}, "native-recovery-capture")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	var count uint64
	for {
		row, ok, err := lease.Next(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		if row.Ordinal != count {
			t.Fatal("restored chunk order changed")
		}
		count++
	}
	if count != 4097 {
		t.Fatalf("restored result rows: %d", count)
	}
}
