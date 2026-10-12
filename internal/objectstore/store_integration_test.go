package objectstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

func realStore(t *testing.T) (*objectstore.Store, *common_persistence.PrivateObjectStorage, *minio.Client, string) {
	t.Helper()
	endpoint := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("isolated MinIO integration endpoint not configured")
	}
	key := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY")
	secret := os.Getenv("OPEN_SPLUNK_TEST_MINIO_SECRET_KEY")
	if key == "" || secret == "" {
		t.Fatal("isolated MinIO credentials are required")
	}
	tls := false
	partSize := int64(5 << 20)
	bucket := fmt.Sprintf("os-private-test-%d", time.Now().UnixNano())
	source := config.Static(&config.Config{MinioEndpoint: endpoint, MinioAccessKey: key, MinioSecretKey: config.NewSecret(secret), MinioUseSSL: &tls, MinioBucket: bucket, ObjectStorageStreamPartSize: &partSize})
	raw, err := common_persistence.NewPrivateObjectStorage(context.Background(), source.Persistence(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(key, secret, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.SetBucketVersioning(t.Context(), bucket, minio.BucketVersioningConfiguration{Status: minio.Enabled}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if object.Err == nil {
				_ = client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{VersionID: object.VersionID})
			}
		}
		_ = client.RemoveBucket(ctx, bucket)
	})
	store, err := objectstore.NewContext(context.Background(), raw)
	if err != nil {
		t.Fatalf("objectstore init: %v", err)
	}
	return store, raw, client, bucket
}

func TestRealMinIOImmutableVersionPinsAndPendingCleanup(t *testing.T) {
	store, raw, client, bucket := realStore(t)
	ctx := context.Background()
	key := "open-splunk/captures/test/00000000"
	first, err := store.Put(ctx, key, bytes.NewBufferString("first"), 5)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version == "" || first.ETag == "" {
		t.Fatal("PUT did not return immutable identity")
	}
	if _, err := store.Put(ctx, key, bytes.NewBufferString("collision"), 9); !errors.Is(err, searchartifacts.ErrConflict) {
		t.Fatalf("create-only collision=%v", err)
	}
	replacement, err := raw.PutObjectStreamVersion(ctx, key, bytes.NewBufferString("replacement"), 11, common_persistence.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := store.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	content, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || string(content) != "first" {
		t.Fatalf("version-pinned read=%q %v", content, err)
	}
	if err := store.Delete(ctx, first); err != nil {
		t.Fatal(err)
	}
	current, err := store.Current(ctx, key)
	if err != nil || current.Version != replacement.VersionID {
		t.Fatalf("delete removed replacement: %#v %v", current, err)
	}
	if err := store.Delete(ctx, current); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Current(ctx, key); !errors.Is(err, searchartifacts.ErrNotFound) {
		t.Fatalf("deleted HEAD=%v", err)
	}
	// A lost PUT receipt must not cause pending-key cleanup to delete a foreign object.
	foreignKey := "open-splunk/captures/foreign/00000000"
	foreign, err := raw.PutObjectStreamVersion(ctx, foreignKey, bytes.NewBufferString("foreign"), 7, common_persistence.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, searchartifacts.BlobIdentity{Key: foreignKey}); !errors.Is(err, searchartifacts.ErrConflict) {
		t.Fatalf("foreign pending cleanup=%v", err)
	}
	if current, err := store.Current(ctx, foreignKey); err != nil || current.Version != foreign.VersionID {
		t.Fatalf("pending cleanup removed foreign version: %#v %v", current, err)
	}
	configuration, err := client.GetBucketLifecycle(ctx, bucket)
	if store.PrivateUploadLifecycleConfigured() && (err != nil || configuration == nil || len(configuration.Rules) != 1 || configuration.Rules[0].AbortIncompleteMultipartUpload.DaysAfterInitiation != 1) {
		t.Fatalf("incomplete lifecycle=%#v %v", configuration, err)
	}
	pending := "open-splunk/exports/pending/part"
	core := minio.Core{Client: client}
	uploadID, err := core.NewMultipartUpload(ctx, bucket, pending, minio.PutObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := core.PutObjectPart(ctx, bucket, pending, uploadID, 1, bytes.NewReader(bytes.Repeat([]byte("x"), 5<<20)), 5<<20, minio.PutObjectPartOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, searchartifacts.BlobIdentity{Key: pending}); err != nil {
		t.Fatal(err)
	}
	for upload := range client.ListIncompleteUploads(ctx, bucket, pending, true) {
		if upload.Err != nil {
			t.Fatal(upload.Err)
		}
		if upload.Key == pending {
			t.Fatalf("pending multipart upload survived cleanup: %s", upload.UploadID)
		}
	}
}

func TestRealMinIOUnknownLengthStream(t *testing.T) {
	store, _, _, _ := realStore(t)
	payload := bytes.Repeat([]byte("stream"), 2<<20)
	identity, err := store.Put(context.Background(), "open-splunk/exports/stream/blob", bytes.NewReader(payload), -1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), identity.Key, bytes.NewReader([]byte("collision")), -1); !errors.Is(err, searchartifacts.ErrConflict) {
		t.Fatalf("multipart create-only collision=%v", err)
	}
	if identity.Bytes != int64(len(payload)) {
		t.Fatalf("stream size=%d", identity.Bytes)
	}
	reader, err := store.Open(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	received, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil || !bytes.Equal(payload, received) {
		t.Fatalf("stream round trip=%v", err)
	}
}
