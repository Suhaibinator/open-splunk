package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

func runtimePatternPrivateBlobs(t *testing.T) searchartifacts.BlobStore {
	t.Helper()
	endpoint, key, secret := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ENDPOINT"), os.Getenv("OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY"), os.Getenv("OPEN_SPLUNK_TEST_MINIO_SECRET_KEY")
	if endpoint == "" || key == "" || secret == "" {
		t.Fatal("isolated native pattern export fixture requires MinIO endpoint and credentials")
	}
	bucket := fmt.Sprintf("os-pattern-runtime-%d", time.Now().UnixNano())
	source := gcconfig.Static(&gcconfig.Config{MinioEndpoint: endpoint, MinioAccessKey: key, MinioSecretKey: gcconfig.NewSecret(secret), MinioUseSSL: new(false), MinioBucket: bucket, ObjectStorageStreamPartSize: new(int64(5 << 20))})
	raw, err := common_persistence.NewPrivateObjectStorage(t.Context(), source.Persistence(), zap.NewNop())
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
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
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
	store, err := objectstore.NewContext(t.Context(), raw)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
