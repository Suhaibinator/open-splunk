package queryexec

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/config"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

func TestFullCaptureExceedsPreviewAgainstClickHouseAndMinIO(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1 to run the database integration test")
	}
	const rowCount = 10_003
	payload := bytes.Repeat([]byte("x"), 9<<10)
	indexTime := time.Now().UTC().Truncate(time.Second)
	earliest, latest := indexTime.Add(-time.Hour), indexTime.Add(-time.Minute)
	events := make([]semanticBytesLineageEvent, rowCount)
	for index := range events {
		events[index] = semanticBytesLineageEvent{
			id: fmt.Sprintf("full-capture-%05d", index), at: earliest.Add(time.Minute),
			raw: payload, encoding: opensplunk.RawEncoding_RAW_ENCODING_UTF8,
		}
	}
	ctx, executor := semanticBytesLineageStartClickHouse(t, indexTime, events)
	blobs := fullCaptureMinIO(t, ctx)
	database, err := control.Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	directory := t.TempDir()
	artifactConfig := searchartifacts.Config{DB: database.SQLDB(), Directory: directory, Blobs: blobs, CleanupInterval: -1}
	artifacts, err := searchartifacts.New(ctx, artifactConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	factory, err := searchartifacts.NewCaptureFactory(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor: executor, Snapshotter: queryIntegrationSnapshotter(1), Compiler: clickhouse.Compiler{},
		Journal: artifacts, CaptureFactory: fullCaptureDiagnostics{Store: artifacts, delegate: factory, test: t}, MaxConcurrent: 1, MaxQueued: 1,
		MaxRows: 10_000, MaxBytes: 64 << 20, MaxRuntime: 2 * time.Minute, CleanupInterval: -1,
		Now: func() time.Time { return indexTime.Add(500 * time.Microsecond) },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	job, err := manager.Create(ctx, searchjobs.CreateRequest{
		SPL: "index=" + semanticBytesLineageIndex + " | table _raw", OwnerID: "owner", TenantID: "tenant", OrganizationID: "1",
		AuthorizedIndexes: []string{semanticBytesLineageIndex}, RequestedIndexes: []string{semanticBytesLineageIndex},
		TimeRange: queryIntegrationTimeRange(t, earliest, latest),
	})
	if err != nil {
		t.Fatal(err)
	}
	for !job.State.Terminal() {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
		job, err = manager.Get(job.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if job.State != searchjobs.StateCompleted || !job.CaptureComplete || job.RowCount != rowCount || job.CaptureRows != rowCount || job.ResultsTruncated || !job.PreviewTruncated || job.CaptureBytes < rowCount*uint64(len(payload)) {
		t.Fatalf("full native capture was limited by preview: state=%v rows=%d capture=%t/%d/%d preview_truncated=%t results_truncated=%t failure=%#v", job.State, job.RowCount, job.CaptureComplete, job.CaptureRows, job.CaptureBytes, job.PreviewTruncated, job.ResultsTruncated, job.Failure)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if err := artifacts.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := searchartifacts.New(ctx, artifactConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	access := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner", OrganizationID: "1"}
	lease, err := reopened.AcquireCapture(ctx, access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !lease.RowCountExact() || lease.RowCount() != rowCount {
		_ = lease.Close()
		t.Fatal("sealed capture lost its exact complete row count")
	}
	seekable, ok := lease.(searchartifacts.SeekableResultLease)
	if !ok {
		_ = lease.Close()
		t.Fatal("full snapshot does not support bounded late-page reads")
	}
	if err := seekable.Seek(ctx, rowCount-1); err != nil {
		_ = lease.Close()
		t.Fatal(err)
	}
	last, present, err := lease.Next(ctx)
	if err != nil || !present || last.Ordinal != rowCount-1 {
		_ = lease.Close()
		t.Fatalf("last captured row: ordinal=%d present=%t error=%v", last.Ordinal, present, err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := exportjobs.NewCaptureSource(reopened)
	if err != nil {
		t.Fatal(err)
	}
	exportLease, err := source.AcquireResultsFor(ctx, access, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer exportLease.Close()
	var exportedRows, exportedBytes uint64
	for {
		row, present, err := exportLease.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !present {
			break
		}
		value, valid := row.Values[0].String()
		if !valid || value != string(payload) {
			t.Fatalf("captured export row %d lost its exact value", row.Ordinal)
		}
		exportedRows++
		exportedBytes += uint64(len(value))
	}
	if exportedRows != rowCount || exportedBytes != rowCount*uint64(len(payload)) {
		t.Fatalf("durable export source used a preview: rows=%d bytes=%d", exportedRows, exportedBytes)
	}
	t.Logf("real ClickHouse full capture and restarted MinIO export source: %d rows, %d bytes", exportedRows, exportedBytes)
}

type fullCaptureDiagnostics struct {
	*searchartifacts.Store
	delegate searchjobs.CaptureFactory
	test     *testing.T
}

func (factory fullCaptureDiagnostics) BeginCapture(ctx context.Context, job searchjobs.Job, compiled clickhouse.CompiledQuery, generation uint64) (searchjobs.CaptureTransaction, error) {
	capture, err := factory.delegate.BeginCapture(ctx, job, compiled, generation)
	if err != nil {
		return nil, err
	}
	return fullCapturePublicationDiagnostics{CaptureTransaction: capture, test: factory.test}, nil
}

type fullCapturePublicationDiagnostics struct {
	searchjobs.CaptureTransaction
	test *testing.T
}

func (capture fullCapturePublicationDiagnostics) PreparePublication(ctx context.Context, job searchjobs.Job) error {
	preparer, ok := capture.CaptureTransaction.(searchjobs.CapturePublicationPreparer)
	if !ok {
		return fmt.Errorf("real capture does not provide publication preparation")
	}
	err := preparer.PreparePublication(ctx, job)
	if err != nil {
		capture.test.Logf("full capture preparation failed: %v", err)
	}
	return err
}

func (capture fullCapturePublicationDiagnostics) Publish(ctx context.Context, job searchjobs.Job) error {
	err := capture.CaptureTransaction.Publish(ctx, job)
	if err != nil {
		capture.test.Logf("full capture publication failed: %v", err)
	}
	return err
}

func fullCaptureMinIO(t *testing.T, ctx context.Context) *objectstore.Store {
	t.Helper()
	endpoint, accessKey, secretKey := os.Getenv("OPEN_SPLUNK_TEST_MINIO_ENDPOINT"), os.Getenv("OPEN_SPLUNK_TEST_MINIO_ACCESS_KEY"), os.Getenv("OPEN_SPLUNK_TEST_MINIO_SECRET_KEY")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		t.Fatal("isolated MinIO endpoint and credentials are required for the full capture integration test")
	}
	useTLS, partBytes := false, int64(5<<20)
	bucket := fmt.Sprintf("os-real-capture-%d", time.Now().UnixNano())
	settings := config.Static(&config.Config{MinioEndpoint: endpoint, MinioAccessKey: accessKey, MinioSecretKey: config.NewSecret(secretKey), MinioUseSSL: &useTLS, MinioBucket: bucket, ObjectStorageStreamPartSize: &partBytes})
	objects, err := common_persistence.NewPrivateObjectStorage(ctx, settings.Persistence(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = objects.Close() })
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: useTLS})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for object := range client.ListObjects(cleanup, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if object.Err != nil {
				t.Error(object.Err)
				continue
			}
			if err := client.RemoveObject(cleanup, bucket, object.Key, minio.RemoveObjectOptions{VersionID: object.VersionID}); err != nil {
				t.Error(err)
			}
		}
		if err := client.RemoveBucket(cleanup, bucket); err != nil {
			t.Error(err)
		}
	})
	blobs, err := objectstore.NewContext(ctx, objects)
	if err != nil {
		t.Fatal(err)
	}
	return blobs
}
