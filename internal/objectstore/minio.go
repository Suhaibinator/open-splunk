package objectstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"time"

	"fortio.org/safecast"
	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"go.uber.org/zap"
)

var ErrPrivateObjectNotFound = errors.New("private object not found")
var ErrPrivateObjectAlreadyExists = errors.New("private object already exists")
var ErrPrivateObjectVersioningRequired = errors.New("private artifact bucket must already have versioning enabled")
var ErrPrivateLifecycleUnsupported = errors.New("object backend does not support incomplete upload lifecycle rules")

// Version identity and create-only publication belong to OpenSplunk's capture
// journal, not GoCommon's attachment registry. Keep their types application-owned.
type MinioObjectInfo struct {
	VersionID    string
	UserMetadata map[string]string
	Size         int64
	ETag         string
}
type PutObjectOptions struct {
	NoOverwrite                                                    bool
	UserMetadata                                                   map[string]string
	ContentType, ContentEncoding, ContentDisposition, CacheControl string
}
type ObjectPrecondition struct{ MatchETag, VersionID string }

// PrivateObjectStorage owns only the application's dedicated artifact client.
// It never initializes a persistence handler, changes bucket versioning or
// publishes attachments. Identity continues to use GoCommon's full handler.
type PrivateObjectStorage struct {
	client    *minio.Client
	bucket    string
	partSize  int64
	transport *http.Transport
}

func NewPrivateObjectStorage(ctx context.Context, provider config.PersistenceProvider, logger *zap.Logger) (*PrivateObjectStorage, error) {
	if ctx == nil || provider == nil {
		return nil, errors.New("private object storage requires context and configuration")
	}
	view := provider()
	if view == nil {
		return nil, errors.New("private object storage requires a configuration view")
	}
	if view.MinioBucket() == "" || (view.MinioPublicBucket() != "" && view.MinioPublicBucket() == view.MinioBucket()) {
		return nil, errors.New("artifact storage requires a dedicated private bucket")
	}
	partSize := view.ObjectStorageStreamPartSize()
	if partSize < 5<<20 || partSize > 512<<20 {
		return nil, errors.New("artifact stream part size must be between 5 and 512 MiB")
	}
	transport, err := minio.DefaultTransport(view.MinioUseSSL())
	if err != nil {
		return nil, err
	}
	client, err := minio.New(view.MinioEndpoint(), &minio.Options{Creds: credentials.NewStaticV4(view.MinioAccessKey(), view.MinioSecretKey().StringValue(), ""), Secure: view.MinioUseSSL(), Transport: transport})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	startup, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	exists, err := client.BucketExists(startup, view.MinioBucket())
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(startup, view.MinioBucket(), minio.MakeBucketOptions{}); err != nil {
			transport.CloseIdleConnections()
			return nil, err
		}
		if logger != nil {
			logger.Debug("Created private artifact bucket; operator versioning is still required", zap.String("bucket", view.MinioBucket()))
		}
	}
	return &PrivateObjectStorage{client: client, bucket: view.MinioBucket(), partSize: partSize, transport: transport}, nil
}
func (storage *PrivateObjectStorage) Close() error {
	storage.transport.CloseIdleConnections()
	return nil
}

func (storage *PrivateObjectStorage) CheckPrivateObjectVersioning(ctx context.Context) error {
	configuration, err := storage.client.GetBucketVersioning(ctx, storage.bucket)
	if err != nil {
		return err
	}
	if !configuration.Enabled() {
		return ErrPrivateObjectVersioningRequired
	}
	return nil
}

func (storage *PrivateObjectStorage) PutObjectStreamVersion(ctx context.Context, key string, reader io.Reader, size int64, opts PutObjectOptions) (*MinioObjectInfo, error) {
	if reader == nil || size < -1 {
		return nil, errors.New("object stream requires a reader and size -1 or non-negative")
	}
	if !validKey(key) {
		return nil, errors.New("private artifact key is outside the application namespace")
	}
	var partSize int64
	if size < 0 {
		partSize = storage.partSize
		reader = &cappedReader{reader: reader, remaining: partSize * 10000}
	} else {
		reader = boundedReader(reader, size)
	}
	options := minio.PutObjectOptions{ContentType: opts.ContentType, ContentEncoding: opts.ContentEncoding, ContentDisposition: opts.ContentDisposition, CacheControl: opts.CacheControl, PartSize: safecast.MustConv[uint64](partSize), UserMetadata: detachedMetadata(opts.UserMetadata)}
	if opts.NoOverwrite {
		options.SetMatchETagExcept("*")
	}
	result, err := storage.client.PutObject(ctx, storage.bucket, key, reader, size, options)
	if err != nil {
		response := minio.ToErrorResponse(err)
		if opts.NoOverwrite && (response.Code == "PreconditionFailed" || response.StatusCode == http.StatusPreconditionFailed) {
			return nil, ErrPrivateObjectAlreadyExists
		}
		return nil, err
	}
	// Use the PUT receipt itself; a subsequent HEAD could observe a replacement.
	return &MinioObjectInfo{Size: result.Size, ETag: result.ETag, VersionID: result.VersionID, UserMetadata: detachedMetadata(opts.UserMetadata)}, nil
}

func (storage *PrivateObjectStorage) GetObjectReader(ctx context.Context, key string, precondition ObjectPrecondition) (io.ReadCloser, error) {
	options := minio.GetObjectOptions{VersionID: precondition.VersionID}
	if precondition.MatchETag != "" {
		if err := options.SetMatchETag(precondition.MatchETag); err != nil {
			return nil, err
		}
	}
	object, err := storage.client.GetObject(ctx, storage.bucket, key, options)
	if err != nil {
		return nil, objectReadError(err)
	}
	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		return nil, objectReadError(err)
	}
	return &pinnedReader{object: object}, nil
}
func (storage *PrivateObjectStorage) GetObjectInfo(ctx context.Context, key string) (*MinioObjectInfo, error) {
	info, err := storage.client.StatObject(ctx, storage.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		response := minio.ToErrorResponse(err)
		if response.Code == "NoSuchKey" || response.Code == "NoSuchVersion" {
			return nil, ErrPrivateObjectNotFound
		}
		return nil, err
	}
	return &MinioObjectInfo{Size: info.Size, ETag: info.ETag, VersionID: info.VersionID, UserMetadata: detachedMetadata(info.UserMetadata)}, nil
}
func (storage *PrivateObjectStorage) RemoveObjectVersion(ctx context.Context, key, version string) error {
	if version == "" || version == "null" {
		return errors.New("an immutable object version is required")
	}
	return storage.client.RemoveObject(ctx, storage.bucket, key, minio.RemoveObjectOptions{VersionID: version})
}
func (storage *PrivateObjectStorage) AbortPrivateObjectUpload(ctx context.Context, key string) error {
	return storage.client.RemoveIncompleteUpload(ctx, storage.bucket, key)
}
func (storage *PrivateObjectStorage) EnsurePrivateUploadLifecycle(ctx context.Context, prefix string) error {
	if prefix == "" || strings.HasPrefix(prefix, "/") {
		return errors.New("an owned object prefix is required")
	}
	configuration, err := storage.client.GetBucketLifecycle(ctx, storage.bucket)
	if err != nil && minio.ToErrorResponse(err).Code != "NoSuchLifecycleConfiguration" {
		return err
	}
	if configuration == nil {
		configuration = &lifecycle.Configuration{}
	}
	digest := sha256.Sum256([]byte(prefix))
	id := "private-incomplete-" + hex.EncodeToString(digest[:])
	rule := lifecycle.Rule{ID: id, Status: "Enabled", RuleFilter: lifecycle.Filter{Prefix: prefix}, AbortIncompleteMultipartUpload: lifecycle.AbortIncompleteMultipartUpload{DaysAfterInitiation: 1}}
	found := false
	for index, existing := range configuration.Rules {
		if existing.ID == id {
			configuration.Rules[index] = rule
			found = true
		}
	}
	if !found {
		configuration.Rules = append(configuration.Rules, rule)
	}
	err = storage.client.SetBucketLifecycle(ctx, storage.bucket, configuration)
	response := minio.ToErrorResponse(err)
	if response.Code == "MalformedXML" || response.Code == "NotImplemented" || (response.Code == "InvalidArgument" && response.Message == "The XML you provided was not well-formed or did not validate against our published schema") {
		return fmt.Errorf("%w: %w", ErrPrivateLifecycleUnsupported, err)
	}
	return err
}
func detachedMetadata(original map[string]string) map[string]string {
	return maps.Clone(original)
}
func objectReadError(err error) error {
	response := minio.ToErrorResponse(err)
	if response.Code == "PreconditionFailed" || response.StatusCode == http.StatusPreconditionFailed {
		return fmt.Errorf("%w: %w", common_persistence.ErrObjectPreconditionFailed, err)
	}
	return err
}

type pinnedReader struct {
	object  *minio.Object
	lastErr error
}

func (reader *pinnedReader) Read(bytes []byte) (int, error) {
	n, err := reader.object.Read(bytes)
	if err != nil && !errors.Is(err, io.EOF) {
		err = objectReadError(err)
		reader.lastErr = err
	}
	return n, err
}
func (reader *pinnedReader) Close() error { return reader.object.Close() }
func (reader *pinnedReader) Seek(offset int64, whence int) (int64, error) {
	position, err := reader.object.Seek(offset, whence)
	if err != nil {
		err = objectReadError(err)
		reader.lastErr = err
	}
	return position, err
}
func (reader *pinnedReader) LastReadError() error { return reader.lastErr }

// Keep known-length uploads at the reader's current offset, and unknown-length
// multipart uploads bounded. minio-go otherwise chooses ~537 MiB part buffers.
func boundedReader(reader io.Reader, size int64) io.Reader {
	if at, ok := reader.(io.ReaderAt); ok {
		if seeker, ok := reader.(io.Seeker); ok {
			if offset, err := seeker.Seek(0, io.SeekCurrent); err == nil {
				return io.NewSectionReader(at, offset, size)
			}
		}
	}
	return io.LimitReader(reader, size)
}

type cappedReader struct {
	reader    io.Reader
	remaining int64
}

func (reader *cappedReader) Read(bytes []byte) (int, error) {
	if reader.remaining <= 0 {
		return 0, io.EOF
	}
	n, err := reader.reader.Read(bytes[:min(int64(len(bytes)), reader.remaining)])
	reader.remaining -= int64(n)
	if reader.remaining > 0 || err != nil {
		return n, err
	}
	var probe [1]byte
	if extra, probeErr := io.ReadFull(reader.reader, probe[:]); extra > 0 {
		return 0, common_persistence.ErrObjectStreamTooLarge
	} else if !errors.Is(probeErr, io.EOF) {
		return 0, probeErr
	}
	return n, io.EOF
}
