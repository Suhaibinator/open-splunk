package common_persistence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
	"go.uber.org/zap"
)

var ErrPrivateObjectNotFound = errors.New("private object not found")
var ErrPrivateObjectAlreadyExists = errors.New("private object already exists")
var ErrPrivateObjectVersioningRequired = errors.New("private artifact bucket must already have versioning enabled")

var ErrPrivateLifecycleUnsupported = errors.New("object backend does not support incomplete upload lifecycle rules")

// PrivateObjectStorage exposes low-level private objects without database,
// identity, attachments, schema bootstrap, or background monitor ownership.
type PrivateObjectStorage struct {
	storage *minioObjectStorage
	cancel  context.CancelFunc
}

func NewPrivateObjectStorage(ctx context.Context, provider config.PersistenceProvider, logger *zap.Logger) (*PrivateObjectStorage, error) {
	if ctx == nil || provider == nil {
		return nil, errors.New("private object storage requires context and configuration")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	startup := provider()
	if startup == nil {
		return nil, errors.New("private object storage requires a configuration view")
	}
	storageCtx, cancel := context.WithCancel(ctx)
	handler := &minioObjectStorage{runtime: provider, logger: logger, ctx: storageCtx, objectHashSalt: startup.ObjectHashSalt()}
	if err := handler.initMinioClient(startup); err != nil {
		cancel()
		return nil, err
	}
	return &PrivateObjectStorage{storage: handler, cancel: cancel}, nil
}
func (store *PrivateObjectStorage) Close() error { store.cancel(); return nil }
func (store *PrivateObjectStorage) PutObjectStream(ctx context.Context, key string, reader io.Reader, size int64, options PutObjectOptions) error {
	return store.storage.PutObjectStream(ctx, key, reader, size, options)
}
func (store *PrivateObjectStorage) PutObjectStreamVersion(ctx context.Context, key string, reader io.Reader, size int64, options PutObjectOptions) (*MinioObjectInfo, error) {
	return store.storage.PutObjectStreamVersion(ctx, key, reader, size, options)
}
func (store *PrivateObjectStorage) GetObjectReader(ctx context.Context, key string, precondition ObjectPrecondition) (io.ReadCloser, error) {
	return store.storage.GetObjectReader(ctx, key, precondition)
}
func (store *PrivateObjectStorage) GetObjectInfo(ctx context.Context, key string) (*MinioObjectInfo, error) {
	return store.storage.GetObjectInfo(ctx, key)
}
func (store *PrivateObjectStorage) RemoveObject(ctx context.Context, key string) error {
	return store.storage.RemoveObject(ctx, key)
}
func (store *PrivateObjectStorage) RemoveObjectVersion(ctx context.Context, key, version string) error {
	return store.storage.RemoveObjectVersion(ctx, key, version)
}
func (store *PrivateObjectStorage) CheckPrivateObjectVersioning(ctx context.Context) error {
	return store.storage.CheckPrivateObjectVersioning(ctx)
}

func (store *PrivateObjectStorage) EnsurePrivateUploadLifecycle(ctx context.Context, prefix string) error {
	return store.storage.EnsurePrivateUploadLifecycle(ctx, prefix)
}
func (store *PrivateObjectStorage) AbortPrivateObjectUpload(ctx context.Context, key string) error {
	return store.storage.AbortPrivateObjectUpload(ctx, key)
}

// EnsurePrivateUploadLifecycle retains other owners' rules and bounds
// crash-orphaned multipart uploads under one application-owned prefix.
func (handler *minioObjectStorage) EnsurePrivateUploadLifecycle(ctx context.Context, prefix string) error {
	if prefix == "" || strings.HasPrefix(prefix, "/") {
		return errors.New("an owned object prefix is required")
	}
	configuration, err := handler.minioClient.GetBucketLifecycle(ctx, handler.minioBucket)
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
	err = handler.minioClient.SetBucketLifecycle(ctx, handler.minioBucket, configuration)
	if response := minio.ToErrorResponse(err); response.Code == "MalformedXML" || response.Code == "NotImplemented" || (response.Code == "InvalidArgument" && response.Message == "The XML you provided was not well-formed or did not validate against our published schema") {
		return fmt.Errorf("%w: %v", ErrPrivateLifecycleUnsupported, err)
	}
	if err != nil {
		return fmt.Errorf("private upload lifecycle response %s: %w", minio.ToErrorResponse(err).Code, err)
	}
	return nil
}

// AbortPrivateObjectUpload is safe for a journaled pending key whose owning
// writer has stopped. Consumers must not call it for a live shared writer.
func (handler *minioObjectStorage) AbortPrivateObjectUpload(ctx context.Context, key string) error {
	return handler.minioClient.RemoveIncompleteUpload(ctx, handler.minioBucket, key)
}

// CheckPrivateObjectVersioning verifies an operator-provisioned artifact bucket.
// It never changes bucket versioning: enabling it on a shared attachment bucket
// would make ordinary attachment deletions leave historical payloads behind.
func (handler *minioObjectStorage) CheckPrivateObjectVersioning(ctx context.Context) error {
	configuration, err := handler.minioClient.GetBucketVersioning(ctx, handler.minioBucket)
	if err != nil {
		return err
	}
	if configuration.Enabled() {
		return nil
	}
	return ErrPrivateObjectVersioningRequired
}

// RemoveObjectVersion never deletes a replacement's current version or leaves
// a delete marker in place of physical reclamation of the pinned bytes.
func (handler *minioObjectStorage) RemoveObjectVersion(ctx context.Context, key, version string) error {
	if version == "" || version == "null" {
		return errors.New("an immutable object version is required")
	}
	return handler.minioClient.RemoveObject(ctx, handler.minioBucket, key, minio.RemoveObjectOptions{VersionID: version})
}

// PutObjectStreamVersion returns the identity of the PUT itself, avoiding a
// post-upload HEAD race against a replacement. Unknown-length streams retain
// the same bounded multipart and overflow checks as PutObjectStream.
func (handler *minioObjectStorage) PutObjectStreamVersion(ctx context.Context, key string, reader io.Reader, size int64, options PutObjectOptions) (*MinioObjectInfo, error) {
	if reserved := strings.TrimLeft(key, "/"); strings.HasPrefix(reserved, TempObjectPrefix) || strings.HasPrefix(reserved, VerifiedObjectPrefix) {
		return nil, ErrReservedObjectName
	}
	if size < -1 {
		return nil, fmt.Errorf("object stream size must be -1 or non-negative")
	}
	var partSize int64
	if size < 0 {
		partSize = handler.runtime().ObjectStorageStreamPartSize()
		reader = &cappedReader{r: reader, remaining: partSize * maxMultipartParts}
	} else {
		reader = boundedReader(reader, size)
	}
	return handler.putObjectResult(ctx, "put_object_stream_version", key, reader, size, options, partSize)
}
