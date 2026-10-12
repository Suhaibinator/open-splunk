package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
	"io"
	"time"
)

// minioObjectStorage owns only the resources used by object operations. It has
// no database, identity, attachment registry or persistence monitor state.
// Both full persistence and standalone private storage use these same methods.
type minioObjectStorage struct {
	runtime                                        config.PersistenceProvider
	logger                                         *zap.Logger
	ctx                                            context.Context
	objectHashSalt                                 config.Secret
	minioClient                                    *minio.Client
	minioBucket, minioPublicBucket, minioAccessKey string
	minioSecretKey                                 config.Secret
}

// objectStorage shares the full handler's initialized storage resources without
// exposing database or lifecycle operations to the storage implementation.
func (h *CommonPersistenceHandler[U]) objectStorage() *minioObjectStorage {
	return &minioObjectStorage{runtime: h.runtime, logger: h.logger, ctx: h.ctx,
		objectHashSalt: h.objectHashSalt, minioClient: h.minioClient,
		minioBucket: h.minioBucket, minioPublicBucket: h.minioPublicBucket,
		minioAccessKey: h.minioAccessKey, minioSecretKey: h.minioSecretKey}
}

func (h *minioObjectStorage) initMinioClient(v config.PersistenceView) error {
	// Config shape is validated before any bucket or policy call: the public
	// bucket is given an anonymous read policy below, so pointing it at the
	// private bucket would silently publish every private object to the world.
	if v.MinioPublicBucket() != "" && v.MinioPublicBucket() == v.MinioBucket() {
		return appLogger.WithErrorFields(
			errors.New("minio_public_bucket must differ from minio_bucket: the private bucket must never carry an anonymous read policy"),
			zap.String(logkeys.Operation, "initialize_minio_client"),
			zap.String(logkeys.Bucket, v.MinioBucket()))
	}

	// Initialize MinIO client
	accessKey, secretKey := v.MinioAccessKey(), v.MinioSecretKey()
	minioClient, err := minio.New(v.MinioEndpoint(), &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey.StringValue(), ""),
		Secure: v.MinioUseSSL(),
	})
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to initialize MinIO client: %w", err),
			zap.String(logkeys.Operation, "initialize_minio_client"),
			zap.String(logkeys.Stage, "create_client"),
			zap.String(appLogger.KeyEndpoint, v.MinioEndpoint()))
	}

	// Use the handler's context for initialization operations with timeout
	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()
	logger := appLogger.For(ctx, h.logger, loggerName)

	// Check if bucket exists
	exists, err := minioClient.BucketExists(ctx, v.MinioBucket())
	if err != nil {
		return appLogger.WithErrorFields(fmt.Errorf("failed to check if bucket exists: %w", err),
			zap.String(logkeys.Operation, "initialize_minio_client"),
			zap.String(logkeys.Stage, "check_private_bucket"),
			zap.String(logkeys.Bucket, v.MinioBucket()))
	}

	// Create bucket if it doesn't exist
	if !exists {
		err = minioClient.MakeBucket(ctx, v.MinioBucket(), minio.MakeBucketOptions{})
		if err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to create bucket: %w", err),
				zap.String(logkeys.Operation, "initialize_minio_client"),
				zap.String(logkeys.Stage, "create_private_bucket"),
				zap.String(logkeys.Bucket, v.MinioBucket()))
		}
		logger.Info("Created MinIO bucket",
			zap.String(logkeys.Operation, "initialize_minio_client"),
			zap.String(logkeys.Bucket, v.MinioBucket()))
	}

	// Public storage is optional. When configured, the public bucket is created on
	// demand and carries an anonymous read-only policy so vetted public objects can
	// be fetched (and CDN-cached) without a presigned URL.
	if v.MinioPublicBucket() != "" {
		publicExists, err := minioClient.BucketExists(ctx, v.MinioPublicBucket())
		if err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to check if public bucket exists: %w", err),
				zap.String(logkeys.Operation, "initialize_minio_client"),
				zap.String(logkeys.Stage, "check_public_bucket"),
				zap.String(logkeys.Bucket, v.MinioPublicBucket()))
		}

		if !publicExists {
			err = minioClient.MakeBucket(ctx, v.MinioPublicBucket(), minio.MakeBucketOptions{})
			if err != nil {
				return appLogger.WithErrorFields(fmt.Errorf("failed to create public bucket: %w", err),
					zap.String(logkeys.Operation, "initialize_minio_client"),
					zap.String(logkeys.Stage, "create_public_bucket"),
					zap.String(logkeys.Bucket, v.MinioPublicBucket()))
			}
			logger.Info("Created MinIO public bucket",
				zap.String(logkeys.Operation, "initialize_minio_client"),
				zap.String(logkeys.Bucket, v.MinioPublicBucket()))
		}

		// Anonymous GetObject only. Listing stays denied so the salted-hash object
		// names remain unguessable. Setting the policy is idempotent.
		if err := minioClient.SetBucketPolicy(ctx, v.MinioPublicBucket(), anonymousReadOnlyBucketPolicy(v.MinioPublicBucket())); err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("failed to set public bucket policy: %w", err),
				zap.String(logkeys.Operation, "initialize_minio_client"),
				zap.String(logkeys.Stage, "set_public_bucket_policy"),
				zap.String(logkeys.Bucket, v.MinioPublicBucket()))
		}
		if ce := logger.Check(zap.DebugLevel, "Reconciled anonymous read-only policy for MinIO public bucket"); ce != nil {
			ce.Write(
				zap.String(logkeys.Operation, "initialize_minio_client"),
				zap.String(logkeys.Bucket, v.MinioPublicBucket()),
			)
		}
	}

	h.minioClient = minioClient
	h.minioBucket = v.MinioBucket()
	h.minioPublicBucket = v.MinioPublicBucket()
	h.minioAccessKey = accessKey
	h.minioSecretKey = secretKey
	return nil
}

func (h *CommonPersistenceHandler[U]) GetPresignedURL(ctx context.Context, objectName string, expiry time.Duration) (string, error) {
	return h.objectStorage().GetPresignedURL(ctx, objectName, expiry)
}

func (h *CommonPersistenceHandler[U]) GeneratePresignedPostURL(ctx context.Context, objectName string, expiresAt time.Time, minSize, maxSize int64, contentType string) (url string, formData map[string]string, err error) {
	return h.objectStorage().GeneratePresignedPostURL(ctx, objectName, expiresAt, minSize, maxSize, contentType)
}

func (h *CommonPersistenceHandler[U]) GetObjectBytes(ctx context.Context, objectName string) ([]byte, error) {
	return h.objectStorage().GetObjectBytes(ctx, objectName)
}

func (h *CommonPersistenceHandler[U]) GetObjectReader(ctx context.Context, objectName string, precondition ObjectPrecondition) (io.ReadCloser, error) {
	return h.objectStorage().GetObjectReader(ctx, objectName, precondition)
}

func (h *CommonPersistenceHandler[U]) PutObjectBytes(ctx context.Context, objectName string, data []byte, contentType string) error {
	return h.objectStorage().PutObjectBytes(ctx, objectName, data, contentType)
}

func (h *CommonPersistenceHandler[U]) PutObjectStream(ctx context.Context, objectName string, reader io.Reader, size int64, opts PutObjectOptions) error {
	return h.objectStorage().PutObjectStream(ctx, objectName, reader, size, opts)
}

func (h *CommonPersistenceHandler[U]) putObject(ctx context.Context, operation, objectName string, reader io.Reader, size int64, opts PutObjectOptions, partSize int64) error {
	return h.objectStorage().putObject(ctx, operation, objectName, reader, size, opts, partSize)
}

func (h *CommonPersistenceHandler[U]) putObjectResult(ctx context.Context, operation, objectName string, reader io.Reader, size int64, opts PutObjectOptions, partSize int64) (*MinioObjectInfo, error) {
	return h.objectStorage().putObjectResult(ctx, operation, objectName, reader, size, opts, partSize)
}

func (h *CommonPersistenceHandler[U]) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string, source ObjectPrecondition) error {
	return h.objectStorage().CopyObject(ctx, sourceObjectName, destinationObjectName, source)
}

func (h *CommonPersistenceHandler[U]) CopyObjectToPublicBucket(ctx context.Context, sourceObjectName, destinationObjectName string, source ObjectPrecondition) error {
	return h.objectStorage().CopyObjectToPublicBucket(ctx, sourceObjectName, destinationObjectName, source)
}

func (h *CommonPersistenceHandler[U]) RemoveObject(ctx context.Context, objectName string) error {
	return h.objectStorage().RemoveObject(ctx, objectName)
}

func (h *CommonPersistenceHandler[U]) removeObjectFromBucket(ctx context.Context, bucket, objectName string) error {
	return h.objectStorage().removeObjectFromBucket(ctx, bucket, objectName)
}

func (h *CommonPersistenceHandler[U]) ObjectExists(ctx context.Context, objectName string) (bool, error) {
	return h.objectStorage().ObjectExists(ctx, objectName)
}

func (h *CommonPersistenceHandler[U]) PublicStorageEnabled() bool {
	return h.objectStorage().PublicStorageEnabled()
}

func (h *CommonPersistenceHandler[U]) ObjectExistsInPublicBucket(ctx context.Context, objectName string) (bool, error) {
	return h.objectStorage().ObjectExistsInPublicBucket(ctx, objectName)
}

func (h *CommonPersistenceHandler[U]) GetObjectInfo(ctx context.Context, objectName string) (*MinioObjectInfo, error) {
	return h.objectStorage().GetObjectInfo(ctx, objectName)
}

func (h *CommonPersistenceHandler[U]) CalculateDualHashesStream(ctx context.Context, objectName string, precondition ObjectPrecondition) (*DualHashes, error) {
	return h.objectStorage().CalculateDualHashesStream(ctx, objectName, precondition)
}

func (h *CommonPersistenceHandler[U]) EnsurePrivateUploadLifecycle(ctx context.Context, prefix string) error {
	return h.objectStorage().EnsurePrivateUploadLifecycle(ctx, prefix)
}

func (h *CommonPersistenceHandler[U]) AbortPrivateObjectUpload(ctx context.Context, key string) error {
	return h.objectStorage().AbortPrivateObjectUpload(ctx, key)
}

func (h *CommonPersistenceHandler[U]) CheckPrivateObjectVersioning(ctx context.Context) error {
	return h.objectStorage().CheckPrivateObjectVersioning(ctx)
}

func (h *CommonPersistenceHandler[U]) RemoveObjectVersion(ctx context.Context, key, version string) error {
	return h.objectStorage().RemoveObjectVersion(ctx, key, version)
}

func (h *CommonPersistenceHandler[U]) PutObjectStreamVersion(ctx context.Context, key string, reader io.Reader, size int64, options PutObjectOptions) (*MinioObjectInfo, error) {
	return h.objectStorage().PutObjectStreamVersion(ctx, key, reader, size, options)
}
