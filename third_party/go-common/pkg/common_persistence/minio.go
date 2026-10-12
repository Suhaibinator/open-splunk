package common_persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ErrPublicStorageDisabled is returned by public-bucket operations when no
// public bucket is configured (MinioPublicBucket is empty).
var ErrPublicStorageDisabled = errors.New("public object storage is not configured")

// TempObjectPrefix and VerifiedObjectPrefix are the private-bucket key prefixes
// owned by the attachment lifecycle: unvetted uploads and content-addressed
// vetted objects that several attachment rows may share.
const (
	TempObjectPrefix     = "temp/"
	VerifiedObjectPrefix = "verified/"
)

// ErrReservedObjectName is returned by PutObjectStream for keys under an
// attachment-owned prefix.
var ErrReservedObjectName = errors.New("object name is under an attachment-owned prefix")

// ErrObjectStreamTooLarge is returned by an unknown-length PutObjectStream whose
// stream outlasts object_storage_stream_part_size x maxMultipartParts bytes.
var ErrObjectStreamTooLarge = errors.New("object stream exceeds the multipart part limit")

// maxMultipartParts is the S3 limit on parts per multipart upload.
const maxMultipartParts = 10000

// ErrObjectPreconditionFailed is returned when an ObjectPrecondition no longer
// holds: the object was overwritten (or removed and recreated) after the
// caller observed the pinned ETag. Callers must treat the previously observed
// bytes as untrusted and must not promote them.
var ErrObjectPreconditionFailed = errors.New("object precondition failed: content changed after it was observed")

// objectPreconditionGetOptions translates a precondition into request headers
// for GET/HEAD-style calls. minio-go persists these options across the range
// re-requests a seekable object issues, so every read stays conditional.
func objectPreconditionGetOptions(precondition ObjectPrecondition) (minio.GetObjectOptions, error) {
	opts := minio.GetObjectOptions{}
	opts.VersionID = precondition.VersionID
	if precondition.MatchETag != "" {
		if err := opts.SetMatchETag(precondition.MatchETag); err != nil {
			return opts, err
		}
	}
	return opts, nil
}

// mapObjectPreconditionError wraps a MinIO 412 PreconditionFailed response in
// ErrObjectPreconditionFailed so callers can errors.Is on it; every other
// error is returned unchanged.
func mapObjectPreconditionError(err error) error {
	if err == nil {
		return nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.Code == "PreconditionFailed" || resp.StatusCode == 412 {
		return fmt.Errorf("%w: %w", ErrObjectPreconditionFailed, err)
	}
	return err
}

// ObjectReadErrorReporter is implemented by the reader GetObjectReader returns
// for a pinned precondition. Streaming consumers such as attachment validators
// report only pass/fail and swallow the underlying read error, so callers that
// need to distinguish "the object changed underneath us" from an ordinary
// validation failure ask the reader for the last Read/Seek error instead.
type ObjectReadErrorReporter interface {
	// LastReadError returns the most recent non-EOF error produced by Read or
	// Seek, already mapped (ErrObjectPreconditionFailed for a 412), or nil.
	LastReadError() error
}

// preconditionedObjectReader surfaces a precondition failure raised by a
// later range re-request as ErrObjectPreconditionFailed from Read/Seek and
// remembers it for ObjectReadErrorReporter.
type preconditionedObjectReader struct {
	*minio.Object
	lastErr error
}

func (r *preconditionedObjectReader) Read(p []byte) (int, error) {
	n, err := r.Object.Read(p)
	if err != nil && err != io.EOF {
		err = mapObjectPreconditionError(err)
		r.lastErr = err
	}
	return n, err
}

func (r *preconditionedObjectReader) Seek(offset int64, whence int) (int64, error) {
	pos, err := r.Object.Seek(offset, whence)
	if err != nil {
		err = mapObjectPreconditionError(err)
		r.lastErr = err
	}
	return pos, err
}

func (r *preconditionedObjectReader) LastReadError() error {
	return r.lastErr
}

// GetPresignedURL executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetPresignedURL(ctx context.Context, objectName string, expiry time.Duration) (string, error) {

	// Generate the presigned URL
	presignedURL, err := h.minioClient.PresignedGetObject(ctx, h.minioBucket, objectName, expiry, nil)
	if err != nil {
		return "", appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_presigned_url"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, expiry))
	}

	return presignedURL.String(), nil
}

// GeneratePresignedPostURL executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GeneratePresignedPostURL(ctx context.Context, objectName string, expiresAt time.Time, minSize, maxSize int64, contentType string) (url string, formData map[string]string, err error) {
	// Create a new post policy
	policy := minio.NewPostPolicy()

	// Set bucket and key
	policy.SetBucket(h.minioBucket)
	policy.SetKey(objectName)

	// Set content length range
	if minSize > 0 || maxSize > 0 {
		policy.SetContentLengthRange(minSize, maxSize)
	}

	// Set content type if provided
	if contentType != "" {
		policy.SetContentType(contentType)
	}

	// Set expiry
	policy.SetExpires(expiresAt.UTC())

	// Generate the presigned POST URL
	presignedURL, formData, err := h.minioClient.PresignedPostPolicy(ctx, policy)
	if err != nil {
		return "", nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "generate_presigned_post_url"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.Int64(appLogger.KeyMinSize, minSize),
			zap.Int64(appLogger.KeyMaxSize, maxSize),
			zap.String(appLogger.KeyContentType, contentType))
	}

	return presignedURL.String(), formData, nil
}

// GetObjectBytes executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetObjectBytes(ctx context.Context, objectName string) ([]byte, error) {

	// Get the object
	object, err := h.minioClient.GetObject(ctx, h.minioBucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_bytes"),
			zap.String(logkeys.Stage, "open"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}
	defer object.Close()

	// Read the object into a byte slice
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, object)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_bytes"),
			zap.String(logkeys.Stage, "read"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	return buf.Bytes(), nil
}

// GetObjectReader executes its operation in the persistence flow between service handlers and the database layer.
// When precondition pins an ETag, the first request is issued eagerly so a
// mismatch fails here rather than from the caller's first Read, and later
// range re-requests (after Seek) carry the same If-Match header.
func (h *CommonPersistenceHandler[U]) GetObjectReader(ctx context.Context, objectName string, precondition ObjectPrecondition) (io.ReadCloser, error) {
	opts, err := objectPreconditionGetOptions(precondition)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_reader"),
			zap.String(logkeys.Stage, "build_precondition"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.String(appLogger.KeyETag, precondition.MatchETag))
	}

	// Get the object
	object, err := h.minioClient.GetObject(ctx, h.minioBucket, objectName, opts)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_reader"),
			zap.String(logkeys.Stage, "open"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	if precondition.MatchETag == "" {
		return object, nil
	}

	// minio-go opens the object lazily; force the request now so a stale ETag
	// is reported at open time.
	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		err = mapObjectPreconditionError(err)
		if errors.Is(err, ErrObjectPreconditionFailed) {
			return nil, appLogger.WithErrorLevel(err, zapcore.DebugLevel,
				zap.String(logkeys.Operation, "get_object_reader"),
				zap.String(logkeys.Stage, "verify_precondition"),
				zap.String(logkeys.Bucket, h.minioBucket),
				zap.String(appLogger.KeyObjectName, objectName),
				zap.String(appLogger.KeyETag, precondition.MatchETag))
		}
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_reader"),
			zap.String(logkeys.Stage, "verify_precondition"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	return &preconditionedObjectReader{Object: object}, nil
}

// PutObjectBytes executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) PutObjectBytes(ctx context.Context, objectName string, data []byte, contentType string) error {
	return h.putObject(ctx, "put_object_bytes", objectName, bytes.NewReader(data), int64(len(data)), PutObjectOptions{ContentType: contentType}, 0)
}

// PutObjectStream streams reader into the private bucket; see the interface for the contract.
func (h *CommonPersistenceHandler[U]) PutObjectStream(ctx context.Context, objectName string, reader io.Reader, size int64, opts PutObjectOptions) error {
	// Leading slashes are trimmed for the check because a path-cleaning server may drop them.
	if key := strings.TrimLeft(objectName, "/"); strings.HasPrefix(key, TempObjectPrefix) || strings.HasPrefix(key, VerifiedObjectPrefix) {
		return fmt.Errorf("put object stream %q: %w", objectName, ErrReservedObjectName)
	}
	if size < -1 {
		return fmt.Errorf("put object stream %q: size must be -1 (unknown) or non-negative, got %d", objectName, size)
	}
	var partSize int64
	if size < 0 {
		// Without a part size minio-go assumes a 5 TiB object and buffers ~537 MiB per part.
		partSize = h.runtime().ObjectStorageStreamPartSize()
		reader = &cappedReader{r: reader, remaining: partSize * maxMultipartParts}
	} else {
		reader = boundedReader(reader, size)
	}
	return h.putObject(ctx, "put_object_stream", objectName, reader, size, opts, partSize)
}

// boundedReader limits r to size bytes from its current position. minio-go uploads a
// ReaderAt from offset zero, and fills a whole part buffer from a plain reader, which
// blocks on a source that stays open after its last byte.
func boundedReader(r io.Reader, size int64) io.Reader {
	if ra, ok := r.(io.ReaderAt); ok {
		if seeker, ok := r.(io.Seeker); ok {
			if offset, err := seeker.Seek(0, io.SeekCurrent); err == nil {
				return io.NewSectionReader(ra, offset, size)
			}
		}
	}
	return io.LimitReader(r, size)
}

// cappedReader fails an unknown-length stream that outlasts the multipart part limit;
// minio-go would otherwise complete the upload with the first remaining bytes. Overflow
// is reported with zero bytes because minio-go drops an error returned on the read
// that fills its part buffer.
type cappedReader struct {
	r         io.Reader
	remaining int64
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.remaining <= 0 {
		return 0, io.EOF
	}
	n, err := c.r.Read(p[:min(int64(len(p)), c.remaining)])
	c.remaining -= int64(n)
	if c.remaining > 0 || err != nil {
		return n, err
	}
	var probe [1]byte
	if extra, probeErr := io.ReadFull(c.r, probe[:]); extra > 0 {
		return 0, ErrObjectStreamTooLarge
	} else if probeErr != io.EOF {
		return 0, probeErr
	}
	return n, io.EOF
}

func (h *CommonPersistenceHandler[U]) putObject(ctx context.Context, operation, objectName string, reader io.Reader, size int64, opts PutObjectOptions, partSize int64) error {
	_, err := h.putObjectResult(ctx, operation, objectName, reader, size, opts, partSize)
	return err
}

func (h *CommonPersistenceHandler[U]) putObjectResult(ctx context.Context, operation, objectName string, reader io.Reader, size int64, opts PutObjectOptions, partSize int64) (*MinioObjectInfo, error) {
	var metadata map[string]string
	if len(opts.UserMetadata) > 0 {
		metadata = make(map[string]string, len(opts.UserMetadata))
	}
	for name, value := range opts.UserMetadata {
		metadata[name] = value
	}
	minioOpts := minio.PutObjectOptions{
		UserMetadata:       metadata,
		ContentType:        opts.ContentType,
		ContentEncoding:    opts.ContentEncoding,
		ContentDisposition: opts.ContentDisposition,
		CacheControl:       opts.CacheControl,
		PartSize:           uint64(partSize),
	}
	if opts.NoOverwrite {
		minioOpts.SetMatchETagExcept("*")
	}
	info, err := h.minioClient.PutObject(ctx, h.minioBucket, objectName, reader, size, minioOpts)
	if err != nil {
		if opts.NoOverwrite && minio.ToErrorResponse(err).Code == "PreconditionFailed" {
			return nil, ErrPrivateObjectAlreadyExists
		}
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, operation),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.Int64(appLogger.KeySize, size),
			zap.String(appLogger.KeyContentType, opts.ContentType))
	}
	return &MinioObjectInfo{Size: info.Size, ETag: info.ETag, VersionID: info.VersionID, UserMetadata: metadata, ContentType: opts.ContentType, CacheControl: opts.CacheControl, ContentDisposition: opts.ContentDisposition, ContentEncoding: opts.ContentEncoding}, nil
}

// CopyObject executes its operation in the persistence flow between service handlers and the database layer.
// source pins the copy to the observed version of the source object via
// x-amz-copy-source-if-match; the server rejects the copy atomically when the
// object has changed, so a stale observation can never be promoted.
func (h *CommonPersistenceHandler[U]) CopyObject(ctx context.Context, sourceObjectName, destinationObjectName string, source ObjectPrecondition) error {

	// Create source info
	srcOpts := minio.CopySrcOptions{
		Bucket:    h.minioBucket,
		Object:    sourceObjectName,
		MatchETag: source.MatchETag,
	}

	// Create destination info
	dstOpts := minio.CopyDestOptions{
		Bucket: h.minioBucket,
		Object: destinationObjectName,
	}

	// Copy the object
	_, err := h.minioClient.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		err = mapObjectPreconditionError(err)
		if errors.Is(err, ErrObjectPreconditionFailed) {
			return appLogger.WithErrorLevel(err, zapcore.DebugLevel,
				zap.String(logkeys.Operation, "copy_object"),
				zap.String(logkeys.Bucket, h.minioBucket),
				zap.String(appLogger.KeySourceObjectName, sourceObjectName),
				zap.String(appLogger.KeyDestinationObjectName, destinationObjectName),
				zap.String(appLogger.KeyETag, source.MatchETag))
		}
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "copy_object"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeySourceObjectName, sourceObjectName),
			zap.String(appLogger.KeyDestinationObjectName, destinationObjectName))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Copied object",
			zap.String(logkeys.Operation, "copy_object"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeySourceObjectName, sourceObjectName),
			zap.String(appLogger.KeyDestinationObjectName, destinationObjectName))
	}
	return nil
}

// CopyObjectToPublicBucket performs a server-side copy of an object from the
// private bucket into the public bucket. The object keeps the same uniform
// verified/[salted-hash] shape in both buckets, so the source and destination
// names are normally identical.
// Parameters:
//   - ctx: context.Context input used by CopyObjectToPublicBucket.
//   - sourceObjectName: string naming the object in the private bucket.
//   - destinationObjectName: string naming the object to write in the public bucket.
//   - source: ObjectPrecondition pinning the private source to its observed version.
func (h *CommonPersistenceHandler[U]) CopyObjectToPublicBucket(ctx context.Context, sourceObjectName, destinationObjectName string, source ObjectPrecondition) error {
	if !h.PublicStorageEnabled() {
		return ErrPublicStorageDisabled
	}

	// Create source info
	srcOpts := minio.CopySrcOptions{
		Bucket:    h.minioBucket,
		Object:    sourceObjectName,
		MatchETag: source.MatchETag,
	}

	// Create destination info
	dstOpts := minio.CopyDestOptions{
		Bucket: h.minioPublicBucket,
		Object: destinationObjectName,
	}

	// Copy the object across buckets
	_, err := h.minioClient.CopyObject(ctx, dstOpts, srcOpts)
	if err != nil {
		err = mapObjectPreconditionError(err)
		if errors.Is(err, ErrObjectPreconditionFailed) {
			return appLogger.WithErrorLevel(err, zapcore.DebugLevel,
				zap.String(logkeys.Operation, "copy_object_to_public_bucket"),
				zap.String(appLogger.KeySourceObjectName, sourceObjectName),
				zap.String(appLogger.KeyDestinationObjectName, destinationObjectName),
				zap.String(logkeys.Bucket, h.minioPublicBucket),
				zap.String(appLogger.KeyETag, source.MatchETag))
		}
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "copy_object_to_public_bucket"),
			zap.String(appLogger.KeySourceObjectName, sourceObjectName),
			zap.String(appLogger.KeyDestinationObjectName, destinationObjectName),
			zap.String(logkeys.Bucket, h.minioPublicBucket))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Copied object to public bucket",
			zap.String(logkeys.Operation, "copy_object_to_public_bucket"),
			zap.String(appLogger.KeySourceObjectName, sourceObjectName),
			zap.String(appLogger.KeyDestinationObjectName, destinationObjectName),
			zap.String(logkeys.Bucket, h.minioPublicBucket))
	}
	return nil
}

// RemoveObject executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) RemoveObject(ctx context.Context, objectName string) error {
	return h.removeObjectFromBucket(ctx, h.minioBucket, objectName)
}

func (h *CommonPersistenceHandler[U]) removeObjectFromBucket(ctx context.Context, bucket, objectName string) error {

	// Delete the object
	err := h.minioClient.RemoveObject(ctx, bucket, objectName, minio.RemoveObjectOptions{})
	if err != nil {
		return appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "remove_object"),
			zap.String(logkeys.Bucket, bucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Removed object",
			zap.String(logkeys.Operation, "remove_object"),
			zap.String(logkeys.Bucket, bucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}
	return nil
}

// ObjectExists executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) ObjectExists(ctx context.Context, objectName string) (bool, error) {

	// Use StatObject to check if the object exists
	_, err := h.minioClient.StatObject(ctx, h.minioBucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		// Check if the error is due to the object not existing
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		// Other errors
		return false, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "object_exists"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	return true, nil
}

// PublicStorageEnabled reports whether a public bucket is configured. When it is
// false, every public-bucket operation fails with ErrPublicStorageDisabled and
// callers must keep objects private.
func (h *CommonPersistenceHandler[U]) PublicStorageEnabled() bool {
	return h.minioPublicBucket != ""
}

// ObjectExistsInPublicBucket checks whether an object exists in the public bucket.
// It mirrors ObjectExists but never inspects the private bucket, so the two
// buckets stay independent namespaces for deduplication purposes.
func (h *CommonPersistenceHandler[U]) ObjectExistsInPublicBucket(ctx context.Context, objectName string) (bool, error) {
	if !h.PublicStorageEnabled() {
		return false, ErrPublicStorageDisabled
	}

	// Use StatObject to check if the object exists
	_, err := h.minioClient.StatObject(ctx, h.minioPublicBucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		// Check if the error is due to the object not existing
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		// Other errors
		return false, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "object_exists_in_public_bucket"),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.String(logkeys.Bucket, h.minioPublicBucket))
	}

	return true, nil
}

// GetObjectInfo executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetObjectInfo(ctx context.Context, objectName string) (*MinioObjectInfo, error) {
	// Get object statistics
	stat, err := h.minioClient.StatObject(ctx, h.minioBucket, objectName, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil, ErrPrivateObjectNotFound
		}
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "get_object_info"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}

	// Convert MinIO ObjectInfo to our MinioObjectInfo struct
	var metadata map[string]string
	if len(stat.UserMetadata) > 0 {
		metadata = make(map[string]string, len(stat.UserMetadata))
	}
	for name, value := range stat.UserMetadata {
		metadata[name] = value
	}
	return &MinioObjectInfo{
		VersionID:          stat.VersionID,
		UserMetadata:       metadata,
		Size:               stat.Size,
		ContentType:        stat.ContentType,
		LastModified:       stat.LastModified,
		ETag:               stat.ETag,
		ContentEncoding:    stat.Metadata.Get("Content-Encoding"),
		ContentDisposition: stat.Metadata.Get("Content-Disposition"),
		CacheControl:       stat.Metadata.Get("Cache-Control"),
	}, nil
}

// DualHashes defines data used in the persistence flow between service handlers and the database layer.
// It is consumed by handlers and helpers that participate in this backend call path.
type DualHashes struct {
	// Unsalted is the plain SHA-256 hash of the file content
	Unsalted string
	// Salted is the SHA-256 hash of (salt + file content)
	Salted string
}

// CalculateDualHashesStream executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CalculateDualHashesStream(ctx context.Context, objectName string, precondition ObjectPrecondition) (*DualHashes, error) {
	// Get object reader
	reader, err := h.GetObjectReader(ctx, objectName, precondition)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
			zap.String(logkeys.Stage, "open"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName))
	}
	defer reader.Close()

	// Create two hash writers
	unsaltedHash := sha256.New()
	saltedHash := sha256.New()

	// Write salt to the salted hash first
	if err := writeHashInvariant(saltedHash, h.objectHashSalt.Value(), "sha256_hash_write_succeeds",
		zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
		zap.String(logkeys.Stage, "initialize_salted_hash"),
		zap.String(appLogger.KeyObjectName, objectName)); err != nil {
		return nil, err
	}

	// Use io.MultiWriter to write to both hashes simultaneously
	multiWriter := io.MultiWriter(unsaltedHash, saltedHash)

	// Stream object content to both hashes in chunks
	chunkSize := h.runtime().StreamingHashChunkSize()
	if chunkSize <= 0 {
		return nil, fmt.Errorf("streaming hash chunk size must be positive")
	}
	buffer := make([]byte, chunkSize)
	totalBytes := int64(0)

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			if writeErr := writeHashInvariant(multiWriter, buffer[:n], "sha256_multiwriter_write_succeeds",
				zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
				zap.String(logkeys.Stage, "hash_chunk"),
				zap.String(appLogger.KeyObjectName, objectName)); writeErr != nil {
				return nil, writeErr
			}
			totalBytes += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, ErrObjectPreconditionFailed) {
				return nil, appLogger.WithErrorLevel(err, zapcore.DebugLevel,
					zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
					zap.String(logkeys.Stage, "read"),
					zap.String(logkeys.Bucket, h.minioBucket),
					zap.String(appLogger.KeyObjectName, objectName),
					zap.Int64(appLogger.KeyTotalBytes, totalBytes))
			}
			return nil, appLogger.WithErrorFields(err,
				zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
				zap.String(logkeys.Stage, "read"),
				zap.String(logkeys.Bucket, h.minioBucket),
				zap.String(appLogger.KeyObjectName, objectName),
				zap.Int64(appLogger.KeyTotalBytes, totalBytes))
		}
	}

	// Calculate final hashes
	result := &DualHashes{
		Unsalted: hex.EncodeToString(unsaltedHash.Sum(nil)),
		Salted:   hex.EncodeToString(saltedHash.Sum(nil)),
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Calculated dual hashes for object",
			zap.String(logkeys.Operation, "calculate_dual_hashes_stream"),
			zap.String(logkeys.Bucket, h.minioBucket),
			zap.String(appLogger.KeyObjectName, objectName),
			zap.Int64(appLogger.KeyTotalBytes, totalBytes),
			zap.String(appLogger.KeyUnsaltedHash, result.Unsalted),
			zap.String(appLogger.KeySaltedHash, result.Salted))
	}

	return result, nil
}

func writeHashInvariant(writer io.Writer, data []byte, invariant string, fields ...zap.Field) error {
	written, err := writer.Write(data)
	if err == nil && written == len(data) {
		return nil
	}
	fields = append(fields,
		zap.Int(logkeys.Expected, len(data)),
		zap.Int(logkeys.Actual, written))
	return appLogger.InvariantError(invariant, err, fields...)
}
