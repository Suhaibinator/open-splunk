// Package objectstore adapts GoCommon's low-level private object storage.
// It deliberately does not register or publish GoCommon attachments.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/common_persistence"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

type PrivateObjects interface {
	PutObjectStreamVersion(context.Context, string, io.Reader, int64, common_persistence.PutObjectOptions) (*common_persistence.MinioObjectInfo, error)
	GetObjectReader(context.Context, string, common_persistence.ObjectPrecondition) (io.ReadCloser, error)
	GetObjectInfo(context.Context, string) (*common_persistence.MinioObjectInfo, error)
	RemoveObjectVersion(context.Context, string, string) error
	CheckPrivateObjectVersioning(context.Context) error
	EnsurePrivateUploadLifecycle(context.Context, string) error
	AbortPrivateObjectUpload(context.Context, string) error
}

type Store struct {
	objects             PrivateObjects
	lifecycleConfigured bool
}

func New(objects PrivateObjects) (*Store, error) { return NewContext(context.Background(), objects) }
func NewContext(ctx context.Context, objects PrivateObjects) (*Store, error) {
	if nilcheck.IsNil(objects) {
		return nil, errors.New("private object storage is required")
	}
	if err := objects.CheckPrivateObjectVersioning(ctx); err != nil {
		return nil, fmt.Errorf("verify private artifact bucket versioning: %w", err)
	}
	lifecycleErr := objects.EnsurePrivateUploadLifecycle(ctx, "open-splunk/")
	if lifecycleErr != nil && !errors.Is(lifecycleErr, common_persistence.ErrPrivateLifecycleUnsupported) {
		return nil, fmt.Errorf("configure incomplete-upload lifecycle: %w", lifecycleErr)
	}
	return &Store{objects: objects, lifecycleConfigured: lifecycleErr == nil}, nil
}

func validKey(key string) bool {
	return (strings.HasPrefix(key, "open-splunk/captures/") || strings.HasPrefix(key, "open-splunk/exports/")) && len(key) <= 1024 && !strings.Contains(key, "..") && !strings.ContainsAny(key, "\\\x00\r\n")
}

func (store *Store) Put(ctx context.Context, key string, reader io.Reader, size int64) (searchartifacts.BlobIdentity, error) {
	if !validKey(key) || reader == nil || size < -1 {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrInvalid
	}
	info, err := store.objects.PutObjectStreamVersion(ctx, key, reader, size, common_persistence.PutObjectOptions{ContentType: "application/octet-stream", CacheControl: "private, no-store", NoOverwrite: true, UserMetadata: map[string]string{"opensplunk-key": key}})
	if errors.Is(err, common_persistence.ErrPrivateObjectAlreadyExists) {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrConflict
	}
	if err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	if info == nil || info.ETag == "" || info.VersionID == "" || info.VersionID == "null" || info.Size < 0 || (size >= 0 && info.Size != size) {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrCorrupt
	}
	return searchartifacts.BlobIdentity{Key: key, ETag: info.ETag, Version: info.VersionID, Bytes: info.Size}, nil
}

func (store *Store) Open(ctx context.Context, identity searchartifacts.BlobIdentity) (io.ReadCloser, error) {
	if !validKey(identity.Key) || identity.ETag == "" || identity.Bytes < 0 || identity.Version == "" || identity.Version == "null" {
		return nil, searchartifacts.ErrInvalid
	}
	return store.objects.GetObjectReader(ctx, identity.Key, common_persistence.ObjectPrecondition{MatchETag: identity.ETag, VersionID: identity.Version})
}

func (store *Store) Delete(ctx context.Context, identity searchartifacts.BlobIdentity) error {
	if !validKey(identity.Key) {
		return searchartifacts.ErrInvalid
	}
	if identity.Version == "" {
		if err := store.objects.AbortPrivateObjectUpload(ctx, identity.Key); err != nil {
			return err
		}
		// A durable pending key was recorded before its create-only PUT. Crash
		// recovery resolves that owned key once, then deletes the exact version.
		info, err := store.objects.GetObjectInfo(ctx, identity.Key)
		if errors.Is(err, common_persistence.ErrPrivateObjectNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if info == nil {
			return nil
		}
		owned := false
		for name, value := range info.UserMetadata {
			if strings.EqualFold(name, "opensplunk-key") && value == identity.Key {
				owned = true
				break
			}
		}
		if !owned {
			return searchartifacts.ErrConflict
		}
		identity.Version = info.VersionID
	}
	return store.objects.RemoveObjectVersion(ctx, identity.Key, identity.Version)
}

func (store *Store) Current(ctx context.Context, key string) (searchartifacts.BlobIdentity, error) {
	if !validKey(key) {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrInvalid
	}
	info, err := store.objects.GetObjectInfo(ctx, key)
	if errors.Is(err, common_persistence.ErrPrivateObjectNotFound) {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrNotFound
	}
	if err != nil {
		return searchartifacts.BlobIdentity{}, err
	}
	if info == nil || info.VersionID == "" || info.VersionID == "null" || info.ETag == "" || info.Size < 0 {
		return searchartifacts.BlobIdentity{}, searchartifacts.ErrCorrupt
	}
	return searchartifacts.BlobIdentity{Key: key, ETag: info.ETag, Version: info.VersionID, Bytes: info.Size}, nil
}

var _ searchartifacts.BlobStore = (*Store)(nil)

// PrivateUploadLifecycleConfigured reports an optional S3 backend capability.
// Exact pending-key multipart cleanup remains authoritative on MinIO.
func (store *Store) PrivateUploadLifecycleConfigured() bool { return store.lifecycleConfigured }
