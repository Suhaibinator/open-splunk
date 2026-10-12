package searchartifacts

import (
	"context"
	"io"
)

// BlobIdentity pins all reads to an immutable object identity. ETag is a
// conditional identity, not a content checksum; manifests separately use SHA256.
type BlobIdentity struct {
	Key, ETag, Version string
	Bytes              int64
}

// BlobReference is private backup/recovery metadata for a committed object.
type BlobReference struct {
	Identity BlobIdentity
	SHA256   []byte
}

// BlobStore uses the private object bucket. Keys and credentials never enter
// public responses. Open must condition all requests/ranges on identity.
type BlobStore interface {
	Put(context.Context, string, io.Reader, int64) (BlobIdentity, error)
	Open(context.Context, BlobIdentity) (io.ReadCloser, error)
	Delete(context.Context, BlobIdentity) error
}

// BlobFunctions adapts existing GoCommon low-level private object operations.
// It intentionally contains no attachment registration/publication lifecycle.
type BlobFunctions struct {
	PutFunc    func(context.Context, string, io.Reader, int64) (BlobIdentity, error)
	OpenFunc   func(context.Context, BlobIdentity) (io.ReadCloser, error)
	DeleteFunc func(context.Context, BlobIdentity) error
}

func (store BlobFunctions) Put(ctx context.Context, key string, reader io.Reader, size int64) (BlobIdentity, error) {
	return store.PutFunc(ctx, key, reader, size)
}
func (store BlobFunctions) Open(ctx context.Context, identity BlobIdentity) (io.ReadCloser, error) {
	return store.OpenFunc(ctx, identity)
}
func (store BlobFunctions) Delete(ctx context.Context, identity BlobIdentity) error {
	return store.DeleteFunc(ctx, identity)
}
