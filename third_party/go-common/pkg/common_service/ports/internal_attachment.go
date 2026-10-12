package ports

import "context"

const (
	// InternalAttachmentObjectTypeProviderState stores opaque provider continuation bytes.
	InternalAttachmentObjectTypeProviderState = "agent-provider-state"
	// InternalAttachmentObjectTypeToolMedia stores large or binary tool-result parts.
	InternalAttachmentObjectTypeToolMedia = "agent-tool-media"
	// MaxInternalAttachmentHydrationBytes is the maximum aggregate private blob payload that one
	// tool result or reconstructed context may retain. It matches the provider's combined file
	// request ceiling and also bounds image/data-URI/provider-state hydration.
	MaxInternalAttachmentHydrationBytes uint64 = 50 * 1024 * 1024
	// ToolMediaSizeMetadataKey is the provider-adapter metadata key carrying the trusted persisted
	// byte size of an attachment-backed media part.
	ToolMediaSizeMetadataKey = "size_bytes"
)

// InternalAttachmentInput describes trusted server-produced bytes that must be externalized
// before a reference is written into the transcript. ObjectType determines the registered
// validation policy; MaxBytes may impose a tighter caller-specific cap.
type InternalAttachmentInput struct {
	UserID      uint64
	ObjectType  string
	EntityID    string
	Filename    string
	ContentType string
	Data        []byte
	MaxBytes    int64
}

// InternalAttachmentRef is the integrity metadata returned for a vetted server-only attachment.
type InternalAttachmentRef struct {
	AttachmentID  uint64
	OwnerUserID   uint64
	ObjectType    string
	EntityID      string
	Filename      string
	ContentType   string
	ContentSHA256 string
	SizeBytes     uint64
	// Created is true only when this StoreInternal call created the row. Callers use it to avoid
	// deleting a pre-existing idempotency winner while rolling back a later transcript failure.
	Created bool
}

// ResolvedToolMedia is the provider-facing source for a vetted tool-media attachment together
// with its persisted byte size. Callers use SizeBytes to enforce an aggregate reconstruction
// budget even when Source is a short presigned URL.
type ResolvedToolMedia struct {
	Source    string
	SizeBytes uint64
}

// InternalAttachmentStore stores opaque provider state and large tool media without
// putting raw bytes in public message rows.
// ResolveInternal is privileged and must reject ordinary/user-visible attachments.
type InternalAttachmentStore interface {
	// ValidateInternal applies the same input policy as StoreInternal without writing an object
	// or row. Multipart callers validate every inline part before their first store.
	ValidateInternal(ctx context.Context, in InternalAttachmentInput) error
	StoreInternal(ctx context.Context, in InternalAttachmentInput) (*InternalAttachmentRef, error)
	// ResolveInternal rejects an attachment larger than maxBytes from trusted persisted
	// metadata before loading its object body and enforces maxBytes while reading.
	ResolveInternal(ctx context.Context, attachmentID uint64, maxBytes uint64) ([]byte, *InternalAttachmentRef, error)
	DeleteInternal(ctx context.Context, attachmentID uint64) error
}

// InternalAttachmentURLStore is the optional URL-capable extension. A resolver may use it when
// the downstream provider can fetch the private object directly; callers must still validate the
// returned attachment metadata before exposing the URL to a provider.
type InternalAttachmentURLStore interface {
	InternalAttachmentStore
	ResolveInternalURL(ctx context.Context, attachmentID uint64, maxBytes uint64) (string, *InternalAttachmentRef, error)
}
