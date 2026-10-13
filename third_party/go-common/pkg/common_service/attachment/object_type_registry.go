package attachment

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ErrPublicVisibilityUnavailable is returned when an object type declares
// ATTACHMENT_VISIBILITY_PUBLIC while the deployment has no public attachment
// storage. Public visibility requires MinioPublicBucket to be configured.
var ErrPublicVisibilityUnavailable = errors.New("public attachment visibility requires a configured public bucket (MinioPublicBucket)")

const (
	ObjectTypeProfilePicture         = common_persistence_models.ObjectTypeProfilePicture
	ObjectTypeAttachment             = "attachment"
	ObjectTypeDocument               = "document"
	ObjectTypeChatAttachment         = common_persistence_models.ObjectTypeChatAttachment
	ObjectTypeArchivedChatAttachment = "archived-chat-attachment"
	// ObjectTypeChatQueuedInputAttachment is a queued agent input's upload until delivery moves it
	// to the message the input becomes (ObjectTypeChatAttachment).
	ObjectTypeChatQueuedInputAttachment = common_persistence_models.ObjectTypeChatQueuedInputAttachment
	ObjectTypeAgentProviderState        = "agent-provider-state"
	ObjectTypeAgentToolMedia            = "agent-tool-media"
)

// ObjectTypeConfig defines the configuration for a specific object type.
// This allows importing applications to register custom object types with
// their own validation rules and constraints.
type ObjectTypeConfig struct {
	// Name is the unique identifier for this object type (e.g., "profile-picture", "document")
	Name string

	// Visibility selects the storage the vetted objects of this object type live in.
	// The zero value (ATTACHMENT_VISIBILITY_UNSPECIFIED) keeps attachments private,
	// reachable only through backend-issued presigned URLs, exactly like an explicit
	// ATTACHMENT_VISIBILITY_PRIVATE. ATTACHMENT_VISIBILITY_PUBLIC stores them in the
	// anonymously readable bucket and therefore requires the deployment to configure
	// MinioPublicBucket; registering a public object type without it fails.
	// Visibility is a server-side decision: it is never accepted from clients.
	Visibility proto_models.AttachmentVisibility

	// MaxSize is the maximum allowed file size in bytes for this object type
	MaxSize int64

	// AllowedMimeTypes is the list of allowed MIME types for this object type
	// If empty, all MIME types are allowed
	AllowedMimeTypes []string

	// ForbiddenMimeTypes is the list of explicitly forbidden MIME types for this object type
	// This provides additional security by blocking specific dangerous types
	ForbiddenMimeTypes []string

	// StreamingValidators is a list of streaming validators to apply to this object type
	// These validators provide memory-efficient validation without loading entire files
	StreamingValidators []StreamingAttachmentValidator

	// PostProcessCallback is an optional function called after successful attachment processing
	// This can be used to trigger side effects like setting a profile picture
	PostProcessCallback func(ctx context.Context, attachment *common_persistence_models.Attachment) error

	// Entity configuration

	// ImpliedEntityType specifies which entity type this object type belongs to
	// For example: "profile-picture" implies "user" entity, "post-attachment" implies "post" entity
	// This creates a single source of truth - the object type determines the entity type
	ImpliedEntityType string

	// RequiresEntity if true, attachments of this type MUST have an entity association
	// This is useful for object types that only make sense in the context of an entity
	RequiresEntity bool

	// SingleEntityOnly if true, attachments of this type can only be linked to one entity at a time
	// This prevents sharing the same attachment across multiple entities
	SingleEntityOnly bool

	// EntityBatchCallback is an optional function called after successful batch attachment processing
	// when multiple attachments are linked to the same entity. This provides efficient processing
	// for finalization operations by grouping attachments by entity and calling the callback once per entity.
	EntityBatchCallback func(ctx context.Context, attachments []*common_persistence_models.Attachment, entityID string) error

	// ValidateEntity is an optional function to validate entity ID format or existence.
	// This can enforce custom business rules about entity ID formats (numeric, UUID, etc.).
	// SECURITY: this hook is not a caller-authorization boundary unless the consuming service
	// explicitly implements authorization here and ensures the hook is called before creating,
	// linking, or synchronizing entity attachments.
	ValidateEntity func(entityID string) error

	// Description provides a human-readable description of this object type
	Description string
}

// ObjectTypeRegistry manages registered object types and their configurations.
// It provides a thread-safe way to register and retrieve object type configurations.
type ObjectTypeRegistry struct {
	mu          sync.RWMutex
	objectTypes map[string]*ObjectTypeConfig
}

// NewObjectTypeRegistry creates a new object type registry with the built-in attachment object types registered for normal backend flows.
//
// Returns:
//   - *ObjectTypeRegistry: Initialized registry populated with default object type definitions.
func NewObjectTypeRegistry() *ObjectTypeRegistry {
	registry := &ObjectTypeRegistry{
		objectTypes: make(map[string]*ObjectTypeConfig),
	}

	// Register default object types
	registry.registerDefaults()

	return registry
}

// registerDefaults registers the built-in object types.
func (r *ObjectTypeRegistry) registerDefaults() {
	// Get default security constraints
	securityConstraints := DefaultSecurityConstraints()

	// Profile picture type - security-focused configuration
	profilePictureConfig := &ObjectTypeConfig{
		Name: ObjectTypeProfilePicture,
		// Avatars are rendered anonymously and cached by CDNs, so they are stored
		// in the public bucket instead of behind presigned URLs.
		Visibility:        proto_models.AttachmentVisibility_ATTACHMENT_VISIBILITY_PUBLIC,
		ImpliedEntityType: "user",           // Profile pictures always belong to users
		MaxSize:           10 * 1024 * 1024, // 10MB
		AllowedMimeTypes:  common_persistence_models.ProfilePictureAllowedMIMETypes(),
		// Block all dangerous file types that could contain scripts
		ForbiddenMimeTypes: securityConstraints.DangerousTypes,
		// Streaming validators for memory-efficient validation
		StreamingValidators: []StreamingAttachmentValidator{
			NewStreamingSizeValidator(10 * 1024 * 1024),           // 10MB
			NewStreamingImageDimensionValidator(4096, 4096, 1, 1), // Max 4K, min 1x1
			NewStreamingSecurityValidator(),
		},
		// Entity configuration for profile pictures
		RequiresEntity:   true, // Profile pictures must be linked to a user
		SingleEntityOnly: true, // A profile picture can only belong to one user
		Description:      "User profile pictures - secure image types only",
	}

	// Generic attachment type - permissive but secure
	attachmentConfig := &ObjectTypeConfig{
		Name:    ObjectTypeAttachment,
		MaxSize: 100 * 1024 * 1024, // 100MB
		// No AllowedMimeTypes = allow most types, but block dangerous ones
		ForbiddenMimeTypes: securityConstraints.DangerousTypes,
		// Streaming validators for memory-efficient validation
		StreamingValidators: []StreamingAttachmentValidator{
			NewStreamingSizeValidator(100 * 1024 * 1024), // 100MB
			NewStreamingSecurityValidator(),
		},
		Description: "Generic file attachments - most types allowed except scripts",
	}

	// Document type
	documentConfig := &ObjectTypeConfig{
		Name:    ObjectTypeDocument,
		MaxSize: 50 * 1024 * 1024, // 50MB
		AllowedMimeTypes: []string{
			"application/pdf",
			"application/msword",
			"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			"application/vnd.ms-excel",
			"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
			"text/plain",
			"text/csv",
		},
		// Streaming validators for memory-efficient validation
		StreamingValidators: []StreamingAttachmentValidator{
			NewStreamingSizeValidator(50 * 1024 * 1024), // 50MB
			NewStreamingSecurityValidator(),
		},
		Description: "Document files",
	}

	providerStateConfig := &ObjectTypeConfig{
		Name:             ObjectTypeAgentProviderState,
		MaxSize:          100 * 1024 * 1024,
		AllowedMimeTypes: []string{"application/json"},
		StreamingValidators: []StreamingAttachmentValidator{
			NewStreamingSizeValidator(100 * 1024 * 1024),
		},
		RequiresEntity: true,
		Description:    "Internal opaque model-provider continuation state",
	}

	toolMediaConfig := &ObjectTypeConfig{
		Name:               ObjectTypeAgentToolMedia,
		MaxSize:            100 * 1024 * 1024,
		ForbiddenMimeTypes: securityConstraints.DangerousTypes,
		StreamingValidators: []StreamingAttachmentValidator{
			NewStreamingSizeValidator(100 * 1024 * 1024),
			NewStreamingSecurityValidator(),
		},
		RequiresEntity: true,
		Description:    "Internal media returned by agent tools",
	}

	// Register default types
	_ = r.Register(profilePictureConfig)
	_ = r.Register(attachmentConfig)
	_ = r.Register(documentConfig)
	_ = r.Register(providerStateConfig)
	_ = r.Register(toolMediaConfig)
}

// Register adds a new object type configuration to the registry so upload and processing flows can resolve validation rules by object type.
//
// Parameters:
//   - config: Object type definition to register.
//
// Returns:
//   - error: Validation error or duplicate-registration error.
func (r *ObjectTypeRegistry) Register(config *ObjectTypeConfig) error {
	if config == nil {
		return fmt.Errorf("object type config cannot be nil")
	}

	if config.Name == "" {
		return fmt.Errorf("object type name cannot be empty")
	}

	if config.MaxSize <= 0 {
		return fmt.Errorf("object type max size must be positive")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.objectTypes[config.Name]; exists {
		return fmt.Errorf("object type '%s' already registered", config.Name)
	}

	r.objectTypes[config.Name] = config
	return nil
}

// ValidateObjectTypeVisibility rejects an object type that requests public visibility while the deployment has no public attachment storage.
// The registry itself holds no persistence reference, so registration entry points that do (the service handler) call this with the
// persistence layer's `PublicStorageEnabled` result before registering built-in or application object types.
//
// Parameters:
//   - config: Object type definition to check; a nil config or private visibility is always accepted.
//   - publicStorageEnabled: Whether the persistence layer has a public bucket configured.
//
// Returns:
//   - error: `ErrPublicVisibilityUnavailable` wrapped with the object type name, or `nil` when the configuration is satisfiable.
func ValidateObjectTypeVisibility(config *ObjectTypeConfig, publicStorageEnabled bool) error {
	if config == nil || !common_persistence_models.IsPublicAttachmentVisibility(config.Visibility) || publicStorageEnabled {
		return nil
	}
	return fmt.Errorf("object type '%s': %w", config.Name, ErrPublicVisibilityUnavailable)
}

// Get retrieves the configuration for a specific object type during upload, validation, and callback resolution flows.
//
// Parameters:
//   - objectType: Object type name to look up.
//
// Returns:
//   - *ObjectTypeConfig: Registered configuration, or `nil` when the object type is unknown.
func (r *ObjectTypeRegistry) Get(objectType string) *ObjectTypeConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.objectTypes[objectType]
}

// Note: MIME type and size validation are now handled by streaming validators
// in each object type's StreamingValidators field. This provides memory-efficient
// validation without loading entire files into memory.

// ListObjectTypes returns all registered object type names for diagnostics and administrative tooling.
//
// Returns:
//   - []string: Registered object type names.
func (r *ObjectTypeRegistry) ListObjectTypes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]string, 0, len(r.objectTypes))
	for name := range r.objectTypes {
		types = append(types, name)
	}

	return types
}

// Has checks whether an object type is registered before upload or processing work begins.
//
// Parameters:
//   - objectType: Object type name to test.
//
// Returns:
//   - bool: `true` when the object type exists in the registry.
func (r *ObjectTypeRegistry) Has(objectType string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.objectTypes[objectType]
	return exists
}

// GetImpliedEntityType returns the entity type implied by this object type.
func (r *ObjectTypeRegistry) GetImpliedEntityType(objectType string) (string, error) {
	config := r.Get(objectType)
	if config == nil {
		return "", fmt.Errorf("unknown object type: %s", objectType)
	}
	if config.ImpliedEntityType == "" {
		return "", fmt.Errorf("object type '%s' has no implied entity type", objectType)
	}
	return config.ImpliedEntityType, nil
}

// ValidateEntityID validates an entity identifier for the given object type before attachment linking proceeds.
//
// Parameters:
//   - objectType: Object type whose entity validator should be used.
//   - entityID: Entity identifier to validate.
//
// Returns:
//   - error: Unknown-object-type error or custom entity validation error.
func (r *ObjectTypeRegistry) ValidateEntityID(objectType string, entityID string) error {
	config := r.Get(objectType)
	if config == nil {
		return fmt.Errorf("unknown object type: %s", objectType)
	}
	if config.ValidateEntity != nil {
		return config.ValidateEntity(entityID)
	}
	return nil
}

// RequiresEntity checks if this object type requires an entity association.
//
// Parameters:
//   - objectType: Object type to inspect.
//
// Returns:
//   - bool: `true` when attachments of the object type must be linked to an entity.
func (r *ObjectTypeRegistry) RequiresEntity(objectType string) bool {
	config := r.Get(objectType)
	return config != nil && config.RequiresEntity
}
