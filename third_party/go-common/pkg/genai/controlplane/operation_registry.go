package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/inference"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ModelSelector identifies a default inference model by its stable (provider, provider_model_id)
// tuple rather than a database id. Registration resolves it to a local model row when seeding a
// default route.
type ModelSelector struct {
	ProviderID      string
	ProviderModelID string
}

// IsZero reports whether no default model was configured.
func (m ModelSelector) IsZero() bool {
	return m.ProviderID == "" && m.ProviderModelID == ""
}

// OperationConfig is the application-owned definition of one inference operation.
type OperationConfig struct {
	DisplayName            string
	SortOrder              int32
	Required               bool
	DefaultModel           ModelSelector
	DefaultRetryPolicy     RetryPolicy
	DefaultTemperature     *float64
	SearchableMetadataKeys []string
	OrgModelChoice         proto_models.InferenceOperationOrgModelChoice
	// SessionRetarget gates moving an existing session's pinned model; UNSPECIFIED means NEVER.
	SessionRetarget proto_models.InferenceSessionRetarget
}

// ModelRouteValidation is the immutable model-specific portion of a route presented to the
// application-supplied validator. ProviderModelID is the stable provider identifier stored in the
// model catalog. HasTemperature distinguishes an explicit zero from the provider default.
type ModelRouteValidation struct {
	API             proto_models.InferenceAPI
	ProviderID      string
	ProviderModelID string
	Temperature     float64
	HasTemperature  bool
}

// ModelRouteValidator validates provider- and model-specific route capabilities. Applications
// should back this with the same runtime/model capability source used to construct inference
// requests; provider-wide hard-coded ranges are insufficient for aggregating providers such as
// Bedrock and OpenRouter.
type ModelRouteValidator interface {
	ValidateModelRoute(ctx context.Context, route ModelRouteValidation) error
}

// ModelRouteValidatorFunc adapts a function to ModelRouteValidator.
type ModelRouteValidatorFunc func(ctx context.Context, route ModelRouteValidation) error

// ValidateModelRoute calls the wrapped function.
func (f ModelRouteValidatorFunc) ValidateModelRoute(ctx context.Context, route ModelRouteValidation) error {
	if f == nil {
		return errors.New("model route validator function is nil")
	}
	return f(ctx, route)
}

// RegistryOption configures immutable registry behavior.
type RegistryOption func(*Registry)

// WithModelRouteValidator installs the provider/model-aware validator used by seeding, readiness,
// request resolution, and admin route updates. A configured temperature fails closed when this
// option is absent.
func WithModelRouteValidator(validator ModelRouteValidator) RegistryOption {
	return func(registry *Registry) {
		if validator != nil {
			registry.modelRouteValidator = validator
		}
	}
}

// ProtoOperation is implemented by generated int32 protobuf enums.
type ProtoOperation interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}

// RegistryStore is the persistence surface needed to seed, validate, and resolve operation routes.
type RegistryStore interface {
	GetRoute(ctx context.Context, operation int32) (*cpm.InferenceOperationRoute, error)
	// GetRouteSnapshot reads an operation's route, its model, and the model's nullable rate-limit
	// policy in one consistent read; invocation resolution uses it on every call.
	GetRouteSnapshot(ctx context.Context, operation int32) (*cpm.InferenceOperationRoute, *cpm.InferenceModel, *cpm.InferenceModelRateLimitPolicy, error)
	ListRoutes(ctx context.Context) ([]cpm.InferenceOperationRoute, error)
	SeedRouteIfMissing(ctx context.Context, route *cpm.InferenceOperationRoute) (bool, error)
	GetModel(ctx context.Context, modelID uint64) (*cpm.InferenceModel, error)
	GetModelByProviderTuple(ctx context.Context, provider string, providerModelID string) (*cpm.InferenceModel, error)
	GetModelRateLimitPolicy(ctx context.Context, modelID uint64) (*cpm.InferenceModelRateLimitPolicy, error)
}

// RegisteredOperation is the immutable, code-owned definition exposed to admin consumers.
type RegisteredOperation struct {
	Operation    Operation
	SymbolicName string
	DisplayName  string
	SortOrder    int32
	Required     bool
}

type registeredOperation struct {
	definition             RegisteredOperation
	config                 OperationConfig
	diagnosticMetadataKeys []recorderMetadataKey
	searchableMetadataKeys []recorderMetadataKey
}

// Registry is the immutable in-process source of truth for application operations.
type Registry struct {
	store               RegistryStore
	modelRouteValidator ModelRouteValidator
	byOperation         map[Operation]registeredOperation
	ordered             []registeredOperation
	metadataKeys        []string
}

// NewRegistry validates and registers one application's complete generated operation enum. Every
// distinct nonzero descriptor value must have a config entry; the enum descriptor is derived from
// E, so configuration cannot accidentally be partial or paired with another enum.
func NewRegistry[E ProtoOperation](store RegistryStore, operations map[E]OperationConfig, opts ...RegistryOption) (*Registry, error) {
	if store == nil {
		return nil, errors.New("control plane: nil registry store")
	}

	var zero E
	descriptor := zero.Descriptor()
	if descriptor == nil {
		return nil, errors.New("control plane: nil operation enum descriptor")
	}

	registry := &Registry{
		store:       store,
		byOperation: make(map[Operation]registeredOperation, len(operations)),
		ordered:     make([]registeredOperation, 0, len(operations)),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(registry)
		}
	}
	metadataKeys := make(map[string]struct{})
	for enumValue, suppliedConfig := range operations {
		value := int32(enumValue)
		if value == 0 {
			return nil, errors.New("control plane: cannot register the unspecified (0) operation")
		}
		valueDescriptor := descriptor.Values().ByNumber(protoreflect.EnumNumber(value))
		if valueDescriptor == nil {
			return nil, fmt.Errorf("control plane: enum value %d is not defined in descriptor %s", value, descriptor.FullName())
		}

		config := cloneOperationConfig(suppliedConfig)
		if _, known := proto_models.InferenceOperationOrgModelChoice_name[int32(config.OrgModelChoice)]; !known {
			return nil, fmt.Errorf("invalid organization model-choice gate %d", config.OrgModelChoice)
		}
		if _, known := proto_models.InferenceSessionRetarget_name[int32(config.SessionRetarget)]; !known {
			return nil, fmt.Errorf("invalid session re-target gate %d", config.SessionRetarget)
		}
		operation := Operation(value)
		registered := registeredOperation{
			definition: RegisteredOperation{
				Operation:    operation,
				SymbolicName: string(valueDescriptor.Name()),
				DisplayName:  config.DisplayName,
				SortOrder:    config.SortOrder,
				Required:     config.Required,
			},
			config:                 config,
			diagnosticMetadataKeys: compileRecorderMetadataKeys(config.SearchableMetadataKeys),
			searchableMetadataKeys: compileSearchableMetadataKeys(config.SearchableMetadataKeys),
		}
		registry.byOperation[operation] = registered
		registry.ordered = append(registry.ordered, registered)
		for _, key := range config.SearchableMetadataKeys {
			metadataKeys[key] = struct{}{}
		}
	}

	missing := make([]string, 0)
	seen := make(map[Operation]struct{})
	descriptorValues := descriptor.Values()
	for i := 0; i < descriptorValues.Len(); i++ {
		valueDescriptor := descriptorValues.Get(i)
		operation := Operation(valueDescriptor.Number())
		if operation == 0 {
			continue
		}
		if _, duplicateAlias := seen[operation]; duplicateAlias {
			continue
		}
		seen[operation] = struct{}{}
		if _, configured := registry.byOperation[operation]; !configured {
			missing = append(missing, fmt.Sprintf("%s(%d)", valueDescriptor.Name(), operation))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("control plane: operation config is incomplete; missing %s", strings.Join(missing, ", "))
	}

	sort.Slice(registry.ordered, func(i, j int) bool {
		left, right := registry.ordered[i].definition, registry.ordered[j].definition
		if left.SortOrder != right.SortOrder {
			return left.SortOrder < right.SortOrder
		}
		return left.Operation < right.Operation
	})
	registry.metadataKeys = make([]string, 0, len(metadataKeys))
	for key := range metadataKeys {
		registry.metadataKeys = append(registry.metadataKeys, key)
	}
	sort.Strings(registry.metadataKeys)
	return registry, nil
}

func cloneOperationConfig(config OperationConfig) OperationConfig {
	if len(config.SearchableMetadataKeys) > 0 {
		keys := make([]string, 0, len(config.SearchableMetadataKeys))
		seen := make(map[string]struct{}, len(config.SearchableMetadataKeys))
		for _, key := range config.SearchableMetadataKeys {
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
		config.SearchableMetadataKeys = keys
	}
	if config.DefaultTemperature != nil {
		temperature := *config.DefaultTemperature
		config.DefaultTemperature = &temperature
	}
	return config
}

func (r *Registry) lookup(operation Operation) (registeredOperation, bool) {
	if r == nil {
		return registeredOperation{}, false
	}
	registered, ok := r.byOperation[operation]
	return registered, ok
}

// Lookup returns one registered operation definition.
func (r *Registry) Lookup(operation Operation) (RegisteredOperation, bool) {
	registered, ok := r.lookup(operation)
	return registered.definition, ok
}

// Operations returns every registered definition ordered by sort order, then numeric operation.
func (r *Registry) Operations() []RegisteredOperation {
	if r == nil {
		return nil
	}
	operations := make([]RegisteredOperation, len(r.ordered))
	for i := range r.ordered {
		operations[i] = r.ordered[i].definition
	}
	return operations
}

func (r *Registry) symbolicName(operation Operation) (string, bool) {
	registered, ok := r.lookup(operation)
	return registered.definition.SymbolicName, ok
}

func (r *Registry) metadataAllowlist(operation Operation) []string {
	registered, ok := r.lookup(operation)
	if !ok {
		return nil
	}
	return registered.config.SearchableMetadataKeys
}

// AllowedMetadataFilterKeys returns the sorted union of registered searchable metadata keys.
func (r *Registry) AllowedMetadataFilterKeys() []string {
	if r == nil {
		return nil
	}
	return append([]string(nil), r.metadataKeys...)
}

// ValidateModelRoute applies structural temperature validation and then the configured
// provider/model-aware validator. Routes without an explicit temperature remain valid without a
// validator; routes with one fail closed because go-common cannot infer model capabilities.
func (r *Registry) ValidateModelRoute(
	ctx context.Context,
	provider string,
	providerModelID string,
	temperature *float64,
	selections ...proto_models.InferenceAPI,
) error {
	if r == nil {
		return errors.New("control plane: nil registry")
	}
	if err := ValidateTemperature(temperature); err != nil {
		return fmt.Errorf("invalid temperature: %w", err)
	}

	api := apishape.Default
	if len(selections) > 0 {
		api = selections[0]
	}
	if err := apishape.ValidateSelection(api); err != nil {
		return err
	}
	validation := ModelRouteValidation{
		API:             api,
		ProviderID:      provider,
		ProviderModelID: providerModelID,
	}
	if temperature != nil {
		validation.HasTemperature = true
		validation.Temperature = *temperature
	}
	if r.modelRouteValidator == nil {
		if validation.HasTemperature || api != apishape.Default {
			return errors.New("configured temperature cannot be validated: no model route validator is configured")
		}
		return nil
	}
	if err := r.modelRouteValidator.ValidateModelRoute(ctx, validation); err != nil {
		return fmt.Errorf("model route validation failed for %s/%s: %w", provider, providerModelID, err)
	}
	return nil
}

// SeedDefaultRoutes inserts code-configured defaults only when no persisted route exists.
func (r *Registry) SeedDefaultRoutes(ctx context.Context) error {
	resolver := NewPersistenceResolver(r)
	for _, registered := range r.ordered {
		config := registered.config
		if config.DefaultModel.IsZero() {
			continue
		}
		model, err := r.store.GetModelByProviderTuple(ctx, config.DefaultModel.ProviderID, config.DefaultModel.ProviderModelID)
		if err != nil {
			return fmt.Errorf("control plane: resolve default model for operation %s: %w", registered.definition.Operation, err)
		}
		route := &cpm.InferenceOperationRoute{
			Operation:        int32(registered.definition.Operation),
			InferenceModelID: model.ID,
			TotalTimeout:     config.DefaultRetryPolicy.TotalTimeout,
			MaxAttempts:      int32(config.DefaultRetryPolicy.MaxAttempts),
			InitialBackoff:   config.DefaultRetryPolicy.InitialBackoff,
			MaxBackoff:       config.DefaultRetryPolicy.MaxBackoff,
			Temperature:      cloneFloat64(config.DefaultTemperature),
		}
		if _, err := resolver.resolveRoute(ctx, registered.definition.Operation, route); err != nil {
			return fmt.Errorf("control plane: invalid default route for operation %s: %w", registered.definition.Operation, err)
		}
		if _, err := r.store.SeedRouteIfMissing(ctx, route); err != nil {
			return fmt.Errorf("control plane: seed route for operation %s: %w", registered.definition.Operation, err)
		}
	}
	return nil
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// ValidateRequiredRoutes rejects stale route rows and requires every registered required operation
// to resolve successfully. Existing routes remain administrator-owned: seeding never overwrites
// them, so an unusable required route fails startup/readiness and must be repaired explicitly.
func (r *Registry) ValidateRequiredRoutes(ctx context.Context) error {
	routes, err := r.store.ListRoutes(ctx)
	if err != nil {
		return fmt.Errorf("control plane: list routes for startup validation: %w", err)
	}
	routed := make(map[Operation]struct{}, len(routes))
	for _, route := range routes {
		operation := Operation(route.Operation)
		if _, ok := r.byOperation[operation]; !ok {
			return fmt.Errorf("control plane: persisted route references unregistered operation %s", operation)
		}
		routed[operation] = struct{}{}
	}

	missing := make([]Operation, 0)
	for _, registered := range r.ordered {
		definition := registered.definition
		if definition.Required {
			if _, ok := routed[definition.Operation]; !ok {
				missing = append(missing, definition.Operation)
			}
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("control plane: %d required operation(s) have no route: %v", len(missing), missing)
	}

	resolver := NewPersistenceResolver(r)
	for _, registered := range r.ordered {
		definition := registered.definition
		if !definition.Required {
			continue
		}
		if _, err := resolver.Resolve(ctx, inference.GenerationRequest{Operation: int32(definition.Operation)}); err != nil {
			return fmt.Errorf("control plane: required operation %s has an unusable route: %w", definition.Operation, err)
		}
	}
	return nil
}
