package main

import (
	"context"
	"errors"
	"fmt"
	"slices"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// runtimeLookupSearchResolver is the narrow bridge from logical visibility to
// the physical compiler payload. The catalog returns exact detached immutable
// versions; this adapter binds their package-immutable backing into compiler
// authority and never exposes asset IDs, versions, digests, or rows to
// authored SPL.
type runtimeLookupSearchResolver struct {
	catalog *lookupcatalog.Catalog
}

func newRuntimeLookupSearchResolver(
	catalog *lookupcatalog.Catalog,
) (*runtimeLookupSearchResolver, error) {
	if catalog == nil {
		return nil, errors.New("create runtime lookup search resolver: catalog is required")
	}
	return &runtimeLookupSearchResolver{catalog: catalog}, nil
}

// ResolveLookupAdmission selects authored and automatic lookups in one catalog
// read snapshot and preserves the catalog's pre-load combined budget check.
// Conversion below is purely in-memory and never consults mutable state again.
func (resolver *runtimeLookupSearchResolver) ResolveLookupAdmission(
	ctx context.Context,
	scope searchjobs.LookupAdmissionResolutionScope,
) (searchjobs.LookupAdmissionResolution, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return searchjobs.LookupAdmissionResolution{}, errors.New(
			"resolve runtime search lookup admission: dependencies are incomplete",
		)
	}
	resolved, err := resolver.catalog.ResolveAdmission(ctx, lookupcatalog.ResolveScope{
		TenantID:    scope.TenantID,
		PrincipalID: scope.PrincipalID,
		AppID:       scope.AppID,
		Names:       slices.Clone(scope.Names),
	})
	if err != nil {
		if errors.Is(err, lookupcatalog.ErrCapacity) {
			return searchjobs.LookupAdmissionResolution{}, searchjobs.ErrCapacity
		}
		return searchjobs.LookupAdmissionResolution{}, err
	}
	explicit, err := runtimeExplicitLookupResolutions(resolved.Explicit, scope.Names)
	if err != nil {
		return searchjobs.LookupAdmissionResolution{}, err
	}
	automatic, err := runtimeAutomaticLookupBindings(resolved.Automatic)
	if err != nil {
		return searchjobs.LookupAdmissionResolution{}, err
	}
	return searchjobs.LookupAdmissionResolution{
		Explicit:  explicit,
		Automatic: automatic,
	}, nil
}

func (resolver *runtimeLookupSearchResolver) ResolveLookups(
	ctx context.Context,
	scope searchjobs.LookupResolutionScope,
) ([]clickhouse.LookupResolution, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return nil, errors.New("resolve runtime search lookups: dependencies are incomplete")
	}
	resolved, err := resolver.catalog.Resolve(ctx, lookupcatalog.ResolveScope{
		TenantID:    scope.TenantID,
		PrincipalID: scope.PrincipalID,
		AppID:       scope.AppID,
		Names:       slices.Clone(scope.Names),
	})
	if err != nil {
		return nil, err
	}
	if len(resolved) != len(scope.Names) {
		return nil, errors.New("resolve runtime search lookups: catalog result count disagrees")
	}
	return runtimeExplicitLookupResolutions(resolved, scope.Names)
}

func runtimeExplicitLookupResolutions(
	resolved []lookupcatalog.Resolved,
	names []string,
) ([]clickhouse.LookupResolution, error) {
	if len(resolved) != len(names) {
		return nil, errors.New("resolve runtime search lookups: catalog result count disagrees")
	}
	result := make([]clickhouse.LookupResolution, len(resolved))
	for index, item := range resolved {
		if item.Lookup == nil || item.Lookup.GetDefinition() == nil ||
			item.Lookup.GetDefinition().GetName() != names[index] {
			return nil, errors.New("resolve runtime search lookups: catalog order disagrees")
		}
		contract, _, contractErr := runtimeLookupContract(item)
		if contractErr != nil {
			return nil, contractErr
		}
		resolution, err := clickhouse.NewLookupResolutionFromVersionWithContract(
			contract,
			item.Lookup.GetLookupId(),
			item.Lookup.GetVersion(),
			item.Asset,
		)
		if err != nil {
			return nil, err
		}
		result[index] = resolution
	}
	return result, nil
}

func (resolver *runtimeLookupSearchResolver) ResolveAutomaticLookups(
	ctx context.Context,
	scope searchjobs.AutomaticLookupResolutionScope,
) ([]clickhouse.AutomaticLookupBinding, error) {
	return resolver.resolveAutomaticLookupBindings(ctx, lookupcatalog.ResolveScope{
		TenantID:    scope.TenantID,
		PrincipalID: scope.PrincipalID,
		AppID:       scope.AppID,
	})
}

func (resolver *runtimeLookupSearchResolver) resolveAutomaticLookupBindings(
	ctx context.Context,
	scope lookupcatalog.ResolveScope,
) ([]clickhouse.AutomaticLookupBinding, error) {
	if resolver == nil || resolver.catalog == nil || ctx == nil {
		return nil, errors.New(
			"resolve runtime automatic lookups: dependencies are incomplete",
		)
	}
	resolved, err := resolver.catalog.ResolveAutomatic(ctx, scope)
	if err != nil {
		return nil, err
	}
	return runtimeAutomaticLookupBindings(resolved)
}

func runtimeAutomaticLookupBindings(
	resolved []lookupcatalog.Resolved,
) ([]clickhouse.AutomaticLookupBinding, error) {
	bindings := make([]clickhouse.AutomaticLookupBinding, len(resolved))
	for index, item := range resolved {
		if item.Lookup == nil || item.Lookup.GetDefinition() == nil ||
			!item.Lookup.GetDefinition().GetAutomatic() ||
			item.Lookup.GetLookupId() == "" {
			return nil, errors.New(
				"resolve runtime automatic lookups: catalog result is invalid",
			)
		}
		contract, selector, contractErr := runtimeLookupContract(item)
		if contractErr != nil {
			return nil, contractErr
		}
		resolution, resolutionErr := clickhouse.NewLookupResolutionFromVersionWithContract(
			contract,
			item.Lookup.GetLookupId(),
			item.Lookup.GetVersion(),
			item.Asset,
		)
		if resolutionErr != nil {
			return nil, resolutionErr
		}
		bindings[index] = clickhouse.AutomaticLookupBinding{
			StableID:   item.Lookup.GetLookupId(),
			Lookup:     contract,
			Selector:   selector,
			Resolution: resolution,
		}
	}
	return bindings, nil
}

// runtimeLookupContract revalidates the persisted logical definition against
// its exact pinned asset schema and detaches the compiler-facing mapping and
// selector authority. Explicit and automatic admission share this conversion
// so neither path can reinterpret mutable protobuf state.
func runtimeLookupContract(
	resolved lookupcatalog.Resolved,
) (plan.Lookup, *knowledge.Selector, error) {
	if resolved.Lookup == nil || resolved.Lookup.GetDefinition() == nil ||
		resolved.Asset.Asset == nil {
		return plan.Lookup{}, nil, errors.New(
			"resolve runtime search lookup: catalog result is incomplete",
		)
	}
	normalized, err := lookupdefinition.Normalize(
		resolved.Lookup.GetDefinition(),
		resolved.Asset.Asset.Headers(),
	)
	if err != nil {
		return plan.Lookup{}, nil, fmt.Errorf(
			"resolve runtime search lookup: stored definition is invalid: %w",
			err,
		)
	}
	contract := plan.Lookup{
		DefinitionName: normalized.Definition.GetName(),
		Keys: make(
			[]plan.LookupKey,
			len(normalized.Definition.GetKeyMappings()),
		),
		Outputs: make(
			[]plan.LookupOutput,
			len(normalized.Definition.GetOutputMappings()),
		),
	}
	for index, mapping := range normalized.Definition.GetKeyMappings() {
		eventField, resolveErr := plan.ResolveField(mapping.GetEventField(), contract.Range)
		if resolveErr != nil {
			return plan.Lookup{}, nil, fmt.Errorf(
				"resolve runtime search lookup: stored key mapping is invalid: %w",
				resolveErr,
			)
		}
		contract.Keys[index] = plan.LookupKey{
			LookupField: mapping.GetLookupField(),
			EventField:  eventField,
		}
	}
	for index, mapping := range normalized.Definition.GetOutputMappings() {
		eventField, resolveErr := plan.ResolveField(mapping.GetEventField(), contract.Range)
		if resolveErr != nil {
			return plan.Lookup{}, nil, fmt.Errorf(
				"resolve runtime search lookup: stored output mapping is invalid: %w",
				resolveErr,
			)
		}
		contract.Outputs[index] = plan.LookupOutput{
			LookupField: mapping.GetLookupField(),
			EventField:  eventField,
		}
	}
	switch normalized.Definition.GetOverwriteBehavior() {
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING:
		contract.WriteMode = plan.LookupWriteModeOverwrite
	case opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING:
		contract.WriteMode = plan.LookupWriteModePreserveExisting
	default:
		return plan.Lookup{}, nil, errors.New(
			"resolve runtime search lookup: stored overwrite behavior is invalid",
		)
	}
	return contract, normalized.Selector, nil
}

var _ searchjobs.LookupResolver = (*runtimeLookupSearchResolver)(nil)
var _ searchjobs.AutomaticLookupResolver = (*runtimeLookupSearchResolver)(nil)
