package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

// AutomaticLookupBinding is one trusted, fully pinned catalog descriptor.
// Lookup and Selector are normalized logical authority; Resolution carries the
// exact immutable asset version already bound to the same stored contract.
type AutomaticLookupBinding struct {
	StableID   string
	Lookup     plan.Lookup
	Selector   *knowledge.Selector
	Resolution LookupResolution
}

// WithAutomaticLookupBindings atomically injects the opaque generated group
// and configures exact physical resolutions. Automatic resolutions precede
// authored resolutions because the generated group precedes every authored
// operator in the physical lookup walk.
func (compiler Compiler) WithAutomaticLookupBindings(
	query *plan.Query,
	automatic []AutomaticLookupBinding,
	explicit []LookupResolution,
) (*plan.Query, Compiler, error) {
	return compiler.WithAutomaticLookupBindingsContext(
		context.Background(),
		query,
		automatic,
		explicit,
	)
}

func (compiler Compiler) WithAutomaticLookupBindingsContext(
	ctx context.Context,
	query *plan.Query,
	automatic []AutomaticLookupBinding,
	explicit []LookupResolution,
) (*plan.Query, Compiler, error) {
	if ctx == nil {
		return nil, Compiler{}, errors.New(
			"configure ClickHouse automatic lookups: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, Compiler{}, err
	}
	if query == nil {
		return nil, Compiler{}, errors.New(
			"configure ClickHouse automatic lookups: query is nil",
		)
	}
	if len(automatic) == 0 {
		configured, err := compiler.WithLookupResolutionsContext(ctx, explicit)
		return query, configured, err
	}
	if len(automatic) > MaximumLookupStagesPerQuery ||
		len(explicit) > MaximumLookupStagesPerQuery-len(automatic) {
		return nil, Compiler{}, fmt.Errorf(
			"configure ClickHouse automatic lookups: more than %d aggregate stages",
			MaximumLookupStagesPerQuery,
		)
	}
	specs := make([]plan.AutomaticLookupSpec, len(automatic))
	resolutions := make([]LookupResolution, 0, len(automatic)+len(explicit))
	for index, binding := range automatic {
		if err := ctx.Err(); err != nil {
			return nil, Compiler{}, err
		}
		selector, err := knowledgeprogram.NewSelector(binding.Selector)
		if err != nil {
			return nil, Compiler{}, fmt.Errorf(
				"configure ClickHouse automatic lookup %d: %w",
				index,
				err,
			)
		}
		stored, ok := binding.Resolution.LogicalContract()
		if !ok || !lookupResolutionContractsEqual(stored, binding.Lookup) ||
			binding.Resolution.DefinitionName() != binding.Lookup.DefinitionName ||
			binding.StableID == "" ||
			binding.StableID != binding.Resolution.LogicalID() ||
			binding.Resolution.LogicalVersion() == 0 {
			return nil, Compiler{}, fmt.Errorf(
				"configure ClickHouse automatic lookup %d: binding authority disagrees",
				index,
			)
		}
		specs[index] = plan.AutomaticLookupSpec{
			StableID: strings.Clone(binding.StableID),
			Lookup:   cloneLookupResolutionContract(binding.Lookup),
			Selector: selector,
		}
		resolutions = append(resolutions, binding.Resolution.clone())
	}
	for _, resolution := range explicit {
		resolutions = append(resolutions, resolution.clone())
	}
	injected, err := plan.InjectAutomaticLookupGroup(query, specs)
	if err != nil {
		return nil, Compiler{}, err
	}
	configured, err := compiler.WithLookupResolutionsContext(ctx, resolutions)
	if err != nil {
		return nil, Compiler{}, err
	}
	return injected, configured, nil
}
