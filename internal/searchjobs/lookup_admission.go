package searchjobs

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func normalizedLookupResolver(resolver LookupResolver) LookupResolver {
	if nilcheck.IsNil(resolver) {
		return nil
	}
	return resolver
}

// rejectUnavailableAuthoredLookupAdmission performs only the narrow parse
// needed to prevent a lookup-bearing request from entering the legacy async
// path without app/resolver authority. Parse failures and valid non-lookup SPL
// retain their historical asynchronous behavior.
func (manager *Manager) rejectUnavailableAuthoredLookupAdmission(
	ctx context.Context,
	request CreateRequest,
) error {
	if manager.knowledgeAdmissionEnabled(request) && manager.lookupResolver != nil {
		return nil
	}
	parsed, err := parseSPLQuery(ctx, request.SPL)
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return nil
	}
	names, err := authoredLookupNames(parsed)
	if err != nil {
		return ErrKnowledgeUnavailable
	}
	if len(names) != 0 {
		return ErrKnowledgeUnavailable
	}
	return nil
}

func authoredLookupNames(query *spl.Query) ([]string, error) {
	if query == nil {
		return nil, errors.New("resolve search lookups: parsed query is nil")
	}
	names := make([]string, 0)
	for _, command := range query.Commands {
		lookup, ok := command.(*spl.LookupCommand)
		if !ok {
			continue
		}
		if lookup == nil || lookup.DefinitionName == "" {
			return nil, errors.New("resolve search lookups: parsed lookup is incomplete")
		}
		names = append(names, lookup.DefinitionName)
		if len(names) > clickhouse.MaximumLookupStagesPerQuery {
			return nil, errors.New("resolve search lookups: stage limit exceeded")
		}
	}
	return names, nil
}

func planLookupNames(query *plan.Query) ([]string, error) {
	if query == nil {
		return nil, errors.New("resolve search lookups: logical query is nil")
	}
	names := make([]string, 0)
	for _, operator := range query.Operators {
		lookup, ok := operator.(*plan.Lookup)
		if !ok {
			continue
		}
		if lookup == nil || lookup.DefinitionName == "" {
			return nil, errors.New("resolve search lookups: logical lookup is incomplete")
		}
		names = append(names, lookup.DefinitionName)
	}
	return names, nil
}

func (manager *Manager) resolveLookupAdmission(
	ctx context.Context,
	request CreateRequest,
	parsed *spl.Query,
) ([]clickhouse.LookupResolution, []clickhouse.AutomaticLookupBinding, error) {
	names, err := authoredLookupNames(parsed)
	if err != nil {
		return nil, nil, err
	}
	if manager.lookupResolver == nil {
		if len(names) != 0 {
			return nil, nil, ErrKnowledgeUnavailable
		}
		return nil, nil, nil
	}
	if request.AppID == "" {
		return nil, nil, ErrKnowledgeUnavailable
	}
	scope := LookupAdmissionResolutionScope{
		TenantID:    request.TenantID,
		PrincipalID: request.OwnerID,
		AppID:       request.AppID,
		Names:       slices.Clone(names),
	}
	resolved, err := manager.lookupResolver.ResolveLookupAdmission(ctx, scope)
	if err != nil {
		return nil, nil, err
	}
	if len(resolved.Explicit) != len(names) ||
		len(resolved.Automatic) > clickhouse.MaximumLookupStagesPerQuery ||
		len(resolved.Explicit) > clickhouse.MaximumLookupStagesPerQuery-len(resolved.Automatic) {
		return nil, nil, ErrKnowledgeUnavailable
	}
	explicit := make([]clickhouse.LookupResolution, len(resolved.Explicit))
	for index, resolution := range resolved.Explicit {
		if resolution.TenantID() != request.TenantID ||
			resolution.DefinitionName() != names[index] ||
			resolution.LogicalID() == "" || resolution.LogicalVersion() == 0 ||
			resolution.ObjectID() == "" || resolution.Version() == 0 ||
			resolution.ContentSHA256() == ([sha256.Size]byte{}) {
			return nil, nil, ErrKnowledgeUnavailable
		}
		explicit[index] = resolution
	}
	automatic := make([]clickhouse.AutomaticLookupBinding, len(resolved.Automatic))
	for index, binding := range resolved.Automatic {
		contract, contractPresent := binding.Resolution.LogicalContract()
		if binding.StableID == "" || binding.Selector == nil ||
			!contractPresent || binding.Resolution.TenantID() != request.TenantID ||
			binding.StableID != binding.Resolution.LogicalID() ||
			binding.Resolution.LogicalVersion() == 0 ||
			binding.Resolution.ObjectID() == "" || binding.Resolution.Version() == 0 ||
			binding.Resolution.ContentSHA256() == ([sha256.Size]byte{}) ||
			binding.Resolution.DefinitionName() != binding.Lookup.DefinitionName ||
			contract.DefinitionName != binding.Lookup.DefinitionName {
			return nil, nil, ErrKnowledgeUnavailable
		}
		automatic[index] = binding
	}
	return explicit, automatic, nil
}

func compilerForPlanLookups(
	ctx context.Context,
	base clickhouse.Compiler,
	query *plan.Query,
	all []clickhouse.LookupResolution,
) (clickhouse.Compiler, error) {
	names, err := planLookupNames(query)
	if err != nil {
		return clickhouse.Compiler{}, err
	}
	if len(names) > len(all) {
		return clickhouse.Compiler{}, ErrKnowledgeUnavailable
	}
	for index, name := range names {
		if all[index].DefinitionName() != name {
			return clickhouse.Compiler{}, ErrKnowledgeUnavailable
		}
	}
	return base.WithLookupResolutionsContext(ctx, all[:len(names)])
}

func configureResolvedPlanLookups(
	ctx context.Context,
	base clickhouse.Compiler,
	query *plan.Query,
	automatic []clickhouse.AutomaticLookupBinding,
	explicit []clickhouse.LookupResolution,
) (*plan.Query, clickhouse.Compiler, error) {
	if len(automatic) > clickhouse.MaximumLookupStagesPerQuery ||
		len(explicit) > clickhouse.MaximumLookupStagesPerQuery-len(automatic) {
		return nil, clickhouse.Compiler{}, ErrKnowledgeUnavailable
	}
	if len(automatic) == 0 {
		compiler, err := compilerForPlanLookups(ctx, base, query, explicit)
		return query, compiler, err
	}
	names, err := planLookupNames(query)
	if err != nil || len(names) > len(explicit) {
		return nil, clickhouse.Compiler{}, ErrKnowledgeUnavailable
	}
	for index, name := range names {
		if explicit[index].DefinitionName() != name {
			return nil, clickhouse.Compiler{}, ErrKnowledgeUnavailable
		}
	}
	return base.WithAutomaticLookupBindingsContext(
		ctx,
		query,
		automatic,
		explicit[:len(names)],
	)
}
