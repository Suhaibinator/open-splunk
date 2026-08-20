package searchanalysis

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type retainedLookupAuthorityCompiler interface {
	WithRetainedLookupAuthorityContext(
		context.Context,
		clickhouse.CompiledQuery,
		*plan.Query,
	) (*plan.Query, clickhouse.Compiler, error)
}

// restoreCompletedSearchLookupAuthority reattaches only compiler-sealed lookup
// authority. It deliberately has no catalog resolver and therefore cannot
// replace a pinned version with current mutable state.
func restoreCompletedSearchLookupAuthority(
	ctx context.Context,
	snapshot searchjobs.ExecutionSnapshot,
	logical *plan.Query,
	compiler any,
) (*plan.Query, clickhouse.Compiler, bool, error) {
	retained, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil {
		return nil, clickhouse.Compiler{}, false, fmt.Errorf(
			"open retained lookup authority: %w",
			err,
		)
	}
	if retained == nil {
		return logical, clickhouse.Compiler{}, false, nil
	}
	present, err := retained.CompiledQuery.HasLookupAuthorityContext(ctx)
	if err != nil {
		return nil, clickhouse.Compiler{}, false, err
	}
	if !present {
		for _, operator := range logical.Operators {
			if _, authored := operator.(*plan.Lookup); authored {
				return nil, clickhouse.Compiler{}, false, errors.New(
					"completed search lookup plan has no retained physical authority",
				)
			}
		}
		return logical, clickhouse.Compiler{}, false, nil
	}
	restorer, ok := compiler.(retainedLookupAuthorityCompiler)
	if !ok {
		return nil, clickhouse.Compiler{}, false, errors.New(
			"completed search compiler cannot restore retained lookup authority",
		)
	}
	restored, configured, err := restorer.WithRetainedLookupAuthorityContext(
		ctx,
		retained.CompiledQuery,
		logical,
	)
	if err != nil {
		return nil, clickhouse.Compiler{}, false, err
	}
	return restored, configured, true, nil
}
