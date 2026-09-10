package queryexec

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

func (executor *Executor) eventExecutionSurfaceContext(
	ctx context.Context,
	query clickhouse.CompiledQuery,
) (string, []any, error) {
	policy, admitted := searchlimits.FromContext(ctx)
	if !admitted || executor.readAdmission == nil {
		// Exports carry their own independent result envelope and reuse the
		// canonical retained query. Diagnostic unsealed fixtures also keep the
		// existing private executor path.
		return query.SQL, query.Args, nil
	}
	if err := searchlimits.Validate(policy); err != nil {
		return "", nil, err
	}
	limited, eligible, err := clickhouse.CompileEventResultLimitContext(
		ctx, query, policy.MaxResultRows+1,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return "", nil, ctxErr
		}
		return "", nil, fmt.Errorf("%w: event result limit authority is invalid", searchjobs.ErrInvalidResult)
	}
	if !eligible {
		return query.SQL, query.Args, nil
	}
	return limited.SQL, limited.Args, nil
}
