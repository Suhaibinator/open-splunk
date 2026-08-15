package queryexec

import (
	"context"
	"fmt"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// issueRead mints a query ID, issues one fixed-schema read against the
// executor connection, and returns the result stream. Every message is derived
// from operation so each caller keeps its own wording.
func (executor *Executor) issueRead(
	ctx context.Context,
	operation string,
	sql string,
	args []any,
	settings clickhousedriver.Settings,
	externalTables []*ext.Table,
) (driver.Rows, error) {
	queryID, err := executor.newQueryID()
	if err != nil {
		return nil, fmt.Errorf("execute ClickHouse %s: create query ID: %w", operation, err)
	}
	if queryID == "" {
		return nil, fmt.Errorf("execute ClickHouse %s: query ID is empty", operation)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	queryOptions := []clickhousedriver.QueryOption{
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(settings),
	}
	queryOptions = appendExternalTableOption(queryOptions, externalTables)
	rows, err := executor.connection.Query(clickhousedriver.Context(ctx, queryOptions...), sql, args...)
	if err != nil {
		return nil, classifyQueryError(ctx, fmt.Errorf("query ClickHouse %s: %w", operation, err))
	}
	if isNilDriverValue(rows) {
		return nil, fmt.Errorf("%w: ClickHouse %s returned no result stream", searchjobs.ErrInvalidResult, operation)
	}
	return rows, nil
}

// closeReadStream is the deferred fallback close for a buffered read. A close
// failure that is the first error discards the buffered result so no partially
// observed stream escapes.
func closeReadStream[T any](
	ctx context.Context,
	rows driver.Rows,
	subject string,
	rowsClosed *bool,
	result *T,
	resultErr *error,
) {
	if *rowsClosed {
		return
	}
	if closeErr := rows.Close(); *resultErr == nil && closeErr != nil {
		var zero T
		*result = zero
		*resultErr = classifyQueryError(
			ctx,
			fmt.Errorf("close ClickHouse %s result stream: %w", subject, closeErr),
		)
	}
}
