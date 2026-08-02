package main

import (
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/queryexec"
)

// newRuntimeQueryExecutor is the production composition boundary for every
// search-derived ClickHouse read. Keeping admission separate from Config
// prevents a caller from accidentally omitting the shared deletion fence when
// adding or changing runtime-specific executor limits.
func newRuntimeQueryExecutor(
	connection driver.Conn,
	config queryexec.Config,
	readAdmission indexread.Admission,
) (*queryexec.Executor, error) {
	if nilRuntimeDependency(readAdmission) {
		return nil, errors.New(
			"create runtime query executor: index read admission is required",
		)
	}
	config.ReadAdmission = readAdmission
	executor, err := queryexec.New(connection, config)
	if err != nil {
		return nil, fmt.Errorf("create runtime query executor: %w", err)
	}
	return executor, nil
}

// newRuntimeIndexStatisticsReader is the production composition boundary for
// admin statistics reads. It deliberately overwrites Config.ReadAdmission so
// the caller cannot substitute a different registry or omit the shared fence.
func newRuntimeIndexStatisticsReader(
	connection driver.Conn,
	config internalclickhouse.IndexStatisticsConfig,
	readAdmission indexread.Admission,
) (*internalclickhouse.IndexStatisticsReader, error) {
	if nilRuntimeDependency(readAdmission) {
		return nil, errors.New(
			"create runtime index statistics reader: index read admission is required",
		)
	}
	config.ReadAdmission = readAdmission
	reader, err := internalclickhouse.NewIndexStatisticsReader(connection, config)
	if err != nil {
		return nil, fmt.Errorf("create runtime index statistics reader: %w", err)
	}
	return reader, nil
}
