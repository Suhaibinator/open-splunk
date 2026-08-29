// Package queryexec streams fully scoped, compiler-produced SQL from
// ClickHouse into the bounded search-job result sink.
package queryexec

import (
	"context"
	"crypto/rand"
	sqldriver "database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"net"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"

	"fortio.org/safecast"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	defaultMaxExecutionTime = 2 * time.Minute
	defaultMaxMemoryBytes   = uint64(1 << 30)
	defaultMaxRowsToRead    = uint64(250_000_000)
	defaultMaxBytesToRead   = uint64(64 << 30)
	defaultMaxResultRows    = uint64(10_001)
	defaultMaxResultBytes   = uint64(128 << 20)
	defaultMaxThreads       = uint64(4)
	defaultMaxQueryBytes    = uint64(1 << 20)
	// Knowledge lowering can legitimately exceed ClickHouse's 50,000-element
	// default even while remaining inside the compiler's one-MiB SQL and
	// 96-level relational ceilings. Pin both parser/analyzer AST budgets so an
	// ambient server profile cannot reject a compiler-admitted query, while the
	// fixed bounds remain independent defenses against expression expansion.
	defaultMaxASTElements         = uint64(100_000)
	defaultMaxExpandedASTElements = uint64(100_000)
	// Keep the pinned ClickHouse analyzer ceiling above the compiler's
	// conservative 96-level SELECT/UNION dependency limit. The compiler and
	// server count relational structure differently, so this remains an
	// independent, fixed defense-in-depth setting rather than a derived value.
	defaultMaxSubqueryDepth = uint64(100)
	maximumTimechartBuckets = uint64(10_000)
	// Runtime-wide timecharts must rank every raw split label before reducing
	// the result to twelve public series. Bound that exact pre-ranking work
	// independently of the much smaller output width.
	maximumRuntimeWideTimechartGroups = uint64(130_000)
	// Percentiles retain a mergeable GK sketch per raw axis/series group,
	// unlike the constant-size count or sum/count state used by other wide
	// pivots. Crossing this independently accounted ceiling fails the complete
	// ClickHouse query instead of approximating or truncating it.
	maximumRuntimeWidePercentileGroups = uint64(20_000)
	maximumChartRows                   = uint64(10_000)
	maximumChartSeries                 = uint16(12)
	maximumChartLabel                  = uint16(256)
	// maximumChartResultBytes bounds the buffered pivot. Unlike timechart,
	// whose buffered row is a fixed-width bucket plus at most 13 counts, a
	// chart row carries a runtime row value with no length ceiling, and the
	// whole pivot is materialized before the first sink call. Without an
	// explicit budget the only remaining ceiling is the server's
	// max_result_bytes, which sits above the search-job byte limits the sink
	// would otherwise enforce incrementally. The guard trips on the offending
	// row, so at most one row beyond the ceiling is ever resident.
	maximumChartResultBytes = uint64(48 << 20)
	// Authored arithmetic queries that can raise a sanitized runtime scalar error
	// are consumed completely before any schema or row is published. Keep that
	// private buffer independently bounded even if a forged driver ignores the
	// ClickHouse max_result_bytes setting.
	maximumAtomicResultBytes = uint64(128 << 20)
	// A buffered row is stored by value in a growing slice. Reserving two
	// complete slots per logical row conservatively covers both the Value and
	// cell-slice headers in the struct plus the slice's unused growth capacity.
	// The cell backing arrays and variable row payload are charged separately.
	chartRowOverheadBytes      = 2 * uint64(unsafe.Sizeof(chartRow{}))
	chartValueRowOverheadBytes = 2 * uint64(unsafe.Sizeof(chartValueRow{}))
	chartCountCellBytes        = uint64(unsafe.Sizeof(uint64(0)))
	chartValueCellBytes        = uint64(unsafe.Sizeof(nullableFloat64{}))
	chartBufferedBaseBytes     = uint64(unsafe.Sizeof(bufferedChart{}))
	chartStringHeaderBytes     = uint64(unsafe.Sizeof(""))

	extendedTypeKey  = "\x00open_splunk_type"
	extendedValueKey = "\x00open_splunk_value"
)

var (
	extendedDecimalPattern        = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?$`)
	requiredTextIndexSettingNames = [...]string{
		"enable_full_text_index",
		"query_plan_direct_read_from_text_index",
		"use_skip_indexes_on_data_read",
		"use_skip_indexes",
	}
)

// Config bounds every ClickHouse query independently of the search-job sink.
// Zero values select conservative single-node defaults.
type Config struct {
	MaxExecutionTime time.Duration
	MaxMemoryBytes   uint64
	MaxRowsToRead    uint64
	MaxBytesToRead   uint64
	MaxResultRows    uint64
	MaxResultBytes   uint64
	// MaxRowsToGroupBy bounds distinct groups before result limits apply. When
	// zero, ordinary queries follow MaxResultRows (including a caller-supplied
	// value); bounded runtime-wide pivots may receive a larger per-query
	// default. A fixed-schema timechart keeps the ordinary cap.
	MaxRowsToGroupBy uint64
	// ExpandTimechartGroupLimit permits validated bounded pivots to raise
	// MaxRowsToGroupBy. A fixed timechart needs only BucketCount; runtime-wide
	// count/sum/average receives a 130,000-group raw-ranking allowance, while a
	// split percentile is capped at 20,000 GK states. Count/sum/average chart
	// uses rows*(MaxSeries+1), also bounded at 130,000; percentile chart has the
	// same hard 20,000-state ceiling. Ordinary queries remain unchanged.
	// Expansion is enabled automatically when MaxRowsToGroupBy is left at zero.
	ExpandTimechartGroupLimit bool
	MaxThreads                uint64
	// ReadAdmission joins every compiler-scoped physical read to the shared
	// index retirement fence. It is required. Isolated tests and diagnostics
	// without an index-deletion lifecycle must explicitly use
	// indexread.UnfencedAdmission.
	ReadAdmission indexread.Admission
}

// ConfigFromPolicy derives the ClickHouse envelope from one admitted search
// policy. Result settings retain one overflow row and a two-times byte margin
// so the search-job sink remains the authoritative truncation boundary.
func ConfigFromPolicy(policy searchlimits.Policy) (Config, error) {
	if err := searchlimits.Validate(policy); err != nil {
		return Config{}, err
	}
	resultRows := policy.MaxResultRows + 1
	resultBytes := policy.MaxResultBytes * 2
	if resultRows <= policy.MaxResultRows || resultBytes < policy.MaxResultBytes {
		return Config{}, errors.New("derive ClickHouse query limits: result guard overflow")
	}
	return Config{
		MaxExecutionTime:          policy.MaxRuntime,
		MaxMemoryBytes:            policy.MaxMemoryBytes,
		MaxRowsToRead:             policy.MaxRowsToRead,
		MaxBytesToRead:            policy.MaxBytesToRead,
		MaxResultRows:             resultRows,
		MaxResultBytes:            resultBytes,
		MaxRowsToGroupBy:          policy.MaxGroupedRows,
		ExpandTimechartGroupLimit: true,
		MaxThreads:                policy.MaxThreads,
	}, nil
}

// Executor is a native ClickHouse implementation of searchjobs.Executor.
type Executor struct {
	connection                queryConnection
	settingsMu                sync.RWMutex
	settings                  *validatedExecutorSettings
	expandTimechartGroupLimit bool
	limitSource               *searchlimits.Source
	newQueryID                func() (string, error)
	withProgress              func(func(*clickhousedriver.Progress)) clickhousedriver.QueryOption
	readAdmission             indexread.Admission
}

// Reconfigure atomically replaces the resource envelope used by operations
// that start after this call. In-flight operations retain their cloned
// ClickHouse settings.
func (executor *Executor) Reconfigure(config Config) error {
	if executor == nil {
		return errors.New("reconfigure ClickHouse query executor: executor is required")
	}
	expand := config.MaxRowsToGroupBy == 0 || config.ExpandTimechartGroupLimit
	settings, err := validatedQuerySettings(config)
	if err != nil {
		return err
	}
	executor.settingsMu.Lock()
	executor.settings = settings
	executor.expandTimechartGroupLimit = expand
	executor.settingsMu.Unlock()
	return nil
}

// BindLimitSource makes one atomic policy source authoritative for operations
// started without an admission snapshot. It is configured during startup
// before the executor is shared.
func (executor *Executor) BindLimitSource(source *searchlimits.Source) error {
	if executor == nil || source == nil {
		return errors.New("bind ClickHouse search limits: source is required")
	}
	if _, err := ConfigFromPolicy(source.Snapshot()); err != nil {
		return err
	}
	executor.settingsMu.Lock()
	executor.limitSource = source
	executor.settingsMu.Unlock()
	return nil
}

func (executor *Executor) settingsSnapshot() (*validatedExecutorSettings, bool) {
	executor.settingsMu.RLock()
	settings, expand, source := executor.settings, executor.expandTimechartGroupLimit, executor.limitSource
	executor.settingsMu.RUnlock()
	if source == nil {
		return settings, expand
	}
	config, err := ConfigFromPolicy(source.Snapshot())
	if err != nil {
		return settings, expand
	}
	derived, err := validatedQuerySettings(config)
	if err != nil {
		return settings, expand
	}
	return derived, true
}

func (executor *Executor) baseSettings() *validatedExecutorSettings {
	settings, _ := executor.settingsSnapshot()
	return settings
}

type queryConnection interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

var _ searchjobs.Executor = (*Executor)(nil)

// New validates config and constructs a read-only, resource-bounded executor.
// The caller owns connection and must close it after the job manager stops.
func New(connection driver.Conn, config Config) (*Executor, error) {
	if connection == nil {
		return nil, errors.New("create ClickHouse query executor: connection is required")
	}
	if config.ReadAdmission == nil || isNilDriverValue(config.ReadAdmission) {
		return nil, errors.New("create ClickHouse query executor: read admission is required")
	}
	expandTimechartGroupLimit := config.MaxRowsToGroupBy == 0 || config.ExpandTimechartGroupLimit
	settings, err := validatedQuerySettings(config)
	if err != nil {
		return nil, err
	}
	return &Executor{
		connection:                connection,
		settings:                  settings,
		expandTimechartGroupLimit: expandTimechartGroupLimit,
		newQueryID:                randomQueryID,
		withProgress:              clickhousedriver.WithProgress,
		readAdmission:             config.ReadAdmission,
	}, nil
}

type validatedExecutorSettings struct {
	values clickhousedriver.Settings
}

var requiredPositiveExecutorSettingNames = [...]string{
	"max_execution_time",
	"max_memory_usage",
	"max_rows_to_read",
	"max_bytes_to_read",
	"max_result_rows",
	"max_result_bytes",
	"max_rows_to_group_by",
	"max_threads",
	"max_query_size",
	"max_ast_elements",
	"max_expanded_ast_elements",
	"max_subquery_depth",
}

var requiredThrowExecutorSettingNames = [...]string{
	"timeout_overflow_mode",
	"read_overflow_mode",
	"result_overflow_mode",
	"group_by_overflow_mode",
}

func newValidatedExecutorSettings(
	values clickhousedriver.Settings,
) (*validatedExecutorSettings, error) {
	if values == nil || values["readonly"] != uint8(2) {
		return nil, errors.New("create ClickHouse query executor: settings are not read-only")
	}
	for _, name := range requiredPositiveExecutorSettingNames {
		value, ok := values[name].(uint64)
		if !ok || value == 0 {
			return nil, fmt.Errorf("create ClickHouse query executor: setting %s is invalid", name)
		}
	}
	for _, name := range requiredThrowExecutorSettingNames {
		if values[name] != "throw" {
			return nil, fmt.Errorf("create ClickHouse query executor: setting %s is unsafe", name)
		}
	}
	for _, name := range requiredTextIndexSettingNames {
		if values[name] != uint8(1) {
			return nil, fmt.Errorf("create ClickHouse query executor: setting %s is not enabled", name)
		}
	}
	if values["enable_materialized_cte"] != uint8(1) ||
		values["short_circuit_function_evaluation"] != "enable" ||
		values["async_insert"] != uint8(0) {
		return nil, errors.New("create ClickHouse query executor: safety settings are invalid")
	}
	return &validatedExecutorSettings{values: maps.Clone(values)}, nil
}

func (settings *validatedExecutorSettings) clone() clickhousedriver.Settings {
	if settings == nil {
		return nil
	}
	return maps.Clone(settings.values)
}

func (settings *validatedExecutorSettings) limit(name string) uint64 {
	if settings == nil {
		return 0
	}
	return settings.values[name].(uint64)
}

// settingLimit is one per-operation ceiling applied on top of the executor's
// validated read-only settings.
type settingLimit struct {
	name    string
	maximum uint64
}

// boundedExecutorSettings clones the executor's validated settings and clamps
// each named limit to the smaller of the executor ceiling and the operation's
// own maximum. Limits the caller omits are inherited from the clone unchanged.
func boundedExecutorSettings(
	base *validatedExecutorSettings,
	limits ...settingLimit,
) clickhousedriver.Settings {
	settings := base.clone()
	for _, limit := range limits {
		settings[limit.name] = min(base.limit(limit.name), limit.maximum)
	}
	return settings
}

func validatedQuerySettings(config Config) (*validatedExecutorSettings, error) {
	settings, err := querySettings(config)
	if err != nil {
		return nil, err
	}
	return newValidatedExecutorSettings(settings)
}

func querySettings(config Config) (clickhousedriver.Settings, error) {
	if config.MaxExecutionTime < 0 {
		return nil, errors.New("create ClickHouse query executor: maximum execution time cannot be negative")
	}
	if config.MaxExecutionTime == 0 {
		config.MaxExecutionTime = defaultMaxExecutionTime
	}
	if config.MaxMemoryBytes == 0 {
		config.MaxMemoryBytes = defaultMaxMemoryBytes
	}
	if config.MaxRowsToRead == 0 {
		config.MaxRowsToRead = defaultMaxRowsToRead
	}
	if config.MaxBytesToRead == 0 {
		config.MaxBytesToRead = defaultMaxBytesToRead
	}
	if config.MaxResultRows == 0 {
		config.MaxResultRows = defaultMaxResultRows
	}
	if config.MaxResultBytes == 0 {
		config.MaxResultBytes = defaultMaxResultBytes
	}
	if config.MaxRowsToGroupBy == 0 {
		// A GROUP BY builds its hash table before max_result_rows can reject
		// the response. Keep both cardinality limits aligned by default to
		// bound distinct groups and reduce memory-exhaustion risk.
		config.MaxRowsToGroupBy = config.MaxResultRows
	}
	if config.MaxThreads == 0 {
		config.MaxThreads = defaultMaxThreads
	}
	seconds := uint64(math.Ceil(config.MaxExecutionTime.Seconds()))
	if seconds == 0 {
		seconds = 1
	}
	settings := clickhousedriver.Settings{
		"readonly":                          uint8(2),
		"max_execution_time":                seconds,
		"timeout_overflow_mode":             "throw",
		"max_memory_usage":                  config.MaxMemoryBytes,
		"max_rows_to_read":                  config.MaxRowsToRead,
		"max_bytes_to_read":                 config.MaxBytesToRead,
		"read_overflow_mode":                "throw",
		"max_result_rows":                   config.MaxResultRows,
		"max_result_bytes":                  config.MaxResultBytes,
		"result_overflow_mode":              "throw",
		"max_rows_to_group_by":              config.MaxRowsToGroupBy,
		"group_by_overflow_mode":            "throw",
		"max_threads":                       config.MaxThreads,
		"max_query_size":                    defaultMaxQueryBytes,
		"max_ast_elements":                  defaultMaxASTElements,
		"max_expanded_ast_elements":         defaultMaxExpandedASTElements,
		"max_subquery_depth":                defaultMaxSubqueryDepth,
		"enable_materialized_cte":           uint8(1),
		"short_circuit_function_evaluation": "enable",
		"async_insert":                      uint8(0),
	}
	for _, name := range requiredTextIndexSettingNames {
		settings[name] = uint8(1)
	}
	return settings, nil
}

type readScopedCompiledQuery interface {
	ReadScope() (string, []string, bool)
}

func (executor *Executor) acquireRead(
	ctx context.Context,
	query readScopedCompiledQuery,
	operation string,
) (context.Context, func(), error) {
	// Only same-package tests can construct an Executor without New. Keep that
	// private diagnostic path able to exercise intentionally unsealed query
	// fixtures; the public constructor always installs an explicit admission.
	if executor.readAdmission == nil {
		return ctx, func() {}, nil
	}
	if isNilDriverValue(executor.readAdmission) {
		return nil, nil, fmt.Errorf("%s: read admission is nil", operation)
	}
	tenantID, indexNames, ok := query.ReadScope()
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: %s compiled query read scope is missing or invalid",
			searchjobs.ErrInvalidResult,
			operation,
		)
	}
	admittedContext, release, err := executor.readAdmission.Acquire(ctx, tenantID, indexNames)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%s: admit index read: %w",
			operation,
			classifyIndexReadError(err),
		)
	}
	if admittedContext == nil || release == nil {
		if release != nil {
			release()
		}
		return nil, nil, fmt.Errorf("%s: read admission returned an incomplete lease", operation)
	}
	return admittedContext, release, nil
}

func preserveReadCancellationCause(ctx context.Context, err error) error {
	cause := context.Cause(ctx)
	if cause == nil || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		return err
	}
	// Retirement is authoritative once it linearizes. Do not retain a driver or
	// sink error produced while cancellation unwinds: besides obscuring the
	// lifecycle cause, native errors may contain SQL or storage details.
	return classifyIndexReadError(cause)
}

func classifyIndexReadError(err error) error {
	if !errors.Is(err, indexread.ErrUnavailable) || errors.Is(err, searchjobs.ErrStorageUnavailable) {
		return err
	}
	return errors.Join(err, searchjobs.ErrStorageUnavailable)
}

// Execute sends schema once and then streams rows in server order. It never
// retains sink or calls it after returning.
func (executor *Executor) Execute(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) (resultErr error) {
	if ctx == nil {
		return errors.New("execute ClickHouse search: context is nil")
	}
	if sink == nil {
		return errors.New("execute ClickHouse search: result sink is required")
	}
	// Every publicly constructed Executor has an explicit read-admission
	// dependency. At that production boundary, detach and validate the complete
	// compiler authority before inspecting it or reaching admission/driver
	// state. Same-package diagnostic fixtures intentionally omit admission and
	// retain their historical ability to exercise hand-built row contracts.
	if executor.readAdmission != nil {
		detached, ok, cloneErr := query.CloneForExecutionContext(ctx)
		if cloneErr != nil {
			return cloneErr
		}
		if !ok {
			return fmt.Errorf("%w: compiled query execution authority is invalid", searchjobs.ErrInvalidResult)
		}
		query = detached
	}
	query.Args = slices.Clone(query.Args)
	if strings.TrimSpace(query.SQL) == "" || len(query.OutputFields) == 0 {
		return errors.New("execute ClickHouse search: compiled query is incomplete")
	}
	if query.Timechart != nil && query.Chart != nil {
		return fmt.Errorf("%w: compiled query declares two wide result contracts", searchjobs.ErrInvalidResult)
	}
	resultPresentations, presentationsOK := query.ValidatedResultFieldPresentations()
	if !presentationsOK {
		return fmt.Errorf("%w: compiled result presentation contract is invalid", searchjobs.ErrInvalidResult)
	}
	if len(resultPresentations) != 0 && (query.Timechart != nil || query.Chart != nil) {
		return fmt.Errorf("%w: wide result declares ordinary presentation metadata", searchjobs.ErrInvalidResult)
	}
	if query.Timechart != nil {
		if err := validateTimechartOutput(query); err != nil {
			return err
		}
	}
	if query.Chart != nil {
		if err := validateChartOutput(query); err != nil {
			return err
		}
	}
	sparseFieldIndex, err := validateSparseFieldsOutput(query)
	if err != nil {
		return err
	}
	admittedContext, releaseRead, err := executor.acquireRead(ctx, query, "execute ClickHouse search")
	if err != nil {
		return err
	}
	defer releaseRead()
	defer func() {
		resultErr = preserveReadCancellationCause(admittedContext, resultErr)
	}()
	ctx = admittedContext
	if err := ctx.Err(); err != nil {
		return preserveReadCancellationCause(ctx, err)
	}
	externalTables, err := query.ExternalTablesForExecution(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return preserveReadCancellationCause(ctx, ctxErr)
		}
		return fmt.Errorf(
			"%w: compiled lookup transport is invalid",
			searchjobs.ErrInvalidResult,
		)
	}
	queryID, err := executor.newQueryID()
	if err != nil {
		return fmt.Errorf("execute ClickHouse search: create query ID: %w", err)
	}
	withProgress := executor.withProgress
	if withProgress == nil {
		withProgress = clickhousedriver.WithProgress
	}
	executionContext, progressReporter, progressOption := startExecutionProgressWith(ctx, sink, withProgress)
	if progressReporter != nil {
		defer func() {
			resultErr = progressReporter.finish(ctx, resultErr)
		}()
	}
	executionSettings, settingsErr := executor.settingsForContext(executionContext, query)
	if settingsErr != nil {
		return settingsErr
	}
	queryOptions := []clickhousedriver.QueryOption{
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(executionSettings),
	}
	if progressOption != nil {
		queryOptions = append(queryOptions, progressOption)
	}
	queryOptions = appendExternalTableOption(queryOptions, externalTables)
	queryContext := clickhousedriver.Context(executionContext, queryOptions...)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return classifyQueryError(executionContext, fmt.Errorf("query ClickHouse: %w", err))
	}
	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			resultErr = classifyQueryError(executionContext, fmt.Errorf("close ClickHouse result stream: %w", closeErr))
		}
	}()

	columnTypes := rows.ColumnTypes()
	columns := rows.Columns()
	if query.Timechart != nil {
		switch query.Timechart.Mode {
		case clickhouse.TimechartModeFixedCount,
			clickhouse.TimechartModeFixedFieldCount:
			buffered, readErr := readFixedTimechartRows(
				executionContext,
				rows,
				columns,
				columnTypes,
				*query.Timechart,
			)
			if readErr != nil {
				return readErr
			}
			closeErr := rows.Close()
			rowsClosed = true
			if closeErr != nil {
				return classifyQueryError(executionContext, fmt.Errorf("close ClickHouse fixed timechart result stream: %w", closeErr))
			}
			return publishFixedTimechart(executionContext, sink, buffered)
		case clickhouse.TimechartModeFixedValue:
			buffered, readErr := readFixedValueTimechartRows(
				executionContext,
				rows,
				columns,
				columnTypes,
				*query.Timechart,
			)
			if readErr != nil {
				return readErr
			}
			closeErr := rows.Close()
			rowsClosed = true
			if closeErr != nil {
				return classifyQueryError(executionContext, fmt.Errorf("close ClickHouse fixed value timechart result stream: %w", closeErr))
			}
			return publishFixedValueTimechart(executionContext, sink, buffered)
		case clickhouse.TimechartModeRuntimeWideValue:
			buffered, readErr := readValueTimechartRows(
				executionContext,
				rows,
				columns,
				columnTypes,
				*query.Timechart,
			)
			if readErr != nil {
				return readErr
			}
			closeErr := rows.Close()
			rowsClosed = true
			if closeErr != nil {
				return classifyQueryError(executionContext, fmt.Errorf("close ClickHouse split value timechart result stream: %w", closeErr))
			}
			return publishValueTimechart(executionContext, sink, buffered)
		}
		buffered, err := readTimechartRows(
			executionContext,
			rows,
			columns,
			columnTypes,
			*query.Timechart,
		)
		if err != nil {
			return err
		}
		closeErr := rows.Close()
		rowsClosed = true
		if closeErr != nil {
			return classifyQueryError(executionContext, fmt.Errorf("close ClickHouse timechart result stream: %w", closeErr))
		}
		return publishTimechart(executionContext, sink, buffered)
	}
	if query.Chart != nil {
		// The column domain is only known after the complete scan, so the
		// bounded pivot is buffered and published as one schema-then-rows unit.
		buffered, err := readChartRows(executionContext, rows, columns, columnTypes, *query.Chart)
		if err != nil {
			return err
		}
		closeErr := rows.Close()
		rowsClosed = true
		if closeErr != nil {
			return classifyQueryError(executionContext, fmt.Errorf("close ClickHouse chart result stream: %w", closeErr))
		}
		return publishChart(executionContext, sink, *query.Chart, buffered)
	}
	containerTransports, optionalMultivalueTransports, err := validateOrdinaryResultColumns(
		query,
		columns,
		columnTypes,
		sparseFieldIndex,
	)
	if err != nil {
		return err
	}
	stringOrBytesTransports, err := validateStringOrBytesResultColumns(
		query,
		columns,
		columnTypes,
	)
	if err != nil {
		return err
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(query.OutputFields))}
	for index, columnType := range columnTypes[:len(query.OutputFields)] {
		kind, multivalue := schemaKind(columns[index], columnType.DatabaseTypeName())
		if optionalMultivalueTransports[index].valid {
			kind = searchjobs.ValueKindList
			multivalue = true
		}
		if stringOrBytesTransports[index].valid {
			kind = searchjobs.ValueKindMixed
			multivalue = false
		}
		column := searchjobs.Column{
			Name:       columns[index],
			Kind:       kind,
			Nullable:   optionalMultivalueTransports[index].valid || columnType.Nullable() || databaseTypeNullable(columnType.DatabaseTypeName()) || kind == searchjobs.ValueKindMixed,
			Multivalue: multivalue,
		}
		if len(resultPresentations) != 0 {
			presentation := resultPresentations[index]
			if presentation.StatsSparkline {
				if kind != searchjobs.ValueKindList || !multivalue {
					return fmt.Errorf(
						"%w: sparkline presentation metadata does not match column %q",
						searchjobs.ErrInvalidResult,
						columns[index],
					)
				}
				column.StatsSparkline = true
			}
			if presentation.HasFlatMultivalueDelimiter {
				if kind != searchjobs.ValueKindList || !multivalue {
					return fmt.Errorf(
						"%w: result presentation metadata does not match column %q",
						searchjobs.ErrInvalidResult,
						columns[index],
					)
				}
				column.FlatMultivalueDelimiter = presentation.FlatMultivalueDelimiter
				column.HasFlatMultivalueDelimiter = true
			}
		}
		schema.Columns[index] = column
	}
	if err := executionContext.Err(); err != nil {
		return err
	}

	schemaPublished := false
	atomicResult := query.RequiresAtomicResult()
	var atomicRows atomicResultBuffer
	destinations, err := scanDestinations(columnTypes)
	if err != nil {
		return fmt.Errorf("%w: prepare ClickHouse row scan: %w", searchjobs.ErrInvalidResult, err)
	}
	defer clearScanDestinations(destinations)
	for rows.Next() {
		if err := executionContext.Err(); err != nil {
			return err
		}
		clearScanDestinations(destinations)
		if err := rows.Scan(destinations...); err != nil {
			return classifyQueryError(executionContext, fmt.Errorf("scan ClickHouse result row: %w", err))
		}
		var fieldNames []string
		if sparseFieldIndex >= 0 {
			var ok bool
			fieldNames, ok = scannedValue(destinations[len(query.OutputFields)]).([]string)
			if !ok {
				return fmt.Errorf("%w: sparse event fields metadata has an invalid native type", searchjobs.ErrInvalidResult)
			}
		}
		values := make([]searchjobs.Value, len(query.OutputFields))
		for index, destination := range destinations[:len(query.OutputFields)] {
			var value searchjobs.Value
			if optionalMultivalueTransports[index].valid {
				value, err = convertOptionalMultivalueOutput(
					destinations,
					index,
					optionalMultivalueTransports[index],
				)
			} else if containerTransports[index].valid {
				names, types, version, metadataErr := scannedContainerMetadata(
					destinations,
					containerTransports[index],
				)
				if metadataErr != nil {
					return fmt.Errorf(
						"%w: convert ClickHouse column %q: %w",
						searchjobs.ErrInvalidResult,
						columns[index],
						metadataErr,
					)
				}
				value, err = convertContainerOutput(
					scannedValue(destination),
					names,
					types,
					version,
				)
			} else if index == sparseFieldIndex {
				value, err = convertSparseEventFields(scannedValue(destination), fieldNames)
			} else if stringOrBytesTransports[index].valid {
				value, err = convertStringOrBytesOutput(
					destinations,
					index,
					stringOrBytesTransports[index],
				)
			} else {
				value, err = convertValue(scannedValue(destination))
			}
			if err != nil {
				return fmt.Errorf("%w: convert ClickHouse column %q: %w", searchjobs.ErrInvalidResult, columns[index], err)
			}
			values[index] = value
		}
		if atomicResult {
			if err := atomicRows.append(values); err != nil {
				return err
			}
			continue
		}
		if !schemaPublished {
			if err := executionContext.Err(); err != nil {
				return err
			}
			if err := sink.SetSchema(schema); err != nil {
				return err
			}
			schemaPublished = true
		}
		if err := sink.AddRow(values); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return classifyQueryError(executionContext, fmt.Errorf("iterate ClickHouse results: %w", err))
	}
	if err := executionContext.Err(); err != nil {
		return err
	}
	if atomicResult {
		closeErr := rows.Close()
		rowsClosed = true
		if closeErr != nil {
			return classifyQueryError(
				executionContext,
				fmt.Errorf("close ClickHouse atomic result stream: %w", closeErr),
			)
		}
		if err := executionContext.Err(); err != nil {
			return err
		}
		if err := sink.SetSchema(schema); err != nil {
			return err
		}
		for block := atomicRows.first; block != nil; block = block.next {
			for index := 0; index < block.count; index++ {
				if err := executionContext.Err(); err != nil {
					return err
				}
				if err := sink.AddRow(block.rows[index]); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if !schemaPublished {
		return sink.SetSchema(schema)
	}
	return nil
}

const atomicRowsPerBlock = 256

// atomicBufferedRowBlock retains a fixed number of outer row-slice headers in
// one allocation. Charging the complete block before allocating it accounts
// for unused capacity exactly while avoiding one heap allocation per result
// row on large atomic Dynamic-expression searches.
type atomicBufferedRowBlock struct {
	rows  [atomicRowsPerBlock][]searchjobs.Value
	count int
	next  *atomicBufferedRowBlock
}

type atomicResultBuffer struct {
	first *atomicBufferedRowBlock
	last  *atomicBufferedRowBlock
	bytes uint64
}

func (buffer *atomicResultBuffer) append(values []searchjobs.Value) error {
	newBlock := buffer.last == nil || buffer.last.count == atomicRowsPerBlock
	structural := uint64(0)
	if newBlock {
		structural = uint64(unsafe.Sizeof(atomicBufferedRowBlock{}))
	}
	nextBytes, err := chargeAtomicResultRow(buffer.bytes, structural, values)
	if err != nil {
		return err
	}
	if newBlock {
		block := new(atomicBufferedRowBlock)
		if buffer.last == nil {
			buffer.first = block
		} else {
			buffer.last.next = block
		}
		buffer.last = block
	}
	buffer.last.rows[buffer.last.count] = values
	buffer.last.count++
	buffer.bytes = nextBytes
	return nil
}

func chargeAtomicResultRow(current, structural uint64, values []searchjobs.Value) (uint64, error) {
	if current > maximumAtomicResultBytes {
		return 0, searchjobs.ErrByteLimit
	}
	if structural > maximumAtomicResultBytes-current {
		return 0, searchjobs.ErrByteLimit
	}
	current += structural
	for _, value := range values {
		remaining := maximumAtomicResultBytes - current
		size, err := value.RetainedSizeBytes()
		if err != nil {
			return 0, fmt.Errorf(
				"%w: size retained atomic ClickHouse result value: %w",
				searchjobs.ErrInvalidResult,
				err,
			)
		}
		if size > remaining {
			return 0, searchjobs.ErrByteLimit
		}
		current += size
	}
	return current, nil
}

func validateSparseFieldsOutput(query clickhouse.CompiledQuery) (int, error) {
	if !query.SparseFields {
		return -1, nil
	}
	fieldIndex := slices.Index(query.OutputFields, "fields")
	if query.Timechart != nil || query.Chart != nil || fieldIndex < 0 {
		return -1, fmt.Errorf("%w: compiled sparse event fields contract is invalid", searchjobs.ErrInvalidResult)
	}
	seen := make(map[string]struct{}, len(query.OutputFields))
	for _, field := range query.OutputFields {
		if field == clickhouse.SparseEventFieldNamesColumn {
			return -1, fmt.Errorf("%w: compiled sparse event fields contract exposes a private column", searchjobs.ErrInvalidResult)
		}
		if _, duplicate := seen[field]; duplicate {
			return -1, fmt.Errorf("%w: compiled sparse event fields contract has duplicate columns", searchjobs.ErrInvalidResult)
		}
		seen[field] = struct{}{}
	}
	return fieldIndex, nil
}

func (executor *Executor) settingsFor(query clickhouse.CompiledQuery) clickhousedriver.Settings {
	settings, _ := executor.settingsForContext(context.Background(), query)
	return settings
}

func (executor *Executor) settingsForContext(
	ctx context.Context,
	query clickhouse.CompiledQuery,
) (clickhousedriver.Settings, error) {
	base, expand := executor.settingsSnapshot()
	if policy, ok := searchlimits.FromContext(ctx); ok {
		config, err := ConfigFromPolicy(policy)
		if err != nil {
			return nil, fmt.Errorf("execute ClickHouse search: admitted limits are invalid: %w", err)
		}
		base, err = validatedQuerySettings(config)
		if err != nil {
			return nil, fmt.Errorf("execute ClickHouse search: admitted limits are invalid: %w", err)
		}
		expand = true
	}
	settings := groupLimitSettingsFor(base, expand, query)
	hint, ok, err := query.StatsPartitionsMaxThreadsHintContext(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return settings, nil
	}
	current, ok := settings["max_threads"].(uint64)
	if !ok || current <= uint64(hint) {
		return settings, nil
	}
	bounded := maps.Clone(settings)
	bounded["max_threads"] = uint64(hint)
	return bounded, nil
}

func groupLimitSettingsFor(
	settings *validatedExecutorSettings,
	expandTimechartGroupLimit bool,
	query clickhouse.CompiledQuery,
) clickhousedriver.Settings {
	if settings == nil {
		return nil
	}
	base := settings.values
	percentileWide := (query.Timechart != nil &&
		query.Timechart.Mode == clickhouse.TimechartModeRuntimeWideValue &&
		query.Timechart.ValueKind == clickhouse.TimechartValueKindPercentile) ||
		(query.Chart != nil &&
			query.Chart.ValueKind == clickhouse.ChartValueKindPercentile)
	if !expandTimechartGroupLimit && !percentileWide {
		return base
	}
	// A fixed timechart groups only by bucket. Runtime-wide timecharts must first
	// aggregate every distinct raw (bucket, label) pair to rank the exact top
	// series, so output width cannot size their group budget. Count, sum, and
	// average receive the fixed 130k raw-work allowance. Split percentiles are
	// capped independently at 20k sketches, even when the general configured
	// group limit is higher. Exceeding either bound fails instead of silently
	// approximating the result. The base setting governs ordinary queries.
	var required uint64
	switch {
	case query.Timechart != nil:
		required = query.Timechart.BucketCount
		if query.Timechart.Mode == clickhouse.TimechartModeRuntimeWide ||
			query.Timechart.Mode == clickhouse.TimechartModeRuntimeWideValue {
			required = maximumRuntimeWideTimechartGroups
			if query.Timechart.Mode == clickhouse.TimechartModeRuntimeWideValue &&
				query.Timechart.ValueKind == clickhouse.TimechartValueKindPercentile {
				required = maximumRuntimeWidePercentileGroups
			}
		}
	case query.Chart != nil:
		if query.Chart.ValueKind == clickhouse.ChartValueKindPercentile {
			required = maximumRuntimeWidePercentileGroups
		} else {
			// Count, sum, and average charts aggregate the exact raw (row, label)
			// pairs before reducing their output to a bounded row-by-series shape.
			// Output width cannot size that pre-ranking work, so use the same fixed
			// 130k allowance as runtime-wide timechart.
			required = maximumRuntimeWideTimechartGroups
		}
	default:
		return base
	}
	current, ok := base["max_rows_to_group_by"].(uint64)
	if !ok {
		return base
	}
	if percentileWide && current > required {
		adjusted := maps.Clone(base)
		adjusted["max_rows_to_group_by"] = required
		return adjusted
	}
	if current >= required || !expandTimechartGroupLimit {
		return base
	}
	adjusted := maps.Clone(base)
	adjusted["max_rows_to_group_by"] = required
	return adjusted
}

// wideTimechartRow is one bucket of a split-by timechart grid: the bucket
// boundary plus one cell per public series column.
type wideTimechartRow[T any] struct {
	bucket time.Time
	cells  []T
}

type timechartRow = wideTimechartRow[uint64]

type bufferedTimechart struct {
	columns []string
	rows    []timechartRow
}

type timechartValueRow = wideTimechartRow[nullableFloat64]

type bufferedValueTimechart struct {
	columns []string
	rows    []timechartValueRow
}

func validateTimechartOutput(query clickhouse.CompiledQuery) error {
	output := query.Timechart
	if output == nil {
		return nil
	}
	if output.Span <= 0 || output.Span%time.Second != 0 || output.Span > 24*time.Hour ||
		output.BucketCount == 0 || output.BucketCount > maximumTimechartBuckets {
		return fmt.Errorf("%w: compiled timechart output contract is invalid", searchjobs.ErrInvalidResult)
	}
	switch output.Mode {
	case clickhouse.TimechartModeFixedCount:
		if !slices.Equal(query.OutputFields, []string{"_time", "count"}) ||
			output.MaxSeries != 1 || output.MaxLabelBytes != 0 ||
			output.ValueField != "" ||
			output.ValueKind != clickhouse.TimechartValueKindInvalid {
			return fmt.Errorf("%w: compiled fixed timechart output contract is invalid", searchjobs.ErrInvalidResult)
		}
	case clickhouse.TimechartModeFixedFieldCount:
		resolved, resolveErr := plan.ResolveField(output.ValueField, spl.Range{})
		if resolveErr != nil || resolved.Name != output.ValueField ||
			output.ValueField == "" || output.ValueField == "_time" ||
			!slices.Equal(query.OutputFields, []string{"_time", output.ValueField}) ||
			output.MaxSeries != 1 || output.MaxLabelBytes != 0 ||
			output.ValueKind != clickhouse.TimechartValueKindInvalid {
			return fmt.Errorf("%w: compiled fixed field-count timechart output contract is invalid", searchjobs.ErrInvalidResult)
		}
	case clickhouse.TimechartModeFixedValue:
		resolvedValueField, valueFieldErr := plan.ResolveField(
			output.ValueField,
			spl.Range{},
		)
		if len(query.OutputFields) != 2 || query.OutputFields[0] != "_time" ||
			query.OutputFields[1] == "" || query.OutputFields[1] == "_time" ||
			query.OutputFields[1] != output.ValueField || output.MaxSeries != 1 ||
			output.MaxLabelBytes != 0 || valueFieldErr != nil ||
			resolvedValueField.Name != output.ValueField ||
			!output.ValueKind.Valid() {
			return fmt.Errorf("%w: compiled fixed value timechart output contract is invalid", searchjobs.ErrInvalidResult)
		}
	case clickhouse.TimechartModeRuntimeWide:
		if !slices.Equal(query.OutputFields, []string{"_time"}) ||
			!output.RuntimeWideBoundsValid() ||
			output.ValueField != "" ||
			output.ValueKind != clickhouse.TimechartValueKindInvalid {
			return fmt.Errorf("%w: compiled dynamic timechart output contract is invalid", searchjobs.ErrInvalidResult)
		}
	case clickhouse.TimechartModeRuntimeWideValue:
		if !slices.Equal(query.OutputFields, []string{"_time"}) ||
			!output.RuntimeWideBoundsValid() ||
			output.ValueField != "" ||
			!output.ValueKind.Valid() {
			return fmt.Errorf("%w: compiled split value timechart output contract is invalid", searchjobs.ErrInvalidResult)
		}
	default:
		return fmt.Errorf("%w: compiled timechart output mode is invalid", searchjobs.ErrInvalidResult)
	}
	first := output.FirstBucket
	spanSeconds := int64(output.Span / time.Second)
	if first.IsZero() || first.Location() != time.UTC ||
		first.Nanosecond() != 0 || first.Unix()%spanSeconds != 0 {
		return fmt.Errorf("%w: compiled timechart bucket origin is invalid", searchjobs.ErrInvalidResult)
	}
	if _, ok := checkedBucketBoundary(first.Unix(), spanSeconds, output.BucketCount); !ok {
		return fmt.Errorf("%w: compiled timechart bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
	}
	return nil
}

type bufferedFixedTimechart struct {
	first      time.Time
	span       time.Duration
	countField string
	counts     []uint64
}

func readFixedTimechartRows(
	ctx context.Context,
	rows driver.Rows,
	columns []string,
	columnTypes []driver.ColumnType,
	output clickhouse.TimechartOutput,
) (bufferedFixedTimechart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedFixedTimechart{}, err
	}
	fieldOccurrenceCount := output.Mode == clickhouse.TimechartModeFixedFieldCount
	contracts := []resultColumnContract{
		{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		{name: clickhouse.TimechartCountColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
	}
	if fieldOccurrenceCount {
		contracts = append(contracts, resultColumnContract{
			name:         clickhouse.TimechartInputPresentColumn,
			databaseType: "UInt8",
			scanType:     reflect.TypeFor[uint8](),
		})
	}
	if err := validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType|resultColumnTrimDatabaseType,
		"ClickHouse fixed timechart",
	); err != nil {
		return bufferedFixedTimechart{}, err
	}

	bucketCapacity := safecast.MustConv[int](output.BucketCount)
	buffered := bufferedFixedTimechart{
		first:      output.FirstBucket,
		span:       output.Span,
		countField: "count",
		counts:     make([]uint64, 0, bucketCapacity),
	}
	if fieldOccurrenceCount {
		buffered.countField = output.ValueField
	}
	sawPositiveCount := false
	var upstreamPresent uint8
	haveUpstreamPresence := false
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedFixedTimechart{}, err
		}
		if uint64(len(buffered.counts)) >= output.BucketCount {
			return bufferedFixedTimechart{}, fmt.Errorf(
				"%w: ClickHouse fixed timechart returned too many buckets",
				searchjobs.ErrInvalidResult,
			)
		}
		var ordinal, count uint64
		var rowUpstreamPresent uint8
		var scanErr error
		if fieldOccurrenceCount {
			scanErr = rows.Scan(&ordinal, &count, &rowUpstreamPresent)
		} else {
			scanErr = rows.Scan(&ordinal, &count)
		}
		if scanErr != nil {
			return bufferedFixedTimechart{}, classifyQueryError(
				ctx,
				fmt.Errorf("scan ClickHouse fixed timechart result row: %w", scanErr),
			)
		}
		if err := ctx.Err(); err != nil {
			return bufferedFixedTimechart{}, err
		}
		rowIndex := len(buffered.counts)
		if ordinal >= output.BucketCount ||
			ordinal != uint64(rowIndex) {
			return bufferedFixedTimechart{}, fmt.Errorf(
				"%w: ClickHouse fixed timechart row is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		if fieldOccurrenceCount {
			if rowUpstreamPresent > 1 ||
				(haveUpstreamPresence && rowUpstreamPresent != upstreamPresent) ||
				(rowUpstreamPresent == 0 && count != 0) {
				return bufferedFixedTimechart{}, fmt.Errorf(
					"%w: ClickHouse fixed timechart input-presence transport is invalid",
					searchjobs.ErrInvalidResult,
				)
			}
			upstreamPresent = rowUpstreamPresent
			haveUpstreamPresence = true
		}
		sawPositiveCount = sawPositiveCount || count > 0
		buffered.counts = append(buffered.counts, count)
	}
	if err := rows.Err(); err != nil {
		return bufferedFixedTimechart{}, classifyQueryError(
			ctx,
			fmt.Errorf("iterate ClickHouse fixed timechart results: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return bufferedFixedTimechart{}, err
	}
	if uint64(len(buffered.counts)) != output.BucketCount {
		return bufferedFixedTimechart{}, fmt.Errorf(
			"%w: ClickHouse fixed timechart returned an incomplete bucket sequence",
			searchjobs.ErrInvalidResult,
		)
	}
	if (fieldOccurrenceCount && upstreamPresent == 0) ||
		(!fieldOccurrenceCount && !sawPositiveCount) {
		// A wholly empty upstream relation is represented by the exact zero grid
		// so truncation remains detectable, but Splunk publishes no result rows.
		buffered.counts = nil
	}
	return buffered, nil
}

type bufferedFixedValueTimechart struct {
	first      time.Time
	span       time.Duration
	valueField string
	values     []nullableFloat64
}

type nullableFloat64 struct {
	value float64
	valid bool
}

// publicValue maps a buffered numeric cell onto the public value the sink
// publishes: NULL when the bucket carried no value.
func (value nullableFloat64) publicValue() searchjobs.Value {
	if !value.valid {
		return searchjobs.NullValue()
	}
	return searchjobs.DoubleValue(value.value)
}

func readFixedValueTimechartRows(
	ctx context.Context,
	rows driver.Rows,
	columns []string,
	columnTypes []driver.ColumnType,
	output clickhouse.TimechartOutput,
) (bufferedFixedValueTimechart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedFixedValueTimechart{}, err
	}
	contracts := []resultColumnContract{
		{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		{
			name:         clickhouse.TimechartValueColumn,
			databaseType: "Nullable(Float64)",
			scanType:     reflect.TypeFor[*float64](),
			nullable:     true,
		},
		{name: clickhouse.TimechartInputPresentColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	if err := validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType|resultColumnTrimDatabaseType,
		"ClickHouse fixed value timechart",
	); err != nil {
		return bufferedFixedValueTimechart{}, err
	}

	bucketCapacity := safecast.MustConv[int](output.BucketCount)
	buffered := bufferedFixedValueTimechart{
		first:      output.FirstBucket,
		span:       output.Span,
		valueField: output.ValueField,
		values:     make([]nullableFloat64, 0, bucketCapacity),
	}
	var upstreamPresent uint8
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedFixedValueTimechart{}, err
		}
		if uint64(len(buffered.values)) >= output.BucketCount {
			return bufferedFixedValueTimechart{}, fmt.Errorf(
				"%w: ClickHouse fixed value timechart returned too many buckets",
				searchjobs.ErrInvalidResult,
			)
		}
		var ordinal uint64
		var value *float64
		var rowUpstreamPresent uint8
		if err := rows.Scan(&ordinal, &value, &rowUpstreamPresent); err != nil {
			return bufferedFixedValueTimechart{}, classifyQueryError(
				ctx,
				fmt.Errorf("scan ClickHouse fixed value timechart result row: %w", err),
			)
		}
		if err := ctx.Err(); err != nil {
			return bufferedFixedValueTimechart{}, err
		}
		rowIndex := len(buffered.values)
		if ordinal >= output.BucketCount || ordinal != uint64(rowIndex) ||
			rowUpstreamPresent > 1 || (rowIndex > 0 && rowUpstreamPresent != upstreamPresent) ||
			(rowUpstreamPresent == 0 && value != nil) {
			return bufferedFixedValueTimechart{}, fmt.Errorf(
				"%w: ClickHouse fixed value timechart row is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		if rowIndex == 0 {
			upstreamPresent = rowUpstreamPresent
		}
		bufferedValue := nullableFloat64{}
		if value != nil {
			if output.ValueKind == clickhouse.TimechartValueKindPercentile &&
				(math.IsNaN(*value) || math.IsInf(*value, 0)) {
				return bufferedFixedValueTimechart{}, fmt.Errorf(
					"%w: ClickHouse fixed percentile timechart value is not finite",
					searchjobs.ErrInvalidResult,
				)
			}
			bufferedValue = nullableFloat64{value: *value, valid: true}
		}
		buffered.values = append(buffered.values, bufferedValue)
	}
	if err := rows.Err(); err != nil {
		return bufferedFixedValueTimechart{}, classifyQueryError(
			ctx,
			fmt.Errorf("iterate ClickHouse fixed value timechart results: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return bufferedFixedValueTimechart{}, err
	}
	if uint64(len(buffered.values)) != output.BucketCount {
		return bufferedFixedValueTimechart{}, fmt.Errorf(
			"%w: ClickHouse fixed value timechart returned an incomplete bucket sequence",
			searchjobs.ErrInvalidResult,
		)
	}
	if upstreamPresent == 0 {
		// The complete zero-presence grid proves both that the transport was not
		// truncated and that no upstream row exists. Preserve the static schema
		// while suppressing every public result row, matching fixed count.
		buffered.values = nil
	}
	return buffered, nil
}

func readTimechartRows(ctx context.Context, rows driver.Rows, columns []string, columnTypes []driver.ColumnType, output clickhouse.TimechartOutput) (bufferedTimechart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedTimechart{}, err
	}
	contracts := []resultColumnContract{
		{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
		{name: clickhouse.TimechartCountsColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()},
		{name: clickhouse.TimechartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	if err := validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType|resultColumnTrimDatabaseType,
		"ClickHouse timechart",
	); err != nil {
		return bufferedTimechart{}, err
	}

	bucketCapacity := safecast.MustConv[int](output.BucketCount)
	buffered := bufferedTimechart{rows: make([]timechartRow, 0, bucketCapacity)}
	var encodedNames []string
	destinations, err := scanDestinations(columnTypes)
	if err != nil {
		return bufferedTimechart{}, fmt.Errorf(
			"%w: prepare ClickHouse timechart row scan: %w",
			searchjobs.ErrInvalidResult,
			err,
		)
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedTimechart{}, err
		}
		if uint64(len(buffered.rows)) >= output.BucketCount {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart returned too many buckets", searchjobs.ErrInvalidResult)
		}
		if err := rows.Scan(destinations...); err != nil {
			return bufferedTimechart{}, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse timechart result row: %w", err))
		}
		ordinal, names, counts, invalid, err := scannedTimechartRow(destinations)
		if err != nil {
			return bufferedTimechart{}, err
		}
		rowIndex := len(buffered.rows)
		if len(counts) > int(output.MaxSeries) ||
			(rowIndex == 0 && len(names) != len(counts)) ||
			(rowIndex > 0 && (len(names) != 0 || len(counts) != len(encodedNames))) {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart series arrays are invalid", searchjobs.ErrInvalidResult)
		}
		if ordinal >= output.BucketCount || ordinal != uint64(rowIndex) {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart ordinal sequence is invalid", searchjobs.ErrInvalidResult)
		}
		bucketUnix, ok := checkedBucketBoundary(output.FirstBucket.Unix(), int64(output.Span/time.Second), ordinal)
		if !ok {
			return bufferedTimechart{}, fmt.Errorf("%w: compiled timechart bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
		}
		bucket := time.Unix(bucketUnix, 0).UTC()
		if rowIndex == 0 {
			publicColumns, validateErr := decodeSeriesNames(names, output.MaxLabelBytes, "_time")
			if validateErr != nil {
				return bufferedTimechart{}, validateErr
			}
			encodedNames = slices.Clone(names)
			buffered.columns = publicColumns
		}
		if invalid != 0 {
			return bufferedTimechart{}, searchjobs.ErrUnsupportedValue
		}
		buffered.rows = append(buffered.rows, timechartRow{bucket: bucket, cells: slices.Clone(counts)})
	}
	if err := rows.Err(); err != nil {
		return bufferedTimechart{}, classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse timechart results: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return bufferedTimechart{}, err
	}
	if uint64(len(buffered.rows)) != output.BucketCount {
		return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart returned an incomplete bucket sequence", searchjobs.ErrInvalidResult)
	}
	return buffered, nil
}

func scannedTimechartRow(destinations []any) (uint64, []string, []uint64, uint8, error) {
	if len(destinations) != 4 {
		return 0, nil, nil, 0, fmt.Errorf("%w: ClickHouse timechart row has an invalid width", searchjobs.ErrInvalidResult)
	}
	ordinal, ordinalOK := scannedValue(destinations[0]).(uint64)
	names, namesOK := scannedValue(destinations[1]).([]string)
	counts, countsOK := scannedValue(destinations[2]).([]uint64)
	invalid, invalidOK := scannedValue(destinations[3]).(uint8)
	if !ordinalOK || !namesOK || !countsOK || !invalidOK {
		return 0, nil, nil, 0, fmt.Errorf("%w: ClickHouse timechart row has invalid native values", searchjobs.ErrInvalidResult)
	}
	return ordinal, names, counts, invalid, nil
}

func readValueTimechartRows(
	ctx context.Context,
	rows driver.Rows,
	columns []string,
	columnTypes []driver.ColumnType,
	output clickhouse.TimechartOutput,
) (bufferedValueTimechart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedValueTimechart{}, err
	}
	contracts := []resultColumnContract{
		{name: clickhouse.TimechartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
		{name: clickhouse.TimechartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
		{name: clickhouse.TimechartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeFor[[]float64]()},
		{name: clickhouse.TimechartValuePresentColumn, databaseType: "Array(UInt8)", scanType: reflect.TypeFor[[]uint8]()},
		{name: clickhouse.TimechartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
	}
	if err := validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType|resultColumnTrimDatabaseType,
		"ClickHouse split value timechart",
	); err != nil {
		return bufferedValueTimechart{}, err
	}

	bucketCapacity := safecast.MustConv[int](output.BucketCount)
	buffered := bufferedValueTimechart{rows: make([]timechartValueRow, 0, bucketCapacity)}
	var encodedNames []string
	destinations, err := scanDestinations(columnTypes)
	if err != nil {
		return bufferedValueTimechart{}, fmt.Errorf(
			"%w: prepare ClickHouse split value timechart row scan: %w",
			searchjobs.ErrInvalidResult,
			err,
		)
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedValueTimechart{}, err
		}
		if uint64(len(buffered.rows)) >= output.BucketCount {
			return bufferedValueTimechart{}, fmt.Errorf(
				"%w: ClickHouse split value timechart returned too many buckets",
				searchjobs.ErrInvalidResult,
			)
		}
		if err := rows.Scan(destinations...); err != nil {
			return bufferedValueTimechart{}, classifyQueryError(
				ctx,
				fmt.Errorf("scan ClickHouse split value timechart result row: %w", err),
			)
		}
		ordinal, names, values, present, invalid, err := scannedValueTimechartRow(destinations)
		if err != nil {
			return bufferedValueTimechart{}, err
		}
		rowIndex := len(buffered.rows)
		if len(values) != len(present) || len(values) > int(output.MaxSeries) ||
			(rowIndex == 0 && len(names) != len(values)) ||
			(rowIndex > 0 && (len(names) != 0 || len(values) != len(encodedNames))) {
			return bufferedValueTimechart{}, fmt.Errorf(
				"%w: ClickHouse split value timechart series arrays are invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		rowValues := make([]nullableFloat64, len(values))
		for index, presence := range present {
			if presence > 1 || (presence == 0 && values[index] != 0) {
				return bufferedValueTimechart{}, fmt.Errorf(
					"%w: ClickHouse split value timechart presence array is invalid",
					searchjobs.ErrInvalidResult,
				)
			}
			if presence == 1 {
				if output.ValueKind == clickhouse.TimechartValueKindPercentile &&
					(math.IsNaN(values[index]) || math.IsInf(values[index], 0)) {
					return bufferedValueTimechart{}, fmt.Errorf(
						"%w: ClickHouse split percentile timechart value is not finite",
						searchjobs.ErrInvalidResult,
					)
				}
				rowValues[index] = nullableFloat64{value: values[index], valid: true}
			}
		}

		if ordinal >= output.BucketCount || ordinal != uint64(rowIndex) {
			return bufferedValueTimechart{}, fmt.Errorf(
				"%w: ClickHouse split value timechart ordinal sequence is invalid",
				searchjobs.ErrInvalidResult,
			)
		}
		bucketUnix, ok := checkedBucketBoundary(
			output.FirstBucket.Unix(),
			int64(output.Span/time.Second),
			ordinal,
		)
		if !ok {
			return bufferedValueTimechart{}, fmt.Errorf(
				"%w: compiled timechart bucket arithmetic overflowed",
				searchjobs.ErrInvalidResult,
			)
		}
		if rowIndex == 0 {
			publicColumns, validateErr := decodeSeriesNames(names, output.MaxLabelBytes, "_time")
			if validateErr != nil {
				return bufferedValueTimechart{}, validateErr
			}
			encodedNames = slices.Clone(names)
			buffered.columns = publicColumns
		}
		if invalid != 0 {
			return bufferedValueTimechart{}, searchjobs.ErrUnsupportedValue
		}
		buffered.rows = append(buffered.rows, timechartValueRow{
			bucket: time.Unix(bucketUnix, 0).UTC(),
			cells:  rowValues,
		})
	}
	if err := rows.Err(); err != nil {
		return bufferedValueTimechart{}, classifyQueryError(
			ctx,
			fmt.Errorf("iterate ClickHouse split value timechart results: %w", err),
		)
	}
	if err := ctx.Err(); err != nil {
		return bufferedValueTimechart{}, err
	}
	if uint64(len(buffered.rows)) != output.BucketCount {
		return bufferedValueTimechart{}, fmt.Errorf(
			"%w: ClickHouse split value timechart returned an incomplete bucket sequence",
			searchjobs.ErrInvalidResult,
		)
	}
	return buffered, nil
}

func scannedValueTimechartRow(
	destinations []any,
) (uint64, []string, []float64, []uint8, uint8, error) {
	if len(destinations) != 5 {
		return 0, nil, nil, nil, 0, fmt.Errorf(
			"%w: ClickHouse split value timechart row has an invalid width",
			searchjobs.ErrInvalidResult,
		)
	}
	ordinal, ordinalOK := scannedValue(destinations[0]).(uint64)
	names, namesOK := scannedValue(destinations[1]).([]string)
	values, valuesOK := scannedValue(destinations[2]).([]float64)
	present, presentOK := scannedValue(destinations[3]).([]uint8)
	invalid, invalidOK := scannedValue(destinations[4]).(uint8)
	if !ordinalOK || !namesOK || !valuesOK || !presentOK || !invalidOK {
		return 0, nil, nil, nil, 0, fmt.Errorf(
			"%w: ClickHouse split value timechart row has invalid native values",
			searchjobs.ErrInvalidResult,
		)
	}
	return ordinal, names, values, present, invalid, nil
}

// decodeSeriesNames validates and decodes the wide transport's encoded series
// array. fixedColumn seeds the public collision domain: no runtime series may
// take the name of the result's first, plan-time column.
func decodeSeriesNames(encoded []string, maxLabelBytes uint16, fixedColumn string) ([]string, error) {
	public := make([]string, len(encoded))
	seenEncoded := make(map[string]struct{}, len(encoded))
	seenPublic := make(map[string]string, len(encoded)+1)
	seenPublic[fixedColumn] = ""
	lastOrdinary := ""
	haveOrdinary := false
	phase := uint8(0) // ordinary, optional NULL, optional OTHER
	for index, name := range encoded {
		if !utf8.ValidString(name) {
			return nil, fmt.Errorf("%w: ClickHouse wide-result series encoding is invalid", searchjobs.ErrInvalidResult)
		}
		if _, exists := seenEncoded[name]; exists {
			return nil, fmt.Errorf("%w: ClickHouse wide-result series is duplicated", searchjobs.ErrInvalidResult)
		}
		seenEncoded[name] = struct{}{}
		var decoded string
		switch {
		case strings.HasPrefix(name, "0:"):
			if phase != 0 {
				return nil, fmt.Errorf("%w: ClickHouse wide-result series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			raw := name[2:]
			if raw == "" || len(raw) > int(maxLabelBytes) || raw == "NULL" || raw == "OTHER" {
				return nil, fmt.Errorf("%w: ClickHouse wide-result series label is invalid", searchjobs.ErrInvalidResult)
			}
			decoded = raw
			if strings.HasPrefix(decoded, "_") {
				decoded = "VALUE" + decoded
			}
			if prior, exists := seenPublic[decoded]; exists {
				if prior != name {
					// Distinct runtime strings can converge after Splunk's
					// leading-underscore normalization.
					return nil, searchjobs.ErrUnsupportedValue
				}
				return nil, fmt.Errorf("%w: ClickHouse wide-result series is duplicated", searchjobs.ErrInvalidResult)
			}
			if haveOrdinary && lastOrdinary >= decoded {
				return nil, fmt.Errorf("%w: ClickHouse wide-result series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			lastOrdinary, haveOrdinary = decoded, true
		case name == "1:":
			if phase != 0 {
				return nil, fmt.Errorf("%w: ClickHouse wide-result series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			decoded = "NULL"
			phase = 1
		case name == "2:":
			if phase == 2 {
				return nil, fmt.Errorf("%w: ClickHouse wide-result series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			decoded = "OTHER"
			phase = 2
		default:
			return nil, fmt.Errorf("%w: ClickHouse wide-result series encoding is invalid", searchjobs.ErrInvalidResult)
		}
		if prior, exists := seenPublic[decoded]; exists {
			if prior != name && strings.HasPrefix(name, "0:") {
				return nil, searchjobs.ErrUnsupportedValue
			}
			return nil, fmt.Errorf("%w: ClickHouse wide-result public series is duplicated", searchjobs.ErrInvalidResult)
		}
		seenPublic[decoded] = name
		public[index] = decoded
	}
	return public, nil
}

// publishFixedGrid publishes a dense single-series timechart grid: one row per
// buffered cell, keyed by the bucket boundary derived from first and span.
func publishFixedGrid[T any](
	ctx context.Context,
	sink searchjobs.ResultSink,
	first time.Time,
	span time.Duration,
	column searchjobs.Column,
	cells []T,
	cell func(T) searchjobs.Value,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	spanSeconds := int64(span / time.Second)
	if len(cells) > 0 {

		if _, ok := checkedBucketBoundary(
			first.Unix(),
			spanSeconds,
			safecast.MustConv[uint64](len(cells)-1),
		); !ok {
			return fmt.Errorf(
				"%w: compiled timechart bucket arithmetic overflowed",
				searchjobs.ErrInvalidResult,
			)
		}
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		column,
	}}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}
	for ordinal, value := range cells {
		if err := ctx.Err(); err != nil {
			return err
		}

		bucketUnix := first.Unix() + safecast.MustConv[int64](ordinal)*spanSeconds
		if err := sink.AddRow([]searchjobs.Value{
			searchjobs.TimeValue(time.Unix(bucketUnix, 0).UTC()),
			cell(value),
		}); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// publishWideGrid publishes a split-by timechart grid: a _time column followed
// by one column per public series.
func publishWideGrid[T any](
	ctx context.Context,
	sink searchjobs.ResultSink,
	columns []string,
	seriesKind searchjobs.ValueKind,
	nullable bool,
	rows []wideTimechartRow[T],
	cell func(T) searchjobs.Value,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(columns)+1)}
	schema.Columns[0] = searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}
	for index, name := range columns {
		schema.Columns[index+1] = searchjobs.Column{
			Name:     name,
			Kind:     seriesKind,
			Nullable: nullable,
		}
	}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}
	if len(columns) == 0 {
		return ctx.Err()
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		values := make([]searchjobs.Value, len(row.cells)+1)
		values[0] = searchjobs.TimeValue(row.bucket)
		for index, value := range row.cells {
			values[index+1] = cell(value)
		}
		if err := sink.AddRow(values); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func publishFixedTimechart(
	ctx context.Context,
	sink searchjobs.ResultSink,
	buffered bufferedFixedTimechart,
) error {
	return publishFixedGrid(
		ctx,
		sink,
		buffered.first,
		buffered.span,
		searchjobs.Column{Name: buffered.countField, Kind: searchjobs.ValueKindUnsigned},
		buffered.counts,
		searchjobs.UnsignedValue,
	)
}

func publishFixedValueTimechart(
	ctx context.Context,
	sink searchjobs.ResultSink,
	buffered bufferedFixedValueTimechart,
) error {
	return publishFixedGrid(
		ctx,
		sink,
		buffered.first,
		buffered.span,
		searchjobs.Column{Name: buffered.valueField, Kind: searchjobs.ValueKindDouble, Nullable: true},
		buffered.values,
		nullableFloat64.publicValue,
	)
}

func publishTimechart(ctx context.Context, sink searchjobs.ResultSink, buffered bufferedTimechart) error {
	return publishWideGrid(
		ctx,
		sink,
		buffered.columns,
		searchjobs.ValueKindUnsigned,
		false,
		buffered.rows,
		searchjobs.UnsignedValue,
	)
}

func publishValueTimechart(
	ctx context.Context,
	sink searchjobs.ResultSink,
	buffered bufferedValueTimechart,
) error {
	return publishWideGrid(
		ctx,
		sink,
		buffered.columns,
		searchjobs.ValueKindDouble,
		true,
		buffered.rows,
		nullableFloat64.publicValue,
	)
}

type chartRow struct {
	value  searchjobs.Value
	counts []uint64
}

type chartValueRow struct {
	value  searchjobs.Value
	values []nullableFloat64
}

type bufferedChart struct {
	columns   []string
	rows      []chartRow
	valueRows []chartValueRow
}

// chartRowScanType binds the compiler's closed row-type contract to the exact
// native value clickhouse-go must expose. Keeping the database type and public
// kind in the same mapping prevents either half of forged metadata from
// weakening the transport check.
func chartRowScanType(kind clickhouse.ChartRowKind, databaseType string) (reflect.Type, bool) {
	switch databaseType {
	case "String":
		if kind == clickhouse.ChartRowKindString || kind == clickhouse.ChartRowKindMixed {
			return reflect.TypeFor[string](), true
		}
	case "Int64":
		if kind == clickhouse.ChartRowKindSigned {
			return reflect.TypeFor[int64](), true
		}
	case "UInt8":
		if kind == clickhouse.ChartRowKindUnsigned {
			return reflect.TypeFor[uint8](), true
		}
	case "UInt64":
		if kind == clickhouse.ChartRowKindUnsigned {
			return reflect.TypeFor[uint64](), true
		}
	case "Float64":
		if kind == clickhouse.ChartRowKindDouble {
			return reflect.TypeFor[float64](), true
		}
	case "Bool":
		if kind == clickhouse.ChartRowKindBool {
			return reflect.TypeFor[bool](), true
		}
	case "DateTime64(9, 'UTC')":
		if kind == clickhouse.ChartRowKindTime {
			return reflect.TypeFor[time.Time](), true
		}
	}
	return nil, false
}

func validateChartOutput(query clickhouse.CompiledQuery) error {
	output := query.Chart
	if output == nil {
		return nil
	}
	if output.RowField == "" || !utf8.ValidString(output.RowField) ||
		!slices.Equal(query.OutputFields, []string{output.RowField}) ||
		output.RowLimit == 0 || output.RowLimit > maximumChartRows ||
		output.MaxSeries == 0 || output.MaxSeries > maximumChartSeries ||
		output.MaxLabelBytes == 0 || output.MaxLabelBytes > maximumChartLabel ||
		strings.TrimSpace(output.RowDatabaseType) == "" {
		return fmt.Errorf("%w: compiled chart output contract is invalid", searchjobs.ErrInvalidResult)
	}
	if output.RowSemanticBytes && output.RowKind != clickhouse.ChartRowKindMixed {
		return fmt.Errorf("%w: compiled chart semantic Bytes transport is invalid", searchjobs.ErrInvalidResult)
	}
	if _, ok := chartRowScanType(output.RowKind, output.RowDatabaseType); !ok {
		return fmt.Errorf("%w: compiled chart row transport is invalid", searchjobs.ErrInvalidResult)
	}
	if !output.ValueKind.Valid() {
		return fmt.Errorf("%w: compiled chart value kind is invalid", searchjobs.ErrInvalidResult)
	}
	return nil
}

// readChartRows buffers the bounded pivot. The row count is runtime data, so
// the invariants replace timechart's fixed-axis equalities: at most RowLimit
// rows, ordinals dense and ascending from zero in the server's declared order,
// and one stable column domain for every row.
func readChartRows(
	ctx context.Context,
	rows driver.Rows,
	columns []string,
	columnTypes []driver.ColumnType,
	output clickhouse.ChartOutput,
) (bufferedChart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedChart{}, err
	}
	rowKind, rowKindOK := searchjobs.ChartRowValueKind(output.RowKind)
	rowScanType, rowScanTypeOK := chartRowScanType(output.RowKind, output.RowDatabaseType)
	if !rowKindOK || !rowScanTypeOK {
		return bufferedChart{}, fmt.Errorf("%w: compiled chart row transport is invalid", searchjobs.ErrInvalidResult)
	}
	contracts := []resultColumnContract(nil)
	switch output.ValueKind {
	case clickhouse.ChartValueKindCount:
		contracts = []resultColumnContract{
			{name: clickhouse.ChartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			{name: clickhouse.ChartRowColumn, databaseType: output.RowDatabaseType, scanType: rowScanType},
		}
		if output.RowSemanticBytes {
			contracts = append(contracts, resultColumnContract{
				name:         clickhouse.ChartRowSemanticBytesColumn,
				databaseType: "UInt8",
				scanType:     reflect.TypeFor[uint8](),
			})
		}
		contracts = append(contracts,
			resultColumnContract{name: clickhouse.ChartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
			resultColumnContract{name: clickhouse.ChartCountsColumn, databaseType: "Array(UInt64)", scanType: reflect.TypeFor[[]uint64]()},
			resultColumnContract{name: clickhouse.ChartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		)
	case clickhouse.ChartValueKindSum, clickhouse.ChartValueKindAverage,
		clickhouse.ChartValueKindPercentile:
		contracts = []resultColumnContract{
			{name: clickhouse.ChartOrdinalColumn, databaseType: "UInt64", scanType: reflect.TypeFor[uint64]()},
			{name: clickhouse.ChartRowColumn, databaseType: output.RowDatabaseType, scanType: rowScanType},
		}
		if output.RowSemanticBytes {
			contracts = append(contracts, resultColumnContract{
				name:         clickhouse.ChartRowSemanticBytesColumn,
				databaseType: "UInt8",
				scanType:     reflect.TypeFor[uint8](),
			})
		}
		contracts = append(contracts,
			resultColumnContract{name: clickhouse.ChartNamesColumn, databaseType: "Array(String)", scanType: reflect.TypeFor[[]string]()},
			resultColumnContract{name: clickhouse.ChartValuesColumn, databaseType: "Array(Float64)", scanType: reflect.TypeFor[[]float64]()},
			resultColumnContract{name: clickhouse.ChartValuePresentColumn, databaseType: "Array(UInt8)", scanType: reflect.TypeFor[[]uint8]()},
			resultColumnContract{name: clickhouse.ChartInvalidColumn, databaseType: "UInt8", scanType: reflect.TypeFor[uint8]()},
		)
	default:
		return bufferedChart{}, fmt.Errorf("%w: compiled chart value kind is invalid", searchjobs.ErrInvalidResult)
	}
	if err := validateResultColumns(
		columns,
		columnTypes,
		contracts,
		resultColumnRequireScanType|resultColumnTrimDatabaseType,
		"ClickHouse chart",
	); err != nil {
		return bufferedChart{}, err
	}

	buffered := bufferedChart{}
	var encodedNames []string
	retainedBytes := chartBufferedBaseBytes
	destinations, err := scanDestinations(columnTypes)
	if err != nil {
		return bufferedChart{}, fmt.Errorf("%w: prepare ClickHouse chart row scan: %w", searchjobs.ErrInvalidResult, err)
	}
	defer clearScanDestinations(destinations)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedChart{}, err
		}
		rowIndex := len(buffered.rows) + len(buffered.valueRows)
		if uint64(rowIndex) >= output.RowLimit {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart returned too many rows", searchjobs.ErrExecutionLimit)
		}
		if err := rows.Scan(destinations...); err != nil {
			return bufferedChart{}, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse chart result row: %w", err))
		}
		var (
			ordinal  uint64
			rowValue searchjobs.Value
			names    []string
			counts   []uint64
			values   []nullableFloat64
			invalid  uint8
		)
		switch output.ValueKind {
		case clickhouse.ChartValueKindCount:
			ordinal, rowValue, names, counts, invalid, err = scannedChartRow(
				destinations,
				rowKind,
				output.RowSemanticBytes,
			)
		case clickhouse.ChartValueKindSum, clickhouse.ChartValueKindAverage,
			clickhouse.ChartValueKindPercentile:
			ordinal, rowValue, names, values, invalid, err = scannedNumericChartRow(
				destinations,
				rowKind,
				output.RowSemanticBytes,
				output.MaxSeries,
				output.ValueKind == clickhouse.ChartValueKindPercentile,
			)
		}
		if err != nil {
			return bufferedChart{}, err
		}
		seriesCount := len(counts)
		if output.ValueKind != clickhouse.ChartValueKindCount {
			seriesCount = len(values)
		}
		if len(names) != seriesCount || len(names) > int(output.MaxSeries) {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart series arrays are invalid", searchjobs.ErrInvalidResult)
		}
		if ordinal != uint64(rowIndex) {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart ordinal sequence is invalid", searchjobs.ErrInvalidResult)
		}
		var domainBytes uint64
		if rowIndex == 0 {
			publicColumns, validateErr := decodeSeriesNames(names, output.MaxLabelBytes, output.RowField)
			if validateErr != nil {
				return bufferedChart{}, validateErr
			}
			encodedNames = slices.Clone(names)
			buffered.columns = publicColumns
			domainBytes = chartDomainRetainedBytes(encodedNames, publicColumns)
		} else if !slices.Equal(names, encodedNames) {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart series changed between rows", searchjobs.ErrInvalidResult)
		}
		if invalid != 0 {
			return bufferedChart{}, searchjobs.ErrUnsupportedValue
		}
		// Every compiler-produced public row has at least one valid split
		// domain member. Empty arrays are reserved for an invalid sentinel or
		// a truly empty result stream, never a row whose data may be dropped.
		if len(names) == 0 {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart row has an empty series domain", searchjobs.ErrInvalidResult)
		}
		rowBytes := chartRowRetainedBytes(scannedValue(destinations[1]), seriesCount, output.ValueKind)
		if domainBytes > maximumChartResultBytes-retainedBytes {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart series domain exceeded the supported result size", searchjobs.ErrExecutionLimit)
		}
		rowBytes += domainBytes
		if rowBytes > maximumChartResultBytes-retainedBytes {
			return bufferedChart{}, fmt.Errorf("%w: ClickHouse chart row values exceeded the supported result size", searchjobs.ErrExecutionLimit)
		}
		retainedBytes += rowBytes
		if output.ValueKind == clickhouse.ChartValueKindCount {
			clonedCounts := make([]uint64, len(counts))
			copy(clonedCounts, counts)
			buffered.rows = append(buffered.rows, chartRow{value: rowValue, counts: clonedCounts})
		} else {
			buffered.valueRows = append(buffered.valueRows, chartValueRow{value: rowValue, values: values})
		}
		clearScanDestinations(destinations)
	}
	if err := rows.Err(); err != nil {
		return bufferedChart{}, classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse chart results: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return bufferedChart{}, err
	}
	return buffered, nil
}

// chartRowRetainedBytes is a conservative reservation for one buffered pivot
// row. It includes the complete row struct, outer-slice capacity slack, and
// the exact cell backing-array element size. Only the cloned row payload is
// unbounded. Measuring the scanned value avoids another copy merely to size it.
func chartRowRetainedBytes(scanned any, seriesCount int, valueKind clickhouse.ChartValueKind) uint64 {
	payload := uint64(0)
	switch typed := scanned.(type) {
	case string:
		payload = uint64(len(typed))
	case []byte:
		payload = uint64(len(typed))
	}
	cellBytes := chartCountCellBytes
	rowBytes := chartRowOverheadBytes
	if valueKind == clickhouse.ChartValueKindSum ||
		valueKind == clickhouse.ChartValueKindAverage ||
		valueKind == clickhouse.ChartValueKindPercentile {
		cellBytes = chartValueCellBytes
		rowBytes = chartValueRowOverheadBytes
	}

	return payload + safecast.MustConv[uint64](seriesCount)*cellBytes + rowBytes
}

// chartDomainRetainedBytes reserves both domain slice backings with up to 2x
// capacity slack and both encoded/public string payloads. Most public names are
// substrings of their encoded names, so charging both is deliberately
// conservative; normalized leading-underscore names really do allocate both.
func chartDomainRetainedBytes(encoded, public []string) uint64 {

	stringSlots := 2 * safecast.MustConv[uint64](len(encoded)+len(public)) * chartStringHeaderBytes
	payload := uint64(0)
	for _, value := range encoded {

		payload += safecast.MustConv[uint64](len(value))
	}
	for _, value := range public {

		payload += safecast.MustConv[uint64](len(value))
	}
	return stringSlots + payload
}

// clearScanDestinations releases driver-owned row/array payloads after every
// value needed by the buffer has been cloned. Reusing the destination pointers
// avoids per-row allocations without retaining a second copy of the last row.
func clearScanDestinations(destinations []any) {
	for _, destination := range destinations {
		value := reflect.ValueOf(destination)
		if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || !value.Elem().CanSet() {
			continue
		}
		value.Elem().Set(reflect.Zero(value.Elem().Type()))
	}
}

// chartRowTransport validates the fixed transport prologue every chart row
// shares: the destination width implied by the optional semantic Bytes flag
// column, and the flag itself when the transport carries one.
func chartRowTransport(
	destinations []any,
	semanticBytesTransport bool,
	dataColumns int,
	invalidWidthMessage string,
) (int, uint8, error) {
	namesIndex := 2
	semanticBytes := uint8(0)
	if semanticBytesTransport {
		namesIndex = 3
	}
	if len(destinations) != namesIndex+dataColumns {
		return 0, 0, fmt.Errorf("%w: %s", searchjobs.ErrInvalidResult, invalidWidthMessage)
	}
	if semanticBytesTransport {
		var ok bool
		semanticBytes, ok = scannedValue(destinations[2]).(uint8)
		if !ok || semanticBytes > 1 {
			return 0, 0, fmt.Errorf("%w: ClickHouse chart semantic Bytes flag is invalid", searchjobs.ErrInvalidResult)
		}
	}
	return namesIndex, semanticBytes, nil
}

// scannedChartRowValue decodes the row value column every chart row shares and
// checks it against the compiled row kind.
func scannedChartRowValue(
	destinations []any,
	rowKind searchjobs.ValueKind,
	semanticBytesTransport bool,
	semanticBytes uint8,
) (searchjobs.Value, error) {
	scanned := scannedValue(destinations[1])
	if scanned == nil {
		return searchjobs.Value{}, fmt.Errorf("%w: ClickHouse chart row value is null", searchjobs.ErrInvalidResult)
	}
	value, err := convertValue(scanned)
	if semanticBytesTransport {
		value, err = convertSemanticStringOrBytes(scanned, semanticBytes, false)
	}
	if err != nil {
		return searchjobs.Value{}, fmt.Errorf("%w: ClickHouse chart row value cannot be converted", searchjobs.ErrInvalidResult)
	}
	if rowKind == searchjobs.ValueKindMixed {
		// The Mixed row column is the String transport stats BY publishes for
		// _raw, whose bytes may not be valid UTF-8. Those are exactly the two
		// cell kinds a String column can produce, and nothing else.
		if kind := value.Kind(); kind != searchjobs.ValueKindString && kind != searchjobs.ValueKindBytes {
			return searchjobs.Value{}, fmt.Errorf("%w: ClickHouse chart row value does not match the compiled row kind", searchjobs.ErrInvalidResult)
		}
	} else if value.Kind() != rowKind {
		return searchjobs.Value{}, fmt.Errorf("%w: ClickHouse chart row value does not match the compiled row kind", searchjobs.ErrInvalidResult)
	}
	return value, nil
}

func scannedChartRow(
	destinations []any,
	rowKind searchjobs.ValueKind,
	semanticBytesTransport bool,
) (uint64, searchjobs.Value, []string, []uint64, uint8, error) {
	invalidResult := func(message string) (uint64, searchjobs.Value, []string, []uint64, uint8, error) {
		return 0, searchjobs.Value{}, nil, nil, 0, fmt.Errorf("%w: %s", searchjobs.ErrInvalidResult, message)
	}
	namesIndex, semanticBytes, err := chartRowTransport(
		destinations,
		semanticBytesTransport,
		3,
		"ClickHouse chart row has an invalid width",
	)
	if err != nil {
		return 0, searchjobs.Value{}, nil, nil, 0, err
	}
	ordinal, ordinalOK := scannedValue(destinations[0]).(uint64)
	names, namesOK := scannedValue(destinations[namesIndex]).([]string)
	counts, countsOK := scannedValue(destinations[namesIndex+1]).([]uint64)
	invalid, invalidOK := scannedValue(destinations[namesIndex+2]).(uint8)
	if !ordinalOK || !namesOK || !countsOK || !invalidOK {
		return invalidResult("ClickHouse chart row has invalid native values")
	}
	value, err := scannedChartRowValue(destinations, rowKind, semanticBytesTransport, semanticBytes)
	if err != nil {
		return 0, searchjobs.Value{}, nil, nil, 0, err
	}
	return ordinal, value, names, counts, invalid, nil
}

func scannedNumericChartRow(
	destinations []any,
	rowKind searchjobs.ValueKind,
	semanticBytesTransport bool,
	maxSeries uint16,
	requireFinite bool,
) (uint64, searchjobs.Value, []string, []nullableFloat64, uint8, error) {
	invalidResult := func(message string) (uint64, searchjobs.Value, []string, []nullableFloat64, uint8, error) {
		return 0, searchjobs.Value{}, nil, nil, 0, fmt.Errorf("%w: %s", searchjobs.ErrInvalidResult, message)
	}
	namesIndex, semanticBytes, err := chartRowTransport(
		destinations,
		semanticBytesTransport,
		4,
		"ClickHouse numeric chart row has an invalid width",
	)
	if err != nil {
		return 0, searchjobs.Value{}, nil, nil, 0, err
	}
	ordinal, ordinalOK := scannedValue(destinations[0]).(uint64)
	names, namesOK := scannedValue(destinations[namesIndex]).([]string)
	rawValues, valuesOK := scannedValue(destinations[namesIndex+1]).([]float64)
	present, presentOK := scannedValue(destinations[namesIndex+2]).([]uint8)
	invalid, invalidOK := scannedValue(destinations[namesIndex+3]).(uint8)
	if !ordinalOK || !namesOK || !valuesOK || !presentOK || !invalidOK {
		return invalidResult("ClickHouse numeric chart row has invalid native values")
	}
	if len(rawValues) != len(present) || len(names) != len(rawValues) || len(rawValues) > int(maxSeries) {
		return invalidResult("ClickHouse numeric chart series arrays are invalid")
	}
	values := make([]nullableFloat64, len(rawValues))
	for index, presence := range present {
		if presence > 1 || (presence == 0 && rawValues[index] != 0) {
			return invalidResult("ClickHouse numeric chart presence array is invalid")
		}
		if presence == 1 {
			if requireFinite && (math.IsNaN(rawValues[index]) || math.IsInf(rawValues[index], 0)) {
				return invalidResult("ClickHouse percentile chart value is not finite")
			}
			values[index] = nullableFloat64{value: rawValues[index], valid: true}
		}
	}
	value, err := scannedChartRowValue(destinations, rowKind, semanticBytesTransport, semanticBytes)
	if err != nil {
		return 0, searchjobs.Value{}, nil, nil, 0, err
	}
	return ordinal, value, names, values, invalid, nil
}

func publishChart(ctx context.Context, sink searchjobs.ResultSink, output clickhouse.ChartOutput, buffered bufferedChart) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rowKind, ok := searchjobs.ChartRowValueKind(output.RowKind)
	if !ok {
		return fmt.Errorf("%w: compiled chart row kind is invalid", searchjobs.ErrInvalidResult)
	}
	var seriesKind searchjobs.ValueKind
	seriesNullable := false
	switch output.ValueKind {
	case clickhouse.ChartValueKindCount:
		seriesKind = searchjobs.ValueKindUnsigned
		if len(buffered.valueRows) != 0 {
			return fmt.Errorf("%w: buffered chart value kind is invalid", searchjobs.ErrInvalidResult)
		}
	case clickhouse.ChartValueKindSum, clickhouse.ChartValueKindAverage,
		clickhouse.ChartValueKindPercentile:
		seriesKind = searchjobs.ValueKindDouble
		seriesNullable = true
		if len(buffered.rows) != 0 {
			return fmt.Errorf("%w: buffered chart value kind is invalid", searchjobs.ErrInvalidResult)
		}
	default:
		return fmt.Errorf("%w: compiled chart value kind is invalid", searchjobs.ErrInvalidResult)
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(buffered.columns)+1)}
	// A Mixed row column mirrors the ordinary result path, which declares every
	// Mixed column nullable so the same field publishes one schema either way.
	schema.Columns[0] = searchjobs.Column{
		Name:     output.RowField,
		Kind:     rowKind,
		Nullable: rowKind == searchjobs.ValueKindMixed,
	}
	for index, name := range buffered.columns {
		schema.Columns[index+1] = searchjobs.Column{Name: name, Kind: seriesKind, Nullable: seriesNullable}
	}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}
	if len(buffered.columns) == 0 {
		// A chart with no eligible input publishes only its declared row column.
		return ctx.Err()
	}
	if output.ValueKind == clickhouse.ChartValueKindCount {
		for _, row := range buffered.rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			values := make([]searchjobs.Value, len(row.counts)+1)
			values[0] = row.value
			for index, count := range row.counts {
				values[index+1] = searchjobs.UnsignedValue(count)
			}
			if err := sink.AddRow(values); err != nil {
				return err
			}
		}
		return ctx.Err()
	}
	for _, row := range buffered.valueRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		values := make([]searchjobs.Value, len(row.values)+1)
		values[0] = row.value
		for index, value := range row.values {
			values[index+1] = searchjobs.NullValue()
			if value.valid {
				values[index+1] = searchjobs.DoubleValue(value.value)
			}
		}
		if err := sink.AddRow(values); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func scanDestinations(columnTypes []driver.ColumnType) ([]any, error) {
	destinations := make([]any, len(columnTypes))
	for index, columnType := range columnTypes {
		base := unwrapType(columnType.DatabaseTypeName())
		if strings.HasPrefix(base, "Dynamic") || strings.HasPrefix(base, "Variant") {
			// clickhouse-go reports interface{} as the scan type for Dynamic,
			// then delegates a concrete alternative directly to that
			// interface destination. Int256/UInt256 do not support that path.
			// The explicit wrapper accepts every Dynamic alternative and
			// preserves its physical type for convertValue.
			destinations[index] = new(chcol.Dynamic)
			continue
		}
		scanType := columnType.ScanType()
		if scanType == nil {
			return nil, fmt.Errorf("column %q has no scan type", columnType.Name())
		}
		destinations[index] = reflect.New(scanType).Interface()
	}
	return destinations, nil
}

func scannedValue(destination any) any {
	value := reflect.ValueOf(destination)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return nil
	}
	return value.Interface()
}

func schemaKind(field, databaseType string) (searchjobs.ValueKind, bool) {
	base := unwrapType(databaseType)
	if (field == "_raw" && base == "String") || strings.HasPrefix(base, "Dynamic") || strings.HasPrefix(base, "Variant") || strings.HasPrefix(base, "Tuple") {
		return searchjobs.ValueKindMixed, false
	}
	switch {
	case base == "Bool":
		return searchjobs.ValueKindBool, false
	case base == "Int128" || base == "Int256" || base == "UInt128" || base == "UInt256":
		return searchjobs.ValueKindDecimal, false
	case strings.HasPrefix(base, "Int"):
		return searchjobs.ValueKindSigned, false
	case strings.HasPrefix(base, "UInt"):
		return searchjobs.ValueKindUnsigned, false
	case strings.HasPrefix(base, "Float"):
		return searchjobs.ValueKindDouble, false
	case strings.HasPrefix(base, "Decimal"):
		return searchjobs.ValueKindDecimal, false
	case base == "String" || strings.HasPrefix(base, "FixedString") || strings.HasPrefix(base, "Enum") || base == "UUID" || strings.HasPrefix(base, "IPv"):
		return searchjobs.ValueKindString, false
	case strings.HasPrefix(base, "Date"):
		return searchjobs.ValueKindTime, false
	case base == "Time" || strings.HasPrefix(base, "Time64"):
		return searchjobs.ValueKindDuration, false
	case strings.HasPrefix(base, "Array"):
		return searchjobs.ValueKindList, true
	case strings.HasPrefix(base, "Map") || strings.HasPrefix(base, "JSON") || strings.HasPrefix(base, "Object"):
		return searchjobs.ValueKindObject, false
	default:
		return searchjobs.ValueKindMixed, false
	}
}

func unwrapType(value string) string {
	value = strings.TrimSpace(value)
	for {
		for _, wrapper := range []string{"LowCardinality", "Nullable"} {
			if inner, ok := unwrapDatabaseTypeWrapper(value, wrapper); ok {
				value = inner
				goto nextWrapper
			}
		}
		return value
	nextWrapper:
	}
}

// databaseTypeNullable supplements database/sql's ColumnType.Nullable result.
// clickhouse-go currently reports false for LowCardinality(Nullable(T)), even
// though the scanned value can be nil. Only wrappers around the entire column
// type count: Array(Nullable(T)) contains nullable elements but is not itself a
// nullable column.
func databaseTypeNullable(databaseType string) bool {
	value := strings.TrimSpace(databaseType)
	for {
		if _, ok := unwrapDatabaseTypeWrapper(value, "Nullable"); ok {
			return true
		}
		inner, ok := unwrapDatabaseTypeWrapper(value, "LowCardinality")
		if !ok {
			return false
		}
		value = inner
	}
}

func unwrapDatabaseTypeWrapper(value, wrapper string) (string, bool) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, wrapper) {
		return "", false
	}
	remainder := strings.TrimSpace(value[len(wrapper):])
	if len(remainder) < 2 || remainder[0] != '(' {
		return "", false
	}

	depth := 0
	var quote byte
	escaped := false
	for index := 0; index < len(remainder); index++ {
		character := remainder[index]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' {
				escaped = true
				continue
			}
			if character == quote {
				quote = 0
			}
			continue
		}
		switch character {
		case '\'', '"':
			quote = character
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 || (depth == 0 && index != len(remainder)-1) {
				return "", false
			}
		}
	}
	if depth != 0 || quote != 0 {
		return "", false
	}
	return strings.TrimSpace(remainder[1 : len(remainder)-1]), true
}

func convertValue(value any) (searchjobs.Value, error) {
	if value == nil {
		return searchjobs.NullValue(), nil
	}
	switch value := value.(type) {
	case chcol.Dynamic:
		if value.Nil() {
			return searchjobs.NullValue(), nil
		}
		return convertValue(value.Any())
	case *chcol.Dynamic:
		if value == nil || value.Nil() {
			return searchjobs.NullValue(), nil
		}
		return convertValue(value.Any())
	case chcol.JSON:
		return convertJSON(&value)
	case *chcol.JSON:
		return convertJSON(value)
	case time.Time:
		return searchjobs.TimeValue(value), nil
	case time.Duration:
		return searchjobs.DurationValue(value), nil
	case decimal.Decimal:
		return searchjobs.DecimalValue(value.String())
	case *decimal.Decimal:
		if value == nil {
			return searchjobs.NullValue(), nil
		}
		return searchjobs.DecimalValue(value.String())
	case big.Int:
		return searchjobs.DecimalValue(value.String())
	case *big.Int:
		if value == nil {
			return searchjobs.NullValue(), nil
		}
		return searchjobs.DecimalValue(value.String())
	case uuid.UUID:
		return searchjobs.StringValue(value.String()), nil
	case net.IP:
		return searchjobs.StringValue(value.String()), nil
	case string:
		if !utf8.ValidString(value) {
			return searchjobs.BytesValue([]byte(value)), nil
		}
		return searchjobs.StringValue(value), nil
	case []byte:
		return searchjobs.BytesValue(value), nil
	case bool:
		return searchjobs.BoolValue(value), nil
	case int:
		return searchjobs.SignedValue(int64(value)), nil
	case int8:
		return searchjobs.SignedValue(int64(value)), nil
	case int16:
		return searchjobs.SignedValue(int64(value)), nil
	case int32:
		return searchjobs.SignedValue(int64(value)), nil
	case int64:
		return searchjobs.SignedValue(value), nil
	case uint:
		return searchjobs.UnsignedValue(uint64(value)), nil
	case uint8:
		return searchjobs.UnsignedValue(uint64(value)), nil
	case uint16:
		return searchjobs.UnsignedValue(uint64(value)), nil
	case uint32:
		return searchjobs.UnsignedValue(uint64(value)), nil
	case uint64:
		return searchjobs.UnsignedValue(value), nil
	case float32:
		return searchjobs.DoubleValue(float64(value)), nil
	case float64:
		return searchjobs.DoubleValue(value), nil
	}
	if decoded, tagged, err := convertExtendedValue(value); tagged {
		return decoded, err
	}

	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return searchjobs.NullValue(), nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() {
		return searchjobs.NullValue(), nil
	}
	switch reflected.Kind() {
	case reflect.Slice, reflect.Array:
		if reflected.Type().Elem().Kind() == reflect.Uint8 {
			bytes := make([]byte, reflected.Len())
			reflect.Copy(reflect.ValueOf(bytes), reflected)
			return searchjobs.BytesValue(bytes), nil
		}
		items := make([]searchjobs.Value, reflected.Len())
		for index := range reflected.Len() {
			item, err := convertValue(reflected.Index(index).Interface())
			if err != nil {
				return searchjobs.Value{}, fmt.Errorf("list item %d: %w", index, err)
			}
			items[index] = item
		}
		list := searchjobs.ListValue(items...)
		if list.Kind() == searchjobs.ValueKindInvalid {
			return searchjobs.Value{}, errors.New("list result exceeds value limits")
		}
		return list, nil
	case reflect.Map:
		if reflected.Type().Key().Kind() != reflect.String {
			return searchjobs.Value{}, fmt.Errorf("map key type %s is not a string", reflected.Type().Key())
		}
		keys := reflected.MapKeys()
		slices.SortFunc(keys, func(left, right reflect.Value) int {
			return strings.Compare(left.String(), right.String())
		})
		fields := make([]searchjobs.ObjectField, len(keys))
		for index, key := range keys {
			child, err := convertValue(reflected.MapIndex(key).Interface())
			if err != nil {
				return searchjobs.Value{}, fmt.Errorf("map field %q: %w", key.String(), err)
			}
			fields[index] = searchjobs.ObjectField{Name: key.String(), Value: child}
		}
		return searchjobs.ObjectValue(fields...)
	default:
		return searchjobs.Value{}, fmt.Errorf("unsupported result type %T", value)
	}
}

// isNilDriverValue recognizes typed nils returned through driver interfaces.
// Keeping this at the package boundary ensures every specialized executor
// rejects them consistently before invoking a driver method.
func isNilDriverValue(value any) bool {
	return nilcheck.IsNil(value)
}

// convertExtendedValue reverses the lossless representation used when an
// ingestion value has no native ClickHouse Dynamic alternative. Only the
// exact reserved two-entry map is treated as an envelope; ordinary maps that
// merely contain a reserved-looking key continue through generic conversion.
func convertExtendedValue(value any) (searchjobs.Value, bool, error) {
	reflected := reflect.ValueOf(value)
	for reflected.IsValid() && (reflected.Kind() == reflect.Pointer || reflected.Kind() == reflect.Interface) {
		if reflected.IsNil() {
			return searchjobs.Value{}, false, nil
		}
		reflected = reflected.Elem()
	}
	if !reflected.IsValid() || reflected.Kind() != reflect.Map || reflected.Type().Key().Kind() != reflect.String || reflected.Len() != 2 {
		return searchjobs.Value{}, false, nil
	}

	var kind, encoded string
	foundType, foundValue := false, false
	for _, key := range reflected.MapKeys() {
		var destination *string
		switch key.String() {
		case extendedTypeKey:
			destination = &kind
			foundType = true
		case extendedValueKey:
			destination = &encoded
			foundValue = true
		default:
			return searchjobs.Value{}, false, nil
		}
		entry := reflected.MapIndex(key)
		for entry.IsValid() && (entry.Kind() == reflect.Pointer || entry.Kind() == reflect.Interface) {
			if entry.IsNil() {
				return searchjobs.Value{}, true, errors.New("decode extended search result: envelope values must be strings")
			}
			entry = entry.Elem()
		}
		if !entry.IsValid() || entry.Kind() != reflect.String {
			return searchjobs.Value{}, true, errors.New("decode extended search result: envelope values must be strings")
		}
		*destination = entry.String()
	}
	if !foundType || !foundValue {
		return searchjobs.Value{}, false, nil
	}

	decoded, err := decodeExtendedValue(kind, encoded)
	if err != nil {
		return searchjobs.Value{}, true, fmt.Errorf("decode extended search result %q: %w", kind, err)
	}
	return decoded, true, nil
}

func decodeExtendedValue(kind, encoded string) (searchjobs.Value, error) {
	switch kind {
	case "bytes/v1":
		decoded, err := base64.RawStdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.RawStdEncoding.EncodeToString(decoded) != encoded {
			return searchjobs.Value{}, errors.New("invalid byte encoding")
		}
		return searchjobs.BytesValue(decoded), nil
	case "timestamp/v1":
		decoded, err := time.Parse(time.RFC3339Nano, encoded)
		if err != nil || decoded.Year() < 1 || decoded.UTC().Format(time.RFC3339Nano) != encoded {
			return searchjobs.Value{}, errors.New("invalid timestamp encoding")
		}
		return searchjobs.TimeValue(decoded), nil
	case "duration/v1":
		decoded, err := decodeExtendedDuration(encoded)
		if err != nil {
			return searchjobs.Value{}, err
		}
		return searchjobs.DurationValue(decoded), nil
	case "decimal/v1":
		if !extendedDecimalPattern.MatchString(encoded) {
			return searchjobs.Value{}, errors.New("invalid decimal encoding")
		}
		decoded, err := searchjobs.DecimalValue(encoded)
		if err != nil {
			return searchjobs.Value{}, errors.New("invalid decimal encoding")
		}
		return decoded, nil
	default:
		return searchjobs.Value{}, errors.New("unknown type tag")
	}
}

func decodeExtendedDuration(encoded string) (time.Duration, error) {
	secondsText, nanosText, found := strings.Cut(encoded, ":")
	if !found || strings.Contains(nanosText, ":") {
		return 0, errors.New("invalid duration encoding")
	}
	seconds, err := strconv.ParseInt(secondsText, 10, 64)
	if err != nil || strconv.FormatInt(seconds, 10) != secondsText {
		return 0, errors.New("invalid duration seconds")
	}
	nanos, err := strconv.ParseInt(nanosText, 10, 32)
	if err != nil || strconv.FormatInt(nanos, 10) != nanosText || nanos < -999_999_999 || nanos > 999_999_999 {
		return 0, errors.New("invalid duration nanoseconds")
	}
	if (seconds < 0 && nanos > 0) || (seconds > 0 && nanos < 0) {
		return 0, errors.New("duration components have inconsistent signs")
	}
	if seconds > math.MaxInt64/int64(time.Second) || seconds < math.MinInt64/int64(time.Second) {
		return 0, errors.New("duration exceeds result range")
	}
	result := time.Duration(seconds) * time.Second
	if (nanos > 0 && result > time.Duration(math.MaxInt64)-time.Duration(nanos)) ||
		(nanos < 0 && result < time.Duration(math.MinInt64)-time.Duration(nanos)) {
		return 0, errors.New("duration exceeds result range")
	}
	return result + time.Duration(nanos), nil
}

func convertJSON(document *chcol.JSON) (searchjobs.Value, error) {
	if document == nil {
		return searchjobs.NullValue(), nil
	}
	values, err := normalizedJSONValues(document)
	if err != nil {
		return searchjobs.Value{}, err
	}
	root := make(map[string]any)
	paths := make([]string, 0, len(values))
	for path := range values {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		segments, parseErr := eventfields.ParseNormalizedDynamicPath(path)
		if parseErr != nil || insertResultPath(root, segments, values[path]) != nil {
			return searchjobs.Value{}, errors.New("JSON result contains invalid field paths")
		}
	}
	return convertValue(root)
}

func convertSparseEventFields(value any, fieldNames []string) (searchjobs.Value, error) {
	var document *chcol.JSON
	switch value := value.(type) {
	case chcol.JSON:
		document = &value
	case *chcol.JSON:
		document = value
	default:
		return searchjobs.Value{}, errors.New("sparse event fields value is not JSON")
	}
	if document == nil {
		return searchjobs.Value{}, errors.New("sparse event fields JSON is nil")
	}
	physicalValues, err := normalizedJSONValues(document)
	if err != nil {
		return searchjobs.Value{}, errors.New("sparse event fields JSON paths are invalid")
	}
	parsedPaths, err := eventfields.ParseStoredFieldNames(fieldNames)
	if err != nil {
		return searchjobs.Value{}, errors.New("sparse event fields metadata is invalid")
	}
	root := make(map[string]any)
	for index, name := range fieldNames {
		fieldValue := any(nil)
		if stored, exists := physicalValues[name]; exists {
			fieldValue = stored
			delete(physicalValues, name)
		}
		if insertResultPath(root, parsedPaths[index], fieldValue) != nil {
			return searchjobs.Value{}, errors.New("sparse event fields metadata paths collide")
		}
	}
	for _, stored := range physicalValues {
		if !isNullJSONPathValue(stored) {
			return searchjobs.Value{}, errors.New("sparse event fields metadata does not match its JSON value")
		}
	}
	return convertValue(root)
}

func normalizedJSONValues(document *chcol.JSON) (map[string]any, error) {
	values := make(map[string]any, len(document.ValuesByPath()))
	for physical, value := range document.ValuesByPath() {
		normalized, err := eventfields.NormalizePhysicalDynamicPath(physical)
		if err != nil {
			return nil, err
		}
		if _, duplicate := values[normalized]; duplicate {
			return nil, errors.New("JSON result paths collide after decoding")
		}
		values[normalized] = value
	}
	return values, nil
}

func insertResultPath(root map[string]any, segments []string, value any) error {
	if len(segments) == 0 {
		return errors.New("result path is empty")
	}
	current := root
	for index, segment := range segments {
		if index == len(segments)-1 {
			if _, exists := current[segment]; exists {
				return errors.New("result path is duplicated")
			}
			current[segment] = value
			return nil
		}
		next, exists := current[segment]
		if !exists {
			nested := make(map[string]any)
			current[segment] = nested
			current = nested
			continue
		}
		nested, ok := next.(map[string]any)
		if !ok {
			return errors.New("result path collides with a scalar")
		}
		current = nested
	}
	return nil
}

func isNullJSONPathValue(value any) bool {
	if isNilDriverValue(value) {
		return true
	}
	switch value := value.(type) {
	case chcol.Dynamic:
		return value.Nil()
	case *chcol.Dynamic:
		return value.Nil()
	default:
		return false
	}
}

func randomQueryID() (string, error) {
	return randomPrefixedQueryID("open-splunk-search-")
}

func randomPrefixedQueryID(prefix string) (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(random[:]), nil
}

var executionLimitMarkers = [...]struct {
	marker  string
	message string
}{
	{clickhouse.RexCaptureLimitMarker, "rex capture bytes exceeded the per-row limit"},
	{clickhouse.SpathInputLimitMarker, "spath input bytes exceeded the per-row limit"},
	{clickhouse.SpathJSONLexemeLimitMarker, "spath JSON tokens exceeded the per-row limit"},
	{clickhouse.NativeMVMembersLimitMarker, "native multivalue members exceeded the per-row limit"},
	{clickhouse.NativeMVPayloadLimitMarker, "native multivalue payload exceeded the per-row limit"},
	{clickhouse.ChartRowLimitMarker, "chart row values exceeded the supported limit"},
	{clickhouse.EventStatsInputLimitMarker, "eventstats input rows exceeded the supported limit"},
	{clickhouse.StreamStatsInputLimitMarker, "streamstats input rows exceeded the supported limit"},
	{clickhouse.StatsMultivalueByExpansionLimitMarker, "stats multivalue BY expansion exceeded the per-event limit"},
	{clickhouse.ExactDistinctLimitMarker, "exact distinct values exceeded the supported limit"},
	{clickhouse.StatsValuesBytesLimitMarker, "stats values bytes exceeded the supported limit"},
	{clickhouse.StatsValuesLimitMarker, "stats values exceeded the supported limit"},
	{clickhouse.EventStatsValuesBytesLimitMarker, "eventstats values bytes exceeded the supported limit"},
	{clickhouse.EventStatsValuesLimitMarker, "eventstats values exceeded the supported limit"},
	{clickhouse.EventStatsListBytesLimitMarker, "eventstats list bytes exceeded the supported limit"},
	{clickhouse.EventStatsListLimitMarker, "eventstats list exceeded the supported limit"},
	{clickhouse.StatsListBytesLimitMarker, "stats list bytes exceeded the supported limit"},
	{clickhouse.StatsListLimitMarker, "stats list exceeded the supported result limit"},
	{clickhouse.StatsSparklineBytesLimitMarker, "stats sparkline bytes exceeded the supported limit"},
	{clickhouse.StatsSparklineLimitMarker, "stats sparkline exceeded the supported limit"},
	{clickhouse.KnowledgeSelectorValueLimitMarker, "knowledge selector value bytes exceeded the per-value limit"},
	{clickhouse.KnowledgeSelectorEventLimitMarker, "knowledge selector input bytes exceeded the per-event limit"},
	{clickhouse.KnowledgeSelectorQueryLimitMarker, "knowledge selector work exceeded the per-query limit"},
	{clickhouse.KnowledgeAliasCopyEventLimitMarker, "knowledge alias copy bytes exceeded the per-event limit"},
	{clickhouse.KnowledgeAliasCopyQueryLimitMarker, "knowledge alias copy work exceeded the per-query limit"},
	{clickhouse.MakeMVRowMembersLimitMarker, "makemv row members exceeded the supported limit"},
	{clickhouse.MakeMVRowBytesLimitMarker, "makemv row bytes exceeded the supported limit"},
	{clickhouse.MakeMVResultMembersLimitMarker, "makemv result members exceeded the supported limit"},
	{clickhouse.MakeMVResultBytesLimitMarker, "makemv result bytes exceeded the supported limit"},
	{clickhouse.MakeMVRetainedBytesLimitMarker, "makemv retained bytes exceeded the supported limit"},
	{clickhouse.MVExpandRowMembersLimitMarker, "mvexpand row members exceeded the supported limit"},
	{clickhouse.MVExpandStageRowsLimitMarker, "mvexpand stage rows exceeded the supported limit"},
	{clickhouse.MVExpandQueryRowsLimitMarker, "mvexpand query rows exceeded the supported limit"},
	{clickhouse.MVExpandRetainedBytesLimitMarker, "mvexpand retained bytes exceeded the supported limit"},
}

func classifyQueryError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		cause := context.Cause(ctx)
		if cause != nil && !errors.Is(cause, context.Canceled) && !errors.Is(cause, context.DeadlineExceeded) {
			return cause
		}
		return ctxErr
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if exception, ok := errors.AsType[*clickhousedriver.Exception](err); ok {
		if exception.Code == 395 {
			for _, classified := range executionLimitMarkers {
				if strings.Contains(exception.Message, classified.marker) {
					return fmt.Errorf("%w: %s", searchjobs.ErrExecutionLimit, classified.message)
				}
			}
		}
		if exception.Code == 395 && (strings.Contains(exception.Message, clickhouse.UnsupportedStatsByValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedExpressionValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedStatsMeasureValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedDedupValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedNumericBinValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedSpathValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedNativeMVValueMarker) ||
			strings.Contains(exception.Message, clickhouse.KnowledgeSelectorInvalidUTF8Marker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedMakeMVValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedMVExpandValueMarker)) {
			// The compiler deliberately emits stable markers when an operation
			// encounters a value outside its supported scalar/type/range
			// contract. Do not retain any surrounding ClickHouse message,
			// generated SQL, or storage detail.
			return searchjobs.ErrUnsupportedValue
		}
		switch exception.Code {
		case 159: // TIMEOUT_EXCEEDED
			return fmt.Errorf("%w: ClickHouse execution timeout", context.DeadlineExceeded)
		case 158, 162, 229, 241, 396: // TOO_MANY_ROWS, TOO_DEEP_SUBQUERIES, QUERY_IS_TOO_LARGE, MEMORY_LIMIT_EXCEEDED, TOO_MANY_ROWS_OR_BYTES
			return fmt.Errorf("%w: ClickHouse resource limit", searchjobs.ErrExecutionLimit)
		case 202, 203, 209, 210, 225, 242, 243, 279, 285, 286, 319, 341, 999:
			return fmt.Errorf("%w: %w", searchjobs.ErrStorageUnavailable, err)
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, clickhousedriver.ErrAcquireConnTimeout) ||
		errors.Is(err, clickhousedriver.ErrConnectionClosed) ||
		errors.Is(err, sqldriver.ErrBadConn) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) {
		return fmt.Errorf("%w: %w", searchjobs.ErrStorageUnavailable, err)
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return fmt.Errorf("%w: %w", searchjobs.ErrStorageUnavailable, err)
	}
	return err
}
