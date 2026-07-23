// Package queryexec streams fully scoped, compiler-produced SQL from
// ClickHouse into the bounded search-job result sink.
package queryexec

import (
	"context"
	"crypto/rand"
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
	"syscall"
	"time"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
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
	maximumTimechartBuckets = uint64(10_000)
	maximumTimechartSeries  = uint16(12)
	maximumTimechartLabel   = uint16(256)

	extendedTypeKey  = "\x00open_splunk_type"
	extendedValueKey = "\x00open_splunk_value"
)

var (
	extendedDecimalPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?(?:0|[1-9][0-9]*))?$`)
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
	// value); bounded timecharts may receive a larger per-query default.
	MaxRowsToGroupBy uint64
	// ExpandTimechartGroupLimit permits only validated timechart queries to
	// raise MaxRowsToGroupBy to BucketCount*(MaxSeries+1), which is bounded at
	// 130,000. It leaves the ordinary GROUP BY cap unchanged. The expansion is
	// enabled automatically when MaxRowsToGroupBy is left at zero.
	ExpandTimechartGroupLimit bool
	MaxThreads                uint64
}

// Executor is a native ClickHouse implementation of searchjobs.Executor.
type Executor struct {
	connection                queryConnection
	settings                  clickhousedriver.Settings
	expandTimechartGroupLimit bool
	newQueryID                func() (string, error)
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
	expandTimechartGroupLimit := config.MaxRowsToGroupBy == 0 || config.ExpandTimechartGroupLimit
	settings, err := querySettings(config)
	if err != nil {
		return nil, err
	}
	return &Executor{
		connection:                connection,
		settings:                  settings,
		expandTimechartGroupLimit: expandTimechartGroupLimit,
		newQueryID:                randomQueryID,
	}, nil
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
	return clickhousedriver.Settings{
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
		"enable_materialized_cte":           uint8(1),
		"short_circuit_function_evaluation": "enable",
		"async_insert":                      uint8(0),
	}, nil
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
	if strings.TrimSpace(query.SQL) == "" || len(query.OutputFields) == 0 {
		return errors.New("execute ClickHouse search: compiled query is incomplete")
	}
	if query.Timechart != nil {
		if err := validateTimechartOutput(query); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	queryID, err := executor.newQueryID()
	if err != nil {
		return fmt.Errorf("execute ClickHouse search: create query ID: %w", err)
	}
	queryContext := clickhousedriver.Context(ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(executor.settingsFor(query)),
	)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return classifyQueryError(ctx, fmt.Errorf("query ClickHouse: %w", err))
	}
	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			resultErr = classifyQueryError(ctx, fmt.Errorf("close ClickHouse result stream: %w", closeErr))
		}
	}()

	columnTypes := rows.ColumnTypes()
	columns := rows.Columns()
	if query.Timechart != nil {
		buffered, err := readTimechartRows(ctx, rows, columns, columnTypes, *query.Timechart)
		if err != nil {
			return err
		}
		closeErr := rows.Close()
		rowsClosed = true
		if closeErr != nil {
			return classifyQueryError(ctx, fmt.Errorf("close ClickHouse timechart result stream: %w", closeErr))
		}
		return publishTimechart(ctx, sink, buffered)
	}
	if len(columns) != len(query.OutputFields) || len(columnTypes) != len(columns) || !slices.Equal(columns, query.OutputFields) {
		return fmt.Errorf("%w: ClickHouse result columns do not match the compiled output", searchjobs.ErrInvalidResult)
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(columns))}
	for index, columnType := range columnTypes {
		kind, multivalue := schemaKind(columns[index], columnType.DatabaseTypeName())
		schema.Columns[index] = searchjobs.Column{
			Name:       columns[index],
			Kind:       kind,
			Nullable:   columnType.Nullable() || databaseTypeNullable(columnType.DatabaseTypeName()) || kind == searchjobs.ValueKindMixed,
			Multivalue: multivalue,
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}

	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		destinations, err := scanDestinations(columnTypes)
		if err != nil {
			return fmt.Errorf("%w: prepare ClickHouse row scan: %v", searchjobs.ErrInvalidResult, err)
		}
		if err := rows.Scan(destinations...); err != nil {
			return classifyQueryError(ctx, fmt.Errorf("scan ClickHouse result row: %w", err))
		}
		values := make([]searchjobs.Value, len(destinations))
		for index, destination := range destinations {
			value, err := convertValue(scannedValue(destination))
			if err != nil {
				return fmt.Errorf("%w: convert ClickHouse column %q: %v", searchjobs.ErrInvalidResult, columns[index], err)
			}
			values[index] = value
		}
		if err := sink.AddRow(values); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse results: %w", err))
	}
	return ctx.Err()
}

func (executor *Executor) settingsFor(query clickhouse.CompiledQuery) clickhousedriver.Settings {
	if query.Timechart == nil || !executor.expandTimechartGroupLimit {
		return executor.settings
	}
	// The first timechart aggregation has one state per non-empty
	// (bucket, series) pair. Reserve room for every public series plus one
	// canonical invalid-value state per bucket. This remains bounded at 130k
	// groups for the validated 10k-bucket/12-series contract. The base setting
	// continues to govern every non-timechart query.
	required := query.Timechart.BucketCount * (uint64(query.Timechart.MaxSeries) + 1)
	current, ok := executor.settings["max_rows_to_group_by"].(uint64)
	if !ok || current >= required {
		return executor.settings
	}
	settings := maps.Clone(executor.settings)
	settings["max_rows_to_group_by"] = required
	return settings
}

type timechartRow struct {
	bucket time.Time
	counts []uint64
}

type bufferedTimechart struct {
	columns []string
	rows    []timechartRow
}

func validateTimechartOutput(query clickhouse.CompiledQuery) error {
	output := query.Timechart
	if output == nil {
		return nil
	}
	if !slices.Equal(query.OutputFields, []string{"_time"}) || output.Span <= 0 || output.Span%time.Second != 0 ||
		output.Span > 24*time.Hour || output.BucketCount == 0 || output.BucketCount > maximumTimechartBuckets ||
		output.MaxSeries == 0 || output.MaxSeries > maximumTimechartSeries ||
		output.MaxLabelBytes == 0 || output.MaxLabelBytes > maximumTimechartLabel {
		return fmt.Errorf("%w: compiled timechart output contract is invalid", searchjobs.ErrInvalidResult)
	}
	first := output.FirstBucket
	spanSeconds := int64(output.Span / time.Second)
	if first.Location() != time.UTC || first.Nanosecond() != 0 || first.Unix()%spanSeconds != 0 {
		return fmt.Errorf("%w: compiled timechart bucket origin is invalid", searchjobs.ErrInvalidResult)
	}
	if _, ok := checkedBucketBoundary(first.Unix(), spanSeconds, output.BucketCount); !ok {
		return fmt.Errorf("%w: compiled timechart bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
	}
	return nil
}

func readTimechartRows(ctx context.Context, rows driver.Rows, columns []string, columnTypes []driver.ColumnType, output clickhouse.TimechartOutput) (bufferedTimechart, error) {
	if err := ctx.Err(); err != nil {
		return bufferedTimechart{}, err
	}
	expectedColumns := []string{
		clickhouse.TimechartOrdinalColumn,
		clickhouse.TimechartNamesColumn,
		clickhouse.TimechartCountsColumn,
		clickhouse.TimechartInvalidColumn,
	}
	if len(columnTypes) != len(expectedColumns) || !slices.Equal(columns, expectedColumns) {
		return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart columns do not match the compiled output", searchjobs.ErrInvalidResult)
	}
	expectedTypes := []string{"UInt64", "Array(String)", "Array(UInt64)", "UInt8"}
	expectedScanTypes := []reflect.Type{
		reflect.TypeOf(uint64(0)),
		reflect.TypeOf([]string{}),
		reflect.TypeOf([]uint64{}),
		reflect.TypeOf(uint8(0)),
	}
	for index, columnType := range columnTypes {
		if isNilDriverValue(columnType) || columnType.Name() != expectedColumns[index] || columnType.Nullable() ||
			strings.TrimSpace(columnType.DatabaseTypeName()) != expectedTypes[index] ||
			columnType.ScanType() != expectedScanTypes[index] {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart column %q has an invalid type", searchjobs.ErrInvalidResult, expectedColumns[index])
		}
	}

	buffered := bufferedTimechart{rows: make([]timechartRow, 0, int(output.BucketCount))}
	var encodedNames []string
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return bufferedTimechart{}, err
		}
		if uint64(len(buffered.rows)) >= output.BucketCount {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart returned too many buckets", searchjobs.ErrInvalidResult)
		}
		destinations, err := scanDestinations(columnTypes)
		if err != nil {
			return bufferedTimechart{}, fmt.Errorf("%w: prepare ClickHouse timechart row scan: %v", searchjobs.ErrInvalidResult, err)
		}
		if err := rows.Scan(destinations...); err != nil {
			return bufferedTimechart{}, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse timechart result row: %w", err))
		}
		ordinal, names, counts, invalid, err := scannedTimechartRow(destinations)
		if err != nil {
			return bufferedTimechart{}, err
		}
		if len(names) != len(counts) || len(names) > int(output.MaxSeries) {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart series arrays are invalid", searchjobs.ErrInvalidResult)
		}
		rowIndex := len(buffered.rows)
		if ordinal >= output.BucketCount || ordinal != uint64(rowIndex) {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart ordinal sequence is invalid", searchjobs.ErrInvalidResult)
		}
		bucketUnix, ok := checkedBucketBoundary(output.FirstBucket.Unix(), int64(output.Span/time.Second), ordinal)
		if !ok {
			return bufferedTimechart{}, fmt.Errorf("%w: compiled timechart bucket arithmetic overflowed", searchjobs.ErrInvalidResult)
		}
		bucket := time.Unix(bucketUnix, 0).UTC()
		if rowIndex == 0 {
			publicColumns, validateErr := decodeTimechartNames(names, output.MaxLabelBytes)
			if validateErr != nil {
				return bufferedTimechart{}, validateErr
			}
			encodedNames = slices.Clone(names)
			buffered.columns = publicColumns
		} else if !slices.Equal(names, encodedNames) {
			return bufferedTimechart{}, fmt.Errorf("%w: ClickHouse timechart series changed between buckets", searchjobs.ErrInvalidResult)
		}
		if invalid != 0 {
			return bufferedTimechart{}, searchjobs.ErrUnsupportedValue
		}
		buffered.rows = append(buffered.rows, timechartRow{bucket: bucket, counts: slices.Clone(counts)})
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

func decodeTimechartNames(encoded []string, maxLabelBytes uint16) ([]string, error) {
	public := make([]string, len(encoded))
	seenEncoded := make(map[string]struct{}, len(encoded))
	seenPublic := make(map[string]string, len(encoded)+1)
	seenPublic["_time"] = ""
	lastOrdinary := ""
	haveOrdinary := false
	phase := uint8(0) // ordinary, optional NULL, optional OTHER
	for index, name := range encoded {
		if !utf8.ValidString(name) {
			return nil, fmt.Errorf("%w: ClickHouse timechart series encoding is invalid", searchjobs.ErrInvalidResult)
		}
		if _, exists := seenEncoded[name]; exists {
			return nil, fmt.Errorf("%w: ClickHouse timechart series is duplicated", searchjobs.ErrInvalidResult)
		}
		seenEncoded[name] = struct{}{}
		var decoded string
		switch {
		case strings.HasPrefix(name, "0:"):
			if phase != 0 {
				return nil, fmt.Errorf("%w: ClickHouse timechart series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			raw := name[2:]
			if raw == "" || len(raw) > int(maxLabelBytes) || raw == "NULL" || raw == "OTHER" {
				return nil, fmt.Errorf("%w: ClickHouse timechart series label is invalid", searchjobs.ErrInvalidResult)
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
				return nil, fmt.Errorf("%w: ClickHouse timechart series is duplicated", searchjobs.ErrInvalidResult)
			}
			if haveOrdinary && lastOrdinary >= decoded {
				return nil, fmt.Errorf("%w: ClickHouse timechart series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			lastOrdinary, haveOrdinary = decoded, true
		case name == "1:":
			if phase != 0 {
				return nil, fmt.Errorf("%w: ClickHouse timechart series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			decoded = "NULL"
			phase = 1
		case name == "2:":
			if phase == 2 {
				return nil, fmt.Errorf("%w: ClickHouse timechart series ordering is invalid", searchjobs.ErrInvalidResult)
			}
			decoded = "OTHER"
			phase = 2
		default:
			return nil, fmt.Errorf("%w: ClickHouse timechart series encoding is invalid", searchjobs.ErrInvalidResult)
		}
		if prior, exists := seenPublic[decoded]; exists {
			if prior != name && strings.HasPrefix(name, "0:") {
				return nil, searchjobs.ErrUnsupportedValue
			}
			return nil, fmt.Errorf("%w: ClickHouse timechart public series is duplicated", searchjobs.ErrInvalidResult)
		}
		seenPublic[decoded] = name
		public[index] = decoded
	}
	return public, nil
}

func publishTimechart(ctx context.Context, sink searchjobs.ResultSink, buffered bufferedTimechart) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(buffered.columns)+1)}
	schema.Columns[0] = searchjobs.Column{Name: "_time", Kind: searchjobs.ValueKindTime}
	for index, name := range buffered.columns {
		schema.Columns[index+1] = searchjobs.Column{Name: name, Kind: searchjobs.ValueKindUnsigned}
	}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}
	if len(buffered.columns) == 0 {
		return ctx.Err()
	}
	for _, row := range buffered.rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		values := make([]searchjobs.Value, len(row.counts)+1)
		values[0] = searchjobs.TimeValue(row.bucket)
		for index, count := range row.counts {
			values[index+1] = searchjobs.UnsignedValue(count)
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
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
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
	if seconds > int64(math.MaxInt64/int64(time.Second)) || seconds < int64(math.MinInt64/int64(time.Second)) {
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
	root := make(map[string]any)
	paths := make([]string, 0, len(document.ValuesByPath()))
	for path := range document.ValuesByPath() {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	for _, path := range paths {
		segments := strings.Split(path, ".")
		current := root
		for index, physical := range segments {
			logical := decodePhysicalPathSegment(physical)
			if logical == "" {
				return searchjobs.Value{}, errors.New("JSON result contains an empty field name")
			}
			if index == len(segments)-1 {
				if _, exists := current[logical]; exists {
					return searchjobs.Value{}, fmt.Errorf("JSON result path %q is duplicated", path)
				}
				current[logical] = document.ValuesByPath()[path]
				continue
			}
			next, exists := current[logical]
			if !exists {
				nested := make(map[string]any)
				current[logical] = nested
				current = nested
				continue
			}
			nested, ok := next.(map[string]any)
			if !ok {
				return searchjobs.Value{}, fmt.Errorf("JSON result path %q collides with a scalar", path)
			}
			current = nested
		}
	}
	return convertValue(root)
}

func decodePhysicalPathSegment(segment string) string {
	segment = strings.ReplaceAll(segment, "%2E", ".")
	return strings.ReplaceAll(segment, "%25", "%")
}

func randomQueryID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "open-splunk-search-" + hex.EncodeToString(random[:]), nil
}

func classifyQueryError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var exception *clickhousedriver.Exception
	if errors.As(err, &exception) {
		if exception.Code == 395 && (strings.Contains(exception.Message, clickhouse.UnsupportedStatsByValueMarker) ||
			strings.Contains(exception.Message, clickhouse.UnsupportedDedupValueMarker)) {
			// The compiler deliberately emits stable markers when a scalar-only
			// operation sees a dynamic non-scalar value. Do not retain any
			// surrounding ClickHouse message, generated SQL, or storage detail.
			return searchjobs.ErrUnsupportedValue
		}
		switch exception.Code {
		case 159: // TIMEOUT_EXCEEDED
			return fmt.Errorf("%w: ClickHouse execution timeout", context.DeadlineExceeded)
		case 158, 229, 241, 396: // TOO_MANY_ROWS, QUERY_IS_TOO_LARGE, MEMORY_LIMIT_EXCEEDED, TOO_MANY_ROWS_OR_BYTES
			return fmt.Errorf("%w: ClickHouse resource limit", searchjobs.ErrExecutionLimit)
		case 202, 203, 209, 210, 225, 242, 243, 279, 285, 286, 319, 341, 999:
			return fmt.Errorf("%w: %v", searchjobs.ErrStorageUnavailable, err)
		}
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, clickhousedriver.ErrConnectionClosed) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ETIMEDOUT) {
		return fmt.Errorf("%w: %v", searchjobs.ErrStorageUnavailable, err)
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		return fmt.Errorf("%w: %v", searchjobs.ErrStorageUnavailable, err)
	}
	return err
}
