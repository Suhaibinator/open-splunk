// Package queryexec streams fully scoped, compiler-produced SQL from
// ClickHouse into the bounded search-job result sink.
package queryexec

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"reflect"
	"slices"
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
	// zero, it follows MaxResultRows (including a caller-supplied value).
	MaxRowsToGroupBy uint64
	MaxThreads       uint64
}

// Executor is a native ClickHouse implementation of searchjobs.Executor.
type Executor struct {
	connection queryConnection
	settings   clickhousedriver.Settings
	newQueryID func() (string, error)
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
	settings, err := querySettings(config)
	if err != nil {
		return nil, err
	}
	return &Executor{connection: connection, settings: settings, newQueryID: randomQueryID}, nil
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
		"readonly":               uint8(2),
		"max_execution_time":     seconds,
		"timeout_overflow_mode":  "throw",
		"max_memory_usage":       config.MaxMemoryBytes,
		"max_rows_to_read":       config.MaxRowsToRead,
		"max_bytes_to_read":      config.MaxBytesToRead,
		"read_overflow_mode":     "throw",
		"max_result_rows":        config.MaxResultRows,
		"max_result_bytes":       config.MaxResultBytes,
		"result_overflow_mode":   "throw",
		"max_rows_to_group_by":   config.MaxRowsToGroupBy,
		"group_by_overflow_mode": "throw",
		"max_threads":            config.MaxThreads,
		"async_insert":           uint8(0),
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
	queryID, err := executor.newQueryID()
	if err != nil {
		return fmt.Errorf("execute ClickHouse search: create query ID: %w", err)
	}
	queryContext := clickhousedriver.Context(ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(executor.settings),
	)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return classifyQueryError(ctx, fmt.Errorf("query ClickHouse: %w", err))
	}
	defer func() {
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			resultErr = classifyQueryError(ctx, fmt.Errorf("close ClickHouse result stream: %w", closeErr))
		}
	}()

	columnTypes := rows.ColumnTypes()
	columns := rows.Columns()
	if len(columns) != len(query.OutputFields) || len(columnTypes) != len(columns) || !slices.Equal(columns, query.OutputFields) {
		return fmt.Errorf("%w: ClickHouse result columns do not match the compiled output", searchjobs.ErrInvalidResult)
	}
	schema := searchjobs.Schema{Columns: make([]searchjobs.Column, len(columns))}
	for index, columnType := range columnTypes {
		kind, multivalue := schemaKind(columns[index], columnType.DatabaseTypeName())
		schema.Columns[index] = searchjobs.Column{
			Name:       columns[index],
			Kind:       kind,
			Nullable:   columnType.Nullable() || kind == searchjobs.ValueKindMixed,
			Multivalue: multivalue,
		}
	}
	if err := sink.SetSchema(schema); err != nil {
		return err
	}

	for rows.Next() {
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
	return nil
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
		unwrapped := false
		for _, wrapper := range []string{"LowCardinality", "Nullable"} {
			prefix := wrapper + "("
			if strings.HasPrefix(value, prefix) && strings.HasSuffix(value, ")") {
				value = strings.TrimSpace(value[len(prefix) : len(value)-1])
				unwrapped = true
				break
			}
		}
		if !unwrapped {
			return value
		}
	}
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
		if exception.Code == 395 && strings.Contains(exception.Message, clickhouse.UnsupportedStatsByValueMarker) {
			// The compiler deliberately emits this stable marker when stats BY
			// sees a dynamic non-scalar value. Do not retain any surrounding
			// ClickHouse message, generated SQL, or storage detail.
			return searchjobs.ErrUnsupportedValue
		}
		switch exception.Code {
		case 159: // TIMEOUT_EXCEEDED
			return fmt.Errorf("%w: ClickHouse execution timeout", context.DeadlineExceeded)
		case 158, 241, 396: // TOO_MANY_ROWS, MEMORY_LIMIT_EXCEEDED, TOO_MANY_ROWS_OR_BYTES
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
