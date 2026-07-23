package queryexec

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// maximumFieldSummaryResultBytes bounds both the ClickHouse wire result and
// the unique variable-width scalar backing retained by the executor. Fixed map
// and slice overhead is independently bounded by the distinct-group limit.
const maximumFieldSummaryResultBytes = uint64(32 << 20)

var fieldSummaryDoublePattern = regexp.MustCompile(
	`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`,
)

// FieldValueCountRow is one exact, non-null scalar frequency. Value is
// detached from the ClickHouse result stream.
type FieldValueCountRow struct {
	Value searchjobs.Value
	Count uint64
}

// FieldSummaryResult is one atomically validated profile and its deterministic
// exact top-value prefix. DistinctCount excludes missing and explicit-null
// values.
type FieldSummaryResult struct {
	FieldName     string
	ObservedTypes []eventfields.StoredValueType
	EventCount    uint64
	NullCount     uint64
	MissingCount  uint64
	DistinctCount uint64
	TopValues     []FieldValueCountRow
}

type canonicalFieldSummaryValue struct {
	value     searchjobs.Value
	kind      searchjobs.ValueKind
	canonical string
	count     uint64
}

type fieldSummaryGroupKey struct {
	kind      searchjobs.ValueKind
	canonical string
}

type fieldSummaryHeader struct {
	fieldName       string
	observedTypes   []uint8
	eventCount      uint64
	nullCount       uint64
	missingCount    uint64
	totalEventCount uint64
	metadataInvalid bool
	unsupported     bool
	oversized       bool
}

// ExecuteFieldSummary reads a compiler-produced exact field summary. It
// buffers and validates the complete bounded result before returning anything.
func (executor *Executor) ExecuteFieldSummary(
	ctx context.Context,
	query clickhouse.CompiledFieldSummary,
) (result FieldSummaryResult, resultErr error) {
	if ctx == nil {
		return FieldSummaryResult{}, errors.New("execute ClickHouse field summary: context is nil")
	}
	if executor == nil || isNilDriverValue(executor.connection) {
		return FieldSummaryResult{}, errors.New("execute ClickHouse field summary: executor connection is required")
	}
	if executor.newQueryID == nil {
		return FieldSummaryResult{}, errors.New("execute ClickHouse field summary: query ID generator is required")
	}
	if err := validateCompiledFieldSummary(query); err != nil {
		return FieldSummaryResult{}, err
	}
	settings, err := settingsForFieldSummary(executor.settings, query.Spec)
	if err != nil {
		return FieldSummaryResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return FieldSummaryResult{}, err
	}

	queryID, err := executor.newQueryID()
	if err != nil {
		return FieldSummaryResult{}, fmt.Errorf("execute ClickHouse field summary: create query ID: %w", err)
	}
	if queryID == "" {
		return FieldSummaryResult{}, errors.New("execute ClickHouse field summary: query ID is empty")
	}
	if err := ctx.Err(); err != nil {
		return FieldSummaryResult{}, err
	}
	queryContext := clickhousedriver.Context(
		ctx,
		clickhousedriver.WithQueryID(queryID),
		clickhousedriver.WithSettings(settings),
	)
	rows, err := executor.connection.Query(queryContext, query.SQL, query.Args...)
	if err != nil {
		return FieldSummaryResult{}, classifyQueryError(ctx, fmt.Errorf("query ClickHouse field summary: %w", err))
	}
	if isNilDriverValue(rows) {
		return FieldSummaryResult{}, invalidFieldSummaryResult("returned no result stream")
	}

	rowsClosed := false
	defer func() {
		if rowsClosed {
			return
		}
		if closeErr := rows.Close(); resultErr == nil && closeErr != nil {
			result = FieldSummaryResult{}
			resultErr = classifyQueryError(ctx, fmt.Errorf("close ClickHouse field summary result stream: %w", closeErr))
		}
	}()

	if err := ctx.Err(); err != nil {
		return FieldSummaryResult{}, err
	}
	if err := validateFieldSummaryColumns(rows.Columns(), rows.ColumnTypes()); err != nil {
		return FieldSummaryResult{}, err
	}

	var header fieldSummaryHeader
	haveHeader := false
	canonicalGroups := make(map[fieldSummaryGroupKey]*canonicalFieldSummaryValue, min(int(query.Spec.MaximumDistinctValues), 1_024))
	var retainedBytes uint64
	var rawGroupCount uint32
	var previousValueType uint8
	var previousEncodedValue string
	for {
		if err := ctx.Err(); err != nil {
			return FieldSummaryResult{}, err
		}
		if !rows.Next() {
			break
		}
		if err := ctx.Err(); err != nil {
			return FieldSummaryResult{}, err
		}

		var rowKind uint8
		var fieldName string
		var observedTypes []uint8
		var eventCount, nullCount, missingCount, totalEventCount uint64
		var valueType uint8
		var encodedValue string
		var valueCount uint64
		var metadataInvalid, unsupported, oversized uint8
		if err := rows.Scan(
			&rowKind,
			&fieldName,
			&observedTypes,
			&eventCount,
			&nullCount,
			&missingCount,
			&totalEventCount,
			&valueType,
			&encodedValue,
			&valueCount,
			&metadataInvalid,
			&unsupported,
			&oversized,
		); err != nil {
			return FieldSummaryResult{}, classifyQueryError(ctx, fmt.Errorf("scan ClickHouse field summary result row: %w", err))
		}
		if err := ctx.Err(); err != nil {
			return FieldSummaryResult{}, err
		}

		switch rowKind {
		case 0:
			if haveHeader || rawGroupCount != 0 {
				return FieldSummaryResult{}, invalidFieldSummaryResult("contains a repeated or misplaced header")
			}
			if valueType != 0 || encodedValue != "" || valueCount != 0 ||
				metadataInvalid > 1 || unsupported > 1 || oversized > 1 {
				return FieldSummaryResult{}, invalidFieldSummaryResult("header row is invalid")
			}
			header = fieldSummaryHeader{
				fieldName: fieldName, observedTypes: slices.Clone(observedTypes),
				eventCount: eventCount, nullCount: nullCount, missingCount: missingCount,
				totalEventCount: totalEventCount, metadataInvalid: metadataInvalid == 1,
				unsupported: unsupported == 1, oversized: oversized == 1,
			}
			haveHeader = true
		case 1:
			if !haveHeader {
				return FieldSummaryResult{}, invalidFieldSummaryResult("value row precedes the header")
			}
			if fieldName != "" || len(observedTypes) != 0 || eventCount != 0 || nullCount != 0 ||
				missingCount != 0 || totalEventCount != 0 || metadataInvalid != 0 ||
				unsupported != 0 || oversized != 0 {
				return FieldSummaryResult{}, invalidFieldSummaryResult("value row control columns are invalid")
			}
			rawGroupCount++
			if rawGroupCount > query.Spec.MaximumDistinctValues {
				return FieldSummaryResult{}, fmt.Errorf("%w: ClickHouse field summary exceeded the distinct-value limit", searchjobs.ErrExecutionLimit)
			}
			if rawGroupCount > 1 &&
				(valueType < previousValueType || valueType == previousValueType && encodedValue <= previousEncodedValue) {
				return FieldSummaryResult{}, invalidFieldSummaryResult("value groups are not strictly sorted")
			}
			previousValueType = valueType
			previousEncodedValue = encodedValue
			if valueCount == 0 {
				return FieldSummaryResult{}, invalidFieldSummaryResult("value row count is zero")
			}
			if uint32(len(encodedValue)) > query.Spec.MaximumValueBytes {
				return FieldSummaryResult{}, fmt.Errorf("%w: ClickHouse field summary value exceeded its byte limit", searchjobs.ErrExecutionLimit)
			}
			decoded, err := decodeFieldSummaryValue(eventfields.StoredValueType(valueType), encodedValue)
			if err != nil {
				return FieldSummaryResult{}, invalidFieldSummaryResult("contains an invalid encoded value")
			}
			if uint32(len(decoded.canonical)) > query.Spec.MaximumValueBytes {
				return FieldSummaryResult{}, fmt.Errorf("%w: ClickHouse field summary canonical value exceeded its byte limit", searchjobs.ErrExecutionLimit)
			}
			// A structured key shares the canonical string backing retained by the
			// accumulator instead of allocating a second concatenated copy.
			key := fieldSummaryGroupKey{kind: decoded.kind, canonical: decoded.canonical}
			if existing := canonicalGroups[key]; existing != nil {
				if existing.count > math.MaxUint64-valueCount {
					return FieldSummaryResult{}, invalidFieldSummaryResult("canonical value count overflows")
				}
				existing.count += valueCount
			} else {
				valueBytes := fieldSummaryRetainedValueBytes(decoded, encodedValue)
				if maximumFieldSummaryResultBytes-retainedBytes < valueBytes {
					return FieldSummaryResult{}, fmt.Errorf("%w: ClickHouse field summary exceeded its retained byte limit", searchjobs.ErrExecutionLimit)
				}
				retainedBytes += valueBytes
				decoded.count = valueCount
				canonicalGroups[key] = &decoded
			}
		default:
			return FieldSummaryResult{}, invalidFieldSummaryResult("row kind is invalid")
		}
	}
	if err := rows.Err(); err != nil {
		return FieldSummaryResult{}, classifyQueryError(ctx, fmt.Errorf("iterate ClickHouse field summary results: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return FieldSummaryResult{}, err
	}
	if !haveHeader {
		return FieldSummaryResult{}, invalidFieldSummaryResult("header row is missing")
	}

	rowsClosed = true
	if err := rows.Close(); err != nil {
		return FieldSummaryResult{}, classifyQueryError(ctx, fmt.Errorf("close ClickHouse field summary result stream: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return FieldSummaryResult{}, err
	}

	if header.metadataInvalid {
		return FieldSummaryResult{}, ErrFieldMetadataUnavailable
	}
	if header.unsupported {
		return FieldSummaryResult{}, searchjobs.ErrUnsupportedValue
	}
	if header.oversized {
		return FieldSummaryResult{}, fmt.Errorf("%w: ClickHouse field summary value exceeded its byte limit", searchjobs.ErrExecutionLimit)
	}
	if err := validateFieldSummaryHeader(header, query); err != nil {
		return FieldSummaryResult{}, err
	}
	if !query.FieldKnown && header.eventCount == 0 {
		return FieldSummaryResult{}, clickhouse.ErrFieldSummaryNotFound
	}

	nonNullCount := header.eventCount - header.nullCount
	var groupedCount uint64
	values := make([]canonicalFieldSummaryValue, 0, len(canonicalGroups))
	groupedKinds := make(map[searchjobs.ValueKind]struct{}, len(header.observedTypes))
	for _, value := range canonicalGroups {
		if !fieldSummaryObservedType(header.observedTypes, value.kind) {
			return FieldSummaryResult{}, invalidFieldSummaryResult("value kind was not observed by the profile")
		}
		groupedKinds[value.kind] = struct{}{}
		if groupedCount > math.MaxUint64-value.count {
			return FieldSummaryResult{}, invalidFieldSummaryResult("value counts overflow")
		}
		groupedCount += value.count
		values = append(values, *value)
	}
	if groupedCount != nonNullCount {
		return FieldSummaryResult{}, invalidFieldSummaryResult("value counts do not cover every non-null event")
	}
	for _, valueType := range header.observedTypes {
		if valueType == uint8(eventfields.StoredValueTypeNull) {
			continue
		}
		kind, ok := fieldSummaryValueKind(valueType)
		if !ok {
			return FieldSummaryResult{}, invalidFieldSummaryResult("observed value kind is invalid")
		}
		if _, ok := groupedKinds[kind]; !ok {
			return FieldSummaryResult{}, invalidFieldSummaryResult("observed value kind has no value group")
		}
	}

	slices.SortFunc(values, func(left, right canonicalFieldSummaryValue) int {
		switch {
		case left.count > right.count:
			return -1
		case left.count < right.count:
			return 1
		case left.canonical < right.canonical:
			return -1
		case left.canonical > right.canonical:
			return 1
		case left.kind < right.kind:
			return -1
		case left.kind > right.kind:
			return 1
		default:
			return 0
		}
	})
	topCount := min(len(values), int(query.Spec.MaximumValues))
	top := make([]FieldValueCountRow, topCount)
	for index := range topCount {
		top[index] = FieldValueCountRow{Value: values[index].value, Count: values[index].count}
	}
	return FieldSummaryResult{
		FieldName: header.fieldName, ObservedTypes: storedValueTypes(header.observedTypes),
		EventCount: header.eventCount, NullCount: header.nullCount, MissingCount: header.missingCount,
		DistinctCount: uint64(len(values)), TopValues: top,
	}, nil
}

func validateCompiledFieldSummary(query clickhouse.CompiledFieldSummary) error {
	spec := query.Spec
	if strings.TrimSpace(query.SQL) == "" ||
		spec.FieldName == "" || len(spec.FieldName) > eventfields.MaximumNormalizedFieldNameBytes || !utf8.ValidString(spec.FieldName) ||
		spec.MaximumValues == 0 || spec.MaximumValues > clickhouse.MaximumFieldSummaryValues ||
		spec.MaximumDistinctValues == 0 || spec.MaximumDistinctValues > clickhouse.MaximumFieldSummaryDistinctValues ||
		spec.MaximumDistinctValues < spec.MaximumValues ||
		spec.MaximumValueBytes == 0 || spec.MaximumValueBytes > clickhouse.MaximumFieldSummaryValueBytes {
		return invalidFieldSummaryResult("compiled contract is invalid")
	}
	return nil
}

func settingsForFieldSummary(
	base clickhousedriver.Settings,
	spec clickhouse.FieldSummarySpec,
) (clickhousedriver.Settings, error) {
	if err := validateCompiledFieldSummary(clickhouse.CompiledFieldSummary{SQL: "SELECT summary", Spec: spec}); err != nil {
		return nil, errors.New("execute ClickHouse field summary: summary limits are invalid")
	}
	if base == nil || base["readonly"] != uint8(2) {
		return nil, errors.New("execute ClickHouse field summary: executor does not have read-only settings")
	}
	for _, name := range []string{
		"max_execution_time",
		"max_memory_usage",
		"max_rows_to_read",
		"max_bytes_to_read",
		"max_result_rows",
		"max_result_bytes",
		"max_rows_to_group_by",
		"max_threads",
		"max_query_size",
	} {
		value, ok := base[name].(uint64)
		if !ok || value == 0 {
			return nil, fmt.Errorf("execute ClickHouse field summary: executor setting %s is invalid", name)
		}
	}
	for _, name := range []string{"timeout_overflow_mode", "read_overflow_mode", "result_overflow_mode", "group_by_overflow_mode"} {
		if base[name] != "throw" {
			return nil, fmt.Errorf("execute ClickHouse field summary: executor setting %s is unsafe", name)
		}
	}

	settings := maps.Clone(base)
	settings["max_result_rows"] = min(base["max_result_rows"].(uint64), uint64(spec.MaximumDistinctValues)+2)
	settings["max_result_bytes"] = min(base["max_result_bytes"].(uint64), maximumFieldSummaryResultBytes)
	settings["max_rows_to_group_by"] = min(base["max_rows_to_group_by"].(uint64), uint64(spec.MaximumDistinctValues))
	return settings, nil
}

func validateFieldSummaryColumns(columns []string, columnTypes []driver.ColumnType) error {
	expectedColumns := []string{
		clickhouse.FieldSummaryRowKindColumn,
		clickhouse.FieldSummaryFieldNameColumn,
		clickhouse.FieldSummaryObservedTypesColumn,
		clickhouse.FieldSummaryEventCountColumn,
		clickhouse.FieldSummaryNullCountColumn,
		clickhouse.FieldSummaryMissingCountColumn,
		clickhouse.FieldSummaryTotalEventCountColumn,
		clickhouse.FieldSummaryValueTypeColumn,
		clickhouse.FieldSummaryEncodedValueColumn,
		clickhouse.FieldSummaryValueCountColumn,
		clickhouse.FieldSummaryMetadataInvalidColumn,
		clickhouse.FieldSummaryUnsupportedColumn,
		clickhouse.FieldSummaryOversizedColumn,
	}
	if !slices.Equal(columns, expectedColumns) || len(columnTypes) != len(expectedColumns) {
		return invalidFieldSummaryResult("columns do not match the compiled output")
	}
	expectedTypes := []string{
		"UInt8", "String", "Array(UInt8)", "UInt64", "UInt64", "UInt64", "UInt64",
		"UInt8", "String", "UInt64", "UInt8", "UInt8", "UInt8",
	}
	for index, columnType := range columnTypes {
		if isNilDriverValue(columnType) || columnType.Name() != expectedColumns[index] || columnType.Nullable() ||
			columnType.DatabaseTypeName() != expectedTypes[index] {
			return invalidFieldSummaryResult(fmt.Sprintf("column %q has an invalid type", expectedColumns[index]))
		}
	}
	return nil
}

func validateFieldSummaryHeader(header fieldSummaryHeader, query clickhouse.CompiledFieldSummary) error {
	if header.fieldName != query.Spec.FieldName ||
		header.nullCount > header.eventCount ||
		header.eventCount > header.totalEventCount ||
		header.missingCount != header.totalEventCount-header.eventCount {
		return invalidFieldSummaryResult("profile identity or counts are invalid")
	}
	if (header.eventCount == 0) != (len(header.observedTypes) == 0) {
		return invalidFieldSummaryResult("observed types do not match field presence")
	}
	hasNull := false
	var previous uint8
	for index, valueType := range header.observedTypes {
		if !eventfields.IsStoredValueType(valueType) || index > 0 && valueType <= previous {
			return invalidFieldSummaryResult("observed types are invalid")
		}
		if valueType == uint8(eventfields.StoredValueTypeList) ||
			valueType == uint8(eventfields.StoredValueTypeObject) {
			return invalidFieldSummaryResult("unsupported observed type was not flagged")
		}
		hasNull = hasNull || valueType == uint8(eventfields.StoredValueTypeNull)
		previous = valueType
	}
	if hasNull != (header.nullCount > 0) {
		return invalidFieldSummaryResult("observed null type does not match the null count")
	}
	return nil
}

func fieldSummaryObservedType(observed []uint8, kind searchjobs.ValueKind) bool {
	for _, valueType := range observed {
		observedKind, ok := fieldSummaryValueKind(valueType)
		if ok && observedKind == kind {
			return true
		}
	}
	return false
}

func fieldSummaryValueKind(valueType uint8) (searchjobs.ValueKind, bool) {
	switch eventfields.StoredValueType(valueType) {
	case eventfields.StoredValueTypeNull:
		return searchjobs.ValueKindNull, true
	case eventfields.StoredValueTypeString:
		return searchjobs.ValueKindString, true
	case eventfields.StoredValueTypeSint64:
		return searchjobs.ValueKindSigned, true
	case eventfields.StoredValueTypeUint64:
		return searchjobs.ValueKindUnsigned, true
	case eventfields.StoredValueTypeDouble:
		return searchjobs.ValueKindDouble, true
	case eventfields.StoredValueTypeBool:
		return searchjobs.ValueKindBool, true
	case eventfields.StoredValueTypeBytes:
		return searchjobs.ValueKindBytes, true
	case eventfields.StoredValueTypeTimestamp:
		return searchjobs.ValueKindTime, true
	case eventfields.StoredValueTypeDuration:
		return searchjobs.ValueKindDuration, true
	case eventfields.StoredValueTypeList:
		return searchjobs.ValueKindList, true
	case eventfields.StoredValueTypeObject:
		return searchjobs.ValueKindObject, true
	case eventfields.StoredValueTypeDecimal:
		return searchjobs.ValueKindDecimal, true
	default:
		return searchjobs.ValueKindInvalid, false
	}
}

func decodeFieldSummaryValue(
	valueType eventfields.StoredValueType,
	encoded string,
) (canonicalFieldSummaryValue, error) {
	result := canonicalFieldSummaryValue{canonical: encoded}
	switch valueType {
	case eventfields.StoredValueTypeString:
		if !utf8.ValidString(encoded) {
			return canonicalFieldSummaryValue{}, errors.New("string value is not UTF-8")
		}
		result.kind = searchjobs.ValueKindString
		result.value = searchjobs.StringValue(encoded)
	case eventfields.StoredValueTypeSint64:
		value, err := strconv.ParseInt(encoded, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != encoded {
			return canonicalFieldSummaryValue{}, errors.New("signed value is not canonical")
		}
		result.kind = searchjobs.ValueKindSigned
		result.canonical = strconv.FormatInt(value, 10)
		result.value = searchjobs.SignedValue(value)
	case eventfields.StoredValueTypeUint64:
		value, err := strconv.ParseUint(encoded, 10, 64)
		if err != nil || strconv.FormatUint(value, 10) != encoded {
			return canonicalFieldSummaryValue{}, errors.New("unsigned value is not canonical")
		}
		result.kind = searchjobs.ValueKindUnsigned
		result.canonical = strconv.FormatUint(value, 10)
		result.value = searchjobs.UnsignedValue(value)
	case eventfields.StoredValueTypeDouble:
		value, err := strconv.ParseFloat(encoded, 64)
		if err != nil || !fieldSummaryDoublePattern.MatchString(encoded) ||
			math.IsNaN(value) || math.IsInf(value, 0) {
			return canonicalFieldSummaryValue{}, errors.New("double value is invalid")
		}
		if value == 0 {
			value = 0
		}
		result.kind = searchjobs.ValueKindDouble
		result.canonical = strconv.FormatFloat(value, 'g', -1, 64)
		result.value = searchjobs.DoubleValue(value)
	case eventfields.StoredValueTypeBool:
		value, err := strconv.ParseBool(encoded)
		if err != nil || strconv.FormatBool(value) != encoded {
			return canonicalFieldSummaryValue{}, errors.New("boolean value is not canonical")
		}
		result.kind = searchjobs.ValueKindBool
		result.canonical = strconv.FormatBool(value)
		result.value = searchjobs.BoolValue(value)
	case eventfields.StoredValueTypeBytes:
		value, err := base64.RawStdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.RawStdEncoding.EncodeToString(value) != encoded {
			return canonicalFieldSummaryValue{}, errors.New("byte value encoding is invalid")
		}
		result.kind = searchjobs.ValueKindBytes
		result.canonical = encoded
		result.value = searchjobs.BytesValue(value)
	case eventfields.StoredValueTypeTimestamp:
		if !validFieldSummaryTimestampEncoding(encoded) {
			return canonicalFieldSummaryValue{}, errors.New("timestamp value encoding is invalid")
		}
		value, err := time.Parse(time.RFC3339Nano, encoded)
		if err != nil {
			return canonicalFieldSummaryValue{}, errors.New("timestamp value is invalid")
		}
		value = value.UTC()
		if value.Year() < 1 || value.Year() > 9999 {
			return canonicalFieldSummaryValue{}, errors.New("timestamp value is outside the supported UTC range")
		}
		result.kind = searchjobs.ValueKindTime
		result.canonical = value.Format(time.RFC3339Nano)
		result.value = searchjobs.TimeValue(value)
	case eventfields.StoredValueTypeDuration:
		value, err := decodeExtendedDuration(encoded)
		if err != nil {
			return canonicalFieldSummaryValue{}, errors.New("duration value is invalid")
		}
		result.kind = searchjobs.ValueKindDuration
		result.canonical = strconv.FormatInt(int64(value/time.Second), 10) + ":" +
			strconv.FormatInt(int64(value%time.Second), 10)
		if result.canonical != encoded {
			return canonicalFieldSummaryValue{}, errors.New("duration value is not canonical")
		}
		result.value = searchjobs.DurationValue(value)
	case eventfields.StoredValueTypeDecimal:
		canonical, err := searchjobs.CanonicalDecimal(encoded)
		if err != nil {
			return canonicalFieldSummaryValue{}, errors.New("decimal value is invalid")
		}
		value, err := searchjobs.DecimalValue(canonical)
		if err != nil {
			return canonicalFieldSummaryValue{}, errors.New("canonical decimal value is invalid")
		}
		result.kind = searchjobs.ValueKindDecimal
		result.canonical = canonical
		result.value = value
	case eventfields.StoredValueTypeNull,
		eventfields.StoredValueTypeList,
		eventfields.StoredValueTypeObject:
		return canonicalFieldSummaryValue{}, errors.New("non-scalar value row is unsupported")
	default:
		return canonicalFieldSummaryValue{}, errors.New("value type is invalid")
	}
	return result, nil
}

func validFieldSummaryTimestampEncoding(encoded string) bool {
	// RFC 3339 permits a period-separated fractional second of up to nine
	// digits. time.Parse is intentionally more permissive (it accepts commas,
	// excess fractional digits, and invalid zone bounds), so validate the wire
	// grammar before parsing and canonicalizing it.
	if len(encoded) < len("0001-01-01T00:00:00Z") ||
		encoded[4] != '-' || encoded[7] != '-' || encoded[10] != 'T' ||
		encoded[13] != ':' || encoded[16] != ':' {
		return false
	}
	for _, index := range [...]int{0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18} {
		if !fieldSummaryASCIIDigit(encoded[index]) {
			return false
		}
	}

	zoneStart := 19
	if encoded[zoneStart] == '.' {
		fractionStart := zoneStart + 1
		zoneStart = fractionStart
		for zoneStart < len(encoded) && fieldSummaryASCIIDigit(encoded[zoneStart]) {
			zoneStart++
		}
		fractionDigits := zoneStart - fractionStart
		if fractionDigits < 1 || fractionDigits > 9 {
			return false
		}
	}
	if zoneStart == len(encoded)-1 && encoded[zoneStart] == 'Z' {
		return true
	}
	if len(encoded)-zoneStart != len("+00:00") ||
		(encoded[zoneStart] != '+' && encoded[zoneStart] != '-') ||
		encoded[zoneStart+3] != ':' ||
		!fieldSummaryASCIIDigit(encoded[zoneStart+1]) ||
		!fieldSummaryASCIIDigit(encoded[zoneStart+2]) ||
		!fieldSummaryASCIIDigit(encoded[zoneStart+4]) ||
		!fieldSummaryASCIIDigit(encoded[zoneStart+5]) {
		return false
	}
	hour := int(encoded[zoneStart+1]-'0')*10 + int(encoded[zoneStart+2]-'0')
	minute := int(encoded[zoneStart+4]-'0')*10 + int(encoded[zoneStart+5]-'0')
	return hour <= 23 && minute <= 59
}

func fieldSummaryASCIIDigit(value byte) bool {
	return value >= '0' && value <= '9'
}

func fieldSummaryRetainedValueBytes(value canonicalFieldSummaryValue, encoded string) uint64 {
	// canonical is retained by both the accumulator and its structured map key.
	// Immutable Value constructors additionally clone variable-width payloads.
	retained := uint64(len(value.canonical))
	switch value.kind {
	case searchjobs.ValueKindString, searchjobs.ValueKindDecimal:
		retained += uint64(len(value.canonical))
	case searchjobs.ValueKindBytes:
		retained += uint64(base64.RawStdEncoding.DecodedLen(len(encoded)))
	}
	return retained
}

func invalidFieldSummaryResult(message string) error {
	return fmt.Errorf("%w: ClickHouse field summary %s", searchjobs.ErrInvalidResult, message)
}
