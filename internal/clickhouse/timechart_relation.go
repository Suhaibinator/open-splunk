package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/ClickHouse/clickhouse-go/v2/ext"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

const timechartRelationName = "__os_timechart_relation"

// RelationColumn describes one validated public scalar of a materialized chart.
// Types are restricted to the existing compiler's scalar domain.
type RelationColumn struct{ Name, Type string }

type compiledTimechartContinuation struct {
	plan     plan.TimechartContinuation
	compiler Compiler
	lookups  []compiledLookupExternalTable
}
type compiledRelationInput struct {
	columns       []RelationColumn
	rows          [][]any
	bucketEnds    []time.Time
	commitment    [sha256.Size]byte
	retainedBytes uint64
}

func timechartStageOutput(operator *plan.Timechart) ([]string, *plan.DynamicSeriesOutput) {
	if operator.Split == nil {
		return []string{"_time", operator.Measure.Output}, nil
	}
	return nil, &plan.DynamicSeriesOutput{FixedFields: []string{"_time"}, MaxSeries: timechartSplitMaxSeries(operator.Split)}
}

// HasContinuation reports whether this executable is an intermediate validated
// pivot. The physical decoder remains independent from suffix presentation.
func (compiled CompiledQuery) HasContinuation() bool {
	return compiled.continuation != nil || compiled.rangeDiscovery != nil
}

// ContinueContext lowers the retained SPL suffix through the ordinary compiler
// over an immutable, exact-schema native external table. No event scan is used.
func (compiled CompiledQuery) ContinueContext(ctx context.Context, columns []RelationColumn, rows [][]any) (CompiledQuery, error) {
	return compiled.ContinueWithTimeBucketsContext(ctx, columns, rows, nil)
}

// ContinueWithTimeBucketsContext preserves exact optional bucket presentation.
func (compiled CompiledQuery) ContinueWithTimeBucketsContext(ctx context.Context, columns []RelationColumn, rows [][]any, ends []time.Time) (CompiledQuery, error) {
	if ctx == nil {
		return CompiledQuery{}, errors.New("continue timechart: context is nil")
	}
	valid, err := compiled.hasValidExecutionSealContext(ctx)
	if err != nil {
		return CompiledQuery{}, err
	}
	if !valid || !compiled.HasContinuation() {
		return CompiledQuery{}, errors.New("continue timechart: continuation authority is invalid")
	}
	input, err := newRelationInput(ctx, columns, rows, compiled.rangeDiscovery != nil)
	if err != nil {
		return CompiledQuery{}, err
	}
	if ends != nil {
		if len(ends) != len(rows) || len(columns) == 0 || columns[0].Name != "_time" {
			return CompiledQuery{}, errors.New("continue timechart: invalid bucket bounds")
		}
		for i, end := range ends {
			start, ok := rows[i][0].(time.Time)
			if !ok || !start.Before(end) {
				return CompiledQuery{}, errors.New("continue timechart: invalid bucket interval")
			}
		}
		policy := searchlimits.Default()
		if admitted, ok := searchlimits.FromContext(ctx); ok {
			policy = admitted
		}
		maximum := min(policy.MaxResultBytes, policy.MaxMemoryBytes)
		if input.retainedBytes > maximum || uint64(len(ends)) > (maximum-input.retainedBytes)/uint64(unsafe.Sizeof(time.Time{})) {
			return CompiledQuery{}, errors.New("continue timechart: bucket bounds exceed byte limit")
		}
		input.bucketEnds = slices.Clone(ends)
		input.retainedBytes += uint64(len(ends)) * uint64(unsafe.Sizeof(time.Time{}))
		digest := sha256.New()
		_, _ = digest.Write(input.commitment[:])
		for _, end := range ends {
			writeCompiledArgument(digest, end, 0)
		}
		copy(input.commitment[:], digest.Sum(nil))
	}
	if compiled.rangeDiscovery != nil {
		result, err := compiled.continueObservedTimechart(ctx, input)
		if err != nil {
			return CompiledQuery{}, err
		}
		result.continuationRoot = compiled.continuationRoot
		if result.continuationRoot == nil {
			root := *compiled.executionSeal
			result.continuationRoot = &root
		}
		return sealCompiledQueryExecutionContext(ctx, result)
	}
	fields := make([]string, len(columns))
	for i, column := range columns {
		fields[i] = column.Name
	}
	query, err := compiled.continuation.plan.Build(fields)
	if err != nil {
		return CompiledQuery{}, err
	}
	compiler, err := compiled.continuation.compilerForQuery(ctx, query)
	if err != nil {
		return CompiledQuery{}, err
	}
	compiler.relationInput = input
	result, err := compiler.CompileContext(ctx, query)
	if err != nil {
		return CompiledQuery{}, err
	}
	result.continuationRoot = compiled.continuationRoot
	if result.continuationRoot == nil {
		root := *compiled.executionSeal
		result.continuationRoot = &root
	}
	return sealCompiledQueryExecutionContext(ctx, result)
}

func newRelationInput(ctx context.Context, columns []RelationColumn, rows [][]any, discovery bool) (*compiledRelationInput, error) {
	if len(columns) == 0 {
		return nil, errors.New("materialize timechart: empty schema")
	}
	policy := searchlimits.Default()
	if admitted, ok := searchlimits.FromContext(ctx); ok {
		policy = admitted
	}
	maximum := min(policy.MaxResultBytes, policy.MaxMemoryBytes)
	maxRows := policy.MaxResultRows
	if discovery {
		maxRows = policy.MaxRowsToRead
	}
	if uint64(len(rows)) > maxRows {
		return nil, errors.New("materialize timechart: row limit exceeded")
	}
	retained := uint64(unsafe.Sizeof(compiledRelationInput{}))
	charge := func(value uint64) bool {
		if value > maximum-retained {
			return false
		}
		retained += value
		return true
	}
	if retained > maximum || uint64(len(columns)) > maximum/uint64(unsafe.Sizeof(RelationColumn{})) ||
		!charge(uint64(len(columns))*uint64(unsafe.Sizeof(RelationColumn{}))) ||
		uint64(len(rows)) > maximum/uint64(unsafe.Sizeof([]any{})) || !charge(uint64(len(rows))*uint64(unsafe.Sizeof([]any{}))) {
		return nil, errors.New("materialize timechart: schema capacity exceeds byte limit")
	}
	for _, column := range columns {
		if !charge(uint64(len(column.Name))) || !charge(uint64(len(column.Type))) {
			return nil, errors.New("materialize timechart: schema exceeds byte limit")
		}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row) != len(columns) || uint64(len(row)) > maximum/uint64(unsafe.Sizeof(any(nil))) || !charge(uint64(len(row))*uint64(unsafe.Sizeof(any(nil)))) {
			return nil, errors.New("materialize timechart: cell capacity exceeds byte limit")
		}
		for j, value := range row {
			if !relationValueValid(columns[j].Type, value) {
				return nil, errors.New("materialize timechart: cell type is invalid")
			}
			size, ok := retainedRelationValue(value, 0)
			if !ok || !charge(size) {
				return nil, errors.New("materialize timechart: cells exceed byte limit")
			}
		}
	}
	input := &compiledRelationInput{columns: slices.Clone(columns), rows: make([][]any, len(rows))}
	digest := sha256.New()
	writeTokenPart(digest, "timechart-external-relation-v1")
	input.retainedBytes = retained
	names := make(map[string]bool, len(columns))
	for i, column := range columns {
		if column.Name == "" || !utf8.ValidString(column.Name) || names[column.Name] {
			return nil, errors.New("materialize timechart: invalid schema name")
		}
		names[column.Name] = true
		if _, _, err := relationField(column, i); err != nil {
			return nil, err
		}
		input.columns[i].Name = strings.Clone(column.Name)
		input.columns[i].Type = strings.Clone(column.Type)
		writeTokenPart(digest, column.Name)
		writeTokenPart(digest, column.Type)
	}
	for i, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(row) != len(columns) {
			return nil, errors.New("materialize timechart: row width is invalid")
		}
		input.rows[i] = make([]any, len(row))
		for j, value := range row {
			if !relationValueValid(columns[j].Type, value) {
				return nil, errors.New("materialize timechart: cell type is invalid")
			}
			if text, ok := value.(string); ok {
				value = strings.Clone(text)
			}
			cloned, ok := cloneRelationValue(value, 0)
			if !ok {
				return nil, errors.New("materialize timechart: cell cannot be cloned")
			}
			input.rows[i][j] = cloned
			if !writeRelationValue(digest, value, 0) {
				return nil, errors.New("materialize timechart: unsupported cell")
			}
		}
	}
	copy(input.commitment[:], digest.Sum(nil))
	return input, nil
}

func relationValueValid(kind string, value any) bool {
	if kind == "Dynamic" {
		return validRelationDynamicValue(value)
	}
	if value == nil {
		return strings.HasPrefix(kind, "Nullable(")
	}
	kind = relationBaseType(kind)
	switch kind {
	case "UInt64":
		_, ok := value.(uint64)
		return ok
	case "Int64":
		_, ok := value.(int64)
		return ok
	case "Float64":
		v, ok := value.(float64)
		return ok && !math.IsInf(v, 0) && !math.IsNaN(v)
	case "String":
		v, ok := value.(string)
		return ok && utf8.ValidString(v)
	case "Bool":
		_, ok := value.(bool)
		return ok
	case "DateTime64(9, 'UTC')":
		_, ok := value.(time.Time)
		return ok
	default:
		return false
	}
}

func relationField(column RelationColumn, index int) (fieldState, string, error) {
	physical := fmt.Sprintf("__os_relation_%d", index)
	field := fieldState{valueSQL: quoteIdentifier(column.Name), existsSQL: "1"}
	kind := relationBaseType(column.Type)
	switch kind {
	case "UInt64", "Int64", "Float64":
		field.kind = fieldKindNumber
		field.numberType = kind
		field.numericIntegral = kind != "Float64"
		field.numericSort = true
	case "String":
		field.kind = fieldKindString
	case "Dynamic":
		field.kind = fieldKindDynamic
		field.dynamicTypeSQL = "dynamicType(" + field.valueSQL + ")"
		field.existsSQL = "isNotNull(" + field.valueSQL + ")"
	case "Bool":
		field.kind = fieldKindBool
	case "DateTime64(9, 'UTC')":
		field.kind = fieldKindTime
		field.canonicalTime = column.Name == "_time"
	default:
		return fieldState{}, "", fmt.Errorf("materialize timechart: unsupported scalar type %q", column.Type)
	}
	if strings.HasPrefix(column.Type, "Nullable(") {
		field.existsSQL = "isNotNull(" + field.valueSQL + ")"
	}
	return field, physical, nil
}

func compileRelationInput(input *compiledRelationInput, query *plan.Query) (string, compileState, []any, error) {
	state := compileState{visible: make(map[string]fieldState, len(input.columns)), context: newCompileContext(query.SearchStart, query.SearchTimezone)}
	scan := query.Operators[0].(*plan.Scan)
	state.context.searchEarliest, state.context.searchLatest = scan.Earliest, scan.Latest
	state.context.hasTimechartStage = true
	projection := make([]string, len(input.columns))
	for i, column := range input.columns {
		field, physical, err := relationField(column, i)
		if err != nil {
			return "", state, nil, err
		}
		state.visible[column.Name] = field
		state.publicOrder = append(state.publicOrder, column.Name)
		projection[i] = quoteIdentifier(physical) + " AS " + quoteIdentifier(column.Name)
	}
	if input.bucketEnds != nil {
		column := quoteIdentifier(ResultTimeBucketEndColumn)
		field := state.visible["_time"]
		field.timeBucketEndSQL = column
		state.visible["_time"] = field
		state.privateColumns = append(state.privateColumns, column)
		projection = append(projection, column)
	}
	if field, ok := state.visible["_time"]; ok {
		state.order = []compiledSortKey{{valueSQL: field.valueSQL}}
	}
	scope := append([]string{scan.TenantID}, scan.Indexes...)
	args := make([]any, len(scope))
	predicates := make([]string, len(scope))
	for i, value := range scope {
		args[i] = compiledReadScopeArgument{ordinal: i, value: value}
		predicates[i] = "notEmpty(?)"
	}
	return "SELECT " + strings.Join(projection, ", ") + " FROM " + quoteIdentifier(timechartRelationName) + " WHERE " + strings.Join(predicates, " AND "), state, args, nil
}

func writeTimechartContinuation(digest hash.Hash, compiled CompiledQuery) {
	writeBool(digest, compiled.rangeDiscovery != nil)
	if compiled.rangeDiscovery != nil {
		compiled.rangeDiscovery.compiler.continuationBudget.write(digest)
		writeTokenPart(digest, compiled.rangeDiscovery.timezone)
		writeCompiledArgument(digest, compiled.rangeDiscovery.searchStart, 0)
		operator := compiled.rangeDiscovery.operator
		split := operator.Split
		operator.Split = nil
		writeTokenPart(digest, fmt.Sprintf("%#v", operator))
		writeBool(digest, split != nil)
		if split != nil {
			writeTokenPart(digest, fmt.Sprintf("%#v", *split))
		}
		if compiled.rangeDiscovery.continuation != nil {
			writeTokenPart(digest, compiled.rangeDiscovery.continuation.plan.Source())
			writeInt64(digest, int64(compiled.rangeDiscovery.continuation.plan.StartCommand()))
		}
	}
	writeBool(digest, compiled.continuationRoot != nil)
	if compiled.continuationRoot != nil {
		_, _ = digest.Write(compiled.continuationRoot[:])
	}
	writeBool(digest, compiled.continuation != nil)
	if compiled.continuation != nil {
		writeTokenPart(digest, compiled.continuation.plan.Source())
		writeInt64(digest, int64(compiled.continuation.plan.StartCommand()))
		compiled.continuation.compiler.continuationBudget.write(digest)
		writeTokenPart(digest, compiled.continuation.compiler.Database)
		writeTokenPart(digest, compiled.continuation.compiler.Table)
	}
	writeBool(digest, compiled.relationInput != nil)
	if compiled.relationInput != nil {
		_, _ = digest.Write(compiled.relationInput.commitment[:])
	}
}

func materializeRelationInput(ctx context.Context, input *compiledRelationInput) (*ext.Table, error) {
	definitions := make([]func(*ext.Table) error, len(input.columns))
	for i, fieldColumn := range input.columns {
		_, physical, err := relationField(fieldColumn, i)
		if err != nil {
			return nil, err
		}
		definitions[i] = ext.Column(physical, column.Type(fieldColumn.Type))
	}
	if input.bucketEnds != nil {
		definitions = append(definitions, ext.Column(ResultTimeBucketEndColumn, "DateTime64(9, 'UTC')"))
	}
	table, err := ext.NewTable(timechartRelationName, definitions...)
	if err != nil {
		return nil, err
	}
	for rowIndex, row := range input.rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row = slices.Clone(row)
		for i, column := range input.columns {
			if column.Type == "Dynamic" {
				row[i] = nativeRelationDynamic(row[i])
			}
		}
		if input.bucketEnds != nil {
			row = append(slices.Clone(row), input.bucketEnds[rowIndex])
		}
		if err := table.Append(row...); err != nil {
			return nil, err
		}
	}
	return table, nil
}

func relationBaseType(kind string) string {
	if base, ok := strings.CutPrefix(kind, "Nullable("); ok {
		return strings.TrimSuffix(base, ")")
	}
	return kind
}

// IsContinuationOf verifies that a sealed final descriptor descends from the
// exact admitted executable, including every upstream stage and suffix.
func (compiled CompiledQuery) IsContinuationOf(source CompiledQuery) bool {
	return compiled.continuationRoot != nil && source.executionSeal != nil &&
		compiled.HasValidExecutionSeal() && source.HasValidExecutionSeal() &&
		*compiled.continuationRoot == *source.executionSeal
}

func validRelationDynamicValue(value any) bool { _, ok := retainedRelationValue(value, 0); return ok }
func retainedRelationValue(value any, depth int) (uint64, bool) {
	if depth > 17 {
		return 0, false
	}
	if items, ok := value.([]any); ok {
		total := uint64(unsafe.Sizeof([]any{})) + uint64(len(items))*uint64(unsafe.Sizeof(any(nil)))
		for _, item := range items {
			size, ok := retainedRelationValue(item, depth+1)
			if !ok {
				return 0, false
			}
			total, ok = retainedAdd(total, size)
			if !ok {
				return 0, false
			}
		}
		return total, true
	}
	switch value.(type) {
	case nil, string, int64, uint64, float64, bool, time.Time:
		return retainedCompiledArgument(value)
	default:
		return 0, false
	}
}
func cloneRelationValue(value any, depth int) (any, bool) {
	if depth > 17 {
		return nil, false
	}
	if items, ok := value.([]any); ok {
		result := make([]any, len(items))
		for i, item := range items {
			cloned, ok := cloneRelationValue(item, depth+1)
			if !ok {
				return nil, false
			}
			result[i] = cloned
		}
		return result, true
	}
	return cloneCompiledArgument(value)
}
func writeRelationValue(digest hash.Hash, value any, depth int) bool {
	if depth > 17 {
		return false
	}
	if items, ok := value.([]any); ok {
		writeTokenPart(digest, "Array(Dynamic)")
		writeBool(digest, items == nil)
		writeUint64(digest, uint64(len(items)))
		for _, item := range items {
			if !writeRelationValue(digest, item, depth+1) {
				return false
			}
		}
		return true
	}
	return writeCompiledArgument(digest, value, 0)
}
func nativeRelationDynamic(value any) chcol.Dynamic {
	if items, ok := value.([]any); ok {
		result := make([]chcol.Dynamic, len(items))
		for i, item := range items {
			result[i] = nativeRelationDynamic(item)
		}
		return chcol.NewDynamicWithType(result, "Array(Dynamic)")
	}
	return chcol.NewDynamic(value)
}
