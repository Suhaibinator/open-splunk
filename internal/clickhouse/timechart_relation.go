package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

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
	observedWorkHeader   bool
	mvExpandRows         uint64
	timechartOccurrences bool
	columns              []RelationColumn
	rows                 [][]any
	bucketEnds           []time.Time
	commitment           [sha256.Size]byte
	retainedBytes        uint64
	nativeBytes          uint64
	dynamicColumnCount   int
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
	if compiled.timechartWorkReceipt {
		return CompiledQuery{}, errors.New("continue timechart: native work receipt is required")
	}
	return compiled.ContinueWithTimeBucketsAndWorkContext(ctx, columns, rows, ends, compiled.timechartWorkFloor)
}

// ContinueWithTimeBucketsAndWorkContext retains the validated cumulative native
// work receipt alongside the immutable intermediate relation.
func (compiled CompiledQuery) ContinueWithTimeBucketsAndWorkContext(ctx context.Context, columns []RelationColumn, rows [][]any, ends []time.Time, work uint64) (CompiledQuery, error) {
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
	if err := validateTimechartWork(work, compiled.timechartWorkFloor); err != nil {
		return CompiledQuery{}, err
	}
	input, err := newRelationInputWithTimeBuckets(ctx, columns, rows, ends, compiled.rangeDiscovery != nil)
	if err != nil {
		return CompiledQuery{}, err
	}
	input.mvExpandRows = work
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

func newRelationInputWithTimeBuckets(
	ctx context.Context,
	columns []RelationColumn,
	rows [][]any,
	ends []time.Time,
	discovery bool,
) (*compiledRelationInput, error) {
	policy := searchlimits.Default()
	if admitted, ok := searchlimits.FromContext(ctx); ok {
		policy = admitted
	}
	maximum := relationInputMaximumBytes(ctx)
	maxRows := policy.MaxResultRows
	if discovery {
		maxRows = policy.MaxRowsToRead
	}
	preflight, err := preflightRelationInput(ctx, columns, rows, ends, maxRows, maximum)
	if err != nil {
		return nil, err
	}
	walk := relationTraversal{ctx: ctx}
	input := &compiledRelationInput{
		columns:            slices.Clone(columns),
		rows:               make([][]any, len(rows)),
		retainedBytes:      preflight.retainedBytes,
		nativeBytes:        preflight.nativeBytes,
		dynamicColumnCount: preflight.dynamicColumnCount,
	}
	if ends != nil {
		input.bucketEnds = make([]time.Time, 0, len(ends))
	}
	digest := sha256.New()
	writeTokenPart(digest, "timechart-external-relation-v1")
	for i, column := range columns {
		if !walk.step() {
			return nil, walk.err
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
			cloned, ok := walk.cloneAndWriteValue(digest, value, 0)
			if !ok {
				if walk.err != nil {
					return nil, walk.err
				}
				return nil, errors.New("materialize timechart: cell cannot be cloned or committed")
			}
			input.rows[i][j] = cloned
		}
		if ends != nil {
			if len(ends) == 0 {
				return nil, errors.New("continue timechart: invalid bucket bounds")
			}
			input.bucketEnds = append(input.bucketEnds, ends[0])
			ends = ends[1:]
		}
	}
	copy(input.commitment[:], digest.Sum(nil))
	if ends != nil {
		input.commitment, err = relationCommitmentWithBucketEnds(ctx, input.commitment, input.bucketEnds)
		if err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return input, nil
}

func relationCommitmentWithBucketEnds(
	ctx context.Context,
	rowCommitment [sha256.Size]byte,
	ends []time.Time,
) ([sha256.Size]byte, error) {
	digest := sha256.New()
	_, _ = digest.Write(rowCommitment[:])
	for index, end := range ends {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return [sha256.Size]byte{}, err
			}
		}
		writeCompiledArgument(digest, end, 0)
	}
	if err := ctx.Err(); err != nil {
		return [sha256.Size]byte{}, err
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func relationScalarValueValid(kind string, value any) bool {
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
		// This is a typed native Float64 cell, including accepted IEEE sum
		// and average results. Aggregate-specific validity is checked before
		// the relation is created; Dynamic values use their existing path.
		_, ok := value.(float64)
		return ok
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
		field.canonicalTime = column.Name == "_time" && !strings.HasPrefix(column.Type, "Nullable(")
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
		field.timechartOccurrences = input.timechartOccurrences && column.Name == timechartObservedMeasure
		state.visible[column.Name] = field
		state.publicOrder = append(state.publicOrder, column.Name)
		projection[i] = quoteIdentifier(physical) + " AS " + quoteIdentifier(column.Name)
	}
	if input.mvExpandRows != 0 {
		projection = append(projection, carriedTimechartWorkProjection(input.mvExpandRows))
		state.context.mvExpandWorkSQL = "toUInt64(" + fmt.Sprint(input.mvExpandRows) + ")"
		state.mvExpandQueryRowsSQL = quoteIdentifier(timechartCarriedWork)
		state.privateColumns = append(state.privateColumns, state.mvExpandQueryRowsSQL)
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
	if input.observedWorkHeader {
		predicates = append(predicates, quoteIdentifier(timechartObservedInputRow)+" = 1")
	}
	return "SELECT " + strings.Join(projection, ", ") + " FROM " + quoteIdentifier(timechartRelationName) + " WHERE " + strings.Join(predicates, " AND "), state, args, nil
}

func writeTimechartContinuation(digest hash.Hash, compiled CompiledQuery) {
	if compiled.timechartWorkReceipt || compiled.timechartWorkFloor != 0 {
		writeTokenPart(digest, "timechart-expansion-work-v1")
		writeBool(digest, compiled.timechartWorkReceipt)
		writeUint64(digest, compiled.timechartWorkFloor)
	}
	if compiled.hasTimechartStage && compiled.logicalExtractionBudget != (authoredKnowledgeCompilation{}) {
		writeTokenPart(digest, "timechart-logical-extraction-v1")
		compiled.logicalExtractionBudget.write(digest)
	}
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
			budget := compiled.rangeDiscovery.continuation.plan.BudgetCommitment()
			_, _ = digest.Write(budget[:])
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
		budget := compiled.continuation.plan.BudgetCommitment()
		_, _ = digest.Write(budget[:])
		writeInt64(digest, int64(compiled.continuation.plan.StartCommand()))
		compiled.continuation.compiler.continuationBudget.write(digest)
		writeTokenPart(digest, compiled.continuation.compiler.Database)
		writeTokenPart(digest, compiled.continuation.compiler.Table)
	}
	writeBool(digest, compiled.relationInput != nil)
	if compiled.relationInput != nil {
		_, _ = digest.Write(compiled.relationInput.commitment[:])
		writeBool(digest, compiled.relationInput.timechartOccurrences)
		writeUint64(digest, compiled.relationInput.mvExpandRows)
		writeBool(digest, compiled.relationInput.observedWorkHeader)
	}
}

func materializeRelationInput(ctx context.Context, input *compiledRelationInput) (*ext.Table, error) {
	nativeBytes, ok, err := relationInputNativeMaterializationBytes(ctx, input)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("materialize timechart: native relation is invalid")
	}
	if nativeBytes > relationInputMaximumBytes(ctx) {
		return nil, fmt.Errorf("%w: materialize timechart native relation exceeds byte limit", ErrTimechartResourceLimit)
	}
	return materializeValidatedRelationInput(ctx, input)
}

func materializeValidatedRelationInput(
	ctx context.Context,
	input *compiledRelationInput,
) (*ext.Table, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	walk := relationTraversal{ctx: ctx}
	width := len(input.columns)
	if input.bucketEnds != nil {
		width++
	}
	definitions := make([]func(*ext.Table) error, width)
	dynamicIndexes := make([]int, 0, input.dynamicColumnCount)
	for i, fieldColumn := range input.columns {
		if !walk.step() {
			return nil, walk.err
		}
		_, physical, err := relationField(fieldColumn, i)
		if err != nil {
			return nil, err
		}
		definitions[i] = ext.Column(physical, column.Type(fieldColumn.Type))
		if fieldColumn.Type == "Dynamic" {
			dynamicIndexes = append(dynamicIndexes, i)
		}
	}
	if len(dynamicIndexes) != input.dynamicColumnCount {
		return nil, errors.New("materialize timechart: native relation is invalid")
	}
	if input.bucketEnds != nil {
		definitions[len(input.columns)] = ext.Column(ResultTimeBucketEndColumn, "DateTime64(9, 'UTC')")
	}
	table, err := ext.NewTable(timechartRelationName, definitions...)
	if err != nil {
		return nil, err
	}
	row := make([]any, width)
	defer clear(row)
	for rowIndex, source := range input.rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		copy(row, source)
		for _, index := range dynamicIndexes {
			row[index] = walk.nativeDynamic(row[index])
			if walk.err != nil {
				return nil, walk.err
			}
		}
		if input.bucketEnds != nil {
			row[len(input.columns)] = input.bucketEnds[rowIndex]
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := table.Append(row...); err != nil {
			return nil, err
		}
		clear(row)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
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

func (walk *relationTraversal) cloneAndWriteValue(digest hash.Hash, value any, depth int) (any, bool) {
	if !walk.step() {
		return nil, false
	}
	if depth > 17 {
		return nil, false
	}
	if items, ok := value.([]any); ok {
		writeTokenPart(digest, "Array(Dynamic)")
		writeBool(digest, items == nil)
		writeUint64(digest, uint64(len(items)))
		result := make([]any, len(items))
		for i, item := range items {
			cloned, ok := walk.cloneAndWriteValue(digest, item, depth+1)
			if !ok {
				return nil, false
			}
			result[i] = cloned
		}
		return result, true
	}
	cloned, ok := cloneCompiledArgument(value)
	if !ok || !writeCompiledArgument(digest, value, 0) {
		return nil, false
	}
	return cloned, true
}
func (walk *relationTraversal) nativeDynamic(value any) chcol.Dynamic {
	if !walk.step() {
		return chcol.Dynamic{}
	}
	if items, ok := value.([]any); ok {
		result := make([]chcol.Dynamic, len(items))
		for i, item := range items {
			result[i] = walk.nativeDynamic(item)
			if walk.err != nil {
				return chcol.Dynamic{}
			}
		}
		return chcol.NewDynamicWithType(result, "Array(Dynamic)")
	}
	return chcol.NewDynamic(value)
}
