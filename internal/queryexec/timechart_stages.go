package queryexec

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchlimits"
)

// stageBudget is shared across physical stages. Progress callbacks may run on
// the driver goroutine, so totals and downstream progress share one mutex.
type stageBudget struct {
	mu                             sync.Mutex
	sink                           searchjobs.ResultSink
	rows, bytes, retained          uint64
	maxRows, maxBytes, maxRetained uint64
}

func (budget *stageBudget) ReportProgress(delta searchjobs.ExecutionProgressDelta) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if delta.ScannedRows > budget.maxRows-budget.rows || delta.ScannedBytes > budget.maxBytes-budget.bytes {
		return searchjobs.ErrExecutionLimit
	}
	budget.rows += delta.ScannedRows
	budget.bytes += delta.ScannedBytes
	if sink, ok := budget.sink.(searchjobs.ProgressSink); ok {
		return sink.ReportProgress(delta)
	}
	return nil
}
func (budget *stageBudget) charge(bytes uint64) error {
	if bytes > budget.maxRetained-budget.retained {
		return searchjobs.ErrExecutionLimit
	}
	budget.retained += bytes
	return nil
}

type timechartStageSink struct {
	*stageBudget
	columns       []clickhouse.RelationColumn
	rows          [][]any
	maxResultRows uint64
	bucketEnds    []time.Time
}

func (sink *timechartStageSink) SetSchema(schema searchjobs.Schema) error {
	if sink.columns != nil {
		return searchjobs.ErrInvalidResult
	}
	if err := sink.charge(uint64(len(schema.Columns)) * uint64(unsafe.Sizeof(clickhouse.RelationColumn{}))); err != nil {
		return err
	}
	sink.columns = make([]clickhouse.RelationColumn, len(schema.Columns))
	for i, column := range schema.Columns {
		kind := ""
		switch column.Kind {
		case searchjobs.ValueKindMixed, searchjobs.ValueKindList:
			kind = "Dynamic"
		case searchjobs.ValueKindTime:
			kind = "DateTime64(9, 'UTC')"
		case searchjobs.ValueKindUnsigned:
			kind = "UInt64"
		case searchjobs.ValueKindSigned:
			kind = "Int64"
		case searchjobs.ValueKindDouble:
			kind = "Float64"
		case searchjobs.ValueKindString:
			kind = "String"
		case searchjobs.ValueKindBool:
			kind = "Bool"
		default:
			return fmt.Errorf("%w: timechart continuation has unsupported column type", searchjobs.ErrInvalidResult)
		}
		if column.Nullable && kind != "Dynamic" {
			kind = "Nullable(" + kind + ")"
		}
		if err := sink.charge(uint64(len(column.Name) + len(kind))); err != nil {
			return err
		}
		sink.columns[i] = clickhouse.RelationColumn{Name: column.Name, Type: kind}
	}
	return nil
}
func (sink *timechartStageSink) AddRow(values []searchjobs.Value) error {
	if sink.columns == nil || len(values) != len(sink.columns) {
		return searchjobs.ErrInvalidResult
	}
	if uint64(len(sink.rows)) >= sink.maxResultRows {
		return searchjobs.ErrExecutionLimit
	}
	// Charge both row-slice capacity and the detached compiler/native transports.
	bytes := 4 * (uint64(unsafe.Sizeof([]any{})) + uint64(len(values))*uint64(unsafe.Sizeof(any(nil))))
	for _, value := range values {
		retained, err := value.RetainedSizeBytes()
		if err != nil || retained > math.MaxUint64-bytes {
			return searchjobs.ErrExecutionLimit
		}
		bytes += retained
	}
	if err := sink.charge(bytes); err != nil {
		return err
	}
	row := make([]any, len(values))
	for i, value := range values {
		switch value.Kind() {
		case searchjobs.ValueKindNull, searchjobs.ValueKindMissing:
			row[i] = nil
		case searchjobs.ValueKindList:
			var err error
			row[i], err = stageDynamicValue(value)
			if err != nil {
				return err
			}
		case searchjobs.ValueKindTime:
			row[i], _ = value.Time()
		case searchjobs.ValueKindUnsigned:
			row[i], _ = value.Unsigned()
		case searchjobs.ValueKindSigned:
			row[i], _ = value.Signed()
		case searchjobs.ValueKindDouble:
			row[i], _ = value.Double()
		case searchjobs.ValueKindString:
			row[i], _ = value.String()
		case searchjobs.ValueKindBool:
			row[i], _ = value.Bool()
		default:
			return searchjobs.ErrInvalidResult
		}
	}
	sink.rows = append(sink.rows, row)
	return nil
}

type stagedFinalSink struct {
	searchjobs.ResultSink
	*stageBudget
}

func (sink stagedFinalSink) ReportProgress(delta searchjobs.ExecutionProgressDelta) error {
	return sink.stageBudget.ReportProgress(delta)
}

func (executor *Executor) executeTimechartStages(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) error {
	if ctx == nil || sink == nil {
		return searchjobs.ErrInvalidResult
	}
	authorityBytes, valid, err := query.RetainedBytesContext(ctx)
	if err != nil {
		return err
	}
	if !valid {
		return searchjobs.ErrInvalidResult
	}
	admissionPolicy := searchlimits.Default()
	if policy, ok := searchlimits.FromContext(ctx); ok {
		admissionPolicy = policy
	}
	if authorityBytes > min(admissionPolicy.MaxResultBytes, admissionPolicy.MaxMemoryBytes)/2 {
		return searchjobs.ErrExecutionLimit
	}
	detached, ok, err := query.CloneForExecutionContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return searchjobs.ErrInvalidResult
	}
	query = detached
	admitted, release, err := executor.acquireRead(ctx, query, "execute staged timechart")
	if err != nil {
		return err
	}
	defer release()
	settings, err := executor.settingsForContext(admitted, query)
	if err != nil {
		return err
	}
	seconds, _ := settings["max_execution_time"].(uint64)
	ctx, cancel := context.WithTimeout(admitted, time.Duration(min(seconds, uint64(math.MaxInt64/int64(time.Second))))*time.Second)
	defer cancel()
	base, expand := executor.settingsSnapshot()
	frozen := &Executor{connection: executor.connection, settings: base, expandTimechartGroupLimit: expand, newQueryID: executor.newQueryID, withProgress: executor.withProgress, readAdmission: executor.readAdmission}
	policy := searchlimits.Default()
	if admittedPolicy, ok := searchlimits.FromContext(ctx); ok {
		policy = admittedPolicy
	}
	maximumRetained := min(policy.MaxResultBytes, policy.MaxMemoryBytes, settings["max_memory_usage"].(uint64), settings["max_result_bytes"].(uint64))
	budget := &stageBudget{sink: sink, maxRows: settings["max_rows_to_read"].(uint64), maxBytes: settings["max_bytes_to_read"].(uint64), maxRetained: maximumRetained}
	// The retained descriptor coexists with its clone and native lookup transport.
	if err := budget.charge(authorityBytes); err != nil {
		return err
	}
	if err := budget.charge(authorityBytes); err != nil {
		return err
	}

	for query.HasContinuation() {
		rowLimit := base.limit("max_result_rows")
		if query.RequiresTimechartInputDiscovery() {
			rowLimit = settings["max_rows_to_read"].(uint64)
		}
		stage := &timechartStageSink{stageBudget: budget, maxResultRows: rowLimit}
		if err := frozen.executeSingle(ctx, query, stage); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		query, err = query.ContinueWithTimeBucketsContext(ctx, stage.columns, stage.rows, stage.bucketEnds)
		if err != nil {
			return err
		}
	}
	if recipient, ok := sink.(searchjobs.CompiledResultSink); ok {
		if err := recipient.SetCompiledQuery(query); err != nil {
			return err
		}
	}
	return frozen.executeSingle(ctx, query, stagedFinalSink{ResultSink: sink, stageBudget: budget})
}

func (sink *timechartStageSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	start, err := time.Parse(time.RFC3339Nano, bounds.Earliest)
	end, endErr := time.Parse(time.RFC3339Nano, bounds.Latest)
	if err != nil || endErr != nil || start.UTC().Format(time.RFC3339Nano) != bounds.Earliest || end.UTC().Format(time.RFC3339Nano) != bounds.Latest || !start.Before(end) || len(values) == 0 {
		return searchjobs.ErrInvalidResult
	}
	actual, ok := values[0].Time()
	if !ok || !actual.Equal(start) {
		return searchjobs.ErrInvalidResult
	}
	if len(sink.rows) != len(sink.bucketEnds) {
		return searchjobs.ErrInvalidResult
	}
	if err := sink.charge(2 * uint64(unsafe.Sizeof(time.Time{}))); err != nil {
		return err
	}
	if err := sink.AddRow(values); err != nil {
		return err
	}
	sink.bucketEnds = append(sink.bucketEnds, end)
	return nil
}
func (sink stagedFinalSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	if err := sink.charge(uint64(unsafe.Sizeof(bounds)) + uint64(len(bounds.Earliest)+len(bounds.Latest))); err != nil {
		return err
	}
	if err := sink.chargeValues(values); err != nil {
		return err
	}
	return publishWithTimeBucket(sink.ResultSink, values, bounds)
}

func stageDynamicValue(value searchjobs.Value) (any, error) {
	switch value.Kind() {
	case searchjobs.ValueKindMissing, searchjobs.ValueKindNull:
		return nil, nil
	case searchjobs.ValueKindString:
		v, _ := value.String()
		return v, nil
	case searchjobs.ValueKindSigned:
		v, _ := value.Signed()
		return v, nil
	case searchjobs.ValueKindUnsigned:
		v, _ := value.Unsigned()
		return v, nil
	case searchjobs.ValueKindDouble:
		v, _ := value.Double()
		return v, nil
	case searchjobs.ValueKindBool:
		v, _ := value.Bool()
		return v, nil
	case searchjobs.ValueKindTime:
		v, _ := value.Time()
		return v, nil
	case searchjobs.ValueKindList:
		items, _ := value.List()
		result := make([]any, len(items))
		for i, item := range items {
			v, err := stageDynamicValue(item)
			if err != nil {
				return nil, err
			}
			result[i] = v
		}
		return result, nil
	default:
		return nil, searchjobs.ErrInvalidResult
	}
}

func publishEmptyObservedTimechart(sink searchjobs.ResultSink, query clickhouse.CompiledQuery) error {
	if query.Timechart == nil {
		return searchjobs.ErrInvalidResult
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_time", Kind: searchjobs.ValueKindTime}}}
	switch query.Timechart.Mode {
	case clickhouse.TimechartModeFixedCount, clickhouse.TimechartModeFixedFieldCount:
		if len(query.OutputFields) != 2 {
			return searchjobs.ErrInvalidResult
		}
		schema.Columns = append(schema.Columns, searchjobs.Column{Name: query.OutputFields[1], Kind: searchjobs.ValueKindUnsigned})
	case clickhouse.TimechartModeFixedValue:
		if len(query.OutputFields) != 2 {
			return searchjobs.ErrInvalidResult
		}
		schema.Columns = append(schema.Columns, searchjobs.Column{Name: query.OutputFields[1], Kind: searchjobs.ValueKindDouble, Nullable: true})
	case clickhouse.TimechartModeRuntimeWide, clickhouse.TimechartModeRuntimeWideValue:
	default:
		return searchjobs.ErrInvalidResult
	}
	return sink.SetSchema(schema)
}

func (sink stagedFinalSink) SetSchema(schema searchjobs.Schema) error {
	if err := sink.charge(uint64(len(schema.Columns)) * uint64(unsafe.Sizeof(searchjobs.Column{}))); err != nil {
		return err
	}
	for _, column := range schema.Columns {
		if err := sink.charge(uint64(len(column.Name) + len(column.FlatMultivalueDelimiter))); err != nil {
			return err
		}
	}
	return sink.ResultSink.SetSchema(schema)
}
func (sink stagedFinalSink) chargeValues(values []searchjobs.Value) error {
	bytes := uint64(unsafe.Sizeof([]searchjobs.Value{})) + uint64(len(values))*uint64(unsafe.Sizeof(searchjobs.Value{}))
	for _, value := range values {
		size, err := value.RetainedSizeBytes()
		if err != nil || size > math.MaxUint64-bytes {
			return searchjobs.ErrExecutionLimit
		}
		bytes += size
	}
	return sink.charge(bytes)
}
func (sink stagedFinalSink) AddRow(values []searchjobs.Value) error {
	if err := sink.chargeValues(values); err != nil {
		return err
	}
	return sink.ResultSink.AddRow(values)
}
