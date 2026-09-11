package queryexec

import (
	"context"
	"errors"
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
	maxMemory                      uint64
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
	ctx  context.Context
	work uint64
	*stageBudget
	columns       []clickhouse.RelationColumn
	rows          [][]any
	maxResultRows uint64
	bucketEnds    []time.Time
}

func (sink *timechartStageSink) SetSchema(schema searchjobs.Schema) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if sink.columns != nil {
		return searchjobs.ErrInvalidResult
	}
	if err := sink.charge(uint64(len(schema.Columns)) * uint64(unsafe.Sizeof(clickhouse.RelationColumn{}))); err != nil {
		return err
	}
	sink.columns = make([]clickhouse.RelationColumn, len(schema.Columns))
	for i, column := range schema.Columns {
		if err := sink.ctx.Err(); err != nil {
			return err
		}
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
	return sink.ctx.Err()
}
func (sink *timechartStageSink) AddRow(values []searchjobs.Value) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if sink.columns == nil || len(values) != len(sink.columns) {
		return searchjobs.ErrInvalidResult
	}
	if uint64(len(sink.rows)) >= sink.maxResultRows {
		return searchjobs.ErrExecutionLimit
	}
	// Charge both row-slice capacity and the detached compiler/native transports.
	bytes := 4 * (uint64(unsafe.Sizeof([]any{})) + uint64(len(values))*uint64(unsafe.Sizeof(any(nil))))
	for _, value := range values {
		retained, err := value.RetainedSizeBytesContext(sink.ctx)
		if err != nil {
			return err
		}
		if retained > math.MaxUint64-bytes {
			return searchjobs.ErrExecutionLimit
		}
		bytes += retained
	}
	if err := sink.charge(bytes); err != nil {
		return err
	}
	row := make([]any, len(values))
	for i, value := range values {
		if err := sink.ctx.Err(); err != nil {
			return err
		}
		switch value.Kind() {
		case searchjobs.ValueKindNull, searchjobs.ValueKindMissing:
			row[i] = nil
		case searchjobs.ValueKindList:
			var err error
			row[i], err = stageDynamicValue(sink.ctx, value)
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
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	sink.rows = append(sink.rows, row)
	return sink.ctx.Err()
}

func (executor *Executor) executeTimechartStages(ctx context.Context, query clickhouse.CompiledQuery, sink searchjobs.ResultSink) (resultErr error) {
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
	defer func() {
		resultErr = preserveReadCancellationCause(admitted, resultErr)
	}()
	base, expand, err := executor.effectiveSettingsSnapshot(admitted)
	if err != nil {
		return err
	}
	frozen := &Executor{connection: executor.connection, settings: base, expandTimechartGroupLimit: expand, newQueryID: executor.newQueryID, withProgress: executor.withProgress, readAdmission: executor.readAdmission}
	settings, err := frozen.settingsForContext(admitted, query)
	if err != nil {
		return err
	}
	seconds, _ := settings["max_execution_time"].(uint64)
	ctx, cancel := context.WithTimeout(admitted, time.Duration(min(seconds, uint64(math.MaxInt64/int64(time.Second))))*time.Second)
	defer cancel()
	policy := searchlimits.Default()
	logicalRowLimit := logicalResultRowLimit(ctx, base.limit("max_result_rows"))
	if admittedPolicy, ok := searchlimits.FromContext(ctx); ok {
		policy = admittedPolicy
	}
	maximumRetained := min(policy.MaxResultBytes, policy.MaxMemoryBytes, settings["max_memory_usage"].(uint64), settings["max_result_bytes"].(uint64))
	budget := &stageBudget{sink: sink, maxRows: settings["max_rows_to_read"].(uint64), maxBytes: settings["max_bytes_to_read"].(uint64), maxRetained: maximumRetained, maxMemory: settings["max_memory_usage"].(uint64)}
	// The manager-pinned descriptor coexists with this detached execution clone.
	// Transient native external tables are bounded separately by the allocation
	// context supplied to each physical stage.
	if err := budget.charge(authorityBytes); err != nil {
		return err
	}
	if err := budget.charge(authorityBytes); err != nil {
		return err
	}

	for query.HasContinuation() {
		rowLimit := logicalRowLimit
		if query.RequiresTimechartInputDiscovery() {
			rowLimit = settings["max_rows_to_read"].(uint64)
		}
		stage := &timechartStageSink{ctx: ctx, stageBudget: budget, maxResultRows: rowLimit, work: query.TimechartWorkFloor()}
		if err := frozen.executeAdmittedStage(budget.allocationContext(ctx), query, stage); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		query, err = query.ContinueWithTimeBucketsAndWorkContext(budget.allocationContext(ctx), stage.columns, stage.rows, stage.bucketEnds, stage.work)
		if err != nil {
			if errors.Is(err, clickhouse.ErrTimechartResourceLimit) {
				return fmt.Errorf("%w: %w", searchjobs.ErrExecutionLimit, err)
			}
			return err
		}
		// The decoder and native transport have returned, and the next
		// compiler owns a detached input. Drop the stage copy before replacing
		// its temporary reservation with the next query's measured residency.
		stage.rows, stage.columns, stage.bucketEnds = nil, nil, nil
		residentBytes, valid, err := query.RetainedBytesContext(ctx)
		if err != nil {
			return err
		}
		if !valid {
			return searchjobs.ErrInvalidResult
		}
		if err := budget.replaceResident(authorityBytes, residentBytes); err != nil {
			return err
		}
	}
	// Keep native input, transport and decoder reservations live while the
	// terminal publication transaction owns its rows. The fourth share covers
	// both those rows and the downstream sink's detached copy.
	stageContext := budget.allocationContext(ctx)
	share, _ := searchlimits.RemainingExecutionBytes(stageContext)
	if err := budget.charge(3 * share); err != nil {
		return err
	}
	if err := budget.charge(uint64(unsafe.Sizeof(stagedFinalSink{}))); err != nil {
		return err
	}
	transaction := &stagedFinalSink{ctx: ctx, stageBudget: budget, maxRows: logicalRowLimit}
	if err := frozen.executeAdmittedStage(stageContext, query, transaction); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if recipient, ok := sink.(searchjobs.CompiledResultSink); ok {
		if err := recipient.SetCompiledQuery(query); err != nil {
			return err
		}
	}
	return transaction.publish(sink)
}

func (sink *timechartStageSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
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
	return sink.ctx.Err()
}
func stageDynamicValue(ctx context.Context, value searchjobs.Value) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type frame struct {
		values []any
		next   int
	}
	var stack [18]frame
	depth := 0
	var result any
	appendValue := func(value any) error {
		if depth == 0 {
			result = value
			return nil
		}
		parent := &stack[depth-1]
		if parent.next >= len(parent.values) {
			return searchjobs.ErrInvalidResult
		}
		parent.values[parent.next] = value
		parent.next++
		return nil
	}
	nodes := 0
	err := value.VisitDetached(func(token searchjobs.ValueVisitToken) error {
		if nodes%128 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		nodes++
		var scalar any
		switch token.Kind {
		case searchjobs.ValueVisitListBegin:
			if depth >= len(stack) {
				return searchjobs.ErrInvalidResult
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			values := make([]any, token.Length)
			if err := appendValue(values); err != nil {
				return err
			}
			stack[depth] = frame{values: values}
			depth++
			return nil
		case searchjobs.ValueVisitListEnd:
			if depth == 0 || stack[depth-1].next != len(stack[depth-1].values) {
				return searchjobs.ErrInvalidResult
			}
			depth--
			return nil
		case searchjobs.ValueVisitMissing, searchjobs.ValueVisitNull:
		case searchjobs.ValueVisitString:
			scalar = token.StringValue
		case searchjobs.ValueVisitSigned:
			scalar = token.SignedValue
		case searchjobs.ValueVisitUnsigned:
			scalar = token.UnsignedValue
		case searchjobs.ValueVisitDouble:
			scalar = token.DoubleValue
		case searchjobs.ValueVisitBool:
			scalar = token.BoolValue
		case searchjobs.ValueVisitTime:
			scalar = token.TimeValue
		default:
			return searchjobs.ErrInvalidResult
		}
		return appendValue(scalar)
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// Four disjoint shares cover native input, server transport, decoder, and
// detached stage output while the prior retained stages stay charged.
// replaceResident releases completed intermediate ownership; only scan work
// is cumulative. The original admitted descriptor remains pinned by the job.
func (budget *stageBudget) replaceResident(original, current uint64) error {
	if original > budget.maxRetained || current > budget.maxRetained-original {
		return searchjobs.ErrExecutionLimit
	}
	budget.retained = original + current
	return nil
}

func (budget *stageBudget) allocationContext(ctx context.Context) context.Context {
	share := (budget.maxRetained - budget.retained) / 4
	ctx = searchlimits.WithRemainingExecutionBytes(ctx, share)
	memory := budget.maxMemory
	if memory == 0 {
		memory = budget.maxRetained
	}
	// Result retention has its own smaller ceiling. Leave the server its
	// admitted working memory after reserving the three live client shares.
	client := budget.retained + 3*share
	if client >= memory {
		return searchlimits.WithRemainingExecutionMemoryBytes(ctx, 0)
	}
	return searchlimits.WithRemainingExecutionMemoryBytes(ctx, memory-client)
}

func validateFixedTimechartAllocation(ctx context.Context, output clickhouse.TimechartOutput, descriptorBytes, cellBytes uint64) error {
	remaining, constrained := searchlimits.RemainingExecutionBytes(ctx)
	if !constrained {
		return nil
	}
	if output.Calendar {
		cellBytes += uint64(unsafe.Sizeof(time.Time{}))
	}
	if output.ExactGrid {
		cellBytes++
		descriptorBytes += uint64(unsafe.Sizeof(timechartGridRows{}))
	}
	if descriptorBytes > remaining || output.BucketCount > (remaining-descriptorBytes)/cellBytes {
		return searchjobs.ErrExecutionLimit
	}
	return nil
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

func (sink *timechartStageSink) SetTimechartWork(work uint64) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if work < sink.work {
		return searchjobs.ErrInvalidResult
	}
	sink.work = work
	return sink.ctx.Err()
}
