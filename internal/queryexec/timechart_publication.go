package queryexec

import (
	"context"
	"math"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

// Terminal publishers transfer their immutable row slices to this transaction.
// Ordinary atomic rows are shared, and compact chart buffers produce one owned
// Value slice per row. No complete result is copied before publication. All
// retained capacity and the eventual public sink copy are reserved first.
type stagedFinalSink struct {
	*stageBudget
	ctx                     context.Context
	schema                  searchjobs.Schema
	hasSchema               bool
	rows                    atomicResultBuffer
	boundsFirst, boundsLast *stagedFinalBoundsBlock
	rowCount, maxRows       uint64
	work                    uint64
	hasWork                 bool
}

type stagedFinalBoundsBlock struct {
	bounds [atomicRowsPerBlock]searchjobs.TimeBucketBounds
	rows   *atomicBufferedRowBlock
	next   *stagedFinalBoundsBlock
}

func (sink *stagedFinalSink) SetSchema(schema searchjobs.Schema) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if sink.hasSchema {
		return searchjobs.ErrInvalidResult
	}
	bytes := uint64(len(schema.Columns)) * uint64(unsafe.Sizeof(searchjobs.Column{}))
	for _, column := range schema.Columns {
		if err := sink.ctx.Err(); err != nil {
			return err
		}
		size := uint64(len(column.Name)) + uint64(len(column.FlatMultivalueDelimiter))
		if size > math.MaxUint64-bytes {
			return searchjobs.ErrExecutionLimit
		}
		bytes += size
	}
	if bytes > math.MaxUint64/2 {
		return searchjobs.ErrExecutionLimit
	}
	if err := sink.charge(2 * bytes); err != nil {
		return err
	}
	sink.schema, sink.hasSchema = schema, true
	return sink.ctx.Err()
}

func (sink *stagedFinalSink) AddRow(values []searchjobs.Value) error {
	return sink.append(values, searchjobs.TimeBucketBounds{})
}

func (sink *stagedFinalSink) AddRowWithTimeBucket(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	return sink.append(values, bounds)
}

func (sink *stagedFinalSink) append(values []searchjobs.Value, bounds searchjobs.TimeBucketBounds) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if !sink.hasSchema || len(values) != len(sink.schema.Columns) {
		return searchjobs.ErrInvalidResult
	}
	if sink.rowCount >= sink.maxRows {
		return searchjobs.ErrExecutionLimit
	}
	newBlock := sink.rows.last == nil || sink.rows.last.count == atomicRowsPerBlock
	hasBounds := bounds.Earliest != "" || bounds.Latest != ""
	newBounds := hasBounds && (newBlock || sink.boundsLast == nil || sink.boundsLast.rows != sink.rows.last)
	// The transaction's row headers live in charged blocks. The sink's row
	// header and both representations of each immutable value also stay live.
	bytes := uint64(unsafe.Sizeof([]searchjobs.Value{}))
	if newBlock {
		bytes += uint64(unsafe.Sizeof(atomicBufferedRowBlock{}))
	}
	if newBounds {
		bytes += uint64(unsafe.Sizeof(stagedFinalBoundsBlock{}))
	}
	if hasBounds {
		bytes += uint64(unsafe.Sizeof(bounds)) + 2*(uint64(len(bounds.Earliest))+uint64(len(bounds.Latest)))
	}
	for _, value := range values {
		size, err := value.RetainedSizeBytesContext(sink.ctx)
		if err != nil {
			return err
		}
		if size > (math.MaxUint64-bytes)/2 {
			return searchjobs.ErrExecutionLimit
		}
		bytes += 2 * size
	}
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if err := sink.charge(bytes); err != nil {
		return err
	}
	sink.rows.appendRetained(values)
	if newBounds {
		block := &stagedFinalBoundsBlock{rows: sink.rows.last}
		if sink.boundsLast == nil {
			sink.boundsFirst = block
		} else {
			sink.boundsLast.next = block
		}
		sink.boundsLast = block
	}
	if hasBounds {
		sink.boundsLast.bounds[sink.rows.last.count-1] = bounds
	}
	sink.rowCount++
	return sink.ctx.Err()
}

func (sink *stagedFinalSink) SetTimechartWork(work uint64) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if sink.hasWork && work != sink.work {
		return searchjobs.ErrInvalidResult
	}
	sink.work, sink.hasWork = work, true
	return nil
}

func (sink *stagedFinalSink) publish(recipient searchjobs.ResultSink) error {
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if !sink.hasSchema {
		return searchjobs.ErrInvalidResult
	}
	if sink.hasWork {
		if err := publishTimechartWork(recipient, sink.work); err != nil {
			return err
		}
	}
	if err := sink.ctx.Err(); err != nil {
		return err
	}
	if err := recipient.SetSchema(sink.schema); err != nil {
		return err
	}
	bounds := sink.boundsFirst
	for block := sink.rows.first; block != nil; block = block.next {
		for index := 0; index < block.count; index++ {
			if err := sink.ctx.Err(); err != nil {
				return err
			}
			var err error
			if bounds != nil && bounds.rows == block && bounds.bounds[index].Earliest != "" {
				err = publishWithTimeBucket(recipient, block.rows[index], bounds.bounds[index])
			} else {
				err = recipient.AddRow(block.rows[index])
			}
			if err != nil {
				return err
			}
		}
		if bounds != nil && bounds.rows == block {
			bounds = bounds.next
		}
	}
	return sink.ctx.Err()
}

// Bound the transient Value backing before a compact chart publisher expands
// it. The transaction subsequently measures nested payloads and retained bounds
// before taking ownership; ordinary rows already passed decoder preflight.
func preflightStagedPublicationRow(sink searchjobs.ResultSink, width int) error {
	for {
		switch wrapped := sink.(type) {
		case *timechartGridSink:
			sink = wrapped.ResultSink
		case timechartWorkSink:
			sink = wrapped.ResultSink
		case *stagedFinalSink:
			if err := wrapped.ctx.Err(); err != nil {
				return err
			}
			if width < 0 || uint64(width) > (wrapped.maxRetained-wrapped.retained)/uint64(unsafe.Sizeof(searchjobs.Value{})) {
				return searchjobs.ErrExecutionLimit
			}
			return nil
		default:
			return nil
		}
	}
}
