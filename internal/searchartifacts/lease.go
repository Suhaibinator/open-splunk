package searchartifacts

import (
	"context"
	"strings"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type resultLease struct {
	store      *Store
	jobID      string
	generation uint64
	schema     searchjobs.Schema
	rowCount   uint64
	rowExact   bool
	truncated  bool
	rows       artifactRowSource

	mu        sync.Mutex
	closeOnce sync.Once
	closed    bool
	closeErr  error
}

func (lease *resultLease) Schema() searchjobs.Schema { return cloneSchema(lease.schema) }

func (lease *resultLease) RowCount() uint64 { return lease.rowCount }

func (lease *resultLease) RowCountExact() bool { return lease.rowExact }

func (lease *resultLease) ResultsTruncated() bool { return lease.truncated }

func (lease *resultLease) Generation() uint64 { return lease.generation }

func (lease *resultLease) Next(ctx context.Context) (searchjobs.ResultRow, bool, error) {
	if ctx == nil {
		return searchjobs.ResultRow{}, false, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return searchjobs.ResultRow{}, false, err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ResultRow{}, false, searchjobs.ErrResultLeaseClosed
	}
	return lease.rows.Next(ctx)
}

// Seek moves to a zero-based row offset. Framed artifacts use their sparse
// on-disk index and decode at most one index stride; legacy artifacts retain a
// compatibility-only forward scan.
func (lease *resultLease) Seek(ctx context.Context, offset uint64) error {
	if ctx == nil {
		return ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	lease.mu.Lock()
	defer lease.mu.Unlock()
	if lease.closed {
		return searchjobs.ErrResultLeaseClosed
	}
	if offset > lease.rowCount {
		return ErrInvalid
	}
	return lease.rows.Seek(ctx, offset)
}

func (lease *resultLease) Close() error {
	if lease == nil {
		return nil
	}
	lease.closeOnce.Do(func() {
		lease.mu.Lock()
		lease.closed = true
		if lease.rows != nil {
			lease.closeErr = lease.rows.Close()
			lease.rows = nil
		}
		lease.mu.Unlock()
		lease.store.releasePin(lease.jobID)
	})
	return lease.closeErr
}

func cloneSchema(schema searchjobs.Schema) searchjobs.Schema {
	columns := make([]searchjobs.Column, len(schema.Columns))
	copy(columns, schema.Columns)
	for index := range columns {
		columns[index].Name = strings.Clone(columns[index].Name)
		columns[index].FlatMultivalueDelimiter = strings.Clone(columns[index].FlatMultivalueDelimiter)
	}
	return searchjobs.Schema{Columns: columns}
}

var _ ResultLease = (*resultLease)(nil)
var _ SeekableResultLease = (*resultLease)(nil)
var _ searchjobs.ResultLease = (*resultLease)(nil)
