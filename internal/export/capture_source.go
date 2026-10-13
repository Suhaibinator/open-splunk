package export

import (
	"context"
	"errors"
	"sync"

	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const captureConsumerMemoryLimit = uint64(128 << 20)

// CaptureSource consumes SQL-published full snapshots, including snapshots
// whose worker metadata is no longer in memory after restart. It never
// reexecutes a query or treats a truncated live preview as complete output.
type CaptureSource struct {
	store  *searchartifacts.Store
	mu     sync.Mutex
	memory uint64
}

func NewCaptureSource(store *searchartifacts.Store) (*CaptureSource, error) {
	if store == nil {
		return nil, errors.New("captured export source requires artifact store")
	}
	return &CaptureSource{store: store}, nil
}
func (source *CaptureSource) reserve(bytes uint64) (func(), bool) {
	source.mu.Lock()
	if bytes > captureConsumerMemoryLimit-source.memory {
		source.mu.Unlock()
		return nil, false
	}
	source.memory += bytes
	source.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { source.mu.Lock(); source.memory -= bytes; source.mu.Unlock() }) }, true
}
func (source *CaptureSource) AcquireResultsFor(ctx context.Context, access searchjobs.AccessScope, id string) (searchjobs.ResultLease, error) {
	record, err := source.store.Get(ctx, access, id, searchartifacts.AccessInspect)
	if err != nil {
		return nil, mapCaptureSourceError(err)
	}
	if record.State != searchartifacts.StateCompleted || !record.Job.CaptureComplete || record.Job.ResultsTruncated {
		return nil, searchjobs.ErrResultsUnavailable
	}
	pin, err := source.store.AcquireCaptureBounded(ctx, access, id, source.reserve)
	if err != nil {
		return nil, mapCaptureSourceError(err)
	}
	schema := pin.Schema()
	bytes, valid, err := measureTrustedSourceSchema(ctx, schema)
	if err != nil || !valid {
		_ = pin.Close()
		if err != nil {
			return nil, err
		}
		return nil, searchjobs.ErrResultsUnavailable
	}
	return &continuationResultLease{ResultLease: pin, knowledgeSnapshot: record.Job.KnowledgeSnapshot, schema: schema, schemaBytes: bytes}, nil
}
func mapCaptureSourceError(err error) error {
	switch {
	case errors.Is(err, searchartifacts.ErrNotFound):
		return searchjobs.ErrNotFound
	case errors.Is(err, searchartifacts.ErrExpired):
		return searchjobs.ErrExpired
	case errors.Is(err, searchartifacts.ErrNotReady):
		return searchjobs.ErrResultsNotReady
	case errors.Is(err, searchartifacts.ErrCapacity):
		return searchjobs.ErrCapacity
	default:
		return err
	}
}

var _ ResultSource = (*CaptureSource)(nil)
