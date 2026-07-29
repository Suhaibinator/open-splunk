package searchws

import (
	"context"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type synchronousSearchSnapshots interface {
	GetFor(searchjobs.AccessScope, string) (searchjobs.Job, error)
	PreviewForBytes(searchjobs.AccessScope, string, int, uint64) (searchjobs.PreviewSnapshot, error)
	MaximumPreviewRows() uint32
}

type contextualSearchSnapshotAdapter struct {
	synchronousSearchSnapshots
}

func adaptSynchronousSearchSnapshots(reader synchronousSearchSnapshots) SearchSnapshots {
	if isNil(reader) {
		return nil
	}
	return contextualSearchSnapshotAdapter{synchronousSearchSnapshots: reader}
}

func (reader contextualSearchSnapshotAdapter) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.Job{}, err
	}
	return reader.GetFor(scope, id)
}

func (reader contextualSearchSnapshotAdapter) PreviewForBytesContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return reader.PreviewForBytes(scope, id, limit, maximumBytes)
}

func (reader *blockingSearchSnapshots) PreviewForBytesContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	job, err := reader.GetForContext(ctx, scope, id)
	if err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if limit <= 0 {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrPageSize
	}
	if job.Schema == nil {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
	}
	return searchjobs.PreviewSnapshot{Job: job, Revision: job.Version}, nil
}

func (reader configTestSearchSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.Job{}, err
	}
	return reader.GetFor(scope, id)
}

func (reader configTestSearchSnapshots) PreviewForBytesContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	maximumBytes uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return reader.PreviewForBytes(scope, id, limit, maximumBytes)
}

func (reader *blockingPreviewRefreshSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.Job{}, err
	}
	return reader.snapshotFor(scope, id)
}

func (reader *blockingPreviewRefreshSnapshots) PreviewForBytesContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
	limit int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	if _, err := reader.GetForContext(ctx, scope, id); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if limit != 1 {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrPageSize
	}
	reader.calls.Add(1)
	active := reader.active.Add(1)
	defer reader.active.Add(-1)
	for {
		maximum := reader.maximum.Load()
		if active <= maximum || reader.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case reader.entered <- struct{}{}:
	default:
	}
	select {
	case <-reader.release:
		return clonePreviewSnapshot(reader.snapshot), nil
	case <-ctx.Done():
		return searchjobs.PreviewSnapshot{}, ctx.Err()
	}
}

func (reader *previewEdgeBarrierSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	job, ok := reader.jobs[id]
	if !ok || scope.TenantID != job.TenantID || scope.OwnerID != job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	select {
	case reader.entered <- id:
	case <-ctx.Done():
		return searchjobs.Job{}, ctx.Err()
	}
	select {
	case <-reader.releases[id]:
		return cloneSearchSnapshot(job), nil
	case <-ctx.Done():
		return searchjobs.Job{}, ctx.Err()
	}
}

func (reader *previewEdgeBarrierSnapshots) PreviewForBytesContext(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
	_ int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (reader *previewEdgeProjectionGateSnapshots) GetForContext(
	ctx context.Context,
	scope searchjobs.AccessScope,
	id string,
) (searchjobs.Job, error) {
	job := scopedSearchJob(id)
	if scope.TenantID != job.TenantID || scope.OwnerID != job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	active := reader.active.Add(1)
	defer reader.active.Add(-1)
	for {
		maximum := reader.maximum.Load()
		if active <= maximum || reader.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case reader.entered <- id:
	case <-ctx.Done():
		return searchjobs.Job{}, ctx.Err()
	}
	select {
	case <-reader.release:
		return job, nil
	case <-ctx.Done():
		return searchjobs.Job{}, ctx.Err()
	}
}

func (reader *previewEdgeProjectionGateSnapshots) PreviewForBytesContext(
	ctx context.Context,
	_ searchjobs.AccessScope,
	_ string,
	_ int,
	_ uint64,
) (searchjobs.PreviewSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}
