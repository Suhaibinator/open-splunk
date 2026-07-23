package searchws

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/gorilla/websocket"
)

type blockingPreviewRefreshSnapshots struct {
	job      searchjobs.Job
	snapshot searchjobs.PreviewSnapshot
	entered  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
	active   atomic.Int32
	maximum  atomic.Int32
}

func (reader *blockingPreviewRefreshSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	if id != reader.job.ID || scope.TenantID != reader.job.TenantID || scope.OwnerID != reader.job.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(reader.job), nil
}

func (reader *blockingPreviewRefreshSnapshots) PreviewFor(scope searchjobs.AccessScope, id string, limit int) (searchjobs.PreviewSnapshot, error) {
	if _, err := reader.GetFor(scope, id); err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if limit != 1 {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrPageSize
	}
	reader.calls.Add(1)
	active := reader.active.Add(1)
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
	<-reader.release
	reader.active.Add(-1)
	return clonePreviewSnapshot(reader.snapshot), nil
}

func (reader *blockingPreviewRefreshSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*blockingPreviewRefreshSnapshots) MaximumPreviewRows() uint32 { return 1 }

// These methods keep the package's existing focused fakes compatible with the
// live-preview capability. Tests that do not opt into previews should never
// need a row snapshot.
func (configTestSearchSnapshots) PreviewFor(searchjobs.AccessScope, string, int) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (reader configTestSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (configTestSearchSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

func (stubSearchSnapshots) PreviewFor(searchjobs.AccessScope, string, int) (searchjobs.PreviewSnapshot, error) {
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (reader stubSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (stubSearchSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

func (reader *adversarialSearchSnapshots) PreviewFor(searchjobs.AccessScope, string, int) (searchjobs.PreviewSnapshot, error) {
	reader.calls.Add(1)
	return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
}

func (reader *adversarialSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*adversarialSearchSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

func (reader *mutableSearchSnapshots) PreviewFor(scope searchjobs.AccessScope, id string, limit int) (searchjobs.PreviewSnapshot, error) {
	job, err := reader.GetFor(scope, id)
	if err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if job.Schema == nil {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
	}
	return searchjobs.PreviewSnapshot{Job: job, Revision: job.Version}, nil
}

func (reader *mutableSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*mutableSearchSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

func (reader *blockingSearchSnapshots) PreviewFor(scope searchjobs.AccessScope, id string, limit int) (searchjobs.PreviewSnapshot, error) {
	job, err := reader.GetFor(scope, id)
	if err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if job.Schema == nil {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
	}
	return searchjobs.PreviewSnapshot{Job: job, Revision: job.Version}, nil
}

func (reader *blockingSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*blockingSearchSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

func (reader *stagedTerminalSnapshots) PreviewFor(scope searchjobs.AccessScope, id string, limit int) (searchjobs.PreviewSnapshot, error) {
	job, err := reader.GetFor(scope, id)
	if err != nil {
		return searchjobs.PreviewSnapshot{}, err
	}
	if job.Schema == nil {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
	}
	return searchjobs.PreviewSnapshot{Job: job, Revision: job.Version}, nil
}

func (reader *stagedTerminalSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (*stagedTerminalSnapshots) MaximumPreviewRows() uint32 { return defaultMaximumPreviewRows }

// previewSearchSnapshots returns the Job and preview rows from one atomic fake
// snapshot. PreviewFor applies the caller's row cap so transport tests catch a
// design that separately fetches inconsistent Job and result revisions.
type previewSearchSnapshots struct {
	mu             sync.Mutex
	snapshots      map[string]searchjobs.PreviewSnapshot
	previewLimits  []int
	previewFailure error
}

func (*previewSearchSnapshots) MaximumPreviewRows() uint32 { return maximumPreviewRowsCeiling }

func newPreviewSearchSnapshots(snapshots ...searchjobs.PreviewSnapshot) *previewSearchSnapshots {
	reader := &previewSearchSnapshots{snapshots: make(map[string]searchjobs.PreviewSnapshot)}
	for _, snapshot := range snapshots {
		reader.snapshots[snapshot.Job.ID] = clonePreviewSnapshot(snapshot)
	}
	return reader
}

func (reader *previewSearchSnapshots) GetFor(scope searchjobs.AccessScope, id string) (searchjobs.Job, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	snapshot, ok := reader.snapshots[id]
	if !ok || snapshot.Job.TenantID != scope.TenantID || snapshot.Job.OwnerID != scope.OwnerID {
		return searchjobs.Job{}, searchjobs.ErrNotFound
	}
	return cloneSearchSnapshot(snapshot.Job), nil
}

func (reader *previewSearchSnapshots) PreviewFor(scope searchjobs.AccessScope, id string, limit int) (searchjobs.PreviewSnapshot, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.previewLimits = append(reader.previewLimits, limit)
	if reader.previewFailure != nil {
		return searchjobs.PreviewSnapshot{}, reader.previewFailure
	}
	snapshot, ok := reader.snapshots[id]
	if !ok || snapshot.Job.TenantID != scope.TenantID || snapshot.Job.OwnerID != scope.OwnerID {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrNotFound
	}
	if snapshot.Job.Schema == nil {
		switch snapshot.Job.State {
		case searchjobs.StateFailed, searchjobs.StateCanceled, searchjobs.StateExpired:
			return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsUnavailable
		default:
			return searchjobs.PreviewSnapshot{}, searchjobs.ErrResultsNotReady
		}
	}
	if limit <= 0 {
		return searchjobs.PreviewSnapshot{}, searchjobs.ErrPageSize
	}
	result := clonePreviewSnapshot(snapshot)
	if limit < len(result.Rows) {
		result.Rows = result.Rows[:limit:limit]
		result.Truncated = true
	}
	return result, nil
}

func (reader *previewSearchSnapshots) PreviewForBytes(scope searchjobs.AccessScope, id string, limit int, _ uint64) (searchjobs.PreviewSnapshot, error) {
	return reader.PreviewFor(scope, id, limit)
}

func (reader *previewSearchSnapshots) set(snapshot searchjobs.PreviewSnapshot) {
	reader.mu.Lock()
	reader.snapshots[snapshot.Job.ID] = clonePreviewSnapshot(snapshot)
	reader.mu.Unlock()
}

func (reader *previewSearchSnapshots) requestedPreviewLimits() []int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return append([]int(nil), reader.previewLimits...)
}

func clonePreviewSnapshot(snapshot searchjobs.PreviewSnapshot) searchjobs.PreviewSnapshot {
	cloned := snapshot
	cloned.Job = cloneSearchSnapshot(snapshot.Job)
	cloned.Rows = make([]searchjobs.ResultRow, len(snapshot.Rows))
	for index, row := range snapshot.Rows {
		cloned.Rows[index] = searchjobs.ResultRow{
			Ordinal: row.Ordinal,
			Values:  append([]searchjobs.Value(nil), row.Values...),
		}
	}
	return cloned
}

func previewSnapshot(job searchjobs.Job, rows ...searchjobs.ResultRow) searchjobs.PreviewSnapshot {
	job.RowCount = uint64(len(rows))
	return searchjobs.PreviewSnapshot{
		Job:      job,
		Rows:     rows,
		Revision: job.Version,
	}
}

func subscribeWithPreview(requestID, subscriptionID, jobID string, after, limit uint64) *opensplunkv1.SearchWebSocketCommand {
	command := subscribeCommand(requestID, subscriptionID, jobID, after)
	rowLimit := uint32(limit)
	input := command.GetSubscribe().Subscriptions[0]
	input.IncludePreviews = true
	input.PreviewRowLimit = &rowLimit
	return command
}

type previewObservation struct {
	ack      *opensplunkv1.SubscriptionAcknowledged
	schema   *opensplunkv1.SearchWebSocketEvent
	preview  *opensplunkv1.SearchWebSocketEvent
	terminal *opensplunkv1.SearchWebSocketEvent
}

func readPreviewObservations(
	t *testing.T,
	client *websocket.Conn,
	subscriptionIDs ...string,
) map[string]*previewObservation {
	t.Helper()
	wanted := make(map[string]struct{}, len(subscriptionIDs))
	result := make(map[string]*previewObservation, len(subscriptionIDs))
	for _, id := range subscriptionIDs {
		wanted[id] = struct{}{}
		result[id] = &previewObservation{}
	}
	for reads := 0; reads < len(subscriptionIDs)*8; reads++ {
		event := readEvent(t, client)
		if protocolError := event.GetProtocolError(); protocolError != nil {
			t.Fatalf("preview subscription protocol error = %+v", protocolError)
		}
		if resync := event.GetResynchronizationRequired(); resync != nil {
			t.Fatalf("preview subscription unexpectedly resynchronized = %+v", resync)
		}
		if ack := event.GetSubscriptionAcknowledged(); ack != nil {
			if _, ok := wanted[ack.GetSubscriptionId()]; ok {
				result[ack.GetSubscriptionId()].ack = ack
			}
			continue
		}
		id := event.GetSubscriptionId()
		observation := result[id]
		if observation == nil {
			t.Fatalf("event for unexpected subscription %q: %+v", id, event)
		}
		switch {
		case event.GetResultSchemaAvailable() != nil:
			observation.schema = event
		case event.GetResultPreview() != nil:
			observation.preview = event
		case event.GetSearchTerminal() != nil:
			observation.terminal = event
		}
		complete := true
		for _, wantedID := range subscriptionIDs {
			item := result[wantedID]
			if item.ack == nil || item.schema == nil || item.preview == nil {
				complete = false
				break
			}
		}
		if complete {
			return result
		}
	}
	t.Fatalf("did not receive schema and preview for subscriptions %v: %+v", subscriptionIDs, result)
	return nil
}

func TestWebSocketPreviewFollowsSchemaAndResetsBoundedTypedRows(t *testing.T) {
	job := scopedSearchJob("search-preview-typed")
	job.Version = 7
	eventTime := time.Date(2026, 7, 22, 12, 34, 56, 789_000_000, time.UTC)
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "message", Kind: searchjobs.ValueKindString},
		{Name: "count", Kind: searchjobs.ValueKindUnsigned},
		{Name: "healthy", Kind: searchjobs.ValueKindBool},
		{Name: "_time", Kind: searchjobs.ValueKindTime},
	}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{
			searchjobs.StringValue("first"), searchjobs.UnsignedValue(7), searchjobs.BoolValue(true), searchjobs.TimeValue(eventTime),
		}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{
			searchjobs.StringValue("second"), searchjobs.UnsignedValue(8), searchjobs.BoolValue(false), searchjobs.TimeValue(eventTime.Add(time.Second)),
		}},
		searchjobs.ResultRow{Ordinal: 2, Values: []searchjobs.Value{
			searchjobs.StringValue("third"), searchjobs.UnsignedValue(9), searchjobs.BoolValue(true), searchjobs.TimeValue(eventTime.Add(2 * time.Second)),
		}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 3
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("typed", "typed", job.ID, 0, 2))

	observation := readPreviewObservations(t, client, "typed")["typed"]
	if observation.schema.GetSequence() >= observation.preview.GetSequence() {
		t.Fatalf("schema sequence %d must precede preview sequence %d", observation.schema.GetSequence(), observation.preview.GetSequence())
	}
	preview := observation.preview.GetResultPreview()
	if preview.GetSearchJobId() != job.ID || preview.GetSchemaId() != job.ID ||
		preview.GetPreviewRevision() != job.Version ||
		preview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET ||
		len(preview.GetRows()) != 2 || !preview.GetTruncated() {
		t.Fatalf("typed bounded preview = %+v", preview)
	}
	first := preview.GetRows()[0]
	if first.GetOrdinal() != 0 || first.GetRowId() == "" || len(first.GetCells()) != 4 ||
		first.GetCells()[0].GetStringValue() != "first" ||
		first.GetCells()[1].GetUint64Value() != 7 ||
		!first.GetCells()[2].GetBoolValue() ||
		!first.GetCells()[3].GetTimestampValue().AsTime().Equal(eventTime) {
		t.Fatalf("first typed preview row = %+v", first)
	}
	if second := preview.GetRows()[1]; second.GetOrdinal() != 1 || second.GetCells()[0].GetStringValue() != "second" {
		t.Fatalf("second typed preview row = %+v", second)
	}

	job.Version++
	reader.set(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{
			searchjobs.StringValue("first"), searchjobs.UnsignedValue(7), searchjobs.BoolValue(true), searchjobs.TimeValue(eventTime),
		}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{
			searchjobs.StringValue("second"), searchjobs.UnsignedValue(8), searchjobs.BoolValue(false), searchjobs.TimeValue(eventTime.Add(time.Second)),
		}},
		searchjobs.ResultRow{Ordinal: 2, Values: []searchjobs.Value{
			searchjobs.StringValue("third"), searchjobs.UnsignedValue(9), searchjobs.BoolValue(true), searchjobs.TimeValue(eventTime.Add(2 * time.Second)),
		}},
		searchjobs.ResultRow{Ordinal: 3, Values: []searchjobs.Value{
			searchjobs.StringValue("fourth"), searchjobs.UnsignedValue(10), searchjobs.BoolValue(true), searchjobs.TimeValue(eventTime.Add(3 * time.Second)),
		}},
	))
	live := readEvent(t, client)
	if live.GetProtocolError() != nil || live.GetResynchronizationRequired() != nil ||
		live.GetSearchProgress() == nil || live.GetSearchProgress().GetProducedRows() != 4 {
		t.Fatalf("invisible append notification = %+v", live)
	}
	fixture.service.mu.Lock()
	target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
	fixture.service.mu.Unlock()
	if target == nil {
		t.Fatal("preview target is missing")
	}
	target.mu.Lock()
	currentPreview := target.current[eventCategoryPreview]
	target.mu.Unlock()
	if currentPreview == nil || currentPreview.sequence != observation.preview.GetSequence() {
		t.Fatalf("invisible append republished preview sequence: current=%+v initial=%d",
			currentPreview, observation.preview.GetSequence())
	}
}

func TestWebSocketPreviewPublishesWhenVisiblePrefixChanges(t *testing.T) {
	job := scopedSearchJob("search-preview-visible-change")
	job.Version = 3
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 2
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("visible", "visible", job.ID, 0, 2))
	initial := readPreviewObservations(t, client, "visible")["visible"].preview

	job.Version++
	reader.set(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("second")}},
	))
	var updated *opensplunkv1.SearchWebSocketEvent
	for reads := 0; reads < 3; reads++ {
		event := readEvent(t, client)
		if event.GetResultPreview() != nil {
			updated = event
			break
		}
	}
	if updated == nil || updated.GetSequence() <= initial.GetSequence() ||
		len(updated.GetResultPreview().GetRows()) != 2 || updated.GetResultPreview().GetTruncated() {
		t.Fatalf("visible preview update = %+v", updated)
	}
}

func TestWebSocketPreviewUsesOneCanonicalSequenceForDifferentSubscriberLimits(t *testing.T) {
	job := scopedSearchJob("search-preview-shared")
	job.Version = 5
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("second")}},
		searchjobs.ResultRow{Ordinal: 2, Values: []searchjobs.Value{searchjobs.StringValue("third")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 3
	})
	client := fixture.dial()
	command := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "shared",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{
				subscribeWithPreview("unused", "low", job.ID, 0, 1).GetSubscribe().Subscriptions[0],
				subscribeWithPreview("unused", "high", job.ID, 0, 3).GetSubscribe().Subscriptions[0],
			},
		}},
	}
	writeCommand(t, client, command)

	observed := readPreviewObservations(t, client, "low", "high")
	low := observed["low"].preview.GetResultPreview()
	high := observed["high"].preview.GetResultPreview()
	if observed["low"].preview.GetSequence() != observed["high"].preview.GetSequence() {
		t.Fatalf("preview sequences = low:%d high:%d", observed["low"].preview.GetSequence(), observed["high"].preview.GetSequence())
	}
	if low.GetPreviewRevision() != high.GetPreviewRevision() || len(low.GetRows()) != 1 || !low.GetTruncated() ||
		len(high.GetRows()) != 3 || high.GetTruncated() {
		t.Fatalf("filtered canonical previews = low:%+v high:%+v", low, high)
	}
	limits := reader.requestedPreviewLimits()
	if len(limits) == 0 {
		t.Fatal("preview snapshot was never requested")
	}
	for _, limit := range limits {
		if limit != 3 {
			t.Fatalf("PreviewFor limits = %v, want only canonical maximum 3", limits)
		}
	}
}

func TestPreviewDemandRefreshIsSingleflight(t *testing.T) {
	job := scopedSearchJob("search-preview-singleflight")
	job.Version = 3
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	reader := &blockingPreviewRefreshSnapshots{
		job: job,
		snapshot: previewSnapshot(job,
			searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("one")}},
		),
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	service, err := New(Config{
		Searches: reader, Exports: notFoundExportSnapshots{},
		Access:             searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner"},
		MaximumPreviewRows: 1, PollInterval: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(reader.release) }) }
	t.Cleanup(release)
	target, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer service.releaseResolvedTarget(target)

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, refreshErr := target.refreshForSubscription(context.Background(), 1, false)
			errs <- refreshErr
		}()
	}
	select {
	case <-reader.entered:
	case <-time.After(time.Second):
		t.Fatal("first preview refresh did not enter the backing reader")
	}
	time.Sleep(20 * time.Millisecond)
	if calls, active, maximum := reader.calls.Load(), reader.active.Load(), reader.maximum.Load(); calls != 1 || active != 1 || maximum != 1 {
		t.Fatalf("concurrent preview loads = calls:%d active:%d maximum:%d", calls, active, maximum)
	}
	release()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("refreshForSubscription() = %v", err)
		}
	}
	if calls := reader.calls.Load(); calls != 1 {
		t.Fatalf("coalesced preview load count = %d, want 1", calls)
	}
}

func TestWebSocketPreviewOptOutReceivesZeroRowContinuityMarker(t *testing.T) {
	job := scopedSearchJob("search-preview-opt-out")
	job.Version = 4
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("second")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 2
	})
	client := fixture.dial()
	optedOut := subscribeCommand("unused", "metadata-only", job.ID, 0).GetSubscribe().Subscriptions[0]
	command := &opensplunkv1.SearchWebSocketCommand{
		RequestId: "mixed-preview",
		Payload: &opensplunkv1.SearchWebSocketCommand_Subscribe{Subscribe: &opensplunkv1.SubscribeSearchJobsCommand{
			Subscriptions: []*opensplunkv1.SearchSubscription{
				subscribeWithPreview("unused", "with-preview", job.ID, 0, 2).GetSubscribe().Subscriptions[0],
				optedOut,
			},
		}},
	}
	writeCommand(t, client, command)

	observed := readPreviewObservations(t, client, "with-preview", "metadata-only")
	fullEvent := observed["with-preview"].preview
	markerEvent := observed["metadata-only"].preview
	if fullEvent.GetSequence() != markerEvent.GetSequence() {
		t.Fatalf("preview/continuity sequences = %d/%d", fullEvent.GetSequence(), markerEvent.GetSequence())
	}
	if len(fullEvent.GetResultPreview().GetRows()) != 2 {
		t.Fatalf("opted-in preview = %+v", fullEvent.GetResultPreview())
	}
	marker := markerEvent.GetResultPreview()
	if len(marker.GetRows()) != 0 || !marker.GetTruncated() ||
		marker.GetPreviewRevision() != fullEvent.GetResultPreview().GetPreviewRevision() ||
		marker.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET {
		t.Fatalf("opt-out continuity marker = %+v", marker)
	}
}

func TestWebSocketCompletedInitialSubscriptionIncludesPreviewBeforeTerminal(t *testing.T) {
	job := scopedSearchJob("search-preview-completed")
	job.Version = 9
	job.State = searchjobs.StateCompleted
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	job.FinishedAt = job.StartedAt.Add(2 * time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("complete")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 2
		config.Now = func() time.Time { return job.FinishedAt.Add(time.Second) }
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("completed", "completed", job.ID, 0, 2))

	observation := readPreviewObservations(t, client, "completed")["completed"]
	if observation.terminal == nil {
		// The generic helper returns as soon as it sees the preview; a terminal
		// snapshot has exactly one remaining target event.
		observation.terminal = readEvent(t, client)
	}
	if observation.terminal.GetSearchTerminal() == nil {
		t.Fatalf("completed final event = %+v", observation.terminal)
	}
	if observation.preview.GetSequence() != observation.schema.GetSequence()+1 ||
		observation.terminal.GetSequence() != observation.preview.GetSequence()+1 {
		t.Fatalf("completed event order = schema:%d preview:%d terminal:%d",
			observation.schema.GetSequence(), observation.preview.GetSequence(), observation.terminal.GetSequence())
	}
	if rows := observation.preview.GetResultPreview().GetRows(); len(rows) != 1 || rows[0].GetCells()[0].GetStringValue() != "complete" {
		t.Fatalf("completed initial preview = %+v", observation.preview.GetResultPreview())
	}
}

func TestWebSocketFreshFailedJobDoesNotEmitSchemalessPreview(t *testing.T) {
	job := scopedSearchJob("search-preview-failed-fresh")
	job.Version = 4
	job.State = searchjobs.StateFailed
	job.FinishedAt = job.StartedAt.Add(time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: "failed"}
	reader := newPreviewSearchSnapshots(searchjobs.PreviewSnapshot{Job: job, Revision: 1})
	fixture := newWebSocketFixture(t, reader, func(config *Config) { config.MaximumPreviewRows = 2 })
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("failed", "failed", job.ID, 0, 2))

	if ack := readEvent(t, client).GetSubscriptionAcknowledged(); ack == nil {
		t.Fatal("failed-job subscription acknowledgment is missing")
	}
	sawSchema := false
	for reads := 0; reads < 4; reads++ {
		event := readEvent(t, client)
		if event.GetResultSchemaAvailable() != nil {
			sawSchema = true
		}
		if event.GetResultPreview() != nil {
			t.Fatalf("fresh failed job emitted a preview without a retained schema: %+v", event)
		}
		if event.GetSearchTerminal() != nil {
			if sawSchema {
				t.Fatal("fresh failed job unexpectedly retained a result schema")
			}
			return
		}
	}
	t.Fatal("fresh failed job did not emit its terminal event")
}

func TestWebSocketFailureInvalidatesPreviouslyPublishedPreview(t *testing.T) {
	job := scopedSearchJob("search-preview-failed-live")
	job.Version = 5
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("visible")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) { config.MaximumPreviewRows = 1 })
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("live-failure", "live-failure", job.ID, 0, 1))
	initial := readPreviewObservations(t, client, "live-failure")["live-failure"].preview

	job.Version++
	job.State = searchjobs.StateFailed
	job.Schema = nil
	job.FinishedAt = job.StartedAt.Add(time.Second)
	job.ExpiresAt = job.FinishedAt.Add(time.Hour)
	job.Failure = &searchjobs.Failure{Code: searchjobs.FailureExecution, Message: "failed"}
	reader.set(searchjobs.PreviewSnapshot{Job: job, Revision: initial.GetResultPreview().GetPreviewRevision() + 1})

	var reset, terminal *opensplunkv1.SearchWebSocketEvent
	for reads := 0; reads < 6 && terminal == nil; reads++ {
		event := readEvent(t, client)
		if event.GetResultPreview() != nil {
			if reset != nil {
				t.Fatalf("failure emitted duplicate preview resets: first=%+v second=%+v", reset, event)
			}
			reset = event
		}
		if event.GetSearchTerminal() != nil {
			terminal = event
		}
	}
	if reset == nil || terminal == nil || reset.GetSequence() <= initial.GetSequence() ||
		terminal.GetSequence() != reset.GetSequence()+1 || len(reset.GetResultPreview().GetRows()) != 0 ||
		reset.GetResultPreview().GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET {
		t.Fatalf("failed preview invalidation = reset:%+v terminal:%+v", reset, terminal)
	}
}

func TestWebSocketRejectsInvalidPreviewOptions(t *testing.T) {
	job := scopedSearchJob("search-preview-options")
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	searches := newPreviewSearchSnapshots(previewSnapshot(job))
	created := job.CreatedAt
	exportJob := newMutableExportSnapshots()
	exportJob.setValidForPreviewOptions("export-preview-options", created)

	tests := []struct {
		name        string
		command     func() *opensplunkv1.SearchWebSocketCommand
		wantCode    opensplunkv1.SearchWebSocketProtocolErrorCode
		wantField   string
		withExports bool
	}{
		{
			name: "zero row limit",
			command: func() *opensplunkv1.SearchWebSocketCommand {
				return subscribeWithPreview("zero", "zero", job.ID, 0, 0)
			},
			wantCode:  opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND,
			wantField: "preview_row_limit",
		},
		{
			name: "over maximum row limit",
			command: func() *opensplunkv1.SearchWebSocketCommand {
				return subscribeWithPreview("over", "over", job.ID, 0, 4)
			},
			wantCode:  opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND,
			wantField: "preview_row_limit",
		},
		{
			name: "row limit without previews",
			command: func() *opensplunkv1.SearchWebSocketCommand {
				command := subscribeCommand("disabled", "disabled", job.ID, 0)
				limit := uint32(1)
				command.GetSubscribe().Subscriptions[0].PreviewRowLimit = &limit
				return command
			},
			wantCode:  opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND,
			wantField: "preview_row_limit",
		},
		{
			name: "export previews",
			command: func() *opensplunkv1.SearchWebSocketCommand {
				command := subscribeExportCommand("export", "export", "export-preview-options", 0)
				command.GetSubscribe().Subscriptions[0].IncludePreviews = true
				return command
			},
			wantCode:    opensplunkv1.SearchWebSocketProtocolErrorCode_SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_INVALID_COMMAND,
			wantField:   "include_previews",
			withExports: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exports := ExportSnapshots(notFoundExportSnapshots{})
			if test.withExports {
				exports = exportJob
			}
			fixture := newWebSocketFixtureWithReaders(t, searches, exports, func(config *Config) {
				config.MaximumPreviewRows = 3
			})
			client := fixture.dial()
			writeCommand(t, client, test.command())
			protocolError := readEvent(t, client).GetProtocolError()
			if protocolError == nil || protocolError.GetCode() != test.wantCode {
				t.Fatalf("protocol error = %+v, want code %v", protocolError, test.wantCode)
			}
			if len(protocolError.GetViolations()) != 1 ||
				!strings.HasSuffix(protocolError.GetViolations()[0].GetFieldPath(), test.wantField) {
				t.Fatalf("violations = %+v, want field suffix %q", protocolError.GetViolations(), test.wantField)
			}
		})
	}
}

func (reader *mutableExportSnapshots) setValidForPreviewOptions(id string, created time.Time) {
	reader.set(exportJobForPreviewOptions(id, created))
}

func exportJobForPreviewOptions(id string, created time.Time) exportjobs.Job {
	return exportjobs.Job{
		ID: id, Version: 1, State: exportjobs.StateRunning,
		CreatedAt: created, StartedAt: created.Add(time.Second),
	}
}

func TestWebSocketReplayFiltersPreviewToChangedRowLimit(t *testing.T) {
	job := scopedSearchJob("search-preview-replay-limit")
	job.Version = 12
	job.State = searchjobs.StateCompleted
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	job.FinishedAt = job.StartedAt.Add(time.Second)
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("first")}},
		searchjobs.ResultRow{Ordinal: 1, Values: []searchjobs.Value{searchjobs.StringValue("second")}},
		searchjobs.ResultRow{Ordinal: 2, Values: []searchjobs.Value{searchjobs.StringValue("third")}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 3
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("initial", "initial", job.ID, 0, 3))
	initial := readPreviewObservations(t, client, "initial")["initial"]
	if initial.terminal == nil {
		initial.terminal = readEvent(t, client)
	}
	originalPreview := initial.preview.GetResultPreview()
	if len(originalPreview.GetRows()) != 3 {
		t.Fatalf("initial preview rows = %d", len(originalPreview.GetRows()))
	}

	writeCommand(t, client, &opensplunkv1.SearchWebSocketCommand{
		RequestId: "remove-initial",
		Payload: &opensplunkv1.SearchWebSocketCommand_Unsubscribe{Unsubscribe: &opensplunkv1.UnsubscribeSearchJobsCommand{
			SubscriptionIds: []string{"initial"},
		}},
	})
	if removed := readEvent(t, client).GetSubscriptionRemoved(); removed == nil {
		t.Fatal("initial subscription removal is missing")
	}

	writeCommand(t, client, subscribeWithPreview(
		"resume", "resume", job.ID, initial.schema.GetSequence(), 1,
	))
	ackEvent := readEvent(t, client)
	ack := ackEvent.GetSubscriptionAcknowledged()
	if ack == nil || !ack.GetReplayWillFollow() {
		t.Fatalf("resume acknowledgment = %+v", ackEvent)
	}
	var replayed *opensplunkv1.SearchWebSocketEvent
	for reads := 0; reads < 4; reads++ {
		event := readEvent(t, client)
		if event.GetResynchronizationRequired() != nil || event.GetProtocolError() != nil {
			t.Fatalf("resume control event = %+v", event)
		}
		if event.GetResultPreview() != nil {
			replayed = event
			break
		}
	}
	if replayed == nil {
		t.Fatal("preview was not present in replay")
	}
	preview := replayed.GetResultPreview()
	if replayed.GetSequence() != initial.preview.GetSequence() || len(preview.GetRows()) != 1 ||
		preview.GetRows()[0].GetCells()[0].GetStringValue() != "first" || !preview.GetTruncated() {
		t.Fatalf("changed-limit replay = %+v", replayed)
	}
}

func TestWebSocketOversizedPreviewRowDegradesToEmptyTruncatedReset(t *testing.T) {
	job := scopedSearchJob("search-preview-oversized-row")
	job.Version = 3
	job.State = searchjobs.StateCompleted
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	job.FinishedAt = job.StartedAt.Add(time.Second)
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{
			searchjobs.StringValue(strings.Repeat("x", int(2*minimumFrameBytes))),
		}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 1
		config.MaximumFrameBytes = minimumFrameBytes
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("oversized", "oversized", job.ID, 0, 1))

	observation := readPreviewObservations(t, client, "oversized")["oversized"]
	preview := observation.preview.GetResultPreview()
	if len(preview.GetRows()) != 0 || !preview.GetTruncated() ||
		preview.GetUpdateMode() != opensplunkv1.PreviewUpdateMode_PREVIEW_UPDATE_MODE_RESET {
		t.Fatalf("oversized-row fallback preview = %+v", preview)
	}
	fixture.service.mu.Lock()
	target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
	fixture.service.mu.Unlock()
	if target == nil {
		t.Fatal("preview target is missing")
	}
	target.mu.Lock()
	incomplete := target.currentIncomplete
	target.mu.Unlock()
	if incomplete {
		t.Fatal("oversized disposable preview made the canonical target incomplete")
	}
}

func TestWebSocketPreviewHonorsTighterReplayBudget(t *testing.T) {
	job := scopedSearchJob("search-preview-replay-bound")
	job.Version = 3
	job.State = searchjobs.StateCompleted
	job.Schema = &searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}}
	job.FinishedAt = job.StartedAt.Add(time.Second)
	reader := newPreviewSearchSnapshots(previewSnapshot(job,
		searchjobs.ResultRow{Ordinal: 0, Values: []searchjobs.Value{
			searchjobs.StringValue(strings.Repeat("x", int(2*minimumFrameBytes))),
		}},
	))
	fixture := newWebSocketFixture(t, reader, func(config *Config) {
		config.MaximumPreviewRows = 1
		config.MaximumFrameBytes = 4 * minimumFrameBytes
		config.MaximumReplayBytes = minimumFrameBytes
	})
	client := fixture.dial()
	writeCommand(t, client, subscribeWithPreview("replay-bound", "replay-bound", job.ID, 0, 1))

	observation := readPreviewObservations(t, client, "replay-bound")["replay-bound"]
	preview := observation.preview.GetResultPreview()
	if len(preview.GetRows()) != 0 || !preview.GetTruncated() {
		t.Fatalf("replay-bounded preview = %+v", preview)
	}
	fixture.service.mu.Lock()
	target := fixture.service.targets[targetKey{kind: targetKindSearch, id: job.ID}]
	fixture.service.mu.Unlock()
	if target == nil {
		t.Fatal("replay-bounded preview target is missing")
	}
	target.mu.Lock()
	incomplete := target.currentIncomplete
	retainedBytes := target.retainedBytes
	target.mu.Unlock()
	if incomplete || retainedBytes > minimumFrameBytes {
		t.Fatalf("replay-bounded target = incomplete:%t retained:%d", incomplete, retainedBytes)
	}
}

var _ SearchSnapshots = (*previewSearchSnapshots)(nil)
var _ SearchSnapshots = (*mutableSearchSnapshots)(nil)
var _ SearchSnapshots = (*blockingSearchSnapshots)(nil)
var _ SearchSnapshots = (*stagedTerminalSnapshots)(nil)
var _ SearchSnapshots = (*adversarialSearchSnapshots)(nil)
var _ SearchSnapshots = configTestSearchSnapshots{}
var _ SearchSnapshots = stubSearchSnapshots{}
