package collector

import (
	"sync"
	"sync/atomic"

	"github.com/Suhaibinator/open-splunk/internal/collector/input"
)

// checkpointResumeView overlays source coordinates already owned by pending
// WAL batches onto the durable terminal checkpoint store. Reads used for file
// discovery see the furthest safe resume point, while mutations are filtered
// before reaching the durable store. Pending coordinates must not themselves
// be persisted: until the server gives those batches a terminal disposition,
// the WAL remains the only durable owner of their events.
type checkpointResumeView struct {
	durable input.CheckpointStore

	mu      sync.RWMutex
	pending map[inputFileKey]input.Checkpoint
	active  atomic.Bool
}

func newCheckpointResumeView(
	durable input.CheckpointStore,
	pending []input.Checkpoint,
) (input.ManagerCheckpointStore, *checkpointResumeView) {
	if len(pending) == 0 {
		return durable, nil
	}
	byTrackingKey := make(map[inputFileKey]input.Checkpoint, len(pending))
	for _, checkpoint := range pending {
		key := inputFileTrackingKey(checkpoint.InputID, checkpoint.Identity)
		if current, ok := byTrackingKey[key]; !ok || checkpointAfter(checkpoint, current) {
			byTrackingKey[key] = checkpoint
		}
	}
	view := &checkpointResumeView{durable: durable, pending: byTrackingKey}
	view.active.Store(true)
	return view, view
}

func (view *checkpointResumeView) Get(
	inputID string,
	identity input.FileIdentity,
) (input.Checkpoint, bool, error) {
	if !view.active.Load() {
		return view.durable.Get(inputID, identity)
	}
	// Keep the pending coordinate visible for the entire durable lookup. A
	// terminal callback persists before pruning; this read lock makes a lookup
	// observe either the pending coordinate or the terminal durable write,
	// never the old durable coordinate after its overlay was removed.
	view.mu.RLock()
	defer view.mu.RUnlock()
	durable, found, err := view.durable.Get(inputID, identity)
	if err != nil {
		return input.Checkpoint{}, false, err
	}
	pending, pendingFound := view.pending[inputFileTrackingKey(inputID, identity)]
	if pendingFound && (!found || checkpointAfter(pending, durable)) {
		return pending, true, nil
	}
	return durable, found, nil
}

func (view *checkpointResumeView) Set(checkpoint input.Checkpoint) error {
	return view.SetMany([]input.Checkpoint{checkpoint})
}

func (view *checkpointResumeView) SetMany(checkpoints []input.Checkpoint) error {
	if len(checkpoints) == 0 {
		return nil
	}
	if !view.active.Load() {
		return view.durable.SetMany(checkpoints)
	}
	filtered := make([]input.Checkpoint, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		suppress, err := view.suppressPendingMutation(checkpoint)
		if err != nil {
			return err
		}
		if !suppress {
			filtered = append(filtered, checkpoint)
		}
	}
	if err := view.durable.SetMany(filtered); err != nil {
		return err
	}
	view.pruneCovered(filtered)
	return nil
}

func (view *checkpointResumeView) suppressPendingMutation(
	checkpoint input.Checkpoint,
) (bool, error) {
	if !view.active.Load() {
		return false, nil
	}
	view.mu.RLock()
	pending, found := view.pending[inputFileTrackingKey(checkpoint.InputID, checkpoint.Identity)]
	view.mu.RUnlock()
	if !found ||
		checkpoint.Identity.Generation != pending.Identity.Generation ||
		checkpoint.Offset > pending.Offset {
		return false, nil
	}
	durable, durableFound, err := view.durable.Get(checkpoint.InputID, checkpoint.Identity)
	if err != nil {
		return false, err
	}
	return !durableFound || checkpointAfter(pending, durable), nil
}

// pruneCovered releases startup-only overlay entries after a successful
// durable write reaches or supersedes them.
func (view *checkpointResumeView) pruneCovered(checkpoints []input.Checkpoint) {
	if len(checkpoints) == 0 || !view.active.Load() {
		return
	}
	view.mu.Lock()
	defer view.mu.Unlock()
	for _, checkpoint := range checkpoints {
		key := inputFileTrackingKey(checkpoint.InputID, checkpoint.Identity)
		pending, found := view.pending[key]
		if found && !checkpointAfter(pending, checkpoint) {
			delete(view.pending, key)
		}
	}
	if len(view.pending) == 0 {
		view.pending = nil
		view.active.Store(false)
	}
}

func checkpointAfter(candidate, current input.Checkpoint) bool {
	switch {
	case candidate.Identity.Generation != current.Identity.Generation:
		return candidate.Identity.Generation > current.Identity.Generation
	default:
		// Equal byte positions describe the same resume point. Prefer the
		// durable checkpoint in that case: it has passed terminal-delivery
		// validation, while a pending WAL cursor may be legacy or malformed.
		return candidate.Offset > current.Offset
	}
}
