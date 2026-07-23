package collector

import (
	"errors"
	"fmt"
	"sort"

	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
)

type checkpointDiscovery struct {
	checkpoint input.Checkpoint
	found      bool
}

// commitTerminalCheckpoints validates the compact source marks cached beside
// the durable WAL records that are about to join the cumulative acknowledged
// prefix. The sender calls this before mutating the WAL, so any error keeps all
// batches replayable.
func commitTerminalCheckpoints(store input.CheckpointStore, sourceMarks []wal.SourceCheckpointMark) error {
	if store == nil {
		return errors.New("collector: checkpoint store is required")
	}
	marks := make(map[string]checkpointMark)
	discovery := make(map[string]checkpointDiscovery)
	for _, sourceMark := range sourceMarks {
		mark, obsolete, err := checkpointMarkFromSource(store, discovery, sourceMark)
		if err != nil {
			return fmt.Errorf(
				"collector: reconstruct checkpoint from batch %d event %d: %w",
				sourceMark.BatchSequence, sourceMark.EventIndex, err,
			)
		}
		if obsolete {
			continue
		}
		key := mark.identity.TrackingKey()
		current, ok := marks[key]
		if !ok ||
			mark.identity.Generation > current.identity.Generation ||
			(mark.identity.Generation == current.identity.Generation && mark.offset >= current.offset) {
			marks[key] = mark
		}
	}

	ordered := make([]input.Checkpoint, 0, len(marks))
	for _, mark := range marks {
		ordered = append(ordered, input.Checkpoint{
			Identity: mark.identity, Path: mark.path,
			Offset: mark.offset, LineNumber: mark.lineNumber,
		})
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].Identity.String() < ordered[j].Identity.String()
	})
	if err := store.SetMany(ordered); err != nil {
		return fmt.Errorf("persist %d terminal checkpoints: %w", len(ordered), err)
	}
	return nil
}

// checkpointMarkFromSource validates one compact file origin.
// Old WAL records predate source_path and file_fingerprint_length. Their
// discovery checkpoint at offset zero contains both values; use it only when it
// describes the exact same generation. A newer discovery generation means
// this is a delayed pre-copytruncate batch, which is safely obsolete.
func checkpointMarkFromSource(
	store input.CheckpointStore,
	discovery map[string]checkpointDiscovery,
	source wal.SourceCheckpointMark,
) (mark checkpointMark, obsolete bool, err error) {
	if source.BatchSequence == 0 {
		return checkpointMark{}, false, errors.New("source mark has invalid batch sequence")
	}
	if source.ConflictingMetadata {
		return checkpointMark{}, false, errors.New("file origin has conflicting metadata")
	}
	identity, err := input.ParseFileIdentity(source.FileIdentity)
	if err != nil {
		return checkpointMark{}, false, err
	}
	if !source.HasEndOffset {
		return checkpointMark{}, false, errors.New("file origin is missing end_offset")
	}

	key := identity.TrackingKey()
	entry, cached := discovery[key]
	if !cached {
		cp, ok, getErr := store.Get(identity)
		if getErr != nil {
			return checkpointMark{}, false, fmt.Errorf("read discovery checkpoint: %w", getErr)
		}
		entry = checkpointDiscovery{checkpoint: cp, found: ok}
		discovery[key] = entry
	}
	cp := entry.checkpoint
	if entry.found && cp.Identity.Generation > identity.Generation {
		return checkpointMark{}, true, nil
	}
	if entry.found && cp.Identity.Generation == identity.Generation &&
		cp.Identity.String() != identity.String() {
		return checkpointMark{}, false, errors.New("file origin conflicts with discovery checkpoint identity")
	}

	path := source.SourcePath
	if source.HasSourcePath {
		if path == "" {
			return checkpointMark{}, false, errors.New("file origin has empty source_path")
		}
	} else if entry.found && cp.Identity.String() == identity.String() {
		path = cp.Path
	}
	if path == "" {
		return checkpointMark{}, false, errors.New("file origin is missing source_path")
	}
	if source.HasFingerprintLength {
		if entry.found && cp.Identity.String() == identity.String() &&
			cp.Identity.FingerprintLength != 0 &&
			cp.Identity.FingerprintLength != source.FingerprintLength {
			return checkpointMark{}, false, errors.New("file origin conflicts with discovery fingerprint length")
		}
		identity.FingerprintLength = source.FingerprintLength
	} else if entry.found && cp.Identity.String() == identity.String() {
		identity.FingerprintLength = cp.Identity.FingerprintLength
	} else {
		return checkpointMark{}, false, errors.New("file origin is missing file_fingerprint_length")
	}

	return checkpointMark{
		identity: identity, path: path,
		offset: source.EndOffset, lineNumber: source.LineNumber,
	}, false, nil
}
