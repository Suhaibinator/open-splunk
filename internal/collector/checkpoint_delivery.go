package collector

import (
	"errors"
	"fmt"

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
// batches replayable. It returns the coalesced durable checkpoints so a
// startup-only resume overlay can release the positions now covered.
func commitTerminalCheckpoints(
	store input.CheckpointStore,
	sourceMarks []wal.SourceCheckpointMark,
) ([]input.Checkpoint, error) {
	ordered, err := sourceCheckpointsFromWAL(store, sourceMarks)
	if err != nil {
		return nil, err
	}
	if err := store.SetMany(ordered); err != nil {
		return nil, fmt.Errorf("persist %d terminal checkpoints: %w", len(ordered), err)
	}
	return ordered, nil
}

// sourceCheckpointsFromWAL validates and coalesces source coordinates without
// persisting them. Terminal delivery commits the result; collector startup uses
// the same reconstruction as an in-memory resume overlay for pending batches.
func sourceCheckpointsFromWAL(
	store input.CheckpointStore,
	sourceMarks []wal.SourceCheckpointMark,
) ([]input.Checkpoint, error) {
	if store == nil {
		return nil, errors.New("collector: checkpoint store is required")
	}
	marks := make(map[inputFileKey]checkpointMark)
	discovery := make(map[inputFileKey]checkpointDiscovery)
	for _, sourceMark := range sourceMarks {
		mark, obsolete, err := checkpointMarkFromSource(store, discovery, sourceMark)
		if err != nil {
			return nil, fmt.Errorf(
				"collector: reconstruct checkpoint from batch %d event %d: %w",
				sourceMark.BatchSequence, sourceMark.EventIndex, err,
			)
		}
		if obsolete {
			continue
		}
		key := inputFileTrackingKey(mark.inputID, mark.identity)
		current, ok := marks[key]
		if ok && mark.identity.Generation == current.identity.Generation {
			if mark.identity.String() != current.identity.String() {
				return nil, fmt.Errorf(
					"collector: reconstruct checkpoint from batch %d event %d: file origin conflicts with another source identity",
					sourceMark.BatchSequence,
					sourceMark.EventIndex,
				)
			}
			if checkpointLineCursorsConflict(
				mark.offset, mark.nextLineNumber,
				current.offset, current.nextLineNumber,
			) {
				return nil, fmt.Errorf(
					"collector: reconstruct checkpoint from batch %d event %d: line cursor does not advance with source offset",
					sourceMark.BatchSequence,
					sourceMark.EventIndex,
				)
			}
		}
		if entry := discovery[key]; entry.found &&
			mark.identity.Generation == entry.checkpoint.Identity.Generation &&
			checkpointLineCursorsConflict(
				mark.offset, mark.nextLineNumber,
				entry.checkpoint.Offset, entry.checkpoint.NextLineNumber,
			) {
			return nil, fmt.Errorf(
				"collector: reconstruct checkpoint from batch %d event %d: line cursor conflicts with durable checkpoint",
				sourceMark.BatchSequence,
				sourceMark.EventIndex,
			)
		}
		if !ok ||
			mark.identity.Generation > current.identity.Generation ||
			(mark.identity.Generation == current.identity.Generation && mark.offset >= current.offset) {
			marks[key] = mark
		}
	}

	ordered := make([]input.Checkpoint, 0, len(marks))
	for _, mark := range marks {
		ordered = append(ordered, input.Checkpoint{
			InputID: mark.inputID, Identity: mark.identity, Path: mark.path,
			Offset: mark.offset, LineNumber: mark.lineNumber,
			NextLineNumber: mark.nextLineNumber,
		})
	}
	return ordered, nil
}

func checkpointLineCursorsConflict(
	leftOffset, leftNextLine, rightOffset, rightNextLine uint64,
) bool {
	if leftNextLine == 0 || rightNextLine == 0 {
		return false
	}
	switch {
	case leftOffset == rightOffset:
		return leftNextLine != rightNextLine
	case leftOffset > rightOffset:
		return leftNextLine <= rightNextLine
	default:
		return leftNextLine >= rightNextLine
	}
}

// checkpointMarkFromSource validates one compact file origin.
// Old WAL records predate source_path and file_fingerprint_length. Their
// discovery checkpoint at offset zero contains both values; use it only when it
// describes the exact same generation. A newer discovery generation means
// this is a delayed pre-copytruncate batch, which is safely obsolete.
func checkpointMarkFromSource(
	store input.CheckpointStore,
	discovery map[inputFileKey]checkpointDiscovery,
	source wal.SourceCheckpointMark,
) (mark checkpointMark, obsolete bool, err error) {
	if source.BatchSequence == 0 {
		return checkpointMark{}, false, errors.New("source mark has invalid batch sequence")
	}
	if source.InputID == "" {
		return checkpointMark{}, false, errors.New("source mark has empty input ID")
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
	if source.HasNextLineNumber &&
		(source.LineNumber == 0 || source.NextLineNumber == 0 ||
			source.NextLineNumber == ^uint64(0) ||
			source.NextLineNumber <= source.LineNumber) {
		return checkpointMark{}, false, errors.New("file origin has invalid next_line_number")
	}

	key := inputFileTrackingKey(source.InputID, identity)
	entry, cached := discovery[key]
	if !cached {
		cp, ok, getErr := store.Get(source.InputID, identity)
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
		inputID: source.InputID, identity: identity, path: path,
		offset: source.EndOffset, lineNumber: source.LineNumber,
		nextLineNumber: source.NextLineNumber,
	}, false, nil
}
