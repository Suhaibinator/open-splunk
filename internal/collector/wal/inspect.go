package wal

import (
	"errors"
	"fmt"
	"path/filepath"
)

// HasPendingRecords inspects the physical WAL without opening a writer or
// repairing files. Reserved-but-unused sequences are not pending records.
// Concurrent appends/reclamation can return an error; monitoring callers retry.
func HasPendingRecords(dir string) (bool, error) {
	meta, exists, err := readMeta(dir)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, errors.New("collector/wal: metadata is missing")
	}
	for _, ref := range meta.PendingRepacks {
		if ref.Through > meta.LastAckedBatchSequence {
			return true, nil
		}
	}
	names, err := listSegments(dir)
	if err != nil {
		return false, err
	}
	pending := false
	for _, name := range names {
		result, err := walkSegment(filepath.Join(dir, name), func(record scannedRecord) error {
			if record.batch.GetBatchSequence() > meta.LastAckedBatchSequence {
				pending = true
			}
			return nil
		})
		if err != nil {
			return false, err
		}
		if result.corrupt {
			return false, fmt.Errorf("collector/wal: incomplete or corrupt segment %s", name)
		}
	}
	return pending, nil
}
