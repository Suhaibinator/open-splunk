package wal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// metaFileName is the name of the durable metadata file under Options.Dir.
const metaFileName = "meta.json"

// metaTempName is the scratch file used for atomic meta replacement.
const metaTempName = "meta.json.tmp"

// currentFormatVersion is the on-disk format version stamped into meta.json.
const currentFormatVersion uint32 = 1

// walMeta is the durable counter state persisted atomically to meta.json.
//
// next_batch_sequence is the sequence that will be assigned to the next
// appended batch. It is advanced and made durable BEFORE the batch record is
// written, so a crash between the meta write and the record write burns the
// sequence (leaving a gap) rather than ever reusing it.
//
// last_acked_batch_sequence is the cumulative acknowledgment high-water mark:
// every batch with sequence <= last_acked_batch_sequence is considered acked.
type walMeta struct {
	FormatVersion          uint32 `json:"format_version"`
	NextBatchSequence      uint64 `json:"next_batch_sequence"`
	LastAckedBatchSequence uint64 `json:"last_acked_batch_sequence"`
}

// readMeta loads meta.json from dir. It returns (meta, true, nil) when the file
// exists and parses, (zero, false, nil) when it is absent, or an error on I/O or
// parse failure.
func readMeta(dir string) (walMeta, bool, error) {
	var m walMeta
	data, err := os.ReadFile(filepath.Join(dir, metaFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return walMeta{}, false, nil
		}
		return walMeta{}, false, fmt.Errorf("collector/wal: read meta: %w", err)
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return walMeta{}, false, fmt.Errorf("collector/wal: parse meta: %w", err)
	}
	if m.FormatVersion == 0 {
		return walMeta{}, false, fmt.Errorf("collector/wal: meta has unknown format_version 0")
	}
	if m.NextBatchSequence == 0 {
		// A valid meta always has next_batch_sequence >= 1 (sequences start at 1).
		return walMeta{}, false, fmt.Errorf("collector/wal: meta has invalid next_batch_sequence 0")
	}
	return m, true, nil
}

// writeMeta atomically replaces meta.json with m using the temp+fsync+rename
// discipline, then fsyncs dir so the rename itself is durable. On return the new
// meta is guaranteed on stable storage.
func writeMeta(dir string, m walMeta) error {
	m.FormatVersion = currentFormatVersion
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("collector/wal: marshal meta: %w", err)
	}
	tmp := filepath.Join(dir, metaTempName)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("collector/wal: create meta temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("collector/wal: write meta temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("collector/wal: fsync meta temp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("collector/wal: close meta temp: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, metaFileName)); err != nil {
		return fmt.Errorf("collector/wal: rename meta: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("collector/wal: fsync dir after meta rename: %w", err)
	}
	return nil
}

// fsyncDir flushes a directory's metadata so that create/rename/unlink of its
// entries survive a crash.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
