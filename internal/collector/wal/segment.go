package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

// recordHeaderSize is the fixed per-record prefix: a 4-byte big-endian payload
// length followed by a 4-byte big-endian CRC32C (Castagnoli) of the payload.
const recordHeaderSize = 8

// maximumRecordPayloadBytes is a stable on-disk format ceiling as well as a
// defensive allocation bound. It must not depend on the current MaxQueueBytes:
// operators may lower that setting and still need to open and drain records
// accepted under the previous configuration. The ingestion protocol admits at
// most 8 MiB of uncompressed event protobufs; the additional 8 MiB covers
// repeated-field framing and batch envelope metadata while ensuring a forged
// sparse record cannot request a multi-gigabyte slice.
const maximumRecordPayloadBytes uint64 = 16 << 20

// segmentPrefix and segmentSuffix bracket a segment file's zero-padded first
// batch sequence: segment-<20-digit-first-seq>.wal.
const (
	segmentPrefix = "segment-"
	segmentSuffix = ".wal"
	corruptSuffix = ".wal.corrupt"
	seqPadWidth   = 20
)

// castagnoli is the CRC32C table used for record checksums.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// crc32c returns the Castagnoli CRC32C of b.
func crc32c(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// segmentName returns the file name for a segment whose first batch has the
// given sequence.
func segmentName(firstSeq uint64) string {
	return fmt.Sprintf("%s%0*d%s", segmentPrefix, seqPadWidth, firstSeq, segmentSuffix)
}

// parseSegmentName extracts the first sequence from a segment file name,
// reporting whether name is a live (non-corrupt) segment file.
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, segmentPrefix) || !strings.HasSuffix(name, segmentSuffix) {
		return 0, false
	}
	digits := strings.TrimSuffix(strings.TrimPrefix(name, segmentPrefix), segmentSuffix)
	if len(digits) != seqPadWidth {
		return 0, false
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// listSegments returns the live segment file names under dir sorted by ascending
// first sequence. The zero-padded names sort lexically in sequence order, but we
// parse and sort numerically to be explicit.
func listSegments(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type seg struct {
		name string
		seq  uint64
	}
	var segs []seg
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if seq, ok := parseSegmentName(e.Name()); ok {
			segs = append(segs, seg{name: e.Name(), seq: seq})
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].seq < segs[j].seq })
	names := make([]string, len(segs))
	for i, s := range segs {
		names[i] = s.name
	}
	return names, nil
}

// encodeRecord frames payload into a length-prefixed, CRC32C-checksummed record.
func encodeRecord(payload []byte) ([]byte, error) {
	// #nosec G115 -- len is non-negative and every supported Go int value is
	// exactly representable as uint64.
	if uint64(len(payload)) > maximumRecordPayloadBytes {
		return nil, ErrBatchTooLarge
	}
	buf := make([]byte, recordHeaderSize+len(payload))
	// #nosec G115 -- payload length is explicitly bounded by math.MaxUint32 above.
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32c(payload))
	copy(buf[recordHeaderSize:], payload)
	return buf, nil
}

// scannedRecord describes one intact record located within a segment file.
type scannedRecord struct {
	// payloadOff is the byte offset of the marshaled EventBatch within the file,
	// i.e. the record start plus recordHeaderSize.
	payloadOff int64
	payloadLen uint32
	crc        uint32
	batch      *opensplunk.EventBatch
}

// scanResult is the outcome of scanning a single segment file.
type scanResult struct {
	records []scannedRecord
	// fileSize is captured from the same open descriptor used by the walk. When
	// physical corruption makes later record boundaries untrustworthy, recovery
	// uses it to conservatively bound how many allocations could exist in the
	// quarantined tail without reopening or restating the file.
	fileSize int64
	// recordCount is populated by the streaming walker even when records are not
	// retained. Recovery uses it to distinguish an empty rewrite from one with a
	// validated prefix without holding every decoded EventBatch in memory.
	recordCount uint64
	// badOffset is the byte offset at which the first invalid/truncated record
	// begins; corrupt is true when such a record was found. Bytes in [0,badOffset)
	// are all intact records.
	badOffset int64
	corrupt   bool
}

// scanSegment reads path record by record, validating each record's length and
// CRC and unmarshaling its payload. Scanning stops at the first truncated or
// corrupt record; everything before it is returned in records and the corrupt
// tail is reported via badOffset/corrupt.
func scanSegment(path string) (scanResult, error) {
	var records []scannedRecord
	res, err := walkSegment(path, func(record scannedRecord) error {
		records = append(records, record)
		return nil
	})
	res.records = records
	return res, err
}

// walkSegment validates and decodes one record at a time, invoking visit before
// releasing the decoded batch. It deliberately does not retain records, keeping
// recovery peak memory bounded by one stable-size record plus the compact index
// the visitor chooses to build. scanSegment is the collecting wrapper used by
// tests and targeted repair helpers.
func walkSegment(path string, visit func(scannedRecord) error) (scanResult, error) {
	file, err := os.Open(path)
	if err != nil {
		return scanResult{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return scanResult{}, err
	}
	size := info.Size()
	res := scanResult{fileSize: size}
	var off int64
	var header [recordHeaderSize]byte
	// Reuse one bounded payload buffer for the segment. ReadFull overwrites the
	// entire selected slice before validation, so retaining the largest capacity
	// seen so far does not carry bytes between records.
	var payloadScratch []byte
	for off < size {
		remaining := size - off
		if remaining < recordHeaderSize {
			// Truncated header from a crash mid-append.
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		if _, err := io.ReadFull(file, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				res.badOffset, res.corrupt = off, true
				return res, nil
			}
			return scanResult{}, err
		}
		payloadLen := binary.BigEndian.Uint32(header[0:4])
		wantCRC := binary.BigEndian.Uint32(header[4:8])
		if uint64(payloadLen) > maximumRecordPayloadBytes {
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		recordEnd := off + recordHeaderSize + int64(payloadLen)
		if payloadLen == 0 || recordEnd > size {
			// Zero-length record (never written) or truncated payload.
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		payloadSize := int(payloadLen)
		if cap(payloadScratch) < payloadSize {
			payloadScratch = make([]byte, payloadSize)
		} else {
			payloadScratch = payloadScratch[:payloadSize]
		}
		payload := payloadScratch
		if _, err := io.ReadFull(file, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				res.badOffset, res.corrupt = off, true
				return res, nil
			}
			return scanResult{}, err
		}
		if crc32c(payload) != wantCRC {
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		var batch opensplunk.EventBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		record := scannedRecord{
			payloadOff: off + recordHeaderSize,
			payloadLen: payloadLen,
			crc:        wantCRC,
			batch:      &batch,
		}
		res.recordCount++
		if visit != nil {
			if err := visit(record); err != nil {
				return scanResult{}, err
			}
		}
		off = recordEnd
	}
	return res, nil
}

// quarantineTail renames a segment whose tail at badOffset is corrupt to a
// .wal.corrupt sibling and, when there is an intact prefix, rewrites that prefix
// into a fresh segment under the original name. The whole original file (intact
// prefix plus bad tail) is preserved in the .corrupt file for forensics; the
// live segment is left holding only the validated prefix.
//
// When badOffset is 0 the entire segment is garbage and only the .corrupt file
// remains. Operations are ordered and fsynced so a crash mid-quarantine leaves
// either the pre- or post-quarantine state recoverable on the next Open.
func quarantineTail(dir, name string, badOffset int64) error {
	live := filepath.Join(dir, name)
	corrupt := filepath.Join(dir, name+".corrupt")
	// Ensure a unique corrupt name if one already exists from a prior run.
	corrupt, err := uniqueCorruptName(corrupt)
	if err != nil {
		return err
	}

	if badOffset == 0 {
		if err := os.Rename(live, corrupt); err != nil {
			return err
		}
		return fsyncDir(dir)
	}

	// Prepare and fsync the intact replacement before changing the live name. A
	// stale rewrite is harmless while live still names the original and is removed
	// by recoverInterruptedQuarantines on the next Open.
	tmp := live + ".rewrite"
	if err := writeSyncedPrefix(live, tmp, badOffset); err != nil {
		return err
	}

	// Publish a durable hard-link to the complete original for forensics. Unlike
	// rename-first, this leaves live continuously recoverable until the prepared
	// prefix atomically replaces it.
	if err := os.Link(live, corrupt); err != nil {
		return err
	}
	if err := fsyncDir(dir); err != nil {
		return err
	}
	if err := os.Rename(tmp, live); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// recoverInterruptedQuarantines resolves a prepared .rewrite left by either the
// current link-then-replace transaction or the historical rename-first code.
// A .corrupt artifact alone is deliberately never restored: it has no durable
// provenance distinguishing the triggering segment from a successor that was
// quarantined whole behind an earlier gap (and which may independently contain
// a corrupt tail). Guessing from its bytes could resurrect data across a barrier.
func recoverInterruptedQuarantines(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	dirty := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), segmentSuffix+".rewrite") {
			continue
		}
		rewrite := filepath.Join(dir, entry.Name())
		live := strings.TrimSuffix(rewrite, ".rewrite")
		if _, err := os.Stat(live); err == nil {
			prepared, err := preparedRewriteHasDurableOriginal(live, rewrite, entries)
			if err != nil {
				return err
			}
			if prepared {
				// Crash after publishing the hard-link artifact but before replacing
				// live: finish the prepared transaction. The original is already
				// preserved, so re-quarantining it would create duplicate hard-link
				// artifacts and overstate capacity usage.
				if err := os.Rename(rewrite, live); err != nil {
					return err
				}
			} else if err := os.Remove(rewrite); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			dirty = true
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		res, scanErr := walkSegment(rewrite, nil)
		if scanErr != nil {
			return scanErr
		}
		if !res.corrupt && res.recordCount > 0 {
			if err := os.Rename(rewrite, live); err != nil {
				return err
			}
			dirty = true
			continue
		}
		// An unpublishable rewrite is retained as a bounded/visible quarantine
		// artifact instead of lingering outside MaxQueueBytes accounting.
		artifact, err := uniqueCorruptName(live + ".corrupt.rewrite")
		if err != nil {
			return err
		}
		if err := os.Rename(rewrite, artifact); err != nil {
			return err
		}
		dirty = true
	}
	if dirty {
		return fsyncDir(dir)
	}
	return nil
}

// preparedRewriteHasDurableOriginal recognizes the current quarantine
// transaction's crash point without trusting names alone. The replacement must
// be a nonempty, structurally valid exact prefix of live, and a .corrupt sibling
// must be the same inode as live. That hard link is the durable forensic copy
// which makes atomically publishing the prepared prefix safe.
func preparedRewriteHasDurableOriginal(
	live, rewrite string,
	entries []os.DirEntry,
) (bool, error) {
	liveInfo, err := os.Stat(live)
	if err != nil {
		return false, err
	}
	rewriteInfo, err := os.Stat(rewrite)
	if err != nil {
		return false, err
	}
	if rewriteInfo.Size() <= 0 || rewriteInfo.Size() >= liveInfo.Size() {
		return false, nil
	}
	res, err := walkSegment(rewrite, nil)
	if err != nil {
		return false, err
	}
	if res.corrupt || res.recordCount == 0 {
		return false, nil
	}

	artifactPrefix := filepath.Base(live) + ".corrupt"
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), artifactPrefix) {
			continue
		}
		artifactInfo, err := entry.Info()
		if err != nil {
			return false, err
		}
		if !os.SameFile(liveInfo, artifactInfo) {
			continue
		}
		return filePrefixEqual(live, rewrite, rewriteInfo.Size())
	}
	return false, nil
}

func filePrefixEqual(filePath, prefixPath string, prefixLength int64) (bool, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return false, err
	}
	defer file.Close()
	prefix, err := os.Open(prefixPath)
	if err != nil {
		return false, err
	}
	defer prefix.Close()

	const compareBlockBytes = 32 * 1024
	fileBlock := make([]byte, compareBlockBytes)
	prefixBlock := make([]byte, compareBlockBytes)
	remaining := prefixLength
	for remaining > 0 {
		chunk := min(int64(compareBlockBytes), remaining)
		if _, err := io.ReadFull(file, fileBlock[:chunk]); err != nil {
			return false, err
		}
		if _, err := io.ReadFull(prefix, prefixBlock[:chunk]); err != nil {
			return false, err
		}
		if !bytes.Equal(fileBlock[:chunk], prefixBlock[:chunk]) {
			return false, nil
		}
		remaining -= chunk
	}
	return true, nil
}

func writeSyncedPrefix(source, destination string, prefixLength int64) error {
	if prefixLength <= 0 {
		return fmt.Errorf("collector/wal: invalid recovery prefix length %d", prefixLength)
	}
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if copied, err := io.CopyN(out, in, prefixLength); err != nil {
		_ = out.Close()
		return err
	} else if copied != prefixLength {
		_ = out.Close()
		return io.ErrShortWrite
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return nil
}

// uniqueCorruptName returns base, or base with a numeric suffix if base already
// exists, so repeated quarantines never overwrite prior forensic files.
func uniqueCorruptName(base string) (string, error) {
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return base, nil
	} else if err != nil {
		return "", err
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s.%d", base, i)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
}

// readRecordPayload reads and CRC-verifies a single payload located at off with
// the given length from path, returning the unmarshaled EventBatch.
func readRecordPayload(path string, off int64, length uint32, wantCRC uint32) (*opensplunk.EventBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, off); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("collector/wal: short read of record in %s: %w", filepath.Base(path), ErrCorruptSegment)
		}
		return nil, err
	}
	if crc32c(buf) != wantCRC {
		return nil, fmt.Errorf("collector/wal: CRC mismatch reading %s: %w", filepath.Base(path), ErrCorruptSegment)
	}
	var batch opensplunk.EventBatch
	if err := proto.Unmarshal(buf, &batch); err != nil {
		return nil, fmt.Errorf("collector/wal: unmarshal record in %s: %w", filepath.Base(path), ErrCorruptSegment)
	}
	return &batch, nil
}
