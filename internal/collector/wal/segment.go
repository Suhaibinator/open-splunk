package wal

import (
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

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
)

// recordHeaderSize is the fixed per-record prefix: a 4-byte big-endian payload
// length followed by a 4-byte big-endian CRC32C (Castagnoli) of the payload.
const recordHeaderSize = 8

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
func encodeRecord(payload []byte) []byte {
	buf := make([]byte, recordHeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[4:8], crc32c(payload))
	copy(buf[recordHeaderSize:], payload)
	return buf
}

// scannedRecord describes one intact record located within a segment file.
type scannedRecord struct {
	// payloadOff is the byte offset of the marshaled EventBatch within the file,
	// i.e. the record start plus recordHeaderSize.
	payloadOff int64
	payloadLen uint32
	crc        uint32
	batch      *opensplunkv1.EventBatch
}

// scanResult is the outcome of scanning a single segment file.
type scanResult struct {
	records []scannedRecord
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
	data, err := os.ReadFile(path)
	if err != nil {
		return scanResult{}, err
	}
	var res scanResult
	var off int64
	for off < int64(len(data)) {
		remaining := int64(len(data)) - off
		if remaining < recordHeaderSize {
			// Truncated header from a crash mid-append.
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		payloadLen := binary.BigEndian.Uint32(data[off : off+4])
		wantCRC := binary.BigEndian.Uint32(data[off+4 : off+8])
		recordEnd := off + recordHeaderSize + int64(payloadLen)
		if payloadLen == 0 || recordEnd > int64(len(data)) {
			// Zero-length record (never written) or truncated payload.
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		payload := data[off+recordHeaderSize : recordEnd]
		if crc32c(payload) != wantCRC {
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		var batch opensplunkv1.EventBatch
		if err := proto.Unmarshal(payload, &batch); err != nil {
			res.badOffset, res.corrupt = off, true
			return res, nil
		}
		res.records = append(res.records, scannedRecord{
			payloadOff: off + recordHeaderSize,
			payloadLen: payloadLen,
			crc:        wantCRC,
			batch:      &batch,
		})
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
	corrupt = uniqueCorruptName(corrupt)

	if badOffset == 0 {
		if err := os.Rename(live, corrupt); err != nil {
			return err
		}
		return fsyncDir(dir)
	}

	data, err := os.ReadFile(live)
	if err != nil {
		return err
	}
	prefix := data[:badOffset]

	// 1. Quarantine the full original file.
	if err := os.Rename(live, corrupt); err != nil {
		return err
	}
	if err := fsyncDir(dir); err != nil {
		return err
	}

	// 2. Write the intact prefix into a fresh segment under the original name.
	tmp := live + ".rewrite"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(prefix); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, live); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// uniqueCorruptName returns base, or base with a numeric suffix if base already
// exists, so repeated quarantines never overwrite prior forensic files.
func uniqueCorruptName(base string) string {
	if _, err := os.Stat(base); errors.Is(err, os.ErrNotExist) {
		return base
	}
	for i := 1; ; i++ {
		candidate := fmt.Sprintf("%s.%d", base, i)
		if _, err := os.Stat(candidate); errors.Is(err, os.ErrNotExist) {
			return candidate
		}
	}
}

// readRecordPayload reads and CRC-verifies a single payload located at off with
// the given length from path, returning the unmarshaled EventBatch.
func readRecordPayload(path string, off int64, length uint32, wantCRC uint32) (*opensplunkv1.EventBatch, error) {
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
	var batch opensplunkv1.EventBatch
	if err := proto.Unmarshal(buf, &batch); err != nil {
		return nil, fmt.Errorf("collector/wal: unmarshal record in %s: %w", filepath.Base(path), ErrCorruptSegment)
	}
	return &batch, nil
}
