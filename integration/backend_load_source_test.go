//go:build !windows

package integration_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var (
	errBackendLoadSourceTruncated = errors.New("backend load source was truncated")
	errBackendLoadPartialRecord   = errors.New("backend load source has a partial trailing record")
)

type backendLoadSourceProgress struct {
	Records       uint64
	ObservedBytes uint64
	CompleteBytes uint64
	PartialBytes  uint64
}

type backendLoadSourceTracker struct {
	path          string
	offset        int64
	records       uint64
	completeBytes uint64
	partialBytes  uint64
}

func newBackendLoadSourceTracker(path string) *backendLoadSourceTracker {
	return &backendLoadSourceTracker{path: path}
}

func (tracker *backendLoadSourceTracker) Poll() (backendLoadSourceProgress, error) {
	info, err := os.Stat(tracker.path)
	if err != nil {
		return backendLoadSourceProgress{}, err
	}
	if info.Size() < tracker.offset {
		return backendLoadSourceProgress{}, fmt.Errorf(
			"%w: size decreased from %d to %d",
			errBackendLoadSourceTruncated,
			tracker.offset,
			info.Size(),
		)
	}
	file, err := os.Open(tracker.path)
	if err != nil {
		return backendLoadSourceProgress{}, err
	}
	defer file.Close()

	var buffer [64 << 10]byte
	for {
		n, readErr := file.ReadAt(buffer[:], tracker.offset)
		for _, value := range buffer[:n] {
			tracker.offset++
			tracker.partialBytes++
			if value == '\n' {
				tracker.records++
				tracker.completeBytes = uint64(tracker.offset)
				tracker.partialBytes = 0
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return backendLoadSourceProgress{}, readErr
		}
		if n == 0 {
			break
		}
	}
	return backendLoadSourceProgress{
		Records:       tracker.records,
		ObservedBytes: uint64(tracker.offset),
		CompleteBytes: tracker.completeBytes,
		PartialBytes:  tracker.partialBytes,
	}, nil
}

func (tracker *backendLoadSourceTracker) Finalize(expectedRecords uint64) (backendLoadSourceProgress, error) {
	progress, err := tracker.Poll()
	if err != nil {
		return backendLoadSourceProgress{}, err
	}
	if progress.PartialBytes != 0 {
		return progress, fmt.Errorf("%w: %d bytes", errBackendLoadPartialRecord, progress.PartialBytes)
	}
	if progress.Records != expectedRecords {
		return progress, fmt.Errorf("backend load source records = %d, want %d", progress.Records, expectedRecords)
	}
	return progress, nil
}

type backendLoadRawMultiset struct {
	counts    map[[sha256.Size]byte]uint64
	remaining uint64
}

func newBackendLoadRawMultiset(lines [][]byte) *backendLoadRawMultiset {
	result := &backendLoadRawMultiset{counts: make(map[[sha256.Size]byte]uint64, len(lines))}
	for _, line := range lines {
		result.Add(line)
	}
	return result
}

func (set *backendLoadRawMultiset) Add(raw []byte) {
	digest := sha256.Sum256(raw)
	set.counts[digest]++
	set.remaining++
}

func (set *backendLoadRawMultiset) Clone() *backendLoadRawMultiset {
	clone := &backendLoadRawMultiset{
		counts:    make(map[[sha256.Size]byte]uint64, len(set.counts)),
		remaining: set.remaining,
	}
	for digest, count := range set.counts {
		clone.counts[digest] = count
	}
	return clone
}

func (set *backendLoadRawMultiset) Consume(raw string) error {
	digest := sha256.Sum256([]byte(raw))
	count := set.counts[digest]
	if count == 0 {
		return fmt.Errorf("stored backend load raw record %x was not expected", digest)
	}
	if count == 1 {
		delete(set.counts, digest)
	} else {
		set.counts[digest] = count - 1
	}
	set.remaining--
	return nil
}

func (set *backendLoadRawMultiset) Finish() error {
	if set.remaining != 0 || len(set.counts) != 0 {
		return fmt.Errorf(
			"backend load raw multiset has %d record(s) across %d digest(s) remaining",
			set.remaining,
			len(set.counts),
		)
	}
	return nil
}

type backendLoadSourceCorpus struct {
	Records    uint64
	FileBytes  uint64
	RawBytes   uint64
	UserIDs    map[string]struct{}
	RequestIDs map[string]struct{}
	RawRecords *backendLoadRawMultiset
}

func readBackendLoadSourceCorpus(
	path string,
	fixtureStart time.Time,
	plan backendLoadPlan,
) (backendLoadSourceCorpus, error) {
	expectedRecords := plan.eventCount()
	file, err := os.Open(path)
	if err != nil {
		return backendLoadSourceCorpus{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return backendLoadSourceCorpus{}, err
	}
	if info.Size() <= 0 {
		return backendLoadSourceCorpus{}, errors.New("backend load source is empty")
	}
	var trailing [1]byte
	if _, err := file.ReadAt(trailing[:], info.Size()-1); err != nil {
		return backendLoadSourceCorpus{}, err
	}
	if trailing[0] != '\n' {
		return backendLoadSourceCorpus{}, errBackendLoadPartialRecord
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return backendLoadSourceCorpus{}, err
	}

	result := backendLoadSourceCorpus{
		FileBytes:  uint64(info.Size()),
		UserIDs:    make(map[string]struct{}),
		RequestIDs: make(map[string]struct{}),
		RawRecords: &backendLoadRawMultiset{
			counts: make(map[[sha256.Size]byte]uint64, expectedRecords),
		},
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			return backendLoadSourceCorpus{}, errors.New("backend load source contains an empty record")
		}
		var fields struct {
			Timestamp string  `json:"timestamp"`
			UserID    string  `json:"user_id"`
			RequestID string  `json:"request_id"`
			Ordinal   *uint64 `json:"ordinal"`
		}
		if err := json.Unmarshal(line, &fields); err != nil {
			return backendLoadSourceCorpus{}, fmt.Errorf(
				"decode backend load source record %d: %w",
				result.Records+1,
				err,
			)
		}
		if fields.UserID == "" || fields.RequestID == "" {
			return backendLoadSourceCorpus{}, fmt.Errorf(
				"backend load source record %d lacks user_id or request_id",
				result.Records+1,
			)
		}
		timestamp, err := time.Parse(time.RFC3339Nano, fields.Timestamp)
		if err != nil {
			return backendLoadSourceCorpus{}, fmt.Errorf(
				"decode backend load source timestamp for record %d: %w",
				result.Records+1,
				err,
			)
		}
		wantTimestamp := fixtureStart.Add(time.Duration(result.Records) * plan.interval())
		if !timestamp.Equal(wantTimestamp) {
			return backendLoadSourceCorpus{}, fmt.Errorf(
				"backend load source record %d timestamp = %s, want %s",
				result.Records+1,
				timestamp.Format(time.RFC3339Nano),
				wantTimestamp.Format(time.RFC3339Nano),
			)
		}
		wantOrdinal := result.Records
		switch {
		case result.Records >= plan.WarmEvents+plan.offlineGenerationEvents():
			wantOrdinal -= plan.WarmEvents + plan.offlineGenerationEvents()
		case result.Records >= plan.WarmEvents:
			wantOrdinal -= plan.WarmEvents
		}
		if fields.Ordinal == nil || *fields.Ordinal != wantOrdinal {
			return backendLoadSourceCorpus{}, fmt.Errorf(
				"backend load source record %d ordinal = %v, want %d",
				result.Records+1,
				fields.Ordinal,
				wantOrdinal,
			)
		}
		result.Records++
		result.RawBytes += uint64(len(line))
		result.UserIDs[fields.UserID] = struct{}{}
		result.RequestIDs[fields.RequestID] = struct{}{}
		result.RawRecords.Add(line)
	}
	if err := scanner.Err(); err != nil {
		return backendLoadSourceCorpus{}, err
	}
	if result.Records != expectedRecords {
		return backendLoadSourceCorpus{}, fmt.Errorf(
			"backend load source records = %d, want %d",
			result.Records,
			expectedRecords,
		)
	}
	if result.FileBytes != result.RawBytes+result.Records {
		return backendLoadSourceCorpus{}, fmt.Errorf(
			"backend load source bytes = %d, raw = %d, records = %d",
			result.FileBytes,
			result.RawBytes,
			result.Records,
		)
	}
	return result, nil
}

func TestBackendLoadSourceTrackerHandlesPartialGrowthAndTruncation(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.ndjson")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	tracker := newBackendLoadSourceTracker(path)
	appendFile := func(value string) {
		t.Helper()
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(file, value); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}

	appendFile("one\npartial")
	first, err := tracker.Poll()
	if err != nil {
		t.Fatal(err)
	}
	if first.Records != 1 || first.CompleteBytes != 4 || first.PartialBytes != 7 {
		t.Fatalf("first source progress = %+v", first)
	}
	appendFile("-done\ntwo\n")
	second, err := tracker.Finalize(3)
	if err != nil {
		t.Fatal(err)
	}
	if second.ObservedBytes != uint64(len("one\npartial-done\ntwo\n")) ||
		second.CompleteBytes != second.ObservedBytes || second.PartialBytes != 0 {
		t.Fatalf("final source progress = %+v", second)
	}
	if err := os.Truncate(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := tracker.Poll(); !errors.Is(err, errBackendLoadSourceTruncated) {
		t.Fatalf("Poll after truncation = %v", err)
	}
}

func TestBackendLoadSourceTrackerRejectsPartialFinalRecord(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "load.ndjson")
	if err := os.WriteFile(path, []byte("complete\npartial"), 0o600); err != nil {
		t.Fatal(err)
	}
	tracker := newBackendLoadSourceTracker(path)
	if _, err := tracker.Finalize(1); !errors.Is(err, errBackendLoadPartialRecord) {
		t.Fatalf("Finalize(partial) = %v", err)
	}
}

func TestBackendLoadRawMultisetDetectsMissingDuplicateAndSubstitution(t *testing.T) {
	t.Parallel()
	source := [][]byte{[]byte("alpha"), []byte("beta"), []byte("alpha")}
	t.Run("exact", func(t *testing.T) {
		set := newBackendLoadRawMultiset(source)
		verification := set.Clone()
		for _, raw := range []string{"beta", "alpha", "alpha"} {
			if err := verification.Consume(raw); err != nil {
				t.Fatal(err)
			}
		}
		if err := verification.Finish(); err != nil {
			t.Fatal(err)
		}
		if set.remaining != 3 || len(set.counts) != 2 {
			t.Fatalf("verification mutated expected multiset: %+v", set)
		}
	})
	t.Run("missing", func(t *testing.T) {
		set := newBackendLoadRawMultiset(source)
		_ = set.Consume("alpha")
		_ = set.Consume("beta")
		if err := set.Finish(); err == nil {
			t.Fatal("missing raw record unexpectedly succeeded")
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		set := newBackendLoadRawMultiset(source)
		_ = set.Consume("alpha")
		_ = set.Consume("alpha")
		if err := set.Consume("alpha"); err == nil {
			t.Fatal("extra duplicate raw record unexpectedly succeeded")
		}
	})
	t.Run("substitution", func(t *testing.T) {
		set := newBackendLoadRawMultiset(source)
		if err := set.Consume("gamma"); err == nil {
			t.Fatal("substituted raw record unexpectedly succeeded")
		}
	})
}

func TestBackendLoadSourceCorpusPinsThreePhaseTimestampAndOrdinalSchedule(t *testing.T) {
	t.Parallel()
	plan := backendLoadPlan{
		TenantID:      "tenant",
		IndexName:     "index",
		WarmEvents:    2,
		MainEvents:    4,
		OfflineEvents: 1,
		SpoolHeadroom: 1,
		Cardinality:   4,
		Rate:          1_000,
		FlushEvents:   1,
	}
	if err := plan.validate(); err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, time.July, 26, 1, 2, 3, 0, time.UTC)
	writeCorpus := func(path string, skewRecord int) {
		t.Helper()
		var contents bytes.Buffer
		for record := uint64(0); record < plan.eventCount(); record++ {
			timestamp := start.Add(time.Duration(record) * plan.interval())
			if int(record) == skewRecord {
				timestamp = timestamp.Add(plan.interval())
			}
			ordinal := record
			switch {
			case record >= plan.WarmEvents+plan.offlineGenerationEvents():
				ordinal -= plan.WarmEvents + plan.offlineGenerationEvents()
			case record >= plan.WarmEvents:
				ordinal -= plan.WarmEvents
			}
			line, err := json.Marshal(struct {
				Timestamp string `json:"timestamp"`
				UserID    string `json:"user_id"`
				RequestID string `json:"request_id"`
				Ordinal   uint64 `json:"ordinal"`
			}{
				Timestamp: timestamp.Format(time.RFC3339Nano),
				UserID:    fmt.Sprintf("user-%d", record),
				RequestID: fmt.Sprintf("request-%d", record),
				Ordinal:   ordinal,
			})
			if err != nil {
				t.Fatal(err)
			}
			contents.Write(line)
			contents.WriteByte('\n')
		}
		if err := os.WriteFile(path, contents.Bytes(), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	validPath := filepath.Join(t.TempDir(), "valid.ndjson")
	writeCorpus(validPath, -1)
	corpus, err := readBackendLoadSourceCorpus(validPath, start, plan)
	if err != nil {
		t.Fatal(err)
	}
	if corpus.Records != plan.eventCount() || corpus.RawRecords.remaining != plan.eventCount() {
		t.Fatalf("valid backend load corpus = %+v", corpus)
	}

	skewedPath := filepath.Join(t.TempDir(), "skewed.ndjson")
	writeCorpus(skewedPath, 3)
	if _, err := readBackendLoadSourceCorpus(skewedPath, start, plan); err == nil {
		t.Fatal("timestamp-skewed backend load source unexpectedly validated")
	}
}
