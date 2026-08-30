package searchartifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestFramedArtifactDetectsChecksumAndDecodesRowsIncrementally(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job, rows := persistTestResults(t, store, clock, "framed-corruption", testRows(t))
	path := filepath.Join(directory, artifactName(job.ID))
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(payload, artifactMagic[:]) {
		t.Fatalf("artifact does not use framed format: %x", payload[:min(len(payload), len(artifactMagic))])
	}
	secondStart, secondLength := framedRowPayload(t, payload, 1)
	for index := range secondLength {
		payload[secondStart+index] = ' '
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, testAccess(), job.ID); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Acquire(checksum mismatch) error = %v", err)
	}

	digest := sha256.Sum256(payload)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs SET artifact_sha256 = ? WHERE id = ?`, digest[:], job.ID); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatalf("Acquire(structurally valid artifact) error = %v", err)
	}
	defer lease.Close()
	first, ok, err := lease.Next(ctx)
	if err != nil || !ok || !reflect.DeepEqual(first, rows[0]) {
		t.Fatalf("first incremental row = %#v, %t, %v", first, ok, err)
	}
	if _, _, err := lease.Next(ctx); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("second incremental row error = %v, want ErrCorrupt", err)
	}
}

func TestLegacyJSONArtifactRemainsReadableAfterRestart(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 11, 0, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job := testQueuedJob(t, "legacy-compatible", clock.now)
	if err := store.Admit(ctx, job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	if err := store.Finalize(ctx, completed); err != nil {
		t.Fatal(err)
	}
	rows := testRows(t)
	storedRows := make([]storedResultRow, len(rows))
	for index, row := range rows {
		storedRows[index], _ = storedRow(row)
	}
	payload, err := json.Marshal(storedArtifact{
		Version: legacyArtifactFormatVersion, JobID: job.ID, Generation: 9,
		Schema: *completed.Schema, Rows: storedRows, ResultsTruncated: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	name := artifactName(job.ID)
	if err := os.WriteFile(filepath.Join(directory, name), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs
		SET state = ?, artifact_name = ?, artifact_sha256 = ?, artifact_size_bytes = ?
		WHERE id = ?`, StateCompleted, name, digest[:], len(payload), job.ID); err != nil {
		t.Fatal(err)
	}
	store = openTestStore(t, database.SQLDB(), directory, clock, DefaultMaximumBytes)
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatalf("Acquire(legacy) error = %v", err)
	}
	defer lease.Close()
	if lease.Generation() != 9 || lease.RowCount() != uint64(len(rows)) || lease.RowCountExact() ||
		!lease.ResultsTruncated() {
		t.Fatalf("legacy metadata = generation %d count %d exact %t", lease.Generation(), lease.RowCount(), lease.RowCountExact())
	}
	assertLeaseRows(t, lease, rows)
}

func TestFramedArtifactSupportsRepeatedPagedReads(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	rows := make([]searchjobs.ResultRow, 23)
	for index := range rows {
		rows[index] = searchjobs.ResultRow{
			Ordinal: uint64(index),
			Values:  []searchjobs.Value{searchjobs.UnsignedValue(uint64(index))},
		}
	}
	job, _ := persistTestResults(t, store, clock, "framed-pages", rows)
	const pageSize = 7
	var got []searchjobs.ResultRow
	for offset := 0; offset < len(rows); offset += pageSize {
		lease, err := store.Acquire(ctx, testAccess(), job.ID)
		if err != nil {
			t.Fatal(err)
		}
		seekable, ok := lease.(SeekableResultLease)
		if !ok {
			t.Fatal("framed result lease is not seekable")
		}
		if err := seekable.Seek(ctx, uint64(offset)); err != nil {
			t.Fatalf("Seek(%d): %v", offset, err)
		}
		if seekable.Generation() != 1 {
			t.Fatalf("generation changed after seek: %d", seekable.Generation())
		}
		for range pageSize {
			row, ok, err := lease.Next(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			got = append(got, row)
		}
		if err := lease.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("paged rows = %#v, want %#v", got, rows)
	}
}

func TestFramedArtifactSupportsExactEmptyResultsAndEndSeek(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 12, 15, 0, 0, time.UTC)}
	store, _, _ := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "framed-empty", nil)
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	if lease.RowCount() != 0 || !lease.RowCountExact() {
		t.Fatalf("empty metadata = count %d exact %t", lease.RowCount(), lease.RowCountExact())
	}
	seekable := lease.(SeekableResultLease)
	if err := seekable.Seek(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := seekable.Next(ctx); err != nil || ok {
		t.Fatalf("empty Next() = %t, %v", ok, err)
	}
}

func TestArtifactLoadPreservesCancellation(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 8, 30, 12, 20, 0, 0, time.UTC)}
	store, _, directory := newTestStore(t, clock, DefaultMaximumBytes)
	job, _ := persistTestResults(t, store, clock, "framed-canceled", testRows(t))
	payload, err := os.ReadFile(filepath.Join(directory, artifactName(job.ID)))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	file, err := os.Open(filepath.Join(directory, artifactName(job.ID)))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := loadStoredArtifact(ctx, file, uint64(len(payload)), job.ID, digest[:]); !errors.Is(err, context.Canceled) {
		t.Fatalf("loadStoredArtifact(canceled) error = %v", err)
	}
}

func TestSparseIndexSeekSkipsCorruptEarlierFrame(t *testing.T) {
	ctx := context.Background()
	clock := &testClock{now: time.Date(2026, 8, 30, 12, 30, 0, 0, time.UTC)}
	store, database, directory := newTestStore(t, clock, DefaultMaximumBytes)
	rows := make([]searchjobs.ResultRow, 600)
	for index := range rows {
		rows[index] = searchjobs.ResultRow{
			Ordinal: uint64(index), Values: []searchjobs.Value{searchjobs.UnsignedValue(uint64(index))},
		}
	}
	job, _ := persistTestResults(t, store, clock, "sparse-seek", rows)
	path := filepath.Join(directory, artifactName(job.ID))
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	start, length := framedRowPayload(t, payload, 1)
	for index := range length {
		payload[start+index] = ' '
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE durable_search_jobs SET artifact_sha256 = ? WHERE id = ?`, digest[:], job.ID); err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, testAccess(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	seekable := lease.(SeekableResultLease)
	if err := seekable.Seek(ctx, 512); err != nil {
		t.Fatalf("Seek(512): %v", err)
	}
	row, ok, err := seekable.Next(ctx)
	if err != nil || !ok || !reflect.DeepEqual(row, rows[512]) {
		t.Fatalf("row after sparse seek = %#v, %t, %v", row, ok, err)
	}
}

func framedRowPayload(t *testing.T, payload []byte, ordinal int) (int, int) {
	t.Helper()
	position := len(artifactMagic)
	if len(payload) < position+4 {
		t.Fatal("artifact header length is missing")
	}
	headerLength := int(binary.BigEndian.Uint32(payload[position : position+4]))
	position += 4 + headerLength
	for index := 0; index <= ordinal; index++ {
		if len(payload) < position+4 {
			t.Fatal("artifact row length is missing")
		}
		length := int(binary.BigEndian.Uint32(payload[position : position+4]))
		position += 4
		if len(payload) < position+length {
			t.Fatal("artifact row is truncated")
		}
		if index == ordinal {
			return position, length
		}
		position += length
	}
	t.Fatal("artifact row was not found")
	return 0, 0
}

func persistTestResults(
	t *testing.T,
	store *Store,
	clock *testClock,
	id string,
	rows []searchjobs.ResultRow,
) (searchjobs.Job, []searchjobs.ResultRow) {
	t.Helper()
	job := testQueuedJob(t, id, clock.now)
	if err := store.Admit(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	completed := completeTestJob(job, clock.now, time.Hour)
	completed.RowCount = uint64(len(rows))
	if err := store.Finalize(context.Background(), completed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PersistResults(context.Background(), testAccess(), job.ID, &sourceLease{
		schema: *completed.Schema, rows: rows, generation: 1,
	}); err != nil {
		t.Fatal(err)
	}
	return completed, rows
}
