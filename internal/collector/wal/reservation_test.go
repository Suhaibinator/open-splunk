package wal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestSequenceReservationsAmortizeMetadataAndSurviveRestart(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	opts.SequenceReservationSize = 4
	q, err := openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	writes := 0
	q.persistMetaFile = func(dir string, meta walMeta) error { writes++; return writeMeta(dir, meta) }
	for seq := uint64(1); seq <= 10; seq++ {
		batch, err := q.Append(makeEvents(fmt.Sprintf("event-%d", seq)))
		if err != nil || batch.GetBatchSequence() != seq {
			t.Fatalf("append = %v, %v; want %d", batch, err, seq)
		}
	}
	if writes != 3 {
		t.Fatalf("metadata writes = %d, want 3 for ten appends", writes)
	}
	if err := q.AckThrough(5); err != nil {
		t.Fatal(err)
	}
	meta, _, err := readMeta(opts.Dir)
	if err != nil || meta.NextBatchSequence != 13 || meta.LastAckedBatchSequence != 5 {
		t.Fatalf("ack lost reservation: %+v, %v", meta, err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	for seq := uint64(6); seq <= 10; seq++ {
		batch, err := q.NextBatch(context.Background())
		if err != nil || batch.GetBatchSequence() != seq {
			t.Fatalf("replay = %v, %v; want %d", batch, err, seq)
		}
	}
	batch, err := q.Append(makeEvents("after-restart"))
	if err != nil || batch.GetBatchSequence() != 13 {
		t.Fatalf("unused reservations reused: %v, %v", batch, err)
	}
}

func TestSequenceReservationRetriesAmbiguousPersistence(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	opts.SequenceReservationSize = 128
	q, err := openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	syncErr := errors.New("injected ambiguous directory sync")
	q.persistMetaFile = func(dir string, meta walMeta) error {
		if err := writeMeta(dir, meta); err != nil {
			return err
		}
		return syncErr
	}
	if _, err := q.Append(makeEvents("unpublished")); !errors.Is(err, syncErr) {
		t.Fatalf("append error = %v", err)
	}
	if q.reservedUntil != 0 || q.Stats().QueuedBatches != 0 {
		t.Fatal("failed reservation published a batch or reusable range")
	}
	writes := 0
	q.persistMetaFile = func(dir string, meta walMeta) error { writes++; return writeMeta(dir, meta) }
	batch, err := q.Append(makeEvents("published"))
	if err != nil || batch.GetBatchSequence() != 2 || writes != 1 {
		t.Fatalf("retry = %v, %v, writes=%d", batch, err, writes)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q, err = openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	if q.nextSeq != 130 {
		t.Fatalf("restart sequence = %d, want 130", q.nextSeq)
	}
	replayed, err := q.NextBatch(context.Background())
	if err != nil || replayed.GetBatchId() != batch.GetBatchId() {
		t.Fatalf("durable batch changed: %v, %v", replayed, err)
	}
}

func TestSequenceReservationDoesNotOverflow(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	opts.SequenceReservationSize = math.MaxUint64
	q, err := openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.nextSeq = math.MaxUint64 - 1
	if _, err := q.Append(makeEvents("last")); err != nil {
		t.Fatal(err)
	}
	meta, _, err := readMeta(opts.Dir)
	if err != nil || meta.NextBatchSequence != math.MaxUint64 {
		t.Fatalf("overflowed reservation: %+v, %v", meta, err)
	}
	if _, err := q.Append(makeEvents("overflow")); err == nil {
		t.Fatal("exhausted sequence accepted")
	}
}

func BenchmarkAppendSequenceReservation(b *testing.B) {
	for _, size := range []uint64{1, 128} {
		b.Run(fmt.Sprintf("reserve-%d", size), func(b *testing.B) {
			opts := defaultOpts(b.TempDir())
			opts.SequenceReservationSize = size
			q, err := openQueue(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer q.Close()
			events := makeEvents("benchmark")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := q.Append(events); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
