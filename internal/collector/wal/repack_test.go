package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

func repackTestQueue(t *testing.T, opts Options) *queue {
	t.Helper()
	q, err := openQueue(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func nextRepackTestBatch(t *testing.T, q *queue) *opensplunk.EventBatch {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	batch, err := q.NextBatch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func TestRepackPreservesEventsAndCheckpointBarrierWithoutCopyingWAL(t *testing.T) {
	t.Parallel()
	q := repackTestQueue(t, defaultOpts(t.TempDir()))
	parent, err := q.Append(makeEvents("one", "two", "three", "four"))
	if err != nil {
		t.Fatal(err)
	}
	later, err := q.Append(makeEvents("later"))
	if err != nil {
		t.Fatal(err)
	}
	before := q.Stats()
	q.opts.MaxQueueBytes = before.PhysicalBytes
	if err := q.Repack(parent.GetBatchSequence(), 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	if after := q.Stats(); after.PhysicalBytes != before.PhysicalBytes || after.QueuedEvents != before.QueuedEvents || after.QueuedBatches != 5 {
		t.Fatalf("repacking copied or lost data: before=%+v after=%+v", before, after)
	}
	if got := nextRepackTestBatch(t, q); got.GetBatchId() != later.GetBatchId() {
		t.Fatal("parent was redelivered or later batch reordered")
	}
	if err := q.Ack(later.GetBatchSequence()); err != nil {
		t.Fatal(err)
	}
	for i, event := range parent.GetEvents() {
		child := nextRepackTestBatch(t, q)
		if len(child.GetEvents()) != 1 || !proto.Equal(child.GetEvents()[0], event) || child.GetBatchId() == parent.GetBatchId() {
			t.Fatalf("child changed event %d: %v", i, child)
		}
		preview, err := q.PrepareAck(child.GetBatchSequence())
		if err != nil {
			t.Fatal(err)
		}
		if i < len(parent.GetEvents())-1 && preview.BatchCount != 0 {
			t.Fatal("checkpoint advanced before all children were terminal")
		}
		if i == len(parent.GetEvents())-1 && preview.ThroughBatchSequence != child.GetBatchSequence() {
			t.Fatal("final child did not release parent checkpoint barrier")
		}
		if err := q.Ack(child.GetBatchSequence()); err != nil {
			t.Fatal(err)
		}
	}
	if stats := q.Stats(); stats.QueuedBatches != 0 || stats.QueuedEvents != 0 || stats.QueuedBytes != 0 {
		t.Fatalf("queue not drained: %+v", stats)
	}
}

func TestRepackRetainsBackingAfterParentAckUntilChildrenCrossPrefix(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	q := repackTestQueue(t, opts)
	if _, err := q.Append(makeEvents("one", "two")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Append(makeEvents("blocked")); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(1, 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	_ = nextRepackTestBatch(t, q)
	first, second := nextRepackTestBatch(t, q), nextRepackTestBatch(t, q)
	if err := q.Ack(first.GetBatchSequence()); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack(second.GetBatchSequence()); err != nil {
		t.Fatal(err)
	}
	if q.Stats().LastAckedBatchSequence != 1 {
		t.Fatal("expected parent terminal but later physical batch still blocked")
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = repackTestQueue(t, opts)
	if got := nextRepackTestBatch(t, q); got.GetBatchSequence() != 2 {
		t.Fatal("lost blocked physical batch")
	}
	if got := nextRepackTestBatch(t, q); !proto.Equal(got, first) {
		t.Fatalf("first child's identity/bytes changed after restart: %v", got)
	}
	if got := nextRepackTestBatch(t, q); !proto.Equal(got, second) {
		t.Fatalf("second child's identity/bytes changed after restart: %v", got)
	}
	if err := q.AckThrough(second.GetBatchSequence()); err != nil {
		t.Fatal(err)
	}
	if pending, err := HasPendingRecords(opts.Dir); err != nil || pending {
		t.Fatalf("repack queue did not drain: %t, %v", pending, err)
	}
}

func TestNestedRepackingRecoversStableIdentitiesAndBarriers(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	q := repackTestQueue(t, opts)
	if _, err := q.Append(makeEvents("one", "two", "three", "four")); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Append(makeEvents("later")); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(1, 2, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(3, 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	var before []*opensplunk.EventBatch
	for range 4 {
		before = append(before, nextRepackTestBatch(t, q))
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	q = repackTestQueue(t, opts)
	for i, want := range before {
		got := nextRepackTestBatch(t, q)
		if !proto.Equal(got, want) {
			t.Fatalf("nested repack changed batch %d", i)
		}
		if err := q.Ack(got.GetBatchSequence()); err != nil {
			t.Fatal(err)
		}
		if i < len(before)-1 && q.Stats().LastAckedBatchSequence != 0 {
			t.Fatal("nested checkpoint barrier released early")
		}
	}
	if q.Stats().QueuedBatches != 0 {
		t.Fatal("nested queue did not drain")
	}
}

func TestRepackAmbiguousPublicationRecoversSameChildren(t *testing.T) {
	t.Parallel()
	for _, boundary := range []string{"manifest", "inventory"} {
		t.Run(boundary, func(t *testing.T) {
			opts := defaultOpts(t.TempDir())
			q := repackTestQueue(t, opts)
			if _, err := q.Append(makeEvents("one", "two")); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected publication failure")
			if boundary == "manifest" {
				q.syncDirectory = func(string) error { return injected }
			} else {
				calls := 0
				q.persistMetaFile = func(dir string, meta walMeta) error {
					calls++
					if err := writeMeta(dir, meta); err != nil {
						return err
					}
					if calls == 2 {
						return injected
					}
					return nil
				}
			}
			if err := q.Repack(1, 1, 1<<20); !errors.Is(err, injected) {
				t.Fatalf("publication error = %v", err)
			}
			plan, err := readRepackPlan(filepath.Join(opts.Dir, repackFileName(1)))
			if err != nil {
				t.Fatal(err)
			}
			_ = q.Close()
			q = repackTestQueue(t, opts)
			for _, want := range plan.Children {
				if got := nextRepackTestBatch(t, q); got.GetBatchId() != want.BatchID || got.GetBatchSequence() != want.Sequence {
					t.Fatal("ambiguous publication minted replacement identities")
				}
			}
		})
	}
}

func TestRepackMissingRequiredManifestFailsClosed(t *testing.T) {
	t.Parallel()
	opts := defaultOpts(t.TempDir())
	q := repackTestQueue(t, opts)
	if _, err := q.Append(makeEvents("one", "two")); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(1, 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(opts.Dir, repackFileName(1))); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openQueue(opts); err == nil {
		_ = reopened.Close()
		t.Fatal("missing required manifest silently discarded")
	}
}

func TestNestedRepackingCumulativeAckCannotSkipDescendants(t *testing.T) {
	t.Parallel()
	q := repackTestQueue(t, defaultOpts(t.TempDir()))
	events := makeEvents("one", "two", "three", "four")
	for i, event := range events {
		event.Origin = &opensplunk.EventOrigin{InputId: "input", FileIdentity: new("dev=7;ino=9;gen=3;fp=" + strings.Repeat("ab", 32)),
			EndOffset: new(uint64(i+1) * 100), LineNumber: new(uint64(i + 1)), NextLineNumber: new(uint64(i + 2)),
			SourcePath: new("/logs/app.log"), FileFingerprintLength: proto.Uint32(1024)}
	}
	if _, err := q.Append(events); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(1, 2, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := q.Repack(2, 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	for _, parent := range []uint64{1, 2} {
		if err := q.Ack(parent); !errors.Is(err, ErrInvalidAck) {
			t.Fatalf("parent Ack = %v", err)
		}
		if err := q.AckThrough(parent); !errors.Is(err, ErrInvalidAck) {
			t.Fatalf("parent AckThrough = %v", err)
		}
		if _, err := q.PrepareAck(parent); !errors.Is(err, ErrInvalidAck) {
			t.Fatalf("parent PrepareAck = %v", err)
		}
		if _, err := q.PrepareAckThrough(parent); !errors.Is(err, ErrInvalidAck) {
			t.Fatalf("parent PrepareAckThrough = %v", err)
		}
	}
	preview, err := q.PrepareAckThrough(3)
	if err != nil || preview.BatchCount != 0 || len(preview.Marks) != 0 {
		t.Fatalf("preview skipped nested descendants: %+v, %v", preview, err)
	}
	if err := q.AckThrough(3); err != nil {
		t.Fatal(err)
	}
	if q.Stats().LastAckedBatchSequence != 0 {
		t.Fatal("cumulative acknowledgment released the parent before grandchildren")
	}
	preview, err = q.PrepareAckThrough(5)
	if err != nil || preview.ThroughBatchSequence != 5 || len(preview.Marks) != 1 || preview.Marks[0].EndOffset != 400 || preview.Marks[0].NextLineNumber != 5 {
		t.Fatalf("final preview lost the exact source cursor: %+v, %v", preview, err)
	}
	if err := q.AckThrough(5); err != nil {
		t.Fatal(err)
	}
	if q.Stats().QueuedBatches != 0 {
		t.Fatal("nested cumulative acknowledgment did not drain")
	}
}

func TestRepackByteLimitsAndRelaxedPolicyPreserveEvents(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		maximumBytes uint64
		wantChildren int
	}{
		{"byte limit and oversized singleton", 1, 3},
		{"durable fence after policy relaxation", 1 << 20, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			q := repackTestQueue(t, defaultOpts(t.TempDir()))
			parent, err := q.Append(makeEvents("one", "two", "three"))
			if err != nil {
				t.Fatal(err)
			}
			if err := q.Repack(1, 1000, test.maximumBytes); err != nil {
				t.Fatal(err)
			}
			var events []*opensplunk.LogEvent
			for range test.wantChildren {
				child := nextRepackTestBatch(t, q)
				events = append(events, child.GetEvents()...)
				if child.GetUncompressedSizeBytes() != uncompressedEventBytes(child.GetEvents()) {
					t.Fatal("wrong child byte count")
				}
				if err := q.Ack(child.GetBatchSequence()); err != nil {
					t.Fatal(err)
				}
			}
			if len(events) != len(parent.GetEvents()) {
				t.Fatal("lost events")
			}
			for i := range events {
				if !proto.Equal(events[i], parent.GetEvents()[i]) {
					t.Fatal("changed event bytes/order")
				}
			}
		})
	}
}
