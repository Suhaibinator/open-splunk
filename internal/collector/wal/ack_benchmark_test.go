package wal

import (
	"strconv"
	"strings"
	"testing"
)

func BenchmarkPrepareAckThrough100KCompactMarks(b *testing.B) {
	const batches = 100_000
	identity := "dev=1;ino=2;gen=1;fp=" + strings.Repeat("ab", 32)
	q := &queue{
		nextSeq:  uint64(batches + 1),
		terminal: make(map[uint64]struct{}),
		unacked:  make([]batchDesc, batches),
	}
	for index := range q.unacked {
		sequence := uint64(index + 1)
		q.unacked[index] = batchDesc{
			seq: sequence,
			sourceMarks: []SourceCheckpointMark{{
				BatchSequence:        sequence,
				FileIdentity:         identity,
				SourcePath:           "/logs/app-" + strconv.Itoa(index%8) + ".log",
				EndOffset:            sequence * 100,
				FingerprintLength:    1024,
				HasSourcePath:        true,
				HasEndOffset:         true,
				HasFingerprintLength: true,
			}},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		preview, err := q.PrepareAckThrough(batches)
		if err != nil {
			b.Fatal(err)
		}
		if preview.BatchCount != batches || len(preview.Marks) != 1 {
			b.Fatalf("preview = %+v", preview)
		}
	}
}

// BenchmarkPrepareAckThrough100KUniqueMarks is the adversarial counterpart to
// BenchmarkPrepareAckThrough100KCompactMarks. Every descriptor has a distinct
// exact file-generation identity, so the preview must retain and sort all
// 100,000 marks instead of repeatedly replacing one map entry.
func BenchmarkPrepareAckThrough100KUniqueMarks(b *testing.B) {
	const batches = 100_000
	const fingerprint = "abababababababababababababababababababababababababababababababab"
	q := &queue{
		nextSeq:  uint64(batches + 1),
		terminal: make(map[uint64]struct{}),
		unacked:  make([]batchDesc, batches),
	}
	for index := range q.unacked {
		sequence := uint64(index + 1)
		identity := "dev=1;ino=" + strconv.Itoa(index+1) +
			";gen=" + strconv.Itoa(index+1) + ";fp=" + fingerprint
		q.unacked[index] = batchDesc{
			seq: sequence,
			sourceMarks: []SourceCheckpointMark{{
				BatchSequence:        sequence,
				FileIdentity:         identity,
				SourcePath:           "/logs/app.log",
				EndOffset:            sequence * 100,
				FingerprintLength:    1024,
				HasSourcePath:        true,
				HasEndOffset:         true,
				HasFingerprintLength: true,
			}},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		preview, err := q.PrepareAckThrough(batches)
		if err != nil {
			b.Fatal(err)
		}
		if preview.BatchCount != batches || len(preview.Marks) != batches {
			b.Fatalf("preview has %d batches and %d marks, want %d of each",
				preview.BatchCount, len(preview.Marks), batches)
		}
	}
}

// BenchmarkSequentialAckDrain100 exercises the real persistent Ack path in
// ascending sequence order. It also detects backing-array changes to ensure
// geometric compaction copies at most O(N) descriptors over the whole drain.
// Setup is excluded from the timer. One hundred acks keep the benchmark useful
// when running the package's complete benchmark set: each iteration deliberately
// includes 100 durable metadata fsyncs.
func BenchmarkSequentialAckDrain100(b *testing.B) {
	const batches = 100

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		q := &queue{
			dir:      b.TempDir(),
			nextSeq:  uint64(batches + 1),
			terminal: make(map[uint64]struct{}, batches),
			unacked:  make([]batchDesc, batches),
		}
		for index := range q.unacked {
			q.unacked[index] = batchDesc{seq: uint64(index + 1)}
		}
		b.StartTimer()

		var descriptorCopies uint64
		for sequence := uint64(1); sequence <= batches; sequence++ {
			var nextDescriptor *batchDesc
			if len(q.unacked) > 1 {
				nextDescriptor = &q.unacked[1]
			}
			if err := q.Ack(sequence); err != nil {
				b.Fatalf("Ack(%d): %v", sequence, err)
			}
			if len(q.unacked) > 0 && &q.unacked[0] != nextDescriptor {
				descriptorCopies += uint64(len(q.unacked))
			}
		}

		b.StopTimer()
		if q.lastAcked != batches || len(q.unacked) != 0 ||
			q.unackedHeadWaste != 0 || cap(q.unacked) > 256 {
			b.Fatalf("drained queue = lastAcked %d, unacked %d, head waste %d, cap %d; want %d, 0, 0, cap <= 256",
				q.lastAcked, len(q.unacked), q.unackedHeadWaste, cap(q.unacked), batches)
		}
		if descriptorCopies > batches {
			b.Fatalf("geometric compaction copied %d descriptors, want at most %d",
				descriptorCopies, batches)
		}
		b.ReportMetric(float64(descriptorCopies), "descriptor-copies/op")
		b.StartTimer()
	}
}
