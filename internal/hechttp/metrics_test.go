package hechttp

import (
	"runtime"
	"sync"
	"testing"
)

func TestMetricsSnapshotIsCoherentDuringConcurrentUpdates(t *testing.T) {
	metrics := NewMetrics()
	const writers = 4
	const observationsPerWriter = 5_000
	var writersDone sync.WaitGroup
	writersDone.Add(writers)
	for range writers {
		go func() {
			defer writersDone.Done()
			for range observationsPerWriter {
				metrics.observeRequest()
				metrics.observeAccepted(2, 3)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		writersDone.Wait()
		close(done)
	}()
	for {
		snapshot := metrics.Snapshot()
		if snapshot.AcceptedRequests > snapshot.Requests ||
			snapshot.Events != snapshot.AcceptedRequests*2 ||
			snapshot.UncompressedBytes != snapshot.AcceptedRequests*3 {
			t.Fatalf("incoherent metrics snapshot = %+v", snapshot)
		}
		select {
		case <-done:
			final := metrics.Snapshot()
			want := uint64(writers * observationsPerWriter)
			if final.Requests != want || final.AcceptedRequests != want ||
				final.Events != want*2 || final.UncompressedBytes != want*3 {
				t.Fatalf("final metrics snapshot = %+v, want %d requests", final, want)
			}
			return
		default:
			runtime.Gosched()
		}
	}
}
