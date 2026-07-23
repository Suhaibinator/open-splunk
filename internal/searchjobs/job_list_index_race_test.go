package searchjobs

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestJobListIndexMutationDoesNotReadMutableJobState(t *testing.T) {
	const retainedCount = 16

	createdAt := time.Date(2026, time.July, 23, 12, 0, 0, 0, time.UTC)
	manager := &Manager{jobsByScope: make(map[AccessScope]*jobListIndexNode)}
	retained := make([]*jobEntry, retainedCount)
	manager.mu.Lock()
	for index := range retained {
		manager.nextGeneration++
		entry := &jobEntry{
			job: Job{
				ID:        fmt.Sprintf("retained-%02d", index),
				Version:   1,
				TenantID:  "tenant",
				OwnerID:   "owner",
				CreatedAt: createdAt,
				State:     StateRunning,
			},
			generation: manager.nextGeneration,
		}
		retained[index] = entry
		manager.insertJobListEntryLocked(entry)
	}
	manager.mu.Unlock()

	stop := make(chan struct{})
	started := make(chan struct{}, len(retained))
	var writers sync.WaitGroup
	writers.Add(len(retained))
	for _, entry := range retained {
		go func() {
			defer writers.Done()
			entry.mu.Lock()
			entry.job.Version++
			entry.mu.Unlock()
			started <- struct{}{}
			for {
				select {
				case <-stop:
					return
				default:
					entry.mu.Lock()
					entry.job.Version++
					entry.mu.Unlock()
				}
			}
		}()
	}
	for range retained {
		<-started
	}

	// Admission and tombstone removal hold Manager.mu but intentionally do not
	// acquire entry.mu. Repeated mutations make the race detector catch any
	// regression that copies a retained entry's whole, concurrently updated Job.
	for index := range 2_000 {
		manager.mu.Lock()
		manager.nextGeneration++
		transient := &jobEntry{
			job: Job{
				ID:        fmt.Sprintf("transient-%04d", index),
				Version:   1,
				TenantID:  "tenant",
				OwnerID:   "owner",
				CreatedAt: createdAt,
				State:     StateQueued,
			},
			generation: manager.nextGeneration,
		}
		manager.insertJobListEntryLocked(transient)
		manager.removeJobListEntryLocked(transient)
		manager.mu.Unlock()
	}
	close(stop)
	writers.Wait()

	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
	manager.mu.RLock()
	root := manager.jobsByScope[scope]
	if got := jobListIndexSize(root); got != retainedCount {
		manager.mu.RUnlock()
		t.Fatalf("retained index size = %d, want %d", got, retainedCount)
	}
	snapshots := make([]retainedJobListEntry, 0, retainedCount)
	jobListIndexCollectBefore(root, nil, &snapshots, retainedCount)
	manager.mu.RUnlock()
	for index, snapshot := range snapshots {
		wantID := fmt.Sprintf("retained-%02d", retainedCount-1-index)
		if snapshot.key.id != wantID || snapshot.entry != retained[retainedCount-1-index] {
			t.Fatalf("snapshot %d = (%q, %p), want (%q, %p)",
				index, snapshot.key.id, snapshot.entry, wantID, retained[retainedCount-1-index])
		}
	}
}
