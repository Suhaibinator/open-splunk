package searchartifacts

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestConcurrentCaptureAndExportReservationsRespectSharedBudgets(t *testing.T) {
	store, _, _ := captureTestStore(t)
	options := CaptureOptions{MaximumRows: 1000, MaximumBytes: 1 << 20, OrganizationBytes: 2 << 20, GlobalBytes: 3 << 20}
	store.captureOptions = options
	// Both organizations compete for one instance budget. Captures use the real
	// reserve method; exports use the public quota API. No object upload is needed
	// to test the admission transaction that precedes an upload.
	const attempts = 24
	for i := range attempts / 2 {
		org := int64(1 + i%2)
		id := fmt.Sprintf("capture-quota-%d", i)
		if _, err := store.db.ExecContext(t.Context(), `INSERT INTO search_capture_sessions(capture_id,job_id,tenant_id,owner_id,organization_id,reserved_bytes,created_at_us)VALUES($1,$1,'tenant','owner',$2,0,1)`, id, org); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	results := make(chan error, attempts)
	var wait sync.WaitGroup
	for i := range attempts {
		wait.Go(func() {
			<-start
			org := int64(1 + i%2)
			if i < attempts/2 {
				id := fmt.Sprintf("capture-quota-%d", i)
				capture := captureTransaction{store: store, id: id, organization: org, options: options}
				results <- capture.reserve(t.Context(), 1<<20, BlobIdentity{Key: id, Bytes: 1 << 20})
			} else {
				results <- store.ReserveExport(t.Context(), fmt.Sprint(org), fmt.Sprintf("export-quota-%d", i), 1<<20)
			}
		})
	}
	close(start)
	wait.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrCapacity) {
			t.Fatalf("reservation: %v", err)
		}
	}
	if accepted != 3 {
		t.Fatalf("successful reservations=%d, want exactly the 3 MiB global budget", accepted)
	}
	for _, org := range []string{"1", "2"} {
		usage, err := store.RetentionUsage(t.Context(), org)
		if err != nil || usage.GlobalBytes != 3<<20 || usage.OrganizationBytes > 2<<20 {
			t.Fatalf("usage %s: %+v %v", org, usage, err)
		}
	}
}
