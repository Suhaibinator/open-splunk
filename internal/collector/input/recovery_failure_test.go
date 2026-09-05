package input

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

// flakyRejectionHandler fails every framing-recovery write while failing is
// set and records the rejections it accepts once it is allowed to succeed.
type flakyRejectionHandler struct {
	failing  atomic.Bool
	calls    atomic.Int64
	mu       sync.Mutex
	accepted []RawEvent
}

var errInjectedRecoveryFailure = errors.New("injected recovery failure")

func (h *flakyRejectionHandler) handle(_ context.Context, raw RawEvent) error {
	h.calls.Add(1)
	if h.failing.Load() {
		return errInjectedRecoveryFailure
	}
	h.mu.Lock()
	h.accepted = append(h.accepted, raw)
	h.mu.Unlock()
	return nil
}

func (h *flakyRejectionHandler) acceptedCodes() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	codes := make([]string, 0, len(h.accepted))
	for _, raw := range h.accepted {
		codes = append(codes, raw.RejectionCode)
	}
	return codes
}

func waitForCalls(t *testing.T, handler *flakyRejectionHandler, want int64) {
	t.Helper()
	waitFor(t, "rejection handler retries", func() bool {
		return handler.calls.Load() >= want
	})
}

// A failed framing-recovery write must keep the source descriptor open, keep
// the health error, and retry the same range. The file is rotated outside the
// include glob while recovery keeps failing: its unread bytes are reachable
// only through the descriptor the tailer already holds.
func TestManagerFramingRecoveryFailureRetainsRotatedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rotated := filepath.Join(dir, "rotated", "app.log.1")
	if err := os.MkdirAll(filepath.Dir(rotated), 0o755); err != nil {
		t.Fatalf("mkdir rotated: %v", err)
	}
	const oversized = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	content := "ok1\n" + oversized + "\nok2\n"
	writeFileT(t, path, content)

	handler := &flakyRejectionHandler{}
	handler.failing.Store(true)
	h := startManagerWithHooks(t, Config{
		InputID: "in", Include: []string{path},
		StartAt: StartAtBeginning,
		Framing: framing.Options{MaxEventBytes: 8},
	}, newStore(t), managerTestHooks{rejectionHandler: handler.handle})

	waitForCalls(t, handler, 1)
	waitFor(t, "framing recovery failure reported as input health error", func() bool {
		health := h.mgr.Health()
		return health.State == opensplunk.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR &&
			strings.Contains(health.StatusMessage, errInjectedRecoveryFailure.Error())
	})
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("events published before recovery succeeded: %+v", events)
	}

	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rotate input: %v", err)
	}
	// Recovery keeps retrying from the retained descriptor after rotation.
	callsAtRotation := handler.calls.Load()
	waitForCalls(t, handler, callsAtRotation+3)
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("events published while recovery kept failing: %+v", events)
	}
	if health := h.mgr.Health(); health.State != opensplunk.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR ||
		health.ActiveSources != 1 {
		t.Fatalf("health after rotation = %+v, want ERROR with one retained source", health)
	}

	handler.failing.Store(false)
	h.waitForTexts([]string{"ok1", "ok2"})
	events := h.col.snapshot()
	if events[0].Source.StartOffset != 0 || events[0].Source.EndOffset != 4 ||
		events[1].Source.StartOffset != uint64(len("ok1\n"+oversized+"\n")) ||
		events[1].Source.EndOffset != uint64(len(content)) {
		t.Fatalf("recovered events = %+v, want the exact original source range", events)
	}
	if codes := handler.acceptedCodes(); len(codes) != 1 || codes[0] != "FRAMING_EVENT_TOO_LARGE" {
		t.Fatalf("accepted rejections = %v, want one FRAMING_EVENT_TOO_LARGE", codes)
	}
	waitFor(t, "health error cleared after recovery", func() bool {
		return h.mgr.Health().State != opensplunk.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR
	})
}

// A framing-recovery failure inside the retirement commit must not complete
// retirement. The tailer keeps its descriptor and retries the final snapshot.
func TestTailerRetirementRetriesAfterFramingRecoveryFailure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "app.log")
	const content = "ok\nabcdefgh"
	writeFileT(t, path, content)

	tracked, _ := newTrackedTailerForTest(t, path, 0, 1)
	tracked.m.cfg.Framing.MaxEventBytes = 4
	tracked.m.stagedTransaction = make(chan struct{}, 1)
	handler := &flakyRejectionHandler{}
	handler.failing.Store(true)
	tracked.m.rejectionHandler = handler.handle
	tracked.runCtx = context.Background()
	tracked.requestDrain()
	version := tracked.retireVersion.Load()
	size := uint64(len(content))

	done, retryNow := tracked.finalizeRetirement(context.Background(), size, version)
	if done || retryNow {
		t.Fatalf("finalizeRetirement after failed recovery = (done=%v, retryNow=%v), want (false, false)", done, retryNow)
	}
	if handler.calls.Load() == 0 {
		t.Fatal("rejection handler was not invoked during retirement")
	}
	if tracked.retireCommitted {
		t.Fatal("retirement committed although the recovery artifact was not persisted")
	}
	if tracked.offset != 0 || !tracked.readErrorActive {
		t.Fatalf("tailer after failed recovery: offset=%d readErrorActive=%v, want offset 0 with an active read error",
			tracked.offset, tracked.readErrorActive)
	}
	if got := len(tracked.m.events); got != 0 {
		t.Fatalf("%d events published before recovery succeeded, want 0", got)
	}
	if !tracked.retireRequested.Load() {
		t.Fatal("retirement request was dropped by the failed commit")
	}

	// The successful retirement fences its publication with a durability
	// barrier, so drain events and acknowledge barriers like the daemon does.
	col := &collected{}
	drainCtx, stopDrain := context.WithCancel(context.Background())
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case ev := <-tracked.m.events:
				if ev.IsDurabilityBarrier() {
					ev.AcknowledgeDurabilityBarrier()
					continue
				}
				col.add(ev)
			case <-drainCtx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		stopDrain()
		<-drained
	})

	handler.failing.Store(false)
	deadline := time.Now().Add(3 * time.Second)
	for {
		done, retryNow = tracked.finalizeRetirement(context.Background(), size, version)
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retirement did not complete after recovery succeeded (retryNow=%v)", retryNow)
		}
	}
	if !tracked.retireCommitted || tracked.offset != size {
		t.Fatalf("tailer after retirement: committed=%v offset=%d, want committed at %d",
			tracked.retireCommitted, tracked.offset, size)
	}
	if codes := handler.acceptedCodes(); len(codes) != 1 || codes[0] != "FRAMING_EVENT_TOO_LARGE" {
		t.Fatalf("accepted rejections = %v, want one FRAMING_EVENT_TOO_LARGE", codes)
	}
	if texts := col.texts(); len(texts) != 1 || texts[0] != "ok" {
		t.Fatalf("published events = %q, want only the valid leading event", texts)
	}
}
