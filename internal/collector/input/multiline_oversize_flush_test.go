package input

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

func TestManagerMultilineOversizePartialStartFlushesAfterInactivity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	const rejected = "S old\n123456789\n"
	const recovered = "S ok"
	writeFileT(t, path, rejected+recovered)

	h := startManager(t, Config{
		InputID:    "in",
		Include:    []string{path},
		StartAt:    StartAtBeginning,
		Multiline:  true,
		FlushAfter: 20 * time.Millisecond,
		Framing: framing.Options{
			LineStartPattern: regexp.MustCompile(`^S `),
			MaxEventBytes:    8,
		},
	}, newStore(t))

	h.waitForTexts([]string{recovered})
	events := h.col.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one recovered partial start", events)
	}
	wantStart := uint64(len(rejected))
	if events[0].Source.StartOffset != wantStart ||
		events[0].Source.EndOffset != uint64(len(rejected+recovered)) ||
		events[0].Source.LineNumber != 3 || events[0].Source.NextLineNumber != 4 {
		t.Fatalf("recovered event = %+v, want offsets [%d,%d) lines [3,4)",
			events[0], wantStart, len(rejected+recovered))
	}
}

func TestManagerMultilineIncompleteOversizeInactivityStartsNextPhysicalLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	const rejected = "S old\n123456789"
	const recovered = "S ok"
	writeFileT(t, path, rejected)

	resolved := make(chan struct{}, 2)
	h := startManagerWithAfterDrainObserver(t, Config{
		InputID:    "in",
		Include:    []string{path},
		StartAt:    StartAtBeginning,
		Multiline:  true,
		FlushAfter: 20 * time.Millisecond,
		Framing: framing.Options{
			LineStartPattern: regexp.MustCompile(`^S `),
			MaxEventBytes:    8,
		},
	}, newStore(t), func(observation tailerPollObservation) {
		if observation.offset != uint64(len(rejected)) {
			return
		}
		select {
		case resolved <- struct{}{}:
		default:
		}
	})
	for range 2 {
		select {
		case <-resolved:
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for incomplete-line inactivity resolution; health=%+v", h.mgr.Health())
		}
	}
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("events before recovered start = %+v, want none", events)
	}

	appendFileT(t, path, recovered)
	h.waitForTexts([]string{recovered})
	events := h.col.snapshot()
	if len(events) != 1 || events[0].Source.StartOffset != uint64(len(rejected)) ||
		events[0].Source.EndOffset != uint64(len(rejected+recovered)) ||
		events[0].Source.LineNumber != 3 || events[0].Source.NextLineNumber != 4 {
		t.Fatalf("recovered post-inactivity event = %+v", events)
	}
}

func TestManagerMultilineOversizePartialStartFlushesOnRenameRetirement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rotated := filepath.Join(dir, "app.log.rotated")
	const rejected = "S old\n123456789\n"
	const recovered = "S retired"
	writeFileT(t, path, rejected+recovered)

	h := startManager(t, Config{
		InputID:    "in",
		Include:    []string{path},
		StartAt:    StartAtBeginning,
		Multiline:  true,
		FlushAfter: time.Hour,
		Framing: framing.Options{
			LineStartPattern: regexp.MustCompile(`^S `),
			MaxEventBytes:    12,
		},
	}, newStore(t))
	waitFor(t, "oversized multiline event", func() bool {
		return !h.mgr.Health().LastErrorAt.IsZero()
	})
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("events before retirement = %+v, want none", events)
	}

	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename input: %v", err)
	}
	h.waitForTexts([]string{recovered})
	events := h.col.snapshot()
	if len(events) != 1 {
		t.Fatalf("events = %+v, want one retirement-flushed start", events)
	}
	wantStart := uint64(len(rejected))
	if events[0].Source.StartOffset != wantStart ||
		events[0].Source.EndOffset != uint64(len(rejected+recovered)) ||
		events[0].Source.LineNumber != 3 || events[0].Source.NextLineNumber != 4 {
		t.Fatalf("retirement-flushed event = %+v, want offsets [%d,%d) lines [3,4)",
			events[0], wantStart, len(rejected+recovered))
	}
}

func TestManagerMultilineRenameRetirementDrainsStartRetainedByFlush(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")
	rotated := filepath.Join(dir, "app.log.rotated")
	const content = "S old\nS new"
	writeFileT(t, path, content)

	stagedPartial := make(chan struct{}, 1)
	h := startManagerWithAfterDrainObserver(t, Config{
		InputID:    "in",
		Include:    []string{path},
		StartAt:    StartAtBeginning,
		Multiline:  true,
		FlushAfter: time.Hour,
		Framing: framing.Options{
			LineStartPattern: regexp.MustCompile(`^S `),
			MaxEventBytes:    5,
		},
	}, newStore(t), func(observation tailerPollObservation) {
		if observation.offset != 0 {
			return
		}
		select {
		case stagedPartial <- struct{}{}:
		default:
		}
	})
	select {
	case <-stagedPartial:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for retained matching partial; health=%+v", h.mgr.Health())
	}
	if events := h.col.snapshot(); len(events) != 0 {
		t.Fatalf("events before retirement = %+v, want none", events)
	}

	if err := os.Rename(path, rotated); err != nil {
		t.Fatalf("rename input: %v", err)
	}
	h.waitForTexts([]string{"S old", "S new"})
	events := h.col.snapshot()
	if len(events) != 2 || string(events[0].Bytes) != "S old" ||
		string(events[1].Bytes) != "S new" ||
		events[0].Source.StartOffset != 0 || events[0].Source.EndOffset != 6 ||
		events[0].Source.LineNumber != 1 || events[0].Source.NextLineNumber != 2 ||
		events[1].Source.StartOffset != 6 || events[1].Source.EndOffset != uint64(len(content)) ||
		events[1].Source.LineNumber != 2 || events[1].Source.NextLineNumber != 3 {
		t.Fatalf("retirement-drained events = %+v", events)
	}
}

func TestTailerMultilineOversizeInactivityConsumesRejectedContinuation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	const rejected = "S old\n123456789\n"
	const continuation = "more"
	writeFileT(t, path, rejected+continuation)

	tracked, _ := newTrackedTailerForTest(t, path, uint64(len(rejected)), 3)
	tracked.m.cfg.Multiline = true
	tracked.m.cfg.FlushAfter = time.Nanosecond
	tracked.m.cfg.Framing.LineStartPattern = regexp.MustCompile(`^S `)
	tracked.m.cfg.Framing.MaxEventBytes = 8
	tracked.discardingMultilineOversize = true
	tracked.lastSizeChange = time.Now().Add(-time.Hour)

	batch, err := tracked.stageRead(
		context.Background(),
		uint64(len(rejected+continuation)),
		false,
	)
	if err != nil {
		t.Fatalf("stage rejected continuation: %v", err)
	}
	if len(batch.events) != 0 || batch.cursor.offset != uint64(len(rejected+continuation)) ||
		batch.cursor.nextLineNumber != 4 ||
		!batch.cursor.discardingMultilineOversize ||
		batch.cursor.discardingMultilinePartialLine {
		t.Fatalf("resolved rejected continuation batch = %+v", batch)
	}
}

func TestTailerMultilineOversizeRetirementResolvesConsumedPartialAtEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	const rejectedPartial = "S old\n" + "123456789"
	writeFileT(t, path, rejectedPartial)

	tracked, _ := newTrackedTailerForTest(t, path, uint64(len(rejectedPartial)), 2)
	tracked.m.cfg.Multiline = true
	tracked.m.cfg.Framing.LineStartPattern = regexp.MustCompile(`^S `)
	tracked.m.cfg.Framing.MaxEventBytes = 8
	tracked.discardingMultilineOversize = true
	tracked.discardingMultilinePartialLine = true

	batch, err := tracked.stageRead(
		context.Background(),
		uint64(len(rejectedPartial)),
		true,
	)
	if err != nil {
		t.Fatalf("stage forced EOF resolution: %v", err)
	}
	if len(batch.events) != 0 || batch.cursor.offset != uint64(len(rejectedPartial)) ||
		batch.cursor.nextLineNumber != 3 ||
		!batch.cursor.discardingMultilineOversize ||
		batch.cursor.discardingMultilinePartialLine {
		t.Fatalf("forced EOF resolution batch = %+v", batch)
	}
}
