package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
	"github.com/Suhaibinator/open-splunk/internal/collectorlimits"
)

// defaultPollInterval is used when Config.PollInterval is unset.
const defaultPollInterval = 250 * time.Millisecond

// maxConcurrentStagedTransactions prevents one source blocked on downstream
// publication from serializing every other source in the input. A staged
// transaction's dependency is bounded by readWindow+fpBytes, its total frame
// payload by readWindow, and its event count by maxStagedEvents. This fixed
// permit count therefore preserves a hard manager-wide memory bound while
// allowing independent files to make progress.
const maxConcurrentStagedTransactions = 4

// errSourceSnapshotChanged classifies a short exact read as evidence that the
// source changed while a transaction was being assembled or validated. Other
// I/O errors are transient failures: callers report them without burning a
// generation or replaying already-consumed bytes.
var errSourceSnapshotChanged = errors.New("collector/input: source snapshot changed")

func classifyExactReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w: %w", errSourceSnapshotChanged, err)
	}
	return err
}

// manager is the concrete Manager for one file input. It is poll-driven: every
// PollInterval it re-globs, starts a per-file tailer goroutine for newly seen
// files, and asks tailers whose file left the discovery set to drain and stop.
type manager struct {
	cfg         Config
	checkpoints ManagerCheckpointStore
	fpBytes     int
	poll        time.Duration
	readWindow  uint64
	identityFn  func(*os.File, os.FileInfo, int) (FileIdentity, error)
	// afterMatchPathsObserver is an internal test seam after one discovery
	// snapshot is assembled but before it can claim or replace a tailer.
	afterMatchPathsObserver func()
	// afterDrainObserver is an internal test seam installed before Run starts.
	// It runs after a tailer stages a bounded read but before validation and
	// publication.
	afterDrainObserver func(tailerPollObservation)
	// afterSnapshotChunkObserver is an internal test seam invoked while a
	// bounded immutable source snapshot is being assembled.
	afterSnapshotChunkObserver func(tailerPollObservation)
	// beforeSnapshotReadObserver is an internal test seam after Stat selected a
	// bounded end but before the exact snapshot is read.
	beforeSnapshotReadObserver func(tailerPollObservation)
	// beforeStartGuardObserver is an internal test seam between durable resume
	// resolution and the new tailer's initial trailing-guard capture.
	beforeStartGuardObserver func(tailerPollObservation)
	// beforeRetireCommitObserver is an internal test seam at the finite
	// retirement boundary, before the final exact snapshot validation.
	beforeRetireCommitObserver func(tailerPollObservation)
	// afterRetireCancelObserver is an internal test seam for the manager/tailer
	// cancellation handoff.
	afterRetireCancelObserver func(tailerPollObservation)

	events chan RawEvent
	// stagedTransaction is a fixed-capacity manager-wide permit pool. A tailer
	// holds one permit from snapshot allocation through validation and
	// publication, bounding aggregate staged memory even while the shared event
	// consumer is backpressured.
	stagedTransaction chan struct{}

	wg      sync.WaitGroup
	tailers map[string]*tailer // keyed by tracking key (dev/ino or fingerprint)

	// Health counters. Updated with atomics from the poll loop and every tailer.
	discovered  atomic.Uint64
	active      atomic.Int64
	eventsRead  atomic.Uint64
	bytesRead   atomic.Uint64
	lastEventNs atomic.Int64
	lastErrorNs atomic.Int64

	stateMu sync.Mutex
	state   opensplunkv1.CollectorInputState
	status  string

	// sourceErrors retains the current per-tailer failure independently of the
	// discovery pass. Without this aggregation a successful glob would overwrite
	// a persistent Stat/ReadAt failure with HEALTHY on every manager poll. Code
	// needing both health locks acquires sourceErrMu before stateMu.
	sourceErrMu  sync.Mutex
	sourceErrors map[string]string

	runOnce sync.Once
}

// NewManager constructs a file input Manager. checkpoints supplies resume
// offsets at discovery time; the Manager never advances their durable byte
// positions, though it may enrich a legacy checkpoint with a stable
// compatibility cursor in one batched discovery write.
func NewManager(cfg Config, checkpoints ManagerCheckpointStore) (Manager, error) {
	if !ValidInputID(cfg.InputID) {
		return nil, fmt.Errorf(
			"collector/input: input ID %q is not a canonical identifier",
			cfg.InputID,
		)
	}
	if len(cfg.Include) == 0 {
		return nil, errors.New("collector/input: at least one include glob is required")
	}
	if checkpoints == nil {
		return nil, errors.New("collector/input: checkpoint store is required")
	}
	if cfg.Multiline && cfg.Framing.LineStartPattern == nil {
		return nil, errors.New("collector/input: multiline line-start pattern is required")
	}
	fpBytes, err := validatedFingerprintBytes(cfg.FingerprintBytes)
	if err != nil {
		return nil, err
	}
	readWindow, err := stagedReadWindow(cfg)
	if err != nil {
		return nil, err
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	m := &manager{
		cfg:               cfg,
		checkpoints:       checkpoints,
		fpBytes:           fpBytes,
		poll:              poll,
		readWindow:        readWindow,
		events:            make(chan RawEvent),
		stagedTransaction: make(chan struct{}, maxConcurrentStagedTransactions),
		tailers:           make(map[string]*tailer),
		sourceErrors:      make(map[string]string),
		state:             opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_STARTING,
	}
	return m, nil
}

// Events returns the channel of framed raw events; closed when Run returns.
func (m *manager) Events() <-chan RawEvent { return m.events }

// Run blocks tailing until ctx is canceled. It never returns for a per-file
// read error (those surface through Health); it returns nil once every tailer
// has drained and the events channel is closed. Run must be called once.
func (m *manager) Run(ctx context.Context) error {
	started := false
	m.runOnce.Do(func() { started = true })
	if !started {
		return errors.New("collector/input: Run already called")
	}

	ticker := time.NewTicker(m.poll)
	defer ticker.Stop()

	// start_at=end is a startup policy, not a policy for every file ever
	// discovered. Files created or rotated into the glob after this first scan
	// must be read from the beginning or their already-written prefix is lost.
	m.pollOnce(ctx, true)
	for {
		select {
		case <-ctx.Done():
			// Tailers observe ctx cancellation themselves; wait them out, then
			// close the events channel so downstream readers see EOF.
			m.wg.Wait()
			close(m.events)
			return nil
		case <-ticker.C:
			m.pollOnce(ctx, false)
		}
	}
}

// Health returns the current input health snapshot, aggregated atomically.
func (m *manager) Health() Health {
	m.stateMu.Lock()
	state, status := m.state, m.status
	m.stateMu.Unlock()
	// Snapshot active first. Discovery intentionally tolerates transient misses
	// while an existing tailer drains; publishing the later discovery count
	// verbatim could therefore report active_sources > discovered_sources, which
	// is rejected by the fleet heartbeat validator. Lift discovery to the active
	// snapshot and saturate every wire-facing counter at MaxInt64 so downstream
	// int64 storage/validation remains representable.
	// #nosec G115 -- max64 clamps the atomic counter to a non-negative int64.
	active := collectorlimits.ClampFleetCounter(uint64(max64(m.active.Load(), 0)))
	discovered := collectorlimits.ClampFleetCounter(m.discovered.Load())
	if discovered < active {
		discovered = active
	}
	return Health{
		InputID:           m.cfg.InputID,
		State:             state,
		StatusMessage:     status,
		DiscoveredSources: discovered,
		ActiveSources:     active,
		EventsReadTotal:   collectorlimits.ClampFleetCounter(m.eventsRead.Load()),
		BytesReadTotal:    collectorlimits.ClampFleetCounter(m.bytesRead.Load()),
		LastEventAt:       timeFromNanos(m.lastEventNs.Load()),
		LastErrorAt:       timeFromNanos(m.lastErrorNs.Load()),
	}
}

// Close releases resources. Safe to call after Run returns; it does not close
// the events channel (Run owns that) and does not itself stop tailers (ctx
// cancellation does).
func (m *manager) Close() error { return nil }

// pollOnce performs one discovery cycle: glob, reconcile tailers, recompute
// health state.
func (m *manager) pollOnce(ctx context.Context, initial bool) {
	if ctx.Err() != nil {
		return
	}
	paths := m.matchPaths()
	if observer := m.afterMatchPathsObserver; observer != nil {
		observer()
	}
	if ctx.Err() != nil {
		return
	}
	m.discovered.Store(uint64(len(paths)))

	seen := make(map[string]struct{}, len(paths))
	var legacyUpgrades []Checkpoint
	var openErr string

	for _, p := range paths {
		if ctx.Err() != nil {
			return
		}
		fi, err := os.Stat(p)
		if err != nil {
			openErr = fmt.Sprintf("stat %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		if !fi.Mode().IsRegular() {
			openErr = fmt.Sprintf("stat %s: source is not a regular file", p)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		dev, ino, haveID := statDevIno(fi)
		if haveID {
			key := fmt.Sprintf("dev=%d;ino=%d", dev, ino)
			if t, ok := m.tailers[key]; ok {
				if m.claimTailer(key, t, p) {
					seen[key] = struct{}{}
					continue
				}
			}
		}

		// New file (or a platform without stable inode): open it, fingerprint,
		// and start a tailer.
		f, err := openFileForTailing(p)
		if err != nil {
			openErr = fmt.Sprintf("open %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		fi2, err := f.Stat()
		if err != nil {
			_ = f.Close()
			openErr = fmt.Sprintf("stat %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		if !fi2.Mode().IsRegular() {
			_ = f.Close()
			openErr = fmt.Sprintf("stat opened %s: source is not a regular file", p)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		if fi2.Size() < 0 {
			_ = f.Close()
			openErr = fmt.Sprintf("stat %s: negative size", p)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		id, err := m.identifyFile(f, fi2)
		if err != nil {
			_ = f.Close()
			openErr = fmt.Sprintf("fingerprint %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}

		key := id.String()
		if haveID {
			key = fmt.Sprintf("dev=%d;ino=%d", id.Device, id.Inode)
		}
		if existing, ok := m.tailers[key]; ok {
			if m.claimTailer(key, existing, p) {
				// Race: another path with the same inode created it this cycle.
				_ = f.Close()
				seen[key] = struct{}{}
				continue
			}
		}
		if ctx.Err() != nil {
			_ = f.Close()
			return
		}

		// #nosec G115 -- the negative-size case is rejected above.
		start, err := m.resolveStart(
			id, p, uint64(fi2.Size()), initial, f,
		)
		if err != nil {
			_ = f.Close()
			openErr = fmt.Sprintf("checkpoint %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		if start.legacyUpgrade != nil {
			legacyUpgrades = append(legacyUpgrades, *start.legacyUpgrade)
		}
		if ctx.Err() != nil {
			_ = f.Close()
			return
		}
		t, err := m.startTailer(
			ctx,
			key,
			p,
			f,
			start.identity,
			start.offset,
			start.nextLine,
			start.lineCursorKnown,
			start.guardFingerprint,
			start.guardLength,
		)
		if err != nil {
			_ = f.Close()
			openErr = fmt.Sprintf("frame %s: %v", p, err)
			m.lastErrorNs.Store(time.Now().UnixNano())
			continue
		}
		m.tailers[key] = t
		seen[key] = struct{}{}
	}

	// A legacy checkpoint is already a durable, safe resume point. Persist its
	// reconstructed cursor once per discovery pass rather than rewriting and
	// fsyncing the complete checkpoint document once per source. Tailers may
	// advance concurrently; SetMany's monotonic merge makes those older
	// enrichments harmless if terminal delivery wins the race.
	if ctx.Err() != nil {
		return
	}
	if err := m.checkpoints.SetMany(legacyUpgrades); err != nil {
		openErr = fmt.Sprintf("upgrade legacy checkpoints: %v", err)
		m.lastErrorNs.Store(time.Now().UnixNano())
	}

	m.reconcileTailers(seen)

	m.updateState(len(paths), openErr)
}

// claimTailer reuses a live lifecycle for a discovered inode. Cancellation and
// the finished check deliberately bracket the retirement lock: a finalizer may
// finish while discovery waits for that lock, and such a finished entry must
// be removed so this same discovery pass can open its replacement.
func (m *manager) claimTailer(key string, t *tailer, path string) bool {
	if t.finished.Load() {
		delete(m.tailers, key)
		return false
	}
	wasRetiring := t.retireRequested.Load()
	if !wasRetiring {
		t.setPath(path)
		if t.finished.Load() {
			delete(m.tailers, key)
			return false
		}
		return true
	}
	canceled := t.cancelDrain()
	if t.finished.Load() {
		delete(m.tailers, key)
		return false
	}
	if !canceled {
		// Retirement already crossed its exact-validation boundary. It owns the
		// inode until its staged terminal publication completes.
		return true
	}
	t.setPath(path) // rename continuity: same inode, new path, keep offset
	if wasRetiring && m.afterRetireCancelObserver != nil {
		m.afterRetireCancelObserver(tailerPollObservation{
			path: path,
		})
	}
	return true
}

// reconcileTailers requires two consecutive discovery misses before retiring
// a source. A glob result is not an atomic directory snapshot: a rename can
// move a matched path after matchPaths returns but before Stat, making a live
// inode appear absent for one pass. Immediately draining on that stale pass
// would later rediscover the inode at its new path and replay its durable
// checkpoint. One complete confirmation pass closes that race without adding
// extra directory scans to steady-state polling.
func (m *manager) reconcileTailers(seen map[string]struct{}) {
	for key, t := range m.tailers {
		if t.finished.Load() {
			delete(m.tailers, key)
			continue
		}
		if _, ok := seen[key]; ok {
			t.missingDiscoveries = 0
			continue
		}
		if t.missingDiscoveries == 0 {
			t.missingDiscoveries = 1
			continue
		}
		t.requestDrain()
	}
}

type resolvedStart struct {
	identity         FileIdentity
	offset           uint64
	nextLine         uint64
	lineCursorKnown  bool
	guardFingerprint string
	guardLength      uint32
	legacyUpgrade    *Checkpoint
}

// resolveStart chooses both the durable generation identity and initial offset.
// A discovery checkpoint is written before any bytes are emitted. This small
// write is what makes a crash after WAL append but before the first terminal
// delivery checkpoint reproduce the same event IDs on restart.
func (m *manager) resolveStart(
	id FileIdentity,
	path string,
	size uint64,
	initial bool,
	f *os.File,
) (resolvedStart, error) {
	cp, ok, err := m.checkpoints.Get(m.cfg.InputID, id)
	if err != nil {
		return resolvedStart{}, err
	}
	if ok {
		sameGeneration := false
		if cp.Offset <= size {
			prefixMatches, prefixErr := persistedPrefixMatches(f, cp.Identity)
			if prefixErr != nil && !errors.Is(prefixErr, errSourceSnapshotChanged) {
				return resolvedStart{}, prefixErr
			}
			guardMatches, guardErr := persistedCheckpointGuardMatches(f, cp)
			if guardErr != nil && !errors.Is(guardErr, errSourceSnapshotChanged) {
				return resolvedStart{}, guardErr
			}
			sameGeneration = prefixErr == nil && prefixMatches &&
				guardErr == nil && guardMatches
		}
		if sameGeneration {
			nextLine, lineCursorKnown, lineErr := checkpointNextLine(cp, m.cfg.Multiline)
			if lineErr != nil {
				return resolvedStart{}, lineErr
			}
			var legacyUpgrade *Checkpoint
			if cp.NextLineNumber == 0 && lineCursorKnown {
				cp.NextLineNumber = nextLine
				legacyUpgrade = &cp
			}
			if cp.GuardLength == 0 && cp.Offset > 0 {
				guardFingerprint, guardLength, guardErr := captureCheckpointGuard(
					f,
					cp.Offset,
					m.fpBytes,
				)
				if guardErr != nil {
					return resolvedStart{}, guardErr
				}
				cp.GuardFingerprint = guardFingerprint
				cp.GuardLength = guardLength
				legacyUpgrade = &cp
			}
			return resolvedStart{
				identity:         cp.Identity,
				offset:           cp.Offset,
				nextLine:         nextLine,
				lineCursorKnown:  lineCursorKnown,
				guardFingerprint: cp.GuardFingerprint,
				guardLength:      cp.GuardLength,
				legacyUpgrade:    legacyUpgrade,
			}, nil
		}
		// The same inode was truncated/reused. Burn a new generation before
		// offsets restart so identical bytes at identical offsets remain distinct.
		id.Generation = cp.Identity.Generation + 1
		if id.Generation == 0 { // uint overflow is practically impossible, fail safe
			return resolvedStart{}, errors.New("file generation exhausted")
		}
		if err := m.checkpoints.Set(Checkpoint{
			InputID: m.cfg.InputID, Identity: id, Path: path,
			Offset: 0, NextLineNumber: 1,
		}); err != nil {
			return resolvedStart{}, err
		}
		return resolvedStart{
			identity: id, nextLine: 1, lineCursorKnown: true,
		}, nil
	}

	start := uint64(0)
	if initial && m.cfg.StartAt == StartAtEnd {
		start = size
	}
	nextLine := uint64(1)
	guardFingerprint, guardLength, err := captureCheckpointGuard(f, start, m.fpBytes)
	if err != nil {
		return resolvedStart{}, err
	}
	if err := m.checkpoints.Set(Checkpoint{
		InputID: m.cfg.InputID, Identity: id, Path: path,
		Offset:           start,
		NextLineNumber:   nextLine,
		GuardFingerprint: guardFingerprint,
		GuardLength:      guardLength,
	}); err != nil {
		return resolvedStart{}, err
	}
	return resolvedStart{
		identity:         id,
		offset:           start,
		nextLine:         nextLine,
		lineCursorKnown:  true,
		guardFingerprint: guardFingerprint,
		guardLength:      guardLength,
	}, nil
}

func checkpointNextLine(
	checkpoint Checkpoint,
	multiline bool,
) (nextLine uint64, known bool, err error) {
	if checkpoint.NextLineNumber != 0 {
		if checkpoint.NextLineNumber == ^uint64(0) ||
			(checkpoint.LineNumber == 0 && checkpoint.NextLineNumber != 1) ||
			(checkpoint.LineNumber != 0 && checkpoint.NextLineNumber <= checkpoint.LineNumber) {
			return 0, false, errors.New("checkpoint has invalid next line number")
		}
		return checkpoint.NextLineNumber, true, nil
	}
	// The old checkpoint format retained only the first physical line of the
	// last acknowledged logical event. Advancing that value is exact for line
	// framing. A legacy multiline event may have consumed continuation lines
	// that cannot be recovered without an unbounded source-prefix scan, so its
	// internal framer restarts from one while published line metadata remains
	// absent. Byte offsets and event IDs remain exact.
	if checkpoint.LineNumber == 0 {
		// Offset zero is the exact beginning of a monitored stream. A nonzero
		// legacy discovery offset (notably start_at=end) has no physical-line
		// anchor and must remain unknown.
		return 1, checkpoint.Offset == 0, nil
	}
	if multiline {
		return 1, false, nil
	}
	if checkpoint.LineNumber >= ^uint64(0)-1 {
		return 0, false, errors.New("checkpoint line number exhausted")
	}
	return checkpoint.LineNumber + 1, true, nil
}

// persistedPrefixMatches compares exactly the bytes covered by the persisted
// fingerprint. Hashing a fixed prefix length avoids mistaking ordinary append
// growth for a rewrite when the file was initially shorter than fpBytes.
func persistedPrefixMatches(f *os.File, id FileIdentity) (bool, error) {
	if id.FingerprintLength == 0 {
		// Zero-length fingerprints are valid for files discovered empty and for
		// checkpoints written by the format predating FingerprintLength.
		return true, nil
	}
	fp, err := computeFingerprintRange(f, 0, id.FingerprintLength)
	if err != nil {
		return false, classifyExactReadError(err)
	}
	return fp == id.Fingerprint, nil
}

func persistedCheckpointGuardMatches(f *os.File, checkpoint Checkpoint) (bool, error) {
	if checkpoint.GuardLength == 0 {
		return checkpoint.GuardFingerprint == "", nil
	}
	length := uint64(checkpoint.GuardLength)
	if length > checkpoint.Offset {
		return false, errors.New("collector/input: checkpoint rewrite guard exceeds offset")
	}
	offset, err := checkedFileOffset(checkpoint.Offset - length)
	if err != nil {
		return false, err
	}
	fingerprint, err := computeFingerprintRange(f, offset, checkpoint.GuardLength)
	if err != nil {
		return false, classifyExactReadError(err)
	}
	return fingerprint == checkpoint.GuardFingerprint, nil
}

func captureCheckpointGuard(
	f *os.File,
	end uint64,
	maximum int,
) (fingerprint string, length uint32, err error) {
	guardLength := end
	if limit := uint64(maximum); guardLength > limit {
		guardLength = limit
	}
	if guardLength == 0 {
		return "", 0, nil
	}
	offset, err := checkedFileOffset(end - guardLength)
	if err != nil {
		return "", 0, err
	}
	// #nosec G115 -- maximum is validated against maximumFingerprintBytes.
	length = uint32(guardLength)
	fingerprint, err = computeFingerprintRange(f, offset, length)
	if err != nil {
		return "", 0, classifyExactReadError(err)
	}
	return fingerprint, length, nil
}

// updateState recomputes the aggregate health state after a poll.
func (m *manager) updateState(discovered int, openErr string) {
	m.sourceErrMu.Lock()
	defer m.sourceErrMu.Unlock()
	readErr := selectedSourceError(m.sourceErrors)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	switch {
	case discovered == 0:
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_MISSING
		m.status = fmt.Sprintf("no files match include globs %v", m.cfg.Include)
	case readErr != "":
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR
		m.status = readErr
	case openErr != "":
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_UNREADABLE
		m.status = openErr
	default:
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
		m.status = ""
	}
}

// setReadError records an asynchronous per-file read failure into Health and
// retains it across discovery polls until that exact tailer makes progress.
func (m *manager) setReadError(key, path string, err error) {
	m.lastErrorNs.Store(time.Now().UnixNano())
	message := fmt.Sprintf("read %s: %v", path, err)
	m.sourceErrMu.Lock()
	defer m.sourceErrMu.Unlock()
	if m.sourceErrors == nil {
		m.sourceErrors = make(map[string]string)
	}
	m.sourceErrors[key] = message
	status := selectedSourceError(m.sourceErrors)
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR
	m.status = status
}

// selectedSourceError makes aggregate health stable across goroutine timing and
// Go's randomized map iteration: the lexicographically smallest tracking key
// supplies the reported status while its error remains active.
func selectedSourceError(sourceErrors map[string]string) string {
	var selectedKey, selectedMessage string
	found := false
	for key, message := range sourceErrors {
		if !found || key < selectedKey {
			selectedKey = key
			selectedMessage = message
			found = true
		}
	}
	return selectedMessage
}

func (m *manager) clearReadError(key string) {
	m.sourceErrMu.Lock()
	delete(m.sourceErrors, key)
	m.sourceErrMu.Unlock()
}

// matchPaths returns the sorted, de-duplicated set of paths matched by the
// include globs and not removed by the exclude globs.
func (m *manager) matchPaths() []string {
	set := make(map[string]struct{})
	for _, inc := range m.cfg.Include {
		matches, err := filepath.Glob(inc)
		if err != nil {
			continue // malformed pattern: treated as matching nothing
		}
		for _, p := range matches {
			set[p] = struct{}{}
		}
	}
	for p := range set {
		if m.excluded(p) {
			delete(set, p)
		}
	}
	out := make([]string, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// excluded reports whether path matches any exclude glob, tested against both
// the full path and the base name so patterns like "*.tmp" work as expected.
func (m *manager) excluded(path string) bool {
	base := filepath.Base(path)
	for _, exc := range m.cfg.Exclude {
		if ok, _ := filepath.Match(exc, path); ok {
			return true
		}
		if ok, _ := filepath.Match(exc, base); ok {
			return true
		}
	}
	return false
}

// startTailer launches the tailer goroutine for a newly discovered file. The
// tailer owns f from here on and reframes from its current offset whenever
// growth or pending bytes require reading.
func (m *manager) startTailer(
	ctx context.Context,
	key, path string,
	f *os.File,
	id FileIdentity,
	start, nextLine uint64,
	lineCursorKnown bool,
	checkpointGuardFingerprint string,
	checkpointGuardLength uint32,
) (*tailer, error) {
	t := &tailer{
		m:               m,
		key:             key,
		f:               f,
		id:              id,
		offset:          start,
		nextLineNumber:  nextLine,
		lineCursorKnown: lineCursorKnown,
		lastSizeChange:  time.Now(),
	}
	if observer := m.beforeStartGuardObserver; observer != nil {
		observer(tailerPollObservation{
			path:       path,
			offset:     start,
			generation: id.Generation,
		})
	}
	initialGuard, err := t.captureGuard(t.offset)
	if err != nil {
		return nil, err
	}
	if id.FingerprintLength > 0 {
		matches, matchErr := persistedPrefixMatches(f, id)
		if matchErr != nil && !errors.Is(matchErr, errSourceSnapshotChanged) {
			return nil, matchErr
		}
		if matchErr != nil || !matches {
			return nil, errors.New("collector/input: file identity changed while starting tailer")
		}
	}
	if checkpointGuardLength > 0 {
		checkpoint := Checkpoint{
			Offset:           start,
			GuardFingerprint: checkpointGuardFingerprint,
			GuardLength:      checkpointGuardLength,
		}
		matches, matchErr := persistedCheckpointGuardMatches(f, checkpoint)
		if matchErr != nil && !errors.Is(matchErr, errSourceSnapshotChanged) {
			return nil, matchErr
		}
		if matchErr != nil || !matches {
			return nil, errors.New("collector/input: checkpoint rewrite guard changed while starting tailer")
		}
	}
	t.installGuard(initialGuard)
	t.path.Store(&path)
	m.wg.Add(1)
	go t.run(ctx)
	return t, nil
}

// newFramer selects and constructs the framer for a file at the given offset.
func (m *manager) newFramer(r io.Reader, start, nextLine uint64) (framing.Framer, error) {
	options := m.cfg.Framing
	options.StartLineNumber = nextLine
	if m.cfg.Multiline {
		return framing.NewMultilineFramer(r, start, options)
	}
	return framing.NewLineFramer(r, start, options)
}

func (m *manager) identifyFile(f *os.File, info os.FileInfo) (FileIdentity, error) {
	if m.identityFn != nil {
		return m.identityFn(f, info, m.fpBytes)
	}
	return identityFor(f, info, m.fpBytes)
}

// timeFromNanos converts a stored UnixNano (0 = never) to a time.Time.
func timeFromNanos(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// tailer reads one physical file and emits RawEvents. One tailer goroutine owns
// its *os.File, cursor, identity, framing, and quiescence state. The discovery
// goroutine only updates the atomic path and coordinates retirement through
// retireMu/retireRequested before observing finished.
//
// Each active poll frames a bounded private snapshot and publishes it only
// after an exact reread proves every dependency byte is unchanged.
type tailer struct {
	m   *manager
	key string
	f   *os.File
	// runCtx is installed by run before any generation validation. A generation
	// fence waits on it so shutdown can always interrupt an unacknowledged
	// durability barrier.
	runCtx context.Context

	path atomic.Pointer[string]

	// Owned by the tailer goroutine (read only after it finishes): need no lock.
	id     FileIdentity
	offset uint64
	// nextLineNumber is the 1-based physical line beginning at offset. The
	// tailer carries it across one-shot framers built for successive polls.
	nextLineNumber uint64
	// lineCursorKnown controls whether the internal counter is safe to publish.
	// Legacy multiline checkpoints lack the ending physical line; zero-valued
	// origin fields honestly retain that unknown state until a new generation.
	lineCursorKnown bool
	// discardingOversize means offset is inside one oversized physical line.
	// Bytes are skipped incrementally until its delimiter arrives; framing must
	// not resume early and publish the suffix as a separate event.
	discardingOversize bool
	// discardingMultilineOversize skips complete continuation lines after an
	// oversized multiline event until the next configured start line. The flag
	// survives the one-shot framers used for bounded source snapshots.
	discardingMultilineOversize bool
	// discardingMultilinePartialLine distinguishes a bounded cut inside a
	// physical line from a complete-line oversize boundary. Until that line's
	// delimiter is consumed, a matching suffix must never be mistaken for a new
	// multiline start.
	discardingMultilinePartialLine bool

	// Growth tracking for multiline inactivity flushing.
	lastSize       uint64
	lastSizeChange time.Time

	// guard fingerprints a bounded window immediately before offset. It detects
	// copy-truncate-and-rewrite even when the replacement reaches the old size
	// before the next poll. Every guard read is independently capped at fpBytes;
	// a read-active poll uses two-phase capture and validation around the drain.
	guardOffset       uint64
	guardLength       uint64
	guardFingerprint  fingerprintDigest
	guardScratch      []byte
	validationScratch []byte
	// stagedWindow grows exponentially only when an artificial transaction
	// boundary cannot reach a frame boundary. Large productive batches retain
	// it; low-utilization and event-limited batches shrink it to the small base.
	stagedWindow uint64
	// rewritePending forces a generation reset retry after guard validation
	// detected a rewrite but the durable zero checkpoint could not be stored.
	rewritePending bool
	// emittedSinceFence records that this generation has handed at least one
	// event to the daemon since the last acknowledged durability fence. Before a
	// replacement generation can supersede its checkpoint, resetGeneration
	// serializes a barrier behind those events and waits for the daemon's durable
	// ingestion acknowledgment.
	emittedSinceFence bool
	generationBarrier *durabilityBarrier
	// readErrorActive avoids taking the manager's aggregate error-map lock on
	// every healthy poll. It is owned by the tailer goroutine.
	readErrorActive bool

	// missingDiscoveries is owned by the manager discovery goroutine. A single
	// miss is tolerated because glob and Stat do not form an atomic snapshot.
	missingDiscoveries uint8

	retireMu        sync.Mutex
	retireRequested atomic.Bool
	retireVersion   atomic.Uint64
	retireCommitted bool
	retireStable    uint8  // tailer goroutine only
	retireSize      uint64 // tailer goroutine only
	retireSeen      uint64 // tailer goroutine's observed retireVersion

	finished atomic.Bool
}

type tailerPollObservation struct {
	path       string
	offset     uint64
	generation uint64
}

// setPath updates the path the tailer reports in emitted SourceRefs after a
// rename keeps the same inode. Steady discovery polls retain the existing
// pointer and avoid an escaping string allocation.
func (t *tailer) setPath(p string) {
	current := t.path.Load()
	if current != nil && *current == p {
		return
	}
	t.path.Store(&p)
}

// pathStr returns the tailer's current path.
func (t *tailer) pathStr() string {
	if p := t.path.Load(); p != nil {
		return *p
	}
	return ""
}

func (t *tailer) setReadError(err error) {
	t.readErrorActive = true
	t.m.setReadError(t.key, t.pathStr(), err)
}

func (t *tailer) clearReadError() {
	if !t.readErrorActive {
		return
	}
	t.readErrorActive = false
	t.m.clearReadError(t.key)
}

// requestDrain begins a provisional retirement. It remains cancellable until
// the tailer commits a final validated snapshot, so rediscovery cannot split a
// buffered partial frame merely because one discovery snapshot was stale.
func (t *tailer) requestDrain() {
	t.retireMu.Lock()
	if !t.retireCommitted && !t.retireRequested.Load() {
		t.retireRequested.Store(true)
		t.retireVersion.Add(1)
	}
	t.retireMu.Unlock()
}

func (t *tailer) cancelDrain() bool {
	t.retireMu.Lock()
	defer t.retireMu.Unlock()
	if t.retireCommitted {
		return false
	}
	if t.retireRequested.Load() {
		t.retireRequested.Store(false)
		t.retireVersion.Add(1)
	}
	return true
}

func (t *tailer) observeAfterDrain(offset uint64) {
	if t.m.afterDrainObserver == nil {
		return
	}
	t.m.afterDrainObserver(tailerPollObservation{
		path:       t.pathStr(),
		offset:     offset,
		generation: t.id.Generation,
	})
}

func (m *manager) acquireStagedTransaction(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case m.stagedTransaction <- struct{}{}:
		if ctx.Err() != nil {
			m.releaseStagedTransaction()
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *manager) releaseStagedTransaction() {
	<-m.stagedTransaction
}

// tailerPollTimer owns the delay between one tailer's poll attempts.
type tailerPollTimer struct {
	interval time.Duration
	timer    *time.Timer
}

func (timer *tailerPollTimer) wait(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	if timer.timer == nil {
		timer.timer = time.NewTimer(timer.interval)
	} else {
		// The previous wait consumed its tick. Reset immediately before this
		// wait so slow file work cannot accumulate timer debt.
		timer.timer.Reset(timer.interval)
	}
	select {
	case <-ctx.Done():
		return false
	case <-timer.timer.C:
		return ctx.Err() == nil
	}
}

func (timer *tailerPollTimer) stop() {
	if timer.timer != nil {
		timer.timer.Stop()
	}
}

// canWaitAtCleanBoundary reports whether the last observed file size proves
// there is no work and no drain handoff to complete. A drain request must
// reframe even at an observed EOF because a writer can append after Stat.
func (t *tailer) canWaitAtCleanBoundary(size uint64) bool {
	if size != t.offset || t.retireRequested.Load() {
		return false
	}
	// An incomplete oversized multiline record may have consumed every byte
	// while deliberately leaving its physical-line cursor unresolved. Wake for
	// one empty validated snapshot after inactivity so later delimiter-free bytes
	// can be treated as a new line rather than a suffix of the rejected record.
	return !t.discardingMultilineOversize ||
		!t.discardingMultilinePartialLine ||
		!t.inactivityElapsed()
}

// run is the tailer goroutine.
func (t *tailer) run(ctx context.Context) {
	t.runCtx = ctx
	defer t.m.wg.Done()
	defer t.finished.Store(true)
	defer t.clearReadError()
	defer func() { _ = t.f.Close() }()

	t.m.active.Add(1)
	defer t.m.active.Add(-1)
	pollTimer := tailerPollTimer{interval: t.m.poll}
	defer pollTimer.stop()

	for {
		size, trackable := t.trackGrowthAndTruncate()
		if !trackable {
			t.retireStable = 0
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		if t.canWaitAtCleanBoundary(size) {
			t.retireStable = 0
			t.clearReadError()
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}

		if !t.m.acquireStagedTransaction(ctx) {
			return
		}
		// Another tailer may have held the manager permit for an arbitrary
		// interval. Refresh size and rewrite evidence before choosing this
		// transaction's immutable endpoint or inactivity behavior.
		size, trackable = t.trackGrowthAndTruncate()
		if !trackable {
			t.m.releaseStagedTransaction()
			t.retireStable = 0
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		if t.canWaitAtCleanBoundary(size) {
			t.m.releaseStagedTransaction()
			t.retireStable = 0
			t.clearReadError()
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		batch, err := t.stageRead(ctx, size, false)
		if errors.Is(err, context.Canceled) {
			t.m.releaseStagedTransaction()
			return
		}
		if err != nil {
			t.m.releaseStagedTransaction()
			t.retireStable = 0
			if t.handleSourceValidationFailure(false, err) {
				continue
			}
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		t.observeAfterDrain(batch.cursor.offset)
		if batch.stoppedAtArtificialBoundary() {
			grew := t.growStagedReadWindow()
			batch = nil
			t.m.releaseStagedTransaction()
			t.retireStable = 0
			if grew {
				continue
			}
			t.setReadError(
				fmt.Errorf(
					"staged read window %d cannot reach a frame boundary at offset %d",
					t.m.readWindow,
					t.offset,
				),
			)
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		matches, validationErr := t.stagedBatchMatches(batch)
		if validationErr != nil || !matches {
			batch = nil
			t.m.releaseStagedTransaction()
			t.retireStable = 0
			if t.handleSourceValidationFailure(matches, validationErr) {
				continue
			}
			if !pollTimer.wait(ctx) {
				return
			}
			continue
		}
		if !t.commitBatch(ctx, batch) {
			t.m.releaseStagedTransaction()
			return
		}
		t.clearReadError()
		reachedObservedEnd := batch.reachedObservedEnd()
		flushed := batch.flushed
		if t.offset > batch.start {
			t.tuneProductiveStagedReadWindow(batch)
		}
		batch = nil
		t.m.releaseStagedTransaction()
		if !reachedObservedEnd {
			t.retireStable = 0
			continue
		}

		if t.retireRequested.Load() {
			version := t.retireVersion.Load()
			if t.retireSeen != version {
				t.retireSeen = version
				t.retireStable = 0
			}
			if t.retireStable == 0 || t.retireSize != size {
				t.retireSize = size
				t.retireStable = 1
			} else {
				t.retireStable++
			}
			if t.retireStable >= 2 {
				done, retryNow := t.finalizeRetirement(ctx, size, version)
				if done {
					return
				}
				if retryNow {
					continue
				}
			}
		} else {
			t.retireStable = 0
		}
		if flushed {
			continue
		}
		if !pollTimer.wait(ctx) {
			return
		}
	}
}

// trackGrowthAndTruncate detects both an observable shrink and a replacement of
// bytes already consumed. The latter catches truncate-and-rewrite operations
// whose new file has already reached or exceeded the old offset by the poll.
func (t *tailer) trackGrowthAndTruncate() (size uint64, trackable bool) {
	fi, err := t.f.Stat()
	if err != nil {
		t.setReadError(err)
		return 0, false
	}
	if fi.Size() < 0 {
		t.setReadError(errors.New("file has a negative size"))
		return 0, false
	}
	// #nosec G115 -- the negative-size case is rejected above.
	size = uint64(fi.Size())
	changed := t.rewritePending || size < t.offset
	if !changed {
		guardMatches, guardErr := t.installedGuardMatches()
		if guardErr != nil && !errors.Is(guardErr, errSourceSnapshotChanged) {
			t.setReadError(guardErr)
			return 0, false
		}
		changed = guardErr != nil || !guardMatches
	}
	if changed {
		if err := t.resetGeneration(fi); err != nil {
			t.rewritePending = true
			t.setReadError(err)
			return 0, false
		}
	} else if t.offset == 0 && t.id.FingerprintLength == 0 && size > 0 {
		// A file discovered empty has no distinguishing prefix. Establish one
		// before its first event is emitted, retaining the same generation number.
		next, ierr := t.m.identifyFile(t.f, fi)
		if ierr != nil {
			t.setReadError(ierr)
			return 0, false
		}
		next.Generation = t.id.Generation
		if err := t.m.checkpoints.Set(Checkpoint{
			InputID: t.m.cfg.InputID, Identity: next, Path: t.pathStr(),
			Offset: 0, NextLineNumber: 1,
		}); err != nil {
			t.setReadError(err)
			return 0, false
		}
		t.id = next
	}
	if changed || size != t.lastSize {
		t.lastSize = size
		t.lastSizeChange = time.Now()
	}
	return size, true
}

func (t *tailer) resetGeneration(fi os.FileInfo) error {
	if err := t.awaitDurabilityBarrier(); err != nil {
		return err
	}
	next, err := t.m.identifyFile(t.f, fi)
	if err != nil {
		return err
	}
	next.Generation = t.id.Generation + 1
	if next.Generation == 0 {
		return errors.New("file generation exhausted")
	}
	if err := t.m.checkpoints.Set(Checkpoint{
		InputID: t.m.cfg.InputID, Identity: next, Path: t.pathStr(),
		Offset: 0, NextLineNumber: 1,
	}); err != nil {
		return err
	}
	t.id = next
	t.offset = 0
	t.nextLineNumber = 1
	t.lineCursorKnown = true
	t.discardingOversize = false
	t.discardingMultilineOversize = false
	t.discardingMultilinePartialLine = false
	t.guardOffset = 0
	t.guardLength = 0
	t.guardFingerprint = fingerprintDigest{}
	t.rewritePending = false
	t.emittedSinceFence = false
	t.generationBarrier = nil
	t.resetStagedReadWindow()
	return nil
}

// awaitDurabilityBarrier prevents a newly rewritten generation's zero
// checkpoint from overtaking old-generation events that the tailer has emitted
// but the daemon may still be decoding, processing, batching, or appending.
// FIFO ordering on Manager.Events and the daemon's processed channel means the
// acknowledgment covers every prior event from this tailer. Cancellation is a
// non-success result: an unacknowledged barrier must never permit the newer
// checkpoint to be stored.
func (t *tailer) awaitDurabilityBarrier() error {
	if !t.emittedSinceFence {
		return nil
	}
	if t.runCtx == nil {
		return errors.New("collector/input: durability barrier requires a running tailer")
	}
	if t.generationBarrier == nil {
		barrier := newDurabilityBarrier()
		select {
		case t.m.events <- RawEvent{barrier: barrier}:
			t.generationBarrier = barrier
		case <-t.runCtx.Done():
			return t.runCtx.Err()
		}
	}
	select {
	case <-t.generationBarrier.done:
		t.emittedSinceFence = false
		t.generationBarrier = nil
		return nil
	case <-t.runCtx.Done():
		return t.runCtx.Err()
	}
}

// resetCurrentGeneration records a rewrite against the file's latest state.
// It leaves rewritePending armed on failure so a zero-length prior guard cannot
// accidentally make a later retry accept the rewritten bytes as unchanged.
func (t *tailer) resetCurrentGeneration() bool {
	t.rewritePending = true
	fi, err := t.f.Stat()
	if err != nil {
		t.setReadError(err)
		return false
	}
	if fi.Size() < 0 {
		t.setReadError(errors.New("file has a negative size"))
		return false
	}
	if err := t.resetGeneration(fi); err != nil {
		t.setReadError(err)
		return false
	}
	// #nosec G115 -- the negative-size case is rejected above.
	t.lastSize = uint64(fi.Size())
	t.lastSizeChange = time.Now()
	return true
}

// handleSourceValidationFailure reports a failed snapshot check and resets the
// generation only when byte mismatch or a short exact read proves mutation.
// Callers must not hold retireMu: resetCurrentGeneration performs file and
// checkpoint I/O.
func (t *tailer) handleSourceValidationFailure(matches bool, err error) bool {
	if err != nil {
		t.setReadError(err)
	}
	changed := !matches &&
		(err == nil || errors.Is(err, errSourceSnapshotChanged))
	return changed && t.resetCurrentGeneration()
}

type tailerRewriteGuard struct {
	offset      uint64
	length      uint64
	fingerprint fingerprintDigest
}

// refreshGuard fingerprints the trailing part of the consumed prefix. It is
// called only by the tailer goroutine, after offset changes.
func (t *tailer) refreshGuard() error {
	guard, err := t.captureGuard(t.offset)
	if err != nil {
		return err
	}
	t.installGuard(guard)
	return nil
}

func (t *tailer) captureGuard(end uint64) (tailerRewriteGuard, error) {
	length := end
	// validatedFingerprintBytes guarantees a positive bounded int at construction.
	// #nosec G115 -- positive int values are exactly representable as uint64.
	if maximum := uint64(t.m.fpBytes); length > maximum {
		length = maximum
	}
	guard := tailerRewriteGuard{offset: end - length, length: length}
	if length == 0 {
		return guard, nil
	}
	fp, err := t.readFingerprintRange(guard.offset, guard.length)
	if err != nil {
		return tailerRewriteGuard{}, err
	}
	guard.fingerprint = fp
	return guard, nil
}

func (t *tailer) installGuard(guard tailerRewriteGuard) {
	t.guardOffset = guard.offset
	t.guardLength = guard.length
	t.guardFingerprint = guard.fingerprint
}

func (t *tailer) readGuardFingerprint() (fingerprintDigest, error) {
	return t.readFingerprintRange(t.guardOffset, t.guardLength)
}

func (t *tailer) installedGuardMatches() (bool, error) {
	if t.guardLength > 0 {
		fingerprint, err := t.readGuardFingerprint()
		if err != nil {
			return false, err
		}
		return fingerprint == t.guardFingerprint, nil
	}
	if t.id.FingerprintLength > 0 {
		return persistedPrefixMatches(t.f, t.id)
	}
	return true, nil
}

func (t *tailer) readFingerprintRange(
	offset uint64,
	lengthUint64 uint64,
) (fingerprintDigest, error) {
	if lengthUint64 > maximumFingerprintBytes {
		return fingerprintDigest{}, fmt.Errorf(
			"fingerprint guard length %d exceeds absolute maximum %d",
			lengthUint64,
			maximumFingerprintBytes,
		)
	}
	// #nosec G115 -- lengthUint64 is bounded by maximumFingerprintBytes above.
	length := int(lengthUint64)
	t.guardScratch = slices.Grow(t.guardScratch[:0], length)[:length]
	guardOffset, err := checkedFileOffset(offset)
	if err != nil {
		return fingerprintDigest{}, err
	}
	// #nosec G115 -- lengthUint64 is bounded by maximumFingerprintBytes above.
	lengthUint32 := uint32(lengthUint64)
	digest, err := computeFingerprintRangeDigest(
		t.f,
		guardOffset,
		lengthUint32,
		t.guardScratch,
	)
	if err != nil {
		return fingerprintDigest{}, classifyExactReadError(err)
	}
	return digest, nil
}

func checkedFileOffset(offset uint64) (int64, error) {
	if offset > math.MaxInt64 {
		return 0, fmt.Errorf("file offset %d exceeds int64", offset)
	}
	// #nosec G115 -- offset is explicitly bounded by math.MaxInt64 above.
	return int64(offset), nil
}

// shouldFlushInactive reports whether a buffered multiline partial has sat
// unchanged (the file stopped growing) for at least Config.FlushAfter.
func (t *tailer) shouldFlushInactive(pendingLen int) bool {
	return pendingLen > 0 && t.inactivityElapsed()
}

func (t *tailer) inactivityElapsed() bool {
	return t.m.cfg.Multiline && t.m.cfg.FlushAfter > 0 &&
		time.Since(t.lastSizeChange) >= t.m.cfg.FlushAfter
}
