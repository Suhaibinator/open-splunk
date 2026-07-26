package input

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
)

// defaultPollInterval is used when Config.PollInterval is unset.
const defaultPollInterval = 250 * time.Millisecond

// manager is the concrete Manager for one file input. It is poll-driven: every
// PollInterval it re-globs, starts a per-file tailer goroutine for newly seen
// files, and asks tailers whose file left the discovery set to drain and stop.
type manager struct {
	cfg         Config
	checkpoints CheckpointStore
	fpBytes     int
	poll        time.Duration

	events chan RawEvent

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

	runOnce sync.Once
}

// NewManager constructs a file input Manager. checkpoints supplies resume
// offsets at discovery time; the Manager never advances their durable byte
// positions, though it may enrich a legacy checkpoint with a stable
// compatibility cursor in one batched discovery write.
func NewManager(cfg Config, checkpoints CheckpointStore) (Manager, error) {
	if len(cfg.Include) == 0 {
		return nil, errors.New("collector/input: at least one include glob is required")
	}
	if checkpoints == nil {
		return nil, errors.New("collector/input: checkpoint store is required")
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	m := &manager{
		cfg:         cfg,
		checkpoints: checkpoints,
		fpBytes:     fingerprintBytesOr(cfg.FingerprintBytes),
		poll:        poll,
		events:      make(chan RawEvent),
		tailers:     make(map[string]*tailer),
		state:       opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_STARTING,
	}
	return m, nil
}

// Events returns the channel of framed raw events; closed when Run returns.
func (m *manager) Events() <-chan RawEvent { return m.events }

// Run blocks tailing until ctx is cancelled. It never returns for a per-file
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
	return Health{
		InputID:           m.cfg.InputID,
		State:             state,
		StatusMessage:     status,
		DiscoveredSources: m.discovered.Load(),
		ActiveSources:     uint64(max64(m.active.Load(), 0)),
		EventsReadTotal:   m.eventsRead.Load(),
		BytesReadTotal:    m.bytesRead.Load(),
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
	m.discovered.Store(uint64(len(paths)))

	seen := make(map[string]struct{}, len(paths))
	var legacyUpgrades []Checkpoint
	var openErr string

	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil || !fi.Mode().IsRegular() {
			continue // vanished between glob and stat, or not a regular file
		}
		dev, ino, haveID := statDevIno(fi)
		if haveID {
			key := fmt.Sprintf("dev=%d;ino=%d", dev, ino)
			if t, ok := m.tailers[key]; ok {
				seen[key] = struct{}{}
				t.setPath(p) // rename continuity: same inode, new path, keep offset
				continue
			}
		}

		// New file (or a platform without stable inode): open it, fingerprint,
		// and start a tailer.
		f, err := os.Open(p)
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
		id, err := identityFor(f, fi2, m.fpBytes)
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
		if _, ok := m.tailers[key]; ok {
			// Race: another path with the same inode created it this cycle.
			_ = f.Close()
			seen[key] = struct{}{}
			continue
		}

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
		t, err := m.startTailer(
			ctx,
			key,
			p,
			f,
			start.identity,
			start.offset,
			start.nextLine,
			start.lineCursorKnown,
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
	if err := m.checkpoints.SetMany(legacyUpgrades); err != nil {
		openErr = fmt.Sprintf("upgrade legacy checkpoints: %v", err)
		m.lastErrorNs.Store(time.Now().UnixNano())
	}

	// Reap finished tailers; ask tailers whose file left the set to drain.
	for key, t := range m.tailers {
		if t.finished.Load() {
			delete(m.tailers, key)
			continue
		}
		if _, ok := seen[key]; !ok {
			t.requestDrain()
		}
	}

	m.updateState(len(paths), openErr)
}

type resolvedStart struct {
	identity        FileIdentity
	offset          uint64
	nextLine        uint64
	lineCursorKnown bool
	legacyUpgrade   *Checkpoint
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
	cp, ok, err := m.checkpoints.Get(id)
	if err != nil {
		return resolvedStart{}, err
	}
	if ok {
		sameGeneration := cp.Offset <= size && persistedPrefixMatches(f, cp.Identity)
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
			return resolvedStart{
				identity:        cp.Identity,
				offset:          cp.Offset,
				nextLine:        nextLine,
				lineCursorKnown: lineCursorKnown,
				legacyUpgrade:   legacyUpgrade,
			}, nil
		}
		// The same inode was truncated/reused. Burn a new generation before
		// offsets restart so identical bytes at identical offsets remain distinct.
		id.Generation = cp.Identity.Generation + 1
		if id.Generation == 0 { // uint overflow is practically impossible, fail safe
			return resolvedStart{}, errors.New("file generation exhausted")
		}
		if err := m.checkpoints.Set(Checkpoint{
			Identity: id, Path: path, Offset: 0, NextLineNumber: 1,
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
	if err := m.checkpoints.Set(Checkpoint{
		Identity: id, Path: path, Offset: start, NextLineNumber: nextLine,
	}); err != nil {
		return resolvedStart{}, err
	}
	return resolvedStart{
		identity: id, offset: start, nextLine: nextLine, lineCursorKnown: true,
	}, nil
}

func checkpointNextLine(
	checkpoint Checkpoint,
	multiline bool,
) (nextLine uint64, known bool, err error) {
	if checkpoint.NextLineNumber != 0 {
		if checkpoint.NextLineNumber == ^uint64(0) ||
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
func persistedPrefixMatches(f *os.File, id FileIdentity) bool {
	if id.FingerprintLength == 0 {
		// Zero-length fingerprints are valid for files discovered empty and for
		// checkpoints written by the format predating FingerprintLength.
		return true
	}
	fp, err := computeFingerprintRange(f, 0, id.FingerprintLength)
	return err == nil && fp == id.Fingerprint
}

// updateState recomputes the aggregate health state after a poll.
func (m *manager) updateState(discovered int, openErr string) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	switch {
	case discovered == 0:
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_MISSING
		m.status = fmt.Sprintf("no files match include globs %v", m.cfg.Include)
	case openErr != "":
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_UNREADABLE
		m.status = openErr
	default:
		m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_HEALTHY
		m.status = ""
	}
}

// setReadError records an asynchronous per-file read failure into Health.
func (m *manager) setReadError(path string, err error) {
	m.lastErrorNs.Store(time.Now().UnixNano())
	m.stateMu.Lock()
	m.state = opensplunkv1.CollectorInputState_COLLECTOR_INPUT_STATE_ERROR
	m.status = fmt.Sprintf("read %s: %v", path, err)
	m.stateMu.Unlock()
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
	sortStrings(out)
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
// tailer owns f from here on and reframes it from its current offset on every
// poll (the framing package caches EOF, so a fresh framer per cycle is what lets
// appended bytes be seen).
func (m *manager) startTailer(
	ctx context.Context,
	key, path string,
	f *os.File,
	id FileIdentity,
	start, nextLine uint64,
	lineCursorKnown bool,
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
	if err := t.refreshGuard(); err != nil {
		return nil, err
	}
	t.path.Store(&path)
	m.wg.Add(1)
	go t.run(ctx)
	return t, nil
}

// newFramer selects and constructs the framer for a file at the given offset.
func (m *manager) newFramer(f *os.File, start, nextLine uint64) (framing.Framer, error) {
	options := m.cfg.Framing
	options.StartLineNumber = nextLine
	if m.cfg.Multiline {
		return framing.NewMultilineFramer(f, start, options)
	}
	return framing.NewLineFramer(f, start, options)
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

// sortStrings is an allocation-light insertion sort adequate for the small path
// sets a single input produces; it avoids pulling sort into the hot poll path's
// import surface beyond what checkpoint.go already needs.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// tailer reads one physical file and emits RawEvents. One tailer goroutine owns
// its *os.File, offset, and identity; the manager only touches the atomic path
// pointer, drainReq, and finished flags.
//
// Because the framing package caches EOF (once its reader reports io.EOF it will
// not re-read), the tailer builds a fresh framer over the file on every poll,
// seeked to the last durable offset. This is what makes appended bytes visible
// and keeps the tailer decoupled from the framer's one-shot lifecycle.
type tailer struct {
	m   *manager
	key string
	f   *os.File

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

	// Growth tracking for multiline inactivity flushing.
	lastSize       uint64
	lastSizeChange time.Time

	// guard fingerprints a bounded window immediately before offset. It detects
	// copy-truncate-and-rewrite even when the replacement reaches the old size
	// before the next poll, while costing at most fpBytes of IO per poll.
	guardOffset      uint64
	guardLength      uint32
	guardFingerprint string

	drainReq atomic.Bool
	finished atomic.Bool
}

// setPath updates the path the tailer reports in emitted SourceRefs after a
// rename keeps the same inode.
func (t *tailer) setPath(p string) { t.path.Store(&p) }

// pathStr returns the tailer's current path.
func (t *tailer) pathStr() string {
	if p := t.path.Load(); p != nil {
		return *p
	}
	return ""
}

// requestDrain asks the tailer to drain any remaining bytes and exit. Used when
// the file left the discovery set (deletion or rotation out of the glob).
func (t *tailer) requestDrain() { t.drainReq.Store(true) }

// run is the tailer goroutine.
func (t *tailer) run(ctx context.Context) {
	defer t.m.wg.Done()
	defer t.finished.Store(true)
	defer func() { _ = t.f.Close() }()

	t.m.active.Add(1)
	defer t.m.active.Add(-1)

	for {
		if !t.trackGrowthAndTruncate() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(t.m.poll):
				continue
			}
		}

		if t.discardingOversize {
			complete, err := t.discardOversizedRemainder(ctx)
			if errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				t.m.setReadError(t.pathStr(), err)
			}
			if !complete {
				if refreshErr := t.refreshGuard(); refreshErr != nil {
					t.m.setReadError(t.pathStr(), refreshErr)
				}
				if t.drainReq.Load() {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(t.m.poll):
					continue
				}
			}
		}

		fr, err := t.reframe()
		if err != nil {
			t.m.setReadError(t.pathStr(), err)
		} else {
			pendingLen, cont := t.drain(ctx, fr)
			if !cont {
				return // ctx cancelled mid-emit
			}
			if err := t.refreshGuard(); err != nil {
				t.m.setReadError(t.pathStr(), err)
			}
			if t.drainReq.Load() {
				// File left the discovery set: emit any buffered remainder (a
				// trailing partial line, or an unterminated multiline event) so a
				// deleted/rotated file loses nothing, then stop tracking it.
				if pendingLen > 0 {
					t.emitFlush(ctx, fr)
				}
				return
			}
			if t.shouldFlushInactive(pendingLen) {
				if _, ctxOK := t.emitFlush(ctx, fr); !ctxOK {
					return
				}
				continue // recheck immediately in case more is now readable
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(t.m.poll):
		}
	}
}

// trackGrowthAndTruncate detects both an observable shrink and a replacement of
// bytes already consumed. The latter catches truncate-and-rewrite operations
// whose new file has already reached or exceeded the old offset by the poll.
func (t *tailer) trackGrowthAndTruncate() bool {
	fi, err := t.f.Stat()
	if err != nil {
		t.m.setReadError(t.pathStr(), err)
		return false
	}
	size := uint64(fi.Size())
	changed := size < t.offset
	if !changed && t.guardLength > 0 {
		fp, ferr := computeFingerprintRange(t.f, int64(t.guardOffset), t.guardLength)
		changed = ferr != nil || fp != t.guardFingerprint
	}
	if changed {
		next, ierr := identityFor(t.f, fi, t.m.fpBytes)
		if ierr != nil {
			t.m.setReadError(t.pathStr(), ierr)
			return false
		}
		next.Generation = t.id.Generation + 1
		if next.Generation == 0 {
			t.m.setReadError(t.pathStr(), errors.New("file generation exhausted"))
			return false
		}
		if err := t.m.checkpoints.Set(Checkpoint{
			Identity: next, Path: t.pathStr(), Offset: 0, NextLineNumber: 1,
		}); err != nil {
			t.m.setReadError(t.pathStr(), err)
			return false
		}
		t.id = next
		t.offset = 0
		t.nextLineNumber = 1
		t.lineCursorKnown = true
		t.discardingOversize = false
		t.guardOffset = 0
		t.guardLength = 0
		t.guardFingerprint = ""
	} else if t.offset == 0 && t.id.FingerprintLength == 0 && size > 0 {
		// A file discovered empty has no distinguishing prefix. Establish one
		// before its first event is emitted, retaining the same generation number.
		next, ierr := identityFor(t.f, fi, t.m.fpBytes)
		if ierr == nil {
			next.Generation = t.id.Generation
			if err := t.m.checkpoints.Set(Checkpoint{
				Identity: next, Path: t.pathStr(), Offset: 0, NextLineNumber: 1,
			}); err == nil {
				t.id = next
			}
		}
	}
	if size != t.lastSize {
		t.lastSize = size
		t.lastSizeChange = time.Now()
	}
	return true
}

// refreshGuard fingerprints the trailing part of the consumed prefix. It is
// called only by the tailer goroutine, after offset changes.
func (t *tailer) refreshGuard() error {
	length := t.offset
	if max := uint64(t.m.fpBytes); length > max {
		length = max
	}
	t.guardLength = uint32(length)
	t.guardOffset = t.offset - length
	if length == 0 {
		t.guardFingerprint = ""
		return nil
	}
	fp, err := computeFingerprintRange(t.f, int64(t.guardOffset), t.guardLength)
	if err != nil {
		return err
	}
	t.guardFingerprint = fp
	return nil
}

// reframe seeks the file to the current offset and returns a fresh framer whose
// offsets are seeded there.
func (t *tailer) reframe() (framing.Framer, error) {
	if _, err := t.f.Seek(int64(t.offset), io.SeekStart); err != nil {
		return nil, err
	}
	return t.m.newFramer(t.f, t.offset, t.nextLineNumber)
}

// discardOversizedRemainder advances through bytes belonging to an oversized
// physical line without buffering them. It returns complete only after the
// delimiter is consumed, at which point framing may safely resume. This state
// is in-memory only: after a crash the older durable checkpoint replays from a
// true event boundary and reconstructs the same skip.
func (t *tailer) discardOversizedRemainder(ctx context.Context) (bool, error) {
	if _, err := t.f.Seek(int64(t.offset), io.SeekStart); err != nil {
		return false, err
	}
	var buffer [32 << 10]byte
	for {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		n, err := t.f.Read(buffer[:])
		if n > 0 {
			if delimiter := bytes.IndexByte(buffer[:n], '\n'); delimiter >= 0 {
				if t.nextLineNumber >= ^uint64(0)-1 {
					return false, framing.ErrLineNumberOverflow
				}
				t.offset += uint64(delimiter + 1)
				t.nextLineNumber++
				t.discardingOversize = false
				return true, nil
			}
			t.offset += uint64(n)
		}
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, io.ErrNoProgress
		}
	}
}

// drain reads every complete frame currently available from fr and emits each.
// It returns the length of any buffered partial record (from Pending) and false
// only when a ctx cancellation interrupted an emit.
func (t *tailer) drain(ctx context.Context, fr framing.Framer) (pendingLen int, cont bool) {
	for {
		frame, err := fr.Next()
		switch {
		case err == nil:
			if !t.emit(ctx, frame) {
				return 0, false
			}
		case errors.Is(err, framing.ErrEventTooLargeIncomplete):
			// The current EOF is not a record boundary. Retain an explicit
			// discard state so later suffix bytes cannot become a new event.
			t.offset = frame.EndOffset
			t.nextLineNumber = frame.NextLineNumber
			t.discardingOversize = true
			t.m.lastErrorNs.Store(time.Now().UnixNano())
			return 0, true
		case errors.Is(err, framing.ErrEventTooLarge):
			// Oversized event: skip it but advance past it so we do not stall.
			t.offset = frame.EndOffset
			t.nextLineNumber = frame.NextLineNumber
			t.m.lastErrorNs.Store(time.Now().UnixNano())
		case errors.Is(err, framing.ErrPartialFrame):
			_, length := fr.Pending()
			return length, true
		case errors.Is(err, io.EOF):
			return 0, true
		default:
			t.m.setReadError(t.pathStr(), err)
			return 0, true
		}
	}
}

// shouldFlushInactive reports whether a buffered multiline partial has sat
// unchanged (the file stopped growing) for at least Config.FlushAfter.
func (t *tailer) shouldFlushInactive(pendingLen int) bool {
	if !t.m.cfg.Multiline || t.m.cfg.FlushAfter <= 0 || pendingLen == 0 {
		return false
	}
	return time.Since(t.lastSizeChange) >= t.m.cfg.FlushAfter
}

// emitFlush force-emits fr's buffered partial record.
// It returns whether a frame was emitted and whether ctx is still live.
func (t *tailer) emitFlush(ctx context.Context, fr framing.Framer) (emitted, ctxOK bool) {
	frame, has := fr.Flush()
	if !has {
		return false, true
	}
	if !t.emit(ctx, frame) {
		return false, false
	}
	return true, true
}

// emit copies the frame bytes (the receiver owns them), tags the event with the
// tailer's current path and identity, and sends it. It returns false only when
// ctx is cancelled before the send completes.
func (t *tailer) emit(ctx context.Context, fr framing.Frame) bool {
	b := make([]byte, len(fr.Bytes))
	copy(b, fr.Bytes)
	lineNumber, nextLineNumber := fr.LineNumber, fr.NextLineNumber
	if !t.lineCursorKnown {
		lineNumber, nextLineNumber = 0, 0
	}
	ev := RawEvent{
		Bytes: b,
		Source: SourceRef{
			Path:           t.pathStr(),
			Identity:       t.id,
			StartOffset:    fr.StartOffset,
			EndOffset:      fr.EndOffset,
			LineNumber:     lineNumber,
			NextLineNumber: nextLineNumber,
		},
	}
	select {
	case t.m.events <- ev:
	case <-ctx.Done():
		return false
	}
	t.offset = fr.EndOffset
	t.nextLineNumber = fr.NextLineNumber
	t.m.eventsRead.Add(1)
	t.m.bytesRead.Add(uint64(len(b)))
	t.m.lastEventNs.Store(time.Now().UnixNano())
	return true
}
