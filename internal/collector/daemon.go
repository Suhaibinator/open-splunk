package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/collector/config"
	"github.com/Suhaibinator/open-splunk/internal/collector/framing"
	"github.com/Suhaibinator/open-splunk/internal/collector/input"
	"github.com/Suhaibinator/open-splunk/internal/collector/sender"
	"github.com/Suhaibinator/open-splunk/internal/collector/wal"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// collectorVersion is advertised in the CollectorHello. It is a build-time
// identity string, not a protocol version (see protocolMajor/protocolMinor).
const collectorVersion = "0.1.0-dev"

// Wire-protocol version the collector speaks; must match the server's expected
// major (see internal/ingest DefaultConfig). Frozen with the protobuf contract.
const (
	protocolMajor uint32 = 1
	protocolMinor uint32 = 0
)

// Batching, backpressure, and shutdown defaults. Batching is by the COMMON
// rules: flush a pending batch when it reaches defaultBatchMaxEvents events, or
// defaultBatchMaxBytes of encoded event bytes, or after defaultBatchLinger of
// inactivity since the first event of the batch. None of these are YAML config
// surface; they are tunable in-process (e.g. by tests) via the Daemon fields.
const (
	defaultBatchMaxEvents     = 500
	defaultBatchMaxBytes      = 1 << 20 // 1 MiB
	defaultBatchLinger        = 250 * time.Millisecond
	defaultQueueFullRetry     = 100 * time.Millisecond
	defaultShutdownFlushGrace = 5 * time.Second
	defaultDrainWindow        = 10 * time.Second
	defaultSegmentMaxBytes    = 64 << 20 // 64 MiB
)

// State-directory layout. Everything the daemon persists lives under
// cfg.State.Directory so a single directory is the collector's durable state.
const (
	walSubdir         = "wal"
	checkpointsSubdir = "checkpoints"
	deadLetterFile    = "dead-letter.jsonl"
	collectorIDFile   = "collector_id"
)

// Daemon orchestrates the collector: it builds a decoder, framer, and processor
// pipeline per input, tails files, decodes and processes events, appends them to
// the durable queue, and runs the sender that delivers batches to the server.
//
// It is the only type that depends on config, framing, input, wal, and sender;
// those packages do not depend on one another except input -> framing and
// sender -> wal, and none depends on this package, so the graph is acyclic.
//
// # Event flow
//
// One goroutine per input reads [input.RawEvent]s, maps each SourceRef to a
// [SourcePosition], decodes it, runs the shared processor [Pipeline], and hands
// the result to a single batcher goroutine over an unbuffered channel. The
// batcher accumulates events and flushes them to [wal.Queue.Append]; after a
// flush returns durably it advances the per-file checkpoint to the highest
// EndOffset the batch covered. The unbuffered channel plus a single batcher is
// what makes backpressure free: when Append reports [wal.ErrQueueFull] the
// batcher stops draining the channel, which stalls the input readers, which
// stalls the tailers — no event is ever dropped, and the source files persist.
//
// # Decode-failure policy
//
// A record that fails to decode (malformed JSON, an out-of-range timestamp, an
// oversized line) is neither fatal nor silently dropped: it is logged at Warn
// with the input id and durable source position, counted (see
// [Daemon.DecodeFailures]), and skipped. The raw payload is never logged — only
// its byte length — because a source line may carry secret material. A skipped
// record does not advance a checkpoint by itself; its position is covered when a
// later event from the same file is durably appended, or it is re-read (and
// re-skipped, idempotently) after a restart.
type Daemon struct {
	cfg *config.Config
	log *slog.Logger
	now func() time.Time

	collectorID string
	instanceID  string

	checkpoints input.CheckpointStore
	queue       wal.Queue
	deadLetter  sender.DeadLetterSink
	stateLock   *stateDirectoryLock
	sender      *sender.Sender
	inputs      []*inputRuntime
	pipeline    *Pipeline

	// Batching / backpressure / shutdown tunables. Defaulted in New; overridable
	// in-process (tests) before Run. Not exposed as YAML configuration.
	batchMaxEvents     int
	batchLinger        time.Duration
	batchMaxBytes      int
	queueFullRetry     time.Duration
	shutdownFlushGrace time.Duration
	drainWindow        time.Duration

	// lastOffsets guards checkpoint monotonicity per file identity. Only the
	// single batcher goroutine touches it, so it needs no lock.
	lastOffsets map[string]uint64

	decodeFailures atomic.Uint64

	// oversizedDrops counts single events whose durable record exceeds
	// max_queue_bytes and were therefore dead-lettered rather than queued.
	oversizedDrops atomic.Uint64
}

// inputRuntime bundles one input's tailer with the decoder built from its
// InputConfig and the shared processor pipeline.
type inputRuntime struct {
	id       string
	manager  input.Manager
	decoder  *Decoder
	pipeline *Pipeline
}

// Option customizes Daemon construction (logger, identity overrides for tests).
type Option func(*daemonOptions)

// daemonOptions holds resolved construction options.
type daemonOptions struct {
	// framing carries any framer defaults the daemon applies per input; it keeps
	// the framing dependency explicit at the orchestration layer.
	framing framing.Options

	logger      *slog.Logger
	collectorID string
	instanceID  string
}

// WithLogger sets the structured logger used for daemon diagnostics. The logger
// is also handed to the sender; it must never be given the bearer token.
func WithLogger(logger *slog.Logger) Option {
	return func(o *daemonOptions) { o.logger = logger }
}

// WithCollectorID overrides the persisted, stable collector ID. Intended for
// tests; production derives a stable ID from the state directory.
func WithCollectorID(id string) Option {
	return func(o *daemonOptions) { o.collectorID = id }
}

// WithInstanceID overrides the per-process instance ID. Intended for tests;
// production mints a fresh UUID per process.
func WithInstanceID(id string) Option {
	return func(o *daemonOptions) { o.instanceID = id }
}

// New constructs a Daemon from cfg. It validates cfg, opens the checkpoint store
// and durable queue under cfg.State.Directory, builds one input Manager plus a
// decoder per configured input, compiles the processor pipeline, and constructs
// the sender from cfg.Server. It does not start anything; call Run for that.
func New(cfg *config.Config, opts ...Option) (*Daemon, error) {
	if cfg == nil {
		return nil, errors.New("collector: config is required")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("collector: invalid config: %w", err)
	}

	var o daemonOptions
	for _, opt := range opts {
		opt(&o)
	}
	logger := o.logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	stateDir := cfg.State.Directory
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("collector: create state directory %q: %w", stateDir, err)
	}
	// The state directory holds raw payloads (WAL segments, dead-letter file), so
	// tighten it to owner-only even if it pre-existed with looser permissions. A
	// chmod failure is not fatal: log and continue.
	if err := os.Chmod(stateDir, 0o700); err != nil {
		logger.Warn("collector: could not tighten state directory permissions to 0700",
			"directory", stateDir, "error", err.Error())
	}
	// Plaintext transport sends the bearer token in cleartext; warn whenever TLS
	// is disabled, even for loopback (config.Validate gates non-loopback use).
	if !cfg.Server.TLS.Enabled {
		logger.Warn("collector: TLS is disabled; the bearer token is sent to the server in cleartext",
			"address", cfg.Server.Address)
	}
	stateLock, err := acquireStateDirectoryLock(stateDir)
	if err != nil {
		return nil, err
	}

	collectorID := o.collectorID
	if collectorID == "" {
		id, err := loadOrCreateCollectorID(stateDir)
		if err != nil {
			_ = stateLock.Close()
			return nil, err
		}
		collectorID = id
	}
	instanceID := o.instanceID
	if instanceID == "" {
		instanceID = uuid.NewString()
	}

	checkpoints, err := input.NewCheckpointStore(filepath.Join(stateDir, checkpointsSubdir))
	if err != nil {
		_ = stateLock.Close()
		return nil, fmt.Errorf("collector: open checkpoint store: %w", err)
	}

	queue, err := wal.Open(wal.Options{
		Dir:             filepath.Join(stateDir, walSubdir),
		MaxQueueBytes:   uint64(cfg.State.MaxQueueBytes),
		SegmentMaxBytes: defaultSegmentMaxBytes,
		Sync:            wal.SyncAlways,
		CollectorID:     collectorID,
		ProtocolMajor:   protocolMajor,
		ProtocolMinor:   protocolMinor,
	})
	if err != nil {
		_ = checkpoints.Close()
		_ = stateLock.Close()
		return nil, fmt.Errorf("collector: open durable queue: %w", err)
	}

	// From here on, failures must release the queue and checkpoint store.
	fail := func(err error) (*Daemon, error) {
		_ = queue.Close()
		_ = checkpoints.Close()
		_ = stateLock.Close()
		return nil, err
	}

	pipeline, err := buildPipeline(cfg.Processors)
	if err != nil {
		return fail(fmt.Errorf("collector: build processors: %w", err))
	}

	hostname, herr := os.Hostname()
	if herr != nil || strings.TrimSpace(hostname) == "" {
		hostname = "unknown-host"
	}

	var (
		inputs        []*inputRuntime
		registrations []*opensplunkv1.CollectorInputRegistration
		anyMultiline  bool
	)
	for i := range cfg.Inputs {
		ir, reg, multi, berr := buildInput(&cfg.Inputs[i], hostname, checkpoints)
		if berr != nil {
			return fail(fmt.Errorf("collector: %w", berr))
		}
		ir.pipeline = pipeline
		inputs = append(inputs, ir)
		registrations = append(registrations, reg)
		anyMultiline = anyMultiline || multi
	}

	deadLetter, err := sender.NewFileDeadLetterSink(filepath.Join(stateDir, deadLetterFile))
	if err != nil {
		return fail(fmt.Errorf("collector: open dead-letter sink: %w", err))
	}

	senderOpts := sender.Options{
		Address:     cfg.Server.Address,
		TLS:         sender.TLSConfig{Enabled: cfg.Server.TLS.Enabled, CAFile: cfg.Server.TLS.CAFile, ServerName: cfg.Server.TLS.ServerName},
		Token:       tokenLoader(cfg.Server.TokenFile),
		Compression: cfg.Server.Compression,

		CollectorID:   collectorID,
		InstanceID:    instanceID,
		ProtocolMajor: protocolMajor,
		ProtocolMinor: protocolMinor,
		Hello: sender.HelloInfo{
			CollectorVersion: collectorVersion,
			Hostname:         hostname,
			OperatingSystem:  runtime.GOOS,
			Architecture:     runtime.GOARCH,
			StartedAt:        time.Now().UTC(),
			Capabilities:     capabilities(cfg, anyMultiline),
			Inputs:           registrations,
		},

		DialTimeout: 10 * time.Second,
		Backoff:     sender.BackoffPolicy{Initial: time.Second, Max: 30 * time.Second, Multiplier: 2, Jitter: 0.2},
		Logger:      logger,
		InputHealth: func() []*opensplunkv1.CollectorInputHealth { return inputHealthSnapshot(inputs) },
	}

	snd, err := sender.New(senderOpts, queue, deadLetter, nil)
	if err != nil {
		_ = deadLetter.Close()
		return fail(fmt.Errorf("collector: build sender: %w", err))
	}

	d := &Daemon{
		cfg:                cfg,
		log:                logger,
		now:                time.Now,
		collectorID:        collectorID,
		instanceID:         instanceID,
		checkpoints:        checkpoints,
		queue:              queue,
		deadLetter:         deadLetter,
		stateLock:          stateLock,
		sender:             snd,
		inputs:             inputs,
		pipeline:           pipeline,
		batchMaxEvents:     defaultBatchMaxEvents,
		batchLinger:        defaultBatchLinger,
		batchMaxBytes:      defaultBatchMaxBytes,
		queueFullRetry:     defaultQueueFullRetry,
		shutdownFlushGrace: defaultShutdownFlushGrace,
		drainWindow:        defaultDrainWindow,
		lastOffsets:        make(map[string]uint64),
	}
	return d, nil
}

// DecodeFailures returns the running count of records skipped because they could
// not be decoded. It is monotonic for the life of the process.
func (d *Daemon) DecodeFailures() uint64 { return d.decodeFailures.Load() }

// OversizedDrops returns the running count of single events dead-lettered
// because their durable record exceeded max_queue_bytes. Monotonic per process.
func (d *Daemon) OversizedDrops() uint64 { return d.oversizedDrops.Load() }

// Run starts every input, the decode/process/append pipeline, and the sender,
// blocking until ctx is cancelled and shutdown completes. It returns nil on a
// clean context-cancellation shutdown, or the first non-context error observed
// (for example a fatal sender dial error or a resource-close error).
func (d *Daemon) Run(ctx context.Context) error {
	// pipeCtx drives the inputs and readers so we can tear the pipeline down
	// independently of the sender (either on external cancel or a sender fault).
	pipeCtx, cancelPipe := context.WithCancel(ctx)
	defer cancelPipe()
	// senderCtx is separate so the sender keeps delivering during the bounded
	// drain window after the pipeline has stopped producing.
	senderCtx, cancelSender := context.WithCancel(context.Background())
	defer cancelSender()

	senderErrCh := make(chan error, 1)
	go func() { senderErrCh <- d.sender.Run(senderCtx) }()

	var inputWG sync.WaitGroup
	for _, ir := range d.inputs {
		inputWG.Add(1)
		go func(m input.Manager) {
			defer inputWG.Done()
			if err := m.Run(pipeCtx); err != nil {
				d.log.Error("collector: input manager stopped", "error", err.Error())
			}
		}(ir.manager)
	}

	processed := make(chan processedEvent)
	var readWG sync.WaitGroup
	for _, ir := range d.inputs {
		readWG.Add(1)
		go func(ir *inputRuntime) {
			defer readWG.Done()
			d.readInput(pipeCtx, ir, processed)
		}(ir)
	}
	go func() {
		readWG.Wait()
		close(processed)
	}()

	batcherErrCh := make(chan error, 1)
	go func() {
		batcherErrCh <- d.runBatcher(pipeCtx, processed)
	}()

	// Supervise: shut down when the caller cancels, or when the sender returns a
	// fatal error before that.
	var runErr error
	senderDone := false
	batcherDone := false
	select {
	case <-ctx.Done():
	case err := <-batcherErrCh:
		batcherDone = true
		if isRealError(err) {
			runErr = err
			d.log.Error("collector: batcher terminated", "error", err.Error())
		}
	case err := <-senderErrCh:
		senderDone = true
		if isRealError(err) {
			runErr = err
			d.log.Error("collector: sender terminated", "error", err.Error())
		}
	}

	// Graceful shutdown: stop inputs, drain readers, flush the final partial
	// batch, then give the sender a bounded window to deliver what is queued.
	cancelPipe()
	inputWG.Wait()
	if !batcherDone {
		if err := <-batcherErrCh; isRealError(err) && runErr == nil {
			runErr = err
		}
	}

	if !senderDone {
		d.drainSender()
		cancelSender()
		if err := <-senderErrCh; isRealError(err) && runErr == nil {
			runErr = err
		}
	}

	if err := d.closeAll(); err != nil && runErr == nil {
		runErr = err
	}
	return runErr
}

// drainSender polls the queue until it is empty or the drain window elapses, so
// a graceful shutdown delivers everything already durable before the sender's
// stream is closed. Unacked batches that outlast the window remain on disk for
// the next start.
func (d *Daemon) drainSender() {
	deadline := time.Now().Add(d.drainWindow)
	for time.Now().Before(deadline) {
		if d.queue.Stats().QueuedBatches == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// closeAll releases resources in dependency order (sender is already stopped by
// Run before this is called): queue, checkpoint store, dead-letter sink. It
// returns the first close error.
func (d *Daemon) closeAll() error {
	var first error
	record := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	record(d.queue.Close())
	record(d.checkpoints.Close())
	record(d.deadLetter.Close())
	record(d.stateLock.Close())
	return first
}

// isRealError reports whether err is worth surfacing from Run: context
// cancellation/deadline from an expected shutdown is not.
func isRealError(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// loadOrCreateCollectorID returns a stable collector ID persisted under the
// state directory, generating and atomically writing one on first use so the ID
// survives restarts.
func loadOrCreateCollectorID(stateDir string) (string, error) {
	path := filepath.Join(stateDir, collectorIDFile)
	data, err := os.ReadFile(path)
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("collector: read collector id %q: %w", path, err)
	}
	id := uuid.NewString()
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("collector: write collector id: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("collector: persist collector id: %w", err)
	}
	return id, nil
}

// tokenLoader returns a callback that reads the bearer token from path at dial
// time. The secret is never retained by the daemon or logged.
func tokenLoader(path string) func() (string, error) {
	return func() (string, error) { return config.LoadToken(path) }
}

// buildPipeline compiles cfg.Processors into an ordered Pipeline shared by every
// input. An empty processor list yields a pass-through pipeline.
func buildPipeline(procs []config.ProcessorConfig) (*Pipeline, error) {
	processors := make([]Processor, 0, len(procs))
	for i, p := range procs {
		var (
			proc Processor
			err  error
		)
		switch p.Type {
		case "allow":
			proc, err = NewAllowProcessor(p.Fields)
		case "deny":
			proc, err = NewDenyProcessor(p.Fields)
		case "rename":
			proc, err = NewRenameProcessor(p.From, p.To)
		case "redact":
			replacement := p.Replacement
			if replacement == "" {
				replacement = "[REDACTED]"
			}
			proc, err = NewRedactProcessor(p.Fields, replacement)
		default:
			return nil, fmt.Errorf("processors[%d]: unknown type %q", i, p.Type)
		}
		if err != nil {
			return nil, fmt.Errorf("processors[%d] (%s): %w", i, p.Type, err)
		}
		processors = append(processors, proc)
	}
	return NewPipeline(processors...), nil
}

// buildInput constructs the tailer, decoder, and server-side registration for a
// single input. defaultHost is used when the input does not set an explicit
// host. It returns whether the input uses multiline framing.
func buildInput(in *config.InputConfig, defaultHost string, checkpoints input.CheckpointStore) (*inputRuntime, *opensplunkv1.CollectorInputRegistration, bool, error) {
	maxEvent := int(in.MaxEventBytes)

	fo := framing.Options{MaxEventBytes: maxEvent}
	multiline := in.Multiline != nil
	var flushAfter time.Duration
	if multiline {
		re, err := regexp.Compile(in.Multiline.LineStartPattern)
		if err != nil {
			return nil, nil, false, fmt.Errorf("input %q: multiline.line_start_pattern: %w", in.ID, err)
		}
		fo.LineStartPattern = re
		fo.MaxLines = in.Multiline.MaxLines
		flushAfter = in.Multiline.FlushAfter.Duration()
	}

	mgr, err := input.NewManager(input.Config{
		InputID:      in.ID,
		Include:      in.Include,
		Exclude:      in.Exclude,
		StartAt:      input.StartPosition(in.StartAt),
		PollInterval: in.PollInterval.Duration(),
		Multiline:    multiline,
		Framing:      fo,
		FlushAfter:   flushAfter,
	}, checkpoints)
	if err != nil {
		return nil, nil, false, fmt.Errorf("input %q: %w", in.ID, err)
	}

	host := in.Host
	if strings.TrimSpace(host) == "" {
		host = defaultHost
	}
	index := strings.TrimSpace(in.Index)
	if index == "" {
		return nil, nil, false, fmt.Errorf("input %q: index is required", in.ID)
	}
	source := in.Source
	if strings.TrimSpace(source) == "" {
		source = in.ID
	}
	sourcetype := in.Sourcetype
	if strings.TrimSpace(sourcetype) == "" {
		sourcetype = in.Format
	}

	service, constants, err := buildConstants(in.Fields)
	if err != nil {
		return nil, nil, false, fmt.Errorf("input %q: fields: %w", in.ID, err)
	}

	dec, err := NewDecoder(DecodeConfig{
		Format:         InputFormat(in.Format),
		InputID:        in.ID,
		IndexName:      index,
		Source:         source,
		Sourcetype:     sourcetype,
		Host:           host,
		Service:        service,
		ConstantFields: constants,
		MaxLineBytes:   maxEvent,
	})
	if err != nil {
		return nil, nil, false, fmt.Errorf("input %q: %w", in.ID, err)
	}

	reg := &opensplunkv1.CollectorInputRegistration{
		InputId:    in.ID,
		InputType:  opensplunkv1.CollectorInputType_COLLECTOR_INPUT_TYPE_FILE,
		IndexName:  index,
		Source:     proto.String(source),
		Sourcetype: proto.String(sourcetype),
	}
	return &inputRuntime{id: in.ID, manager: mgr, decoder: dec}, reg, multiline, nil
}

// buildConstants converts an input's static fields map into trusted constant
// TypedObject fields, extracting the canonical "service" key into its own return
// value (the decoder rejects it as a constant because it is reserved metadata).
// Keys are emitted in sorted order for deterministic output.
func buildConstants(fields map[string]string) (string, *opensplunkv1.TypedObject, error) {
	if len(fields) == 0 {
		return "", nil, nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var service string
	converted := make([]*opensplunkv1.TypedObjectField, 0, len(keys))
	for _, k := range keys {
		if strings.ToLower(k) == "service" {
			service = fields[k]
			continue
		}
		converted = append(converted, &opensplunkv1.TypedObjectField{
			Name:  k,
			Value: &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_StringValue{StringValue: fields[k]}},
		})
	}
	if len(converted) == 0 {
		return service, nil, nil
	}
	return service, &opensplunkv1.TypedObject{Fields: converted}, nil
}

// capabilities returns the CollectorCapabilities advertised in the Hello.
func capabilities(cfg *config.Config, anyMultiline bool) []opensplunkv1.CollectorCapability {
	caps := []opensplunkv1.CollectorCapability{
		opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_FILE_INPUT,
		opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_DURABLE_QUEUE,
		opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_PARTIAL_EVENT_REJECTION,
		opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_TYPED_FIELDS,
	}
	if cfg.Server.Compression == "gzip" {
		caps = append(caps, opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_GZIP)
	}
	if anyMultiline {
		caps = append(caps, opensplunkv1.CollectorCapability_COLLECTOR_CAPABILITY_MULTILINE)
	}
	return caps
}

// inputHealthSnapshot converts each input Manager's Health into the protobuf
// message the sender embeds in heartbeats.
func inputHealthSnapshot(inputs []*inputRuntime) []*opensplunkv1.CollectorInputHealth {
	out := make([]*opensplunkv1.CollectorInputHealth, 0, len(inputs))
	for _, ir := range inputs {
		h := ir.manager.Health()
		ch := &opensplunkv1.CollectorInputHealth{
			InputId:           h.InputID,
			State:             h.State,
			StatusMessage:     h.StatusMessage,
			DiscoveredSources: h.DiscoveredSources,
			ActiveSources:     h.ActiveSources,
			EventsReadTotal:   h.EventsReadTotal,
			BytesReadTotal:    h.BytesReadTotal,
		}
		if !h.LastEventAt.IsZero() {
			ch.LastEventAt = timestamppb.New(h.LastEventAt.UTC())
		}
		if !h.LastErrorAt.IsZero() {
			ch.LastErrorAt = timestamppb.New(h.LastErrorAt.UTC())
		}
		out = append(out, ch)
	}
	return out
}
