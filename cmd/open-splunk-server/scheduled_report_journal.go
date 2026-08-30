package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/scheduledreports"
	"github.com/Suhaibinator/open-splunk/internal/scheduler"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	defaultScheduledReportFinishQueueCapacity = 256
	defaultScheduledReportFinishWorkers       = 2
	defaultScheduledReportRetryBackoff        = 100 * time.Millisecond
	defaultScheduledReportMaximumRetryBackoff = 10 * time.Second
)

type scheduledReportRunFinisher interface {
	MarkSubmitted(context.Context, string, string) error
	Finish(context.Context, string, scheduledreports.RunOutcome, string) error
}

type scheduledReportFinishTask struct {
	runID           string
	outcome         scheduledreports.RunOutcome
	failureCategory string
}

type runtimeScheduledReportJournalOptions struct {
	clock          scheduler.Clock
	initialBackoff time.Duration
	maximumBackoff time.Duration
	queueCapacity  int
	workers        int
}

// runtimeScheduledReportJournal closes scheduling records from authoritative
// terminal callbacks. A bounded worker pool retries transient persistence
// failures independently of the completed search, so execution is never
// repeated and one unavailable store cannot create unbounded memory growth.
type runtimeScheduledReportJournal struct {
	mu       sync.Mutex
	finisher scheduledReportRunFinisher

	clock          scheduler.Clock
	initialBackoff time.Duration
	maximumBackoff time.Duration
	queueCapacity  int
	workers        int

	ctx     context.Context
	cancel  context.CancelFunc
	queue   chan scheduledReportFinishTask
	pending map[string]scheduledReportFinishTask
	started bool
	closed  bool
	wait    sync.WaitGroup
}

func newRuntimeScheduledReportJournal(options runtimeScheduledReportJournalOptions) (*runtimeScheduledReportJournal, error) {
	journal := &runtimeScheduledReportJournal{
		clock: options.clock, initialBackoff: options.initialBackoff,
		maximumBackoff: options.maximumBackoff, queueCapacity: options.queueCapacity,
		workers: options.workers,
	}
	journal.mu.Lock()
	err := journal.applyDefaultsLocked()
	journal.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return journal, nil
}

func (journal *runtimeScheduledReportJournal) applyDefaultsLocked() error {
	if journal.clock == nil {
		journal.clock = scheduler.RealClock{}
	}
	if journal.initialBackoff == 0 {
		journal.initialBackoff = defaultScheduledReportRetryBackoff
	}
	if journal.maximumBackoff == 0 {
		journal.maximumBackoff = defaultScheduledReportMaximumRetryBackoff
	}
	if journal.queueCapacity == 0 {
		journal.queueCapacity = defaultScheduledReportFinishQueueCapacity
	}
	if journal.workers == 0 {
		journal.workers = defaultScheduledReportFinishWorkers
	}
	if journal.initialBackoff < time.Millisecond || journal.maximumBackoff < journal.initialBackoff || journal.maximumBackoff > time.Minute {
		return errors.New("scheduled-report journal retry backoff is invalid")
	}
	if journal.queueCapacity < 1 || journal.queueCapacity > 4096 || journal.workers < 1 || journal.workers > 16 {
		return errors.New("scheduled-report journal worker bounds are invalid")
	}
	return nil
}

func (journal *runtimeScheduledReportJournal) Bind(finisher scheduledReportRunFinisher) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed {
		return errors.New("scheduled-report journal is closed")
	}
	if finisher == nil {
		return errors.New("scheduled-report journal finisher is required")
	}
	if journal.finisher != nil || journal.started {
		return errors.New("scheduled-report journal is already bound")
	}
	if err := journal.applyDefaultsLocked(); err != nil {
		return err
	}
	journal.finisher = finisher
	journal.ctx, journal.cancel = context.WithCancel(context.Background())
	journal.queue = make(chan scheduledReportFinishTask, journal.queueCapacity)
	journal.pending = make(map[string]scheduledReportFinishTask, journal.queueCapacity)
	journal.started = true
	for range journal.workers {
		journal.wait.Add(1)
		go journal.retryWorker()
	}
	return nil
}

func (journal *runtimeScheduledReportJournal) Admit(ctx context.Context, job searchjobs.Job) error {
	if job.Source.Origin != searchjobs.JobOriginScheduledReport {
		return nil
	}
	if ctx == nil || strings.TrimSpace(job.ID) == "" || strings.TrimSpace(job.Source.ObjectID) == "" {
		return errors.New("scheduled-report journal admission is invalid")
	}
	journal.mu.Lock()
	finisher := journal.finisher
	closed := journal.closed
	journal.mu.Unlock()
	if closed {
		return errors.New("scheduled-report journal is closed")
	}
	if finisher == nil {
		return errors.New("scheduled-report journal is not bound")
	}
	return finisher.MarkSubmitted(ctx, job.Source.ObjectID, job.ID)
}

func (journal *runtimeScheduledReportJournal) Finalize(ctx context.Context, job searchjobs.Job) error {
	if job.Source.Origin != searchjobs.JobOriginScheduledReport {
		return nil
	}
	task := scheduledReportFinishTask{runID: job.Source.ObjectID, outcome: scheduledReportOutcome(job.State)}
	if job.Failure != nil {
		task.failureCategory = strings.TrimSpace(string(job.Failure.Code))
	}
	journal.mu.Lock()
	finisher := journal.finisher
	closed := journal.closed
	journal.mu.Unlock()
	if finisher == nil {
		return nil
	}
	if closed {
		return errors.New("scheduled-report journal is closed")
	}
	err := finisher.Finish(ctx, task.runID, task.outcome, task.failureCategory)
	if scheduledReportFinishComplete(err) {
		return nil
	}
	if !scheduledReportFinishRetryable(err) {
		return err
	}
	if enqueueErr := journal.enqueue(task); enqueueErr != nil {
		return errors.Join(err, enqueueErr)
	}
	return nil
}

func (journal *runtimeScheduledReportJournal) enqueue(task scheduledReportFinishTask) error {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.closed || !journal.started {
		return errors.New("scheduled-report retry queue is unavailable")
	}
	if existing, ok := journal.pending[task.runID]; ok {
		if existing != task {
			return errors.New("scheduled-report retry outcome conflicts with pending completion")
		}
		return nil
	}
	journal.pending[task.runID] = task
	select {
	case journal.queue <- task:
		return nil
	default:
		delete(journal.pending, task.runID)
		return errors.New("scheduled-report retry queue is full")
	}
}

func (journal *runtimeScheduledReportJournal) retryWorker() {
	defer journal.wait.Done()
	for {
		select {
		case <-journal.ctx.Done():
			return
		case task := <-journal.queue:
			journal.retry(task)
		}
	}
}

func (journal *runtimeScheduledReportJournal) retry(task scheduledReportFinishTask) {
	backoff := journal.initialBackoff
	for {
		if err := journal.clock.Wait(journal.ctx, backoff); err != nil {
			return
		}
		journal.mu.Lock()
		finisher := journal.finisher
		journal.mu.Unlock()
		if finisher == nil {
			return
		}
		err := finisher.Finish(journal.ctx, task.runID, task.outcome, task.failureCategory)
		if scheduledReportFinishComplete(err) || !scheduledReportFinishRetryable(err) {
			journal.mu.Lock()
			delete(journal.pending, task.runID)
			journal.mu.Unlock()
			return
		}
		backoff = min(backoff*2, journal.maximumBackoff)
	}
}

func scheduledReportFinishComplete(err error) bool {
	return err == nil || errors.Is(err, scheduledreports.ErrNotFound)
}

func scheduledReportFinishRetryable(err error) bool {
	return err != nil && !errors.Is(err, scheduledreports.ErrInvalidArgument) &&
		!errors.Is(err, scheduledreports.ErrConflict) && !errors.Is(err, scheduledreports.ErrNotFound)
}

// Close cancels retry waits and leaves still-active durable rows for the
// existing startup interruption recovery. It is safe to call more than once.
func (journal *runtimeScheduledReportJournal) Close() {
	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return
	}
	journal.closed = true
	cancel := journal.cancel
	journal.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	journal.wait.Wait()
}

func (journal *runtimeScheduledReportJournal) pendingCount() int {
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return len(journal.pending)
}

func scheduledReportOutcome(state searchjobs.State) scheduledreports.RunOutcome {
	switch state {
	case searchjobs.StateCompleted:
		return scheduledreports.RunOutcomeSucceeded
	case searchjobs.StateCanceled:
		return scheduledreports.RunOutcomeCanceled
	case searchjobs.StateExpired:
		return scheduledreports.RunOutcomeExpired
	case searchjobs.StateFailed:
		return scheduledreports.RunOutcomeFailed
	default:
		return scheduledreports.RunOutcomeInterrupted
	}
}

var _ searchjobs.JobJournal = (*runtimeScheduledReportJournal)(nil)
