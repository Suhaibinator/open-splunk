package searchjobs

import (
	"context"
	"errors"
	"time"
)

const compositeAdmissionCompensationTimeout = 10 * time.Second

// CompositeJournal fans one lifecycle snapshot out to independent durable
// projections. Admission preserves caller order. Completed exact-result
// publishers run before metadata-only projections, which makes the artifact
// the publication barrier observed by schedulers and alerts. Every projection
// must be idempotent because compensation and startup recovery may replay it.
type CompositeJournal struct {
	journals []JobJournal
}

type completedPublicationOutcome struct {
	Finalize   error
	Results    error
	Projection error
}

type completedPublicationJournal interface {
	finalizeCompleted(context.Context, Job, ResultLease) completedPublicationOutcome
}

func NewCompositeJournal(journals ...JobJournal) *CompositeJournal {
	filtered := make([]JobJournal, 0, len(journals))
	for _, journal := range journals {
		if journal != nil {
			filtered = append(filtered, journal)
		}
	}
	return &CompositeJournal{journals: filtered}
}

func (journal *CompositeJournal) Admit(ctx context.Context, job Job) error {
	if journal == nil || len(journal.journals) == 0 {
		return errors.New("search job journal is unavailable")
	}
	for index, target := range journal.journals {
		if err := invokeJournal(func() error {
			return target.Admit(ctx, cloneJob(job))
		}); err != nil {
			// A journal may have committed before returning an ambiguous error.
			// Compensate it and every earlier successful projection so a partial
			// fan-out cannot strand an indefinitely queued durable record.
			compensating := admissionCompensation(job)
			joined := err
			// Compensate known-successful targets before the ambiguously failed
			// target. In the production ordering this guarantees the artifact
			// record cannot be stranded even if a later projection is unhealthy.
			for admitted := 0; admitted <= index; admitted++ {
				compensationContext, cancel := context.WithTimeout(
					context.WithoutCancel(ctx),
					compositeAdmissionCompensationTimeout,
				)
				joined = errors.Join(joined, invokeJournal(func() error {
					return journal.journals[admitted].Finalize(compensationContext, cloneJob(compensating))
				}))
				cancel()
			}
			return joined
		}
	}
	return nil
}

func admissionCompensation(job Job) Job {
	compensating := cloneJob(job)
	compensating.State = StateCanceled
	incrementJobVersion(&compensating)
	compensating.FinishedAt = compensating.CreatedAt.UTC()
	lifetime := compensating.RetentionLifetime
	if lifetime <= 0 {
		lifetime = defaultRetentionTTL
	}
	compensating.ExpiresAt = compensating.FinishedAt.Add(lifetime)
	compensating.Schema = nil
	compensating.RowCount = 0
	compensating.ResultBytes = 0
	compensating.ResultsTruncated = false
	compensating.Failure = nil
	return compensating
}

func (journal *CompositeJournal) Finalize(ctx context.Context, job Job) error {
	if journal == nil || len(journal.journals) == 0 {
		return errors.New("search job journal is unavailable")
	}
	var joined error
	for _, target := range journal.journals {
		joined = errors.Join(joined, invokeJournal(func() error {
			return target.Finalize(ctx, cloneJob(job))
		}))
	}
	return joined
}

func (journal *CompositeJournal) FinalizeResults(ctx context.Context, job Job, results ResultLease) error {
	if journal == nil || results == nil {
		return errors.New("completed search result journal is unavailable")
	}
	var joined error
	for _, target := range journal.journals {
		completed, ok := target.(CompletedResultJournal)
		if ok {
			joined = errors.Join(joined, invokeJournal(func() error {
				return completed.FinalizeResults(ctx, cloneJob(job), results)
			}))
		}
	}
	return joined
}

// finalizeCompleted publishes every durable result projection before any
// metadata-only projection can observe the completed lifecycle state. The
// separate error channels preserve the journal operation reported by Manager.
// Metadata projections are skipped unless every exact-result publication
// succeeds, while a metadata failure can never roll back or suppress an
// already successful result publication.
func (journal *CompositeJournal) finalizeCompleted(
	ctx context.Context,
	job Job,
	results ResultLease,
) completedPublicationOutcome {
	if journal == nil || results == nil || job.State != StateCompleted {
		return completedPublicationOutcome{Finalize: errors.New("completed search journal is unavailable")}
	}
	var outcome completedPublicationOutcome
	publicationTargets := 0
	for _, target := range journal.journals {
		completed, ok := target.(CompletedResultJournal)
		if !ok {
			continue
		}
		publicationTargets++
		if err := invokeJournal(func() error {
			return target.Finalize(ctx, cloneJob(job))
		}); err != nil {
			outcome.Finalize = errors.Join(outcome.Finalize, err)
			continue
		}
		if err := invokeJournal(func() error {
			return completed.FinalizeResults(ctx, cloneJob(job), results)
		}); err != nil {
			outcome.Results = errors.Join(outcome.Results, err)
		}
	}
	if publicationTargets == 0 {
		outcome.Finalize = errors.Join(
			outcome.Finalize,
			errors.New("completed search result journal is unavailable"),
		)
		return outcome
	}
	if outcome.Finalize != nil || outcome.Results != nil {
		// A completed in-memory job whose exact artifact could not be published
		// must not remain an externally queued durable job. Project a terminal,
		// sanitized storage failure only after publication has failed. This wakes
		// scheduled consumers without claiming unavailable results completed.
		compensating := resultPublicationCompensation(
			job,
			errors.Join(outcome.Finalize, outcome.Results),
		)
		for _, target := range journal.journals {
			err := invokeJournal(func() error {
				return target.Finalize(ctx, cloneJob(compensating))
			})
			if _, publishesResults := target.(CompletedResultJournal); publishesResults {
				outcome.Finalize = errors.Join(outcome.Finalize, err)
			} else {
				outcome.Projection = errors.Join(outcome.Projection, err)
			}
		}
		return outcome
	}
	for _, target := range journal.journals {
		if _, publishesResults := target.(CompletedResultJournal); publishesResults {
			continue
		}
		outcome.Projection = errors.Join(outcome.Projection, invokeJournal(func() error {
			return target.Finalize(ctx, cloneJob(job))
		}))
	}
	return outcome
}

// Client-safe explanations for a discarded completed result. They name the
// class of storage fault without paths, filesystem types, or syscall text.
const (
	resultsNotPersistedMessage = "Search completed, but its results could not be persisted to retained storage. " +
		"Run the search again once storage is healthy."
	resultsNotPersistedUnsupportedMessage = "Search completed, but its results could not be persisted: " +
		"the retained-search directory is on a filesystem that does not support atomic no-replace rename. " +
		"The server operator must move it to a local filesystem."
)

func resultPublicationCompensation(job Job, cause error) Job {
	compensating := cloneJob(job)
	compensating.State = StateFailed
	incrementJobVersion(&compensating)
	compensating.Schema = nil
	compensating.RowCount = 0
	compensating.ResultBytes = 0
	compensating.ResultsTruncated = false
	message := resultsNotPersistedMessage
	if errors.Is(cause, ErrResultStorageUnsupported) {
		message = resultsNotPersistedUnsupportedMessage
	}
	compensating.Failure = &Failure{Code: FailureResultsNotPersisted, Message: message}
	return compensating
}

var _ JobJournal = (*CompositeJournal)(nil)
var _ CompletedResultJournal = (*CompositeJournal)(nil)
var _ completedPublicationJournal = (*CompositeJournal)(nil)
