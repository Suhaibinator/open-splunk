package scheduledreports

import "github.com/Suhaibinator/open-splunk/internal/featureops"

func (service *Service) observe(operation featureops.Operation, outcome featureops.Outcome, items uint64) {
	featureops.Emit(service.observer, featureops.Event{
		Feature: featureops.FeatureScheduledReports, Operation: operation,
		Outcome: outcome, Items: items,
	})
}

func (service *Service) observeRunOutcome(outcome RunOutcome) {
	projected := featureops.OutcomeFailed
	switch outcome {
	case RunOutcomeSubmitted:
		projected = featureops.OutcomeSubmitted
	case RunOutcomeSucceeded:
		projected = featureops.OutcomeSucceeded
	case RunOutcomeCanceled:
		projected = featureops.OutcomeCanceled
	case RunOutcomeExpired:
		projected = featureops.OutcomeExpired
	case RunOutcomeInterrupted:
		projected = featureops.OutcomeInterrupted
	case RunOutcomeSkippedOverlap:
		projected = featureops.OutcomeSkipped
	}
	service.observe(featureops.OperationRunOutcome, projected, 1)
}

func nonnegativeCount(count int64) uint64 {
	if count <= 0 {
		return 0
	}
	return uint64(count)
}
