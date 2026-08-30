package searchartifacts

import (
	"errors"
	"math"

	"github.com/Suhaibinator/open-splunk/internal/featureops"
)

func (store *Store) observe(
	operation featureops.Operation,
	outcome featureops.Outcome,
	items uint64,
	bytes uint64,
) {
	featureops.Emit(store.observer, featureops.Event{
		Feature: featureops.FeatureDurableArtifacts, Operation: operation,
		Outcome: outcome, Items: items, Bytes: bytes,
	})
}

func saturatingAdd(current, increment uint64) uint64 {
	if math.MaxUint64-current < increment {
		return math.MaxUint64
	}
	return current + increment
}

func operationOutcome(err error) featureops.Outcome {
	switch {
	case err == nil:
		return featureops.OutcomeSucceeded
	case errors.Is(err, ErrCapacity):
		return featureops.OutcomeCapacityRejected
	case errors.Is(err, ErrConflict):
		return featureops.OutcomeConflict
	default:
		return featureops.OutcomeFailed
	}
}
