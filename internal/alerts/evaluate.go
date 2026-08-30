package alerts

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/alertevaluation"
)

type CountObservation = alertevaluation.CountObservation

type Evaluation = alertevaluation.Evaluation

// Evaluate applies the condition to an exact count or a truncated lower
// bound. It never treats a lower bound as an exact result.
func Evaluate(condition Condition, observation CountObservation) (Evaluation, error) {
	evaluation, err := alertevaluation.Evaluate(condition, observation)
	return evaluation, compatibleEvaluationError(err)
}

func ValidateCondition(condition Condition) error {
	return compatibleEvaluationError(alertevaluation.ValidateCondition(condition))
}

func compatibleEvaluationError(err error) error {
	if errors.Is(err, alertevaluation.ErrInvalidArgument) {
		return fmt.Errorf("%w: unsupported condition operator", ErrInvalidArgument)
	}
	return err
}
