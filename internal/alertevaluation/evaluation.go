// Package alertevaluation evaluates alert result-count conditions without
// depending on alert persistence, scheduling, or delivery concerns.
package alertevaluation

import (
	"errors"
	"fmt"
)

var ErrInvalidArgument = errors.New("alert evaluation: invalid argument")

type ConditionOperator string

const (
	ConditionGreaterThan ConditionOperator = "GREATER_THAN"
	ConditionLessThan    ConditionOperator = "LESS_THAN"
	ConditionEqual       ConditionOperator = "EQUAL"
	ConditionNotEqual    ConditionOperator = "NOT_EQUAL"
)

type EvaluationCertainty string

const (
	EvaluationTrue          EvaluationCertainty = "TRUE"
	EvaluationFalse         EvaluationCertainty = "FALSE"
	EvaluationIndeterminate EvaluationCertainty = "INDETERMINATE"
)

type Condition struct {
	Operator  ConditionOperator
	Threshold uint64
}

type CountObservation struct {
	Count uint64
	Exact bool
}

type Evaluation struct {
	Certainty EvaluationCertainty
	Observed  CountObservation
}

// Evaluate applies condition to an exact count or a truncated lower bound. A
// lower bound is never treated as an exact result.
func Evaluate(condition Condition, observation CountObservation) (Evaluation, error) {
	if err := ValidateCondition(condition); err != nil {
		return Evaluation{}, err
	}
	if observation.Exact {
		certainty := EvaluationFalse
		if compare(condition.Operator, observation.Count, condition.Threshold) {
			certainty = EvaluationTrue
		}
		return Evaluation{Certainty: certainty, Observed: observation}, nil
	}

	certainty := EvaluationIndeterminate
	switch condition.Operator {
	case ConditionGreaterThan, ConditionNotEqual:
		if observation.Count > condition.Threshold {
			certainty = EvaluationTrue
		}
	case ConditionLessThan, ConditionEqual:
		// A truncated result supplies only a lower bound. It cannot prove these
		// operators, even when the bound reaches or exceeds the threshold.
	}
	return Evaluation{Certainty: certainty, Observed: observation}, nil
}

func ValidateCondition(condition Condition) error {
	switch condition.Operator {
	case ConditionGreaterThan, ConditionLessThan, ConditionEqual, ConditionNotEqual:
		return nil
	default:
		return fmt.Errorf("%w: unsupported condition operator", ErrInvalidArgument)
	}
}

func compare(operator ConditionOperator, count, threshold uint64) bool {
	switch operator {
	case ConditionGreaterThan:
		return count > threshold
	case ConditionLessThan:
		return count < threshold
	case ConditionEqual:
		return count == threshold
	case ConditionNotEqual:
		return count != threshold
	default:
		return false
	}
}
