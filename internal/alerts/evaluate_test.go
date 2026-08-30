package alerts

import (
	"errors"
	"testing"
)

func TestEvaluateCompatibilityFacade(t *testing.T) {
	t.Parallel()
	observation := CountObservation{Count: 3, Exact: true}
	got, err := Evaluate(Condition{Operator: ConditionGreaterThan, Threshold: 2}, observation)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if got.Certainty != EvaluationTrue || got.Observed != observation {
		t.Fatalf("Evaluate() = %+v, want true with observation %+v", got, observation)
	}
}

func TestEvaluateCompatibilityFacadePreservesInvalidArgument(t *testing.T) {
	t.Parallel()
	condition := Condition{Operator: "UNKNOWN"}
	if err := ValidateCondition(condition); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ValidateCondition() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := Evaluate(condition, CountObservation{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Evaluate() error = %v, want ErrInvalidArgument", err)
	}
}
