package alertevaluation

import (
	"errors"
	"testing"
)

func TestEvaluateExactConditions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		operator  ConditionOperator
		count     uint64
		threshold uint64
		want      EvaluationCertainty
	}{
		{"greater empty", ConditionGreaterThan, 0, 0, EvaluationFalse},
		{"greater below", ConditionGreaterThan, 1, 2, EvaluationFalse},
		{"greater at boundary", ConditionGreaterThan, 2, 2, EvaluationFalse},
		{"greater above", ConditionGreaterThan, 3, 2, EvaluationTrue},
		{"less empty", ConditionLessThan, 0, 0, EvaluationFalse},
		{"less below", ConditionLessThan, 1, 2, EvaluationTrue},
		{"less at boundary", ConditionLessThan, 2, 2, EvaluationFalse},
		{"less above", ConditionLessThan, 3, 2, EvaluationFalse},
		{"equal empty", ConditionEqual, 0, 0, EvaluationTrue},
		{"equal below", ConditionEqual, 1, 2, EvaluationFalse},
		{"equal at boundary", ConditionEqual, 2, 2, EvaluationTrue},
		{"equal above", ConditionEqual, 3, 2, EvaluationFalse},
		{"not equal empty", ConditionNotEqual, 0, 0, EvaluationFalse},
		{"not equal below", ConditionNotEqual, 1, 2, EvaluationTrue},
		{"not equal at boundary", ConditionNotEqual, 2, 2, EvaluationFalse},
		{"not equal above", ConditionNotEqual, 3, 2, EvaluationTrue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			observation := CountObservation{Count: test.count, Exact: true}
			got, err := Evaluate(Condition{Operator: test.operator, Threshold: test.threshold}, observation)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if got.Certainty != test.want {
				t.Fatalf("certainty = %q, want %q", got.Certainty, test.want)
			}
			if got.Observed != observation {
				t.Fatalf("observed = %+v, want %+v", got.Observed, observation)
			}
		})
	}
}

func TestEvaluateTruncatedLowerBounds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		operator  ConditionOperator
		count     uint64
		threshold uint64
		want      EvaluationCertainty
	}{
		{"greater below", ConditionGreaterThan, 1, 2, EvaluationIndeterminate},
		{"greater at boundary", ConditionGreaterThan, 2, 2, EvaluationIndeterminate},
		{"greater above", ConditionGreaterThan, 3, 2, EvaluationTrue},
		{"less below", ConditionLessThan, 1, 2, EvaluationIndeterminate},
		{"less at boundary", ConditionLessThan, 2, 2, EvaluationFalse},
		{"less above", ConditionLessThan, 3, 2, EvaluationFalse},
		{"equal below", ConditionEqual, 1, 2, EvaluationIndeterminate},
		{"equal at boundary", ConditionEqual, 2, 2, EvaluationIndeterminate},
		{"equal above", ConditionEqual, 3, 2, EvaluationFalse},
		{"not equal below", ConditionNotEqual, 1, 2, EvaluationIndeterminate},
		{"not equal at boundary", ConditionNotEqual, 2, 2, EvaluationIndeterminate},
		{"not equal above", ConditionNotEqual, 3, 2, EvaluationTrue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			observation := CountObservation{Count: test.count}
			got, err := Evaluate(Condition{Operator: test.operator, Threshold: test.threshold}, observation)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if got.Certainty != test.want {
				t.Fatalf("certainty = %q, want %q", got.Certainty, test.want)
			}
			if got.Observed != observation {
				t.Fatalf("observed = %+v, want %+v", got.Observed, observation)
			}
		})
	}
}

func TestValidateCondition(t *testing.T) {
	t.Parallel()
	for _, operator := range []ConditionOperator{
		ConditionGreaterThan,
		ConditionLessThan,
		ConditionEqual,
		ConditionNotEqual,
	} {
		if err := ValidateCondition(Condition{Operator: operator}); err != nil {
			t.Errorf("ValidateCondition(%q) error = %v", operator, err)
		}
	}

	err := ValidateCondition(Condition{Operator: "UNKNOWN"})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ValidateCondition(UNKNOWN) error = %v, want ErrInvalidArgument", err)
	}
}

func TestEvaluateRejectsInvalidCondition(t *testing.T) {
	t.Parallel()
	got, err := Evaluate(Condition{Operator: "UNKNOWN"}, CountObservation{Count: 4, Exact: true})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Evaluate() error = %v, want ErrInvalidArgument", err)
	}
	if got != (Evaluation{}) {
		t.Fatalf("Evaluate() = %+v, want zero evaluation", got)
	}
}
