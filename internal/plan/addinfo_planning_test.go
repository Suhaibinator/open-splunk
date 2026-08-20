package plan

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestAddInfoRequiresRealSIDOrExplicitNonExecutingPlaceholder(t *testing.T) {
	t.Parallel()

	parsed, err := spl.Parse(`index=gradethis | addinfo`)
	if err != nil {
		t.Fatalf("Parse(addinfo): %v", err)
	}
	base := testScope([]string{"gradethis"}, nil)

	if logical, buildErr := Build(parsed, base); buildErr == nil || logical != nil {
		t.Fatalf("Build(addinfo without SID) = (%#v, %v), want fail-closed", logical, buildErr)
	} else {
		var diagnostic *Diagnostic
		if !errors.As(buildErr, &diagnostic) || diagnostic.Code != "SPL_INVALID_SEARCH_JOB_ID" {
			t.Fatalf("Build(addinfo without SID) error = %v", buildErr)
		}
	}

	bound := base
	bound.SearchJobID = "search-addinfo-1"
	logical, err := Build(parsed, bound)
	if err != nil {
		t.Fatalf("Build(bound addinfo): %v", err)
	}
	if got := addInfoSIDLiteral(t, logical); got.Value.Kind != ValueKindString ||
		got.Value.String != bound.SearchJobID {
		t.Fatalf("bound info_sid literal = %#v", got.Value)
	}

	validation := base
	validation.AllowUnboundSearchJobID = true
	logical, err = Build(parsed, validation)
	if err != nil {
		t.Fatalf("Build(validation addinfo): %v", err)
	}
	if got := addInfoSIDLiteral(t, logical); got.Value.Kind != ValueKindNull ||
		got.Value.String != "" {
		t.Fatalf("validation info_sid placeholder = %#v, want explicit null", got.Value)
	}
}

func addInfoSIDLiteral(t *testing.T, logical *Query) *ScalarLiteralExpression {
	t.Helper()
	if logical == nil || len(logical.Operators) < 2 {
		t.Fatalf("addinfo logical plan = %#v", logical)
	}
	extend, ok := logical.Operators[len(logical.Operators)-1].(*Extend)
	if !ok || extend == nil || len(extend.Assignments) != 4 {
		t.Fatalf("addinfo operator = %#v", logical.Operators[len(logical.Operators)-1])
	}
	assignment := extend.Assignments[3]
	if assignment.Output.Name != "info_sid" {
		t.Fatalf("fourth addinfo output = %q", assignment.Output.Name)
	}
	literal, ok := assignment.Expression.(*ScalarLiteralExpression)
	if !ok || literal == nil {
		t.Fatalf("info_sid expression = %T", assignment.Expression)
	}
	return literal
}
