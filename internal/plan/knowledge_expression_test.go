package plan

import (
	"slices"
	"strings"
	"testing"
)

func TestConvertKnowledgeCalculatedExpressionExact(t *testing.T) {
	t.Parallel()

	operation := testKnowledgeProgram(t).CalculatedFields()[0]
	expression, err := ConvertKnowledgeCalculatedExpression(operation)
	if err != nil {
		t.Fatalf("ConvertKnowledgeCalculatedExpression: %v", err)
	}
	conditional, ok := expression.(*ScalarIfExpression)
	if !ok {
		t.Fatalf("expression = %T, want *ScalarIfExpression", expression)
	}
	condition, ok := conditional.Condition.(*EvalComparisonExpression)
	if !ok {
		t.Fatalf("condition = %T, want *EvalComparisonExpression", conditional.Condition)
	}
	left, ok := condition.Left.(*ScalarFieldExpression)
	if !ok || left.Field.Name != "source" || condition.Op != ComparisonOpEqual {
		t.Fatalf("condition = %#v, want source equality", condition)
	}
	right, ok := condition.Right.(*ScalarLiteralExpression)
	if !ok || right.Value.Kind != ValueKindString || right.Value.String != "api" {
		t.Fatalf("condition right = %#v, want string literal api", condition.Right)
	}
	trueValue, trueOK := conditional.True.(*ScalarFieldExpression)
	falseValue, falseOK := conditional.False.(*ScalarFieldExpression)
	if !trueOK || !falseOK || trueValue.Field.Name != "sourcetype" ||
		falseValue.Field.Name != "host" {
		t.Fatalf("conditional branches = (%#v, %#v), want (sourcetype, host)", conditional.True, conditional.False)
	}
}

func TestConvertKnowledgeCalculatedExpressionRejectsInventoryMismatch(t *testing.T) {
	t.Parallel()

	operation := testKnowledgeProgram(t).CalculatedFields()[0]
	inputFields := operation.InputFields()
	tests := []struct {
		name       string
		fields     []string
		nodes      uint32
		predicates uint32
		want       string
	}{
		{
			name:       "input fields",
			fields:     inputFields[:len(inputFields)-1],
			nodes:      operation.Nodes(),
			predicates: operation.Predicates(),
			want:       "input-field inventory disagrees",
		},
		{
			name:       "nodes",
			fields:     inputFields,
			nodes:      operation.Nodes() + 1,
			predicates: operation.Predicates(),
			want:       "node inventory disagrees",
		},
		{
			name:       "predicates",
			fields:     inputFields,
			nodes:      operation.Nodes(),
			predicates: operation.Predicates() + 1,
			want:       "predicate inventory disagrees",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := convertKnowledgeCalculatedExpression(
				operation.Expression(),
				test.fields,
				test.nodes,
				test.predicates,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("convert error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestConvertKnowledgeCalculatedExpressionRejectsDirectBooleanResult(t *testing.T) {
	t.Parallel()

	_, err := convertKnowledgeCalculatedExpression("isnull(host)", []string{"host"}, 2, 0)
	if err == nil || !strings.Contains(err.Error(), "directly assign a Boolean result") {
		t.Fatalf("convert error = %v, want direct Boolean rejection", err)
	}
}

func TestConvertKnowledgeCalculatedExpressionReturnsDetachedTrees(t *testing.T) {
	t.Parallel()

	operation := testKnowledgeProgram(t).CalculatedFields()[0]
	first, err := ConvertKnowledgeCalculatedExpression(operation)
	if err != nil {
		t.Fatalf("first conversion: %v", err)
	}
	second, err := ConvertKnowledgeCalculatedExpression(operation)
	if err != nil {
		t.Fatalf("second conversion: %v", err)
	}

	firstConditional := first.(*ScalarIfExpression)
	secondConditional := second.(*ScalarIfExpression)
	firstTrue := firstConditional.True.(*ScalarFieldExpression)
	secondTrue := secondConditional.True.(*ScalarFieldExpression)
	firstTrue.Field.Name = "mutated"
	firstTrue.Field.Path = []string{"mutated", "path"}
	if secondTrue.Field.Name != "sourcetype" || len(secondTrue.Field.Path) != 0 {
		t.Fatalf("second tree aliased first mutation: %#v", secondTrue.Field)
	}
	if !slices.Equal(operation.InputFields(), []string{"host", "source", "sourcetype"}) ||
		operation.Expression() != `if(source="api", sourcetype, host)` {
		t.Fatal("conversion or result mutation changed the sealed operation")
	}
}
