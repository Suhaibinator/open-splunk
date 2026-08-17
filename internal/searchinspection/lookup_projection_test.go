package searchinspection

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestProjectLogicalPlanProjectsExplicitLookupWithoutCatalogIdentity(t *testing.T) {
	t.Parallel()

	snapshot := validInspectionSnapshot()
	snapshot.SPL = "index=" + snapshot.EffectiveIndexes[0] +
		" | lookup sensitive_catalog secret_key AS service_key" +
		" OUTPUT private_contact AS service_owner"
	logical, err := buildInspectionAuthoredPlan(snapshot)
	if err != nil {
		t.Fatalf("buildInspectionAuthoredPlan: %v", err)
	}

	projected, err := projectLogicalPlan(context.Background(), logical, snapshot.SPL)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	stage := projected.Stages[len(projected.Stages)-1]
	if stage.Operator != "Lookup" || stage.SourceRange == nil {
		t.Fatalf("lookup stage = %#v", stage)
	}
	if !slices.Equal(stage.InputFields, []string{"service_key"}) ||
		!slices.Equal(stage.OutputFields, []string{"service_owner"}) {
		t.Fatalf("lookup logical fields = %#v", stage)
	}
	if len(stage.KnowledgeObjects) != 0 || len(stage.OutputProvenance) != 0 {
		t.Fatalf("lookup stage invented field-object provenance: %#v", stage)
	}
	if _, valid := validInspectionLogicalPlan(projected); !valid {
		t.Fatal("projected explicit lookup failed result validation")
	}
	assertLookupProjectionRedacted(t, projected)
}

func TestProjectLogicalPlanProjectsAutomaticLookupAtGeneratedBoundary(t *testing.T) {
	t.Parallel()

	const source = "index=main status=200"
	authoredRange := spl.Range{
		Start: spl.Position{Offset: 0, Line: 1, Column: 1},
		End: spl.Position{
			Offset: len(source), Line: 1, Column: len(source) + 1,
		},
	}
	authored := &plan.Query{
		Operators: []plan.Operator{
			&plan.Scan{Indexes: []string{"main"}, Range: authoredRange},
			&plan.Filter{
				Expression: &plan.ComparisonExpression{
					Field: plan.FieldRef{Name: "status"},
					Value: plan.Value{Kind: plan.ValueKindString, String: "200"},
					Range: authoredRange,
				},
				Range: authoredRange,
			},
		},
		EffectiveIndexes: []string{"main"},
	}
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	admitted, err := plan.InjectKnowledgePrelude(authored, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	compiledSelector, err := knowledge.CompileSelector(knowledge.SelectorSpec{})
	if err != nil {
		t.Fatalf("CompileSelector: %v", err)
	}
	selector, err := knowledgeprogram.NewSelector(compiledSelector)
	if err != nil {
		t.Fatalf("NewSelector: %v", err)
	}
	key, err := plan.ResolveField("service_key", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service_key): %v", err)
	}
	output, err := plan.ResolveField("service_owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service_owner): %v", err)
	}
	logical, err := plan.InjectAutomaticLookupGroup(
		admitted,
		[]plan.AutomaticLookupSpec{{
			StableID: "sensitive-lookup-object-id",
			Lookup: plan.Lookup{
				DefinitionName: "sensitive_catalog",
				Keys: []plan.LookupKey{{
					LookupField: "secret_key",
					EventField:  key,
				}},
				Outputs: []plan.LookupOutput{{
					LookupField: "private_contact",
					EventField:  output,
				}},
				WriteMode: plan.LookupWriteModeOverwrite,
			},
			Selector: selector,
		}},
	)
	if err != nil {
		t.Fatalf("InjectAutomaticLookupGroup: %v", err)
	}

	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan: %v", err)
	}
	if got := []string{
		projected.Stages[0].Operator,
		projected.Stages[1].Operator,
		projected.Stages[2].Operator,
	}; !slices.Equal(got, []string{"Scan", "AutomaticLookupGroup", "Filter"}) {
		t.Fatalf("projected operator order = %v", got)
	}
	stage := projected.Stages[1]
	if stage.SourceRange != nil ||
		!slices.Equal(stage.InputFields, []string{"service_key"}) ||
		!slices.Equal(stage.OutputFields, []string{"service_owner"}) ||
		len(stage.KnowledgeObjects) != 0 || len(stage.OutputProvenance) != 0 {
		t.Fatalf("automatic lookup stage = %#v", stage)
	}
	if _, valid := validInspectionLogicalPlan(projected); !valid {
		t.Fatal("projected automatic lookup failed result validation")
	}
	assertLookupProjectionRedacted(t, projected)
}

func assertLookupProjectionRedacted(t *testing.T, projected LogicalPlan) {
	t.Helper()
	rendered := fmt.Sprintf("%#v", projected)
	for _, secret := range []string{
		"sensitive_catalog",
		"sensitive-lookup-object-id",
		"secret_key",
		"private_contact",
	} {
		if strings.Contains(rendered, secret) {
			t.Fatalf("lookup inspection projection leaked %q: %s", secret, rendered)
		}
	}
}
