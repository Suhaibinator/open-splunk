package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestInjectAutomaticLookupGroupCommitsPlacementAndRejectsForgery(t *testing.T) {
	t.Parallel()

	newInjected := func(t *testing.T) *Query {
		t.Helper()
		authored, err := Build(
			mustParse(t, `index=gradethis status=200`),
			testScope([]string{"gradethis"}, nil),
		)
		if err != nil {
			t.Fatalf("Build(): %v", err)
		}
		empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
		if err != nil {
			t.Fatalf("Prepare(empty): %v", err)
		}
		admitted, err := InjectKnowledgePrelude(authored, empty)
		if err != nil {
			t.Fatalf("InjectKnowledgePrelude(): %v", err)
		}
		compiledSelector, err := knowledge.CompileSelector(knowledge.SelectorSpec{})
		if err != nil {
			t.Fatalf("CompileSelector(): %v", err)
		}
		selector, err := knowledgeprogram.NewSelector(compiledSelector)
		if err != nil {
			t.Fatalf("NewSelector(): %v", err)
		}
		key, err := ResolveField("service", spl.Range{})
		if err != nil {
			t.Fatalf("ResolveField(service): %v", err)
		}
		output, err := ResolveField("owner", spl.Range{})
		if err != nil {
			t.Fatalf("ResolveField(owner): %v", err)
		}
		injected, err := InjectAutomaticLookupGroup(
			admitted,
			[]AutomaticLookupSpec{{
				StableID: "lookup-object-1",
				Lookup: Lookup{
					DefinitionName: "service_catalog",
					Keys: []LookupKey{{
						LookupField: "service_id",
						EventField:  key,
					}},
					Outputs: []LookupOutput{{
						LookupField: "owner",
						EventField:  output,
					}},
					WriteMode: LookupWriteModeOverwrite,
				},
				Selector: selector,
			}},
		)
		if err != nil {
			t.Fatalf("InjectAutomaticLookupGroup(): %v", err)
		}
		return injected
	}

	valid := newInjected(t)
	if names := []string{
		valid.Operators[0].LogicalName(),
		valid.Operators[1].LogicalName(),
		valid.Operators[2].LogicalName(),
	}; !slices.Equal(names, []string{"Scan", "AutomaticLookupGroup", "Filter"}) {
		t.Fatalf("operator order = %v", names)
	}
	if err := ValidateAutomaticLookupIntegrity(valid); err != nil {
		t.Fatalf("ValidateAutomaticLookupIntegrity(valid): %v", err)
	}
	if _, err := Analyze(valid); err != nil {
		t.Fatalf("Analyze(valid): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Query)
	}{
		{
			name: "unmarked",
			mutate: func(query *Query) {
				query.automaticLookups = queryAutomaticLookupAuthority{}
			},
		},
		{
			name: "marker without group",
			mutate: func(query *Query) {
				query.Operators = append(query.Operators[:1], query.Operators[2:]...)
			},
		},
		{
			name: "moved after authored filter",
			mutate: func(query *Query) {
				query.Operators[1], query.Operators[2] = query.Operators[2], query.Operators[1]
			},
		},
		{
			name: "duplicated",
			mutate: func(query *Query) {
				query.Operators = append(query.Operators, query.Operators[1])
			},
		},
		{
			name: "tampered entry",
			mutate: func(query *Query) {
				query.Operators[1].(*AutomaticLookupGroup).entries[0].stableID = "changed"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			forged := newInjected(t)
			test.mutate(forged)
			if err := ValidateAutomaticLookupIntegrity(forged); err == nil {
				t.Fatal("forged automatic authority was accepted")
			}
			if _, err := Analyze(forged); err == nil {
				t.Fatal("Analyze accepted forged automatic authority")
			}
		})
	}
}
