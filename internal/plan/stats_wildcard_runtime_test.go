package plan

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestPrepareStatsWildcardDefersOpenSchemaWithExactPrefixProvenance(t *testing.T) {
	t.Parallel()

	parsed := mustParse(t, `index=gradethis | where isnotnull(bytes) | stats sum(*) | where 'sum(bytes)'>0`)
	preparation, err := PrepareStatsWildcard(
		parsed,
		testScope([]string{"gradethis"}, nil),
	)
	if err != nil {
		t.Fatalf("PrepareStatsWildcard(): %v", err)
	}
	if preparation.FullPlan() != nil {
		t.Fatal("open-schema wildcard unexpectedly produced a full plan")
	}
	prefix := preparation.Prefix()
	request := preparation.Request()
	if prefix == nil || request.IsZero() || !request.ValidForPrefix(prefix) {
		t.Fatalf("preparation = prefix %#v request zero=%t", prefix, request.IsZero())
	}
	if len(prefix.Operators) != 3 {
		t.Fatalf("prefix operators = %#v, want scan/search-filter/where-filter", prefix.Operators)
	}
	if predicates, ok := prefix.AuthoredScalarPredicateCount(); !ok || predicates != 1 {
		t.Fatalf("prefix predicate provenance = (%d, %t), want 1/true", predicates, ok)
	}
	patterns := request.Patterns()
	if request.MaximumPairs() != 17 || len(patterns) != 1 ||
		patterns[0].Ordinal != 0 || patterns[0].Pattern != "*" {
		t.Fatalf("request = max %d patterns %#v", request.MaximumPairs(), patterns)
	}
	patterns[0].Pattern = "mutated*"
	if request.Patterns()[0].Pattern != "*" {
		t.Fatal("request pattern accessor aliases retained authority")
	}

	expansion, err := ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{
		{Ordinal: 0, Field: "bytes"},
		{Ordinal: 0, Field: "latency"},
	})
	if err != nil {
		t.Fatalf("ValidateStatsWildcardInventory(): %v", err)
	}
	logical, err := BuildWithStatsWildcardExpansion(
		parsed,
		testScope([]string{"gradethis"}, nil),
		expansion,
	)
	if err != nil {
		t.Fatalf("BuildWithStatsWildcardExpansion(): %v", err)
	}
	if !slices.Equal(logical.OutputFields, []string{"sum(bytes)", "sum(latency)"}) {
		t.Fatalf("output fields = %v", logical.OutputFields)
	}
	if predicates, ok := logical.AuthoredScalarPredicateCount(); !ok || predicates != 2 {
		t.Fatalf("full predicate provenance = (%d, %t), want 2/true", predicates, ok)
	}
}

func TestPrepareStatsWildcardPrefixRetainsEvalPredicateProvenanceOnlyThroughCut(t *testing.T) {
	t.Parallel()

	parsed := mustParse(t, `index=gradethis | eval class=if(isnull(bytes),"missing","present") | stats avg(*) | where 'avg(bytes)'>0`)
	preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	prefix := preparation.Prefix()
	if prefix == nil {
		t.Fatal("prefix is nil")
	}
	if predicates, ok := prefix.AuthoredScalarPredicateCount(); !ok || predicates != 1 {
		t.Fatalf("prefix predicate provenance = (%d, %t), want 1/true", predicates, ok)
	}
	expansion, err := ValidateStatsWildcardInventory(
		preparation.Request(),
		[]StatsWildcardInventoryMatch{{Ordinal: 0, Field: "bytes"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := BuildWithStatsWildcardExpansion(
		parsed,
		testScope([]string{"gradethis"}, nil),
		expansion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if predicates, ok := logical.AuthoredScalarPredicateCount(); !ok || predicates != 2 {
		t.Fatalf("full predicate provenance = (%d, %t), want 2/true", predicates, ok)
	}
}

func TestStatsWildcardExpansionRejectsWrongScopeOrderOverflowAndMissingPatterns(t *testing.T) {
	t.Parallel()

	parsed := mustParse(t, `index=gradethis | stats avg(*lay) sum(bytes*)`)
	scope := testScope([]string{"gradethis"}, nil)
	preparation, err := PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	patterns := request.Patterns()
	if len(patterns) != 2 || request.MaximumPairs() != 17 {
		t.Fatalf("request = max %d patterns %#v", request.MaximumPairs(), patterns)
	}
	_, err = ValidateStatsWildcardInventory(request, nil)
	assertDiagnosticCode(t, err, "SPL_NO_MATCHING_STATS_FIELDS")

	_, err = ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{
		{Ordinal: 1, Field: "bytes_read"},
		{Ordinal: 0, Field: "delay"},
	})
	assertDiagnosticCode(t, err, "SPL_INVALID_QUERY")

	_, err = ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{
		{Ordinal: 0, Field: "delay"},
	})
	assertDiagnosticCode(t, err, "SPL_NO_MATCHING_STATS_FIELDS")

	overflow := make([]StatsWildcardInventoryMatch, request.MaximumPairs())
	for index := range overflow {
		overflow[index] = StatsWildcardInventoryMatch{Ordinal: 0, Field: "a"}
	}
	_, err = ValidateStatsWildcardInventory(request, overflow)
	assertDiagnosticCode(t, err, "SPL_QUERY_TOO_COMPLEX")

	expansion, err := ValidateStatsWildcardInventory(request, []StatsWildcardInventoryMatch{
		{Ordinal: 0, Field: "delay"},
		{Ordinal: 1, Field: "bytes_read"},
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongScope := scope
	visibility := *wrongScope.VisibilityCutoff + 1
	wrongScope.VisibilityCutoff = &visibility
	_, err = BuildWithStatsWildcardExpansion(parsed, wrongScope, expansion)
	assertDiagnosticCode(t, err, "SPL_INVALID_QUERY")
}

func TestStatsWildcardRuntimeExpansionPreservesLiteralInputsAndDerivedOutputs(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		source     string
		wantFields []string
	}{
		{
			name:       "default outputs",
			source:     `index=gradethis | stats avg(*)`,
			wantFields: []string{"avg(.com)", "avg(Product Name)"},
		},
		{
			name:       "wildcard AS outputs",
			source:     `index=gradethis | stats avg(*) AS out_*`,
			wantFields: []string{"out_.com", "out_Product Name"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed := mustParse(t, test.source)
			preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
			if err != nil {
				t.Fatal(err)
			}
			expansion, err := ValidateStatsWildcardInventory(
				preparation.Request(),
				[]StatsWildcardInventoryMatch{
					{Ordinal: 0, Field: ".com"},
					{Ordinal: 0, Field: "Product Name"},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			logical, err := BuildWithStatsWildcardExpansion(
				parsed,
				testScope([]string{"gradethis"}, nil),
				expansion,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(logical.OutputFields, test.wantFields) {
				t.Fatalf("output fields = %v, want %v", logical.OutputFields, test.wantFields)
			}
			aggregate := logical.Operators[len(logical.Operators)-1].(*Aggregate)
			for index, input := range []string{".com", "Product Name"} {
				if aggregate.Measures[index].Input.Name != input ||
					!aggregate.Measures[index].OutputLiteral {
					t.Fatalf("measure[%d] = %#v", index, aggregate.Measures[index])
				}
			}
		})
	}
}

func TestStatsWildcardPrefixAuthorityRejectsPublicASTAndPlanMutation(t *testing.T) {
	scope := testScope([]string{"gradethis"}, nil)
	for _, test := range []struct {
		name   string
		mutate func(*spl.Query)
	}{
		{
			name: "wildcard pattern",
			mutate: func(query *spl.Query) {
				query.Commands[1].(*spl.StatsCommand).Aggregates[0].InputGlob.Pattern = "delay*"
			},
		},
		{
			name: "prefix eval",
			mutate: func(query *spl.Query) {
				query.Commands[0].(*spl.EvalCommand).Assignments[0].Field = "changed"
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed := mustParse(t, `index=gradethis | eval latency=bytes | stats sum(*)`)
			test.mutate(parsed)
			if preparation, err := PrepareStatsWildcard(parsed, scope); err == nil || preparation != nil {
				t.Fatalf("PrepareStatsWildcard(mutated AST) = (%#v, %v), want rejection", preparation, err)
			}
		})
	}

	parsed := mustParse(t, `index=gradethis | where isnotnull(bytes) | stats sum(*)`)
	preparation, err := PrepareStatsWildcard(parsed, scope)
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	if request.IsZero() {
		t.Fatal("request is zero")
	}
	mutations := []struct {
		name   string
		mutate func(*Query)
	}{
		{"append filter", func(query *Query) {
			query.Operators = append(query.Operators, &Filter{Expression: &TextExpression{Value: "forged"}})
		}},
		{"append project", func(query *Query) {
			query.Operators = append(query.Operators, &Project{Mode: ProjectModeTable, Fields: []FieldRef{{Name: "bytes"}}})
		}},
		{"append extend", func(query *Query) {
			query.Operators = append(query.Operators, &Extend{Assignments: []ExtendAssignment{{
				Output:     FieldRef{Name: "forged"},
				Expression: &ScalarLiteralExpression{Value: Value{Kind: ValueKindInt64, Int64: 1}},
			}}})
		}},
		{"mutate filter", func(query *Query) {
			query.Operators[2].(*Filter).Expression = &TextExpression{Value: "forged"}
		}},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			prefix := preparation.Prefix()
			if prefix == nil {
				t.Fatal("Prefix() is nil")
			}
			test.mutate(prefix)
			if request.ValidForPrefix(prefix) {
				t.Fatal("mutated prefix retained request authority")
			}
		})
	}

	first := preparation.Prefix()
	first.Operators[0].(*Scan).Indexes[0] = "poisoned"
	first.Operators[2].(*Filter).Expression = &TextExpression{Value: "poisoned"}
	second := preparation.Prefix()
	if second == nil || second.Operators[0].(*Scan).Indexes[0] != "gradethis" ||
		!request.ValidForPrefix(second) {
		t.Fatal("Prefix accessor aliases retained preparation")
	}
}

func TestStatsWildcardFullPlanAccessorIsDetached(t *testing.T) {
	parsed := mustParse(t, `index=gradethis | stats sum(bytes)`)
	preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	first := preparation.FullPlan()
	if first == nil || !preparation.Request().IsZero() || preparation.Prefix() != nil {
		t.Fatalf("closed preparation = full %#v request zero=%t prefix %#v", first, preparation.Request().IsZero(), preparation.Prefix())
	}
	first.Operators[0].(*Scan).Indexes[0] = "poisoned"
	aggregate := first.Operators[len(first.Operators)-1].(*Aggregate)
	aggregate.Measures[0].Output = "poisoned"

	second := preparation.FullPlan()
	if second == nil || second.Operators[0].(*Scan).Indexes[0] != "gradethis" ||
		second.OutputFields[0] != "sum(bytes)" {
		t.Fatalf("FullPlan accessor aliases retained preparation: %#v", second)
	}
	secondAggregate := second.Operators[len(second.Operators)-1].(*Aggregate)
	if secondAggregate.Measures[0].Output != "sum(bytes)" {
		t.Fatalf("detached aggregate output = %q", secondAggregate.Measures[0].Output)
	}
}

func TestStatsWildcardSemanticAuthorityAcceptsAnalyzeValidDeepPrefix(t *testing.T) {
	expression := Expression(&TextExpression{Value: "leaf"})
	// Filter starts expression analysis at depth two and TextExpression charges
	// its canonical _raw field one level deeper. This reaches, but does not
	// exceed, the ordinary planner depth boundary.
	for range maximumAnalysisDepth - 3 {
		expression = &NotExpression{Operand: expression}
	}
	query := &Query{Operators: []Operator{
		&Scan{},
		&Filter{Expression: expression},
	}}
	if _, err := Analyze(query); err != nil {
		t.Fatalf("Analyze(near-limit prefix): %v", err)
	}
	digest, ok := statsWildcardPlanSemanticDigest(query)
	if !ok {
		t.Fatal("semantic authority rejected an Analyze-valid near-limit prefix")
	}
	expression.(*NotExpression).Operand = &TextExpression{Value: "changed"}
	changed, ok := statsWildcardPlanSemanticDigest(query)
	if !ok || changed == digest {
		t.Fatal("near-limit prefix mutation retained semantic authority")
	}
}

func TestStatsWildcardPrefixAuthorityAcceptsOnlyExactKnowledgeInjection(t *testing.T) {
	parsed := mustParse(t, `index=gradethis | where isnotnull(bytes) | stats sum(*)`)
	preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	prefix := preparation.Prefix()
	injected, err := InjectKnowledgePrelude(prefix, testKnowledgeProgram(t))
	if err != nil {
		t.Fatal(err)
	}
	if !request.ValidForPrefix(injected) {
		t.Fatal("valid regex/JSON/alias/calculated knowledge injection was rejected")
	}
	canonical, ok := request.CanonicalPrefixFor(injected)
	if !ok || canonical == nil || !request.ValidForPrefix(canonical) {
		t.Fatal("canonical knowledge prefix was not accepted")
	}

	mutated := cloneQueryHeader(injected)
	mutated.Operators = slices.Clone(injected.Operators)
	mutated.Operators[1] = &ConditionalExtract{}
	if request.ValidForPrefix(mutated) {
		t.Fatal("mutated generated knowledge operator retained request authority")
	}
}

func TestStatsWildcardInventoryRejectsReservedRuntimeRoots(t *testing.T) {
	parsed := mustParse(t, `index=gradethis | stats sum(*)`)
	preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	for _, field := range []string{"tenant_id", "body.foo", "host.child", "__os_private"} {
		if _, err := ValidateStatsWildcardInventory(
			request,
			[]StatsWildcardInventoryMatch{{Ordinal: 0, Field: field}},
		); err == nil {
			t.Fatalf("reserved inventory field %q was accepted", field)
		}
	}
	for _, field := range []string{"host", ".com", "Product Name"} {
		if _, err := ValidateStatsWildcardInventory(
			request,
			[]StatsWildcardInventoryMatch{{Ordinal: 0, Field: field}},
		); err != nil {
			t.Fatalf("safe inventory field %q rejected: %v", field, err)
		}
	}
}

func TestStatsWildcardRequestSealBindsPrivateScopeAndBasePrefix(t *testing.T) {
	parsed := mustParse(t, `index=gradethis | where isnotnull(bytes) | stats sum(*)`)
	preparation, err := PrepareStatsWildcard(parsed, testScope([]string{"gradethis"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	request := preparation.Request()
	if request.IsZero() {
		t.Fatal("request is zero")
	}
	if bytes, ok := request.RetainedBytes(); !ok || bytes <= uint64(len(request.authoredSource)) {
		t.Fatalf("RetainedBytes() = (%d, %t)", bytes, ok)
	}

	mutatedScope := request.Clone()
	mutatedScope.scope.TenantID = "other"
	if _, ok := mutatedScope.AuthorityDigest(); mutatedScope.valid() || ok {
		t.Fatal("same-seal different scope retained authority")
	}
	mutatedBase := request.Clone()
	mutatedBase.basePrefix[0] ^= 0xff
	if _, ok := mutatedBase.AuthorityDigest(); mutatedBase.valid() || ok {
		t.Fatal("same-seal different base prefix retained authority")
	}

	detached := request.Clone()
	detached.scope.AuthorizedIndexes[0] = "mutated"
	*detached.scope.VisibilityCutoff = 999
	if request.scope.AuthorizedIndexes[0] != "gradethis" || *request.scope.VisibilityCutoff == 999 {
		t.Fatal("request Clone aliases retained scope")
	}
}
