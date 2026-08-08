package plan

import (
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
)

func TestInjectKnowledgePreludePlacesExplicitImmutableOperators(t *testing.T) {
	program := testKnowledgeProgram(t)
	authored := testKnowledgeAuthoredQuery()
	authoredScan := authored.Operators[0].(*Scan)

	got, err := InjectKnowledgePrelude(authored, program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	wantNames := []string{
		"Scan",
		"ConditionalExtract",
		"ConditionalExtractJSON",
		"CopyFieldAlias",
		"ParallelExtend",
		"Filter",
	}
	names := make([]string, len(got.Operators))
	for index, operator := range got.Operators {
		names[index] = operator.LogicalName()
	}
	if !slices.Equal(names, wantNames) {
		t.Fatalf("operator names = %v, want %v", names, wantNames)
	}
	for _, operator := range got.Operators[1:5] {
		if sourceRange := operator.SourceRange(); sourceRange != (operatorRange(nil)) {
			t.Fatalf("%s source range = %#v, want zero", operator.LogicalName(), sourceRange)
		}
	}

	retained, ok := got.KnowledgePrelude()
	if !ok || !retained.Equal(program) {
		t.Fatal("query did not retain an equal knowledge-program authority")
	}
	// Every accessor returns detached mutable slices even though the semantic
	// wrappers themselves cannot be constructed outside knowledgeprogram.
	regex := got.Operators[1].(*ConditionalExtract)
	captures := regex.Extraction().Captures()
	captures[0] = knowledgeprogram.Capture{}
	if regex.Extraction().Captures()[0].Name() != "rex_out" {
		t.Fatal("regex capture accessor aliases operator state")
	}
	aliases := got.Operators[3].(*CopyFieldAlias)
	assignments := aliases.Assignments()
	assignments[0] = knowledgeprogram.Alias{}
	if aliases.Assignments()[0].Source() != "status" {
		t.Fatal("alias assignment accessor aliases operator state")
	}
	calculated := got.Operators[4].(*ParallelExtend)
	inputs := calculated.Assignments()[0].InputFields()
	inputs[0] = "mutated"
	if slices.Contains(calculated.Assignments()[0].InputFields(), "mutated") {
		t.Fatal("calculated input accessor aliases operator state")
	}

	// Injection clones all mutable query-header slices and leaves the authored
	// query untouched.
	authored.Operators[0] = &Limit{Count: 1}
	authoredScan.Indexes[0] = "mutated"
	authored.EffectiveIndexes[0] = "mutated"
	authored.OutputFields[0] = "mutated"
	authored.DynamicOutput.FixedFields[0] = "mutated"
	if got.Operators[0].LogicalName() != "Scan" || got.Operators[0].(*Scan).Indexes[0] != "main" ||
		got.EffectiveIndexes[0] != "main" || got.OutputFields[0] != "result" ||
		got.DynamicOutput.FixedFields[0] != "fixed" {
		t.Fatal("injected query aliases authored query header state")
	}
}

func TestKnowledgePreludeIntegrityRejectsDroppedReorderedAndSubstitutedPrefixes(t *testing.T) {
	program := testKnowledgeProgram(t)
	base, err := InjectKnowledgePrelude(testKnowledgeAuthoredQuery(), program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	base.DynamicOutput = nil
	if err := ValidateKnowledgePreludeIntegrity(base); err != nil {
		t.Fatalf("ValidateKnowledgePreludeIntegrity(valid): %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Query)
	}{
		{
			name: "dropped",
			mutate: func(query *Query) {
				query.Operators = append(
					append([]Operator(nil), query.Operators[:1]...),
					query.Operators[2:]...,
				)
			},
		},
		{
			name: "reordered",
			mutate: func(query *Query) {
				query.Operators[1], query.Operators[2] = query.Operators[2], query.Operators[1]
			},
		},
		{
			name: "substituted",
			mutate: func(query *Query) {
				query.Operators[1] = &Limit{Count: 1}
			},
		},
		{
			name: "duplicate outside prefix",
			mutate: func(query *Query) {
				query.Operators = append(query.Operators, query.Operators[1])
			},
		},
		{
			name: "marker removed",
			mutate: func(query *Query) {
				query.knowledgePrelude = queryKnowledgePrelude{}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := cloneQueryHeader(base)
			test.mutate(query)
			if err := ValidateKnowledgePreludeIntegrity(query); err == nil {
				t.Fatal("ValidateKnowledgePreludeIntegrity(mutated) succeeded")
			}
			if _, err := Analyze(query); err == nil {
				t.Fatal("Analyze(mutated) succeeded")
			}
			if count, ok := query.AuthoredScalarPredicateCount(); ok || count != 0 {
				t.Fatalf("AuthoredScalarPredicateCount(mutated) = (%d, %t)", count, ok)
			}
			if err := ValidateFieldAnalysisEligibility(query); err == nil {
				t.Fatal("ValidateFieldAnalysisEligibility(mutated) succeeded")
			}
			if err := ValidateTimelineEligibility(query); err == nil {
				t.Fatal("ValidateTimelineEligibility(mutated) succeeded")
			}
		})
	}
}

func TestKnowledgePreludeRemainsEligibleForFieldAnalysisAndTimeline(t *testing.T) {
	programs := []struct {
		name    string
		program knowledgeprogram.Program
	}{
		{name: "nonempty", program: testKnowledgeProgram(t)},
	}
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	programs = append(programs, struct {
		name    string
		program knowledgeprogram.Program
	}{name: "empty", program: empty})

	for _, test := range programs {
		t.Run(test.name, func(t *testing.T) {
			authored := testKnowledgeAuthoredQuery()
			authored.DynamicOutput = nil
			logical, err := InjectKnowledgePrelude(authored, test.program)
			if err != nil {
				t.Fatalf("InjectKnowledgePrelude: %v", err)
			}
			if err := ValidateFieldAnalysisEligibility(logical); err != nil {
				t.Fatalf("ValidateFieldAnalysisEligibility: %v", err)
			}
			if err := ValidateTimelineEligibility(logical); err != nil {
				t.Fatalf("ValidateTimelineEligibility: %v", err)
			}
		})
	}
}

func TestInjectKnowledgePreludeSealsValidEmptyAndRejectsDuplicates(t *testing.T) {
	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	authored := testKnowledgeAuthoredQuery()
	got, err := InjectKnowledgePrelude(authored, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(empty): %v", err)
	}
	if len(got.Operators) != len(authored.Operators) {
		t.Fatalf("empty operator count = %d, want %d", len(got.Operators), len(authored.Operators))
	}
	retained, ok := got.KnowledgePrelude()
	if !ok || !retained.Equal(empty) {
		t.Fatal("valid empty program marker was lost")
	}
	if _, err := InjectKnowledgePrelude(got, empty); err == nil {
		t.Fatal("duplicate program injection succeeded")
	}
	forged := testKnowledgeAuthoredQuery()
	forged.Operators = append(
		[]Operator{forged.Operators[0], &ConditionalExtract{}},
		forged.Operators[1:]...,
	)
	if _, err := InjectKnowledgePrelude(forged, empty); err == nil {
		t.Fatal("injection accepted a pre-existing forged knowledge operator")
	}
	if _, err := InjectKnowledgePrelude(authored, knowledgeprogram.Program{}); err == nil {
		t.Fatal("injection accepted the absent zero program")
	}
	if _, ok := (&Query{}).KnowledgePrelude(); ok {
		t.Fatal("legacy query reported a knowledge program")
	}
}

func TestAnalyzeKnowledgePreludeTracksInputsButNotKnowledgePredicates(t *testing.T) {
	authored := testKnowledgeAuthoredQuery()
	program := testKnowledgeProgram(t)
	logical, err := InjectKnowledgePrelude(authored, program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}

	analysis, err := Analyze(logical)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	want := []string{"_raw", "host", "index", "source", "sourcetype", "status"}
	if !slices.Equal(analysis.ReferencedFields, want) {
		t.Fatalf("referenced fields = %v, want %v", analysis.ReferencedFields, want)
	}
	if count, ok := logical.AuthoredScalarPredicateCount(); !ok || count != 1 {
		t.Fatalf("authored predicate evidence = (%d, %t), want (1, true)", count, ok)
	}

	for _, operator := range []Operator{
		&ConditionalExtract{},
		&ConditionalExtractJSON{},
		&CopyFieldAlias{},
		&ParallelExtend{},
	} {
		if _, err := Analyze(&Query{Operators: []Operator{operator}}); err == nil {
			t.Fatalf("Analyze accepted forged %T", operator)
		}
	}
}

func testKnowledgeAuthoredQuery() *Query {
	return &Query{
		Operators: []Operator{
			&Scan{Indexes: []string{"main"}},
			&Filter{Expression: &EvalComparisonExpression{
				Left:  &ScalarFieldExpression{Field: FieldRef{Name: "source"}},
				Op:    ComparisonOpEqual,
				Right: &ScalarLiteralExpression{Value: Value{Kind: ValueKindString, String: "api"}},
			}},
		},
		EffectiveIndexes:     []string{"main"},
		OutputFields:         []string{"result"},
		DynamicOutput:        &DynamicSeriesOutput{FixedFields: []string{"fixed"}, MaxSeries: 1},
		parsedEvalPredicates: 1,
		parsedSPL:            true,
	}
}

func testKnowledgeProgram(t *testing.T) knowledgeprogram.Program {
	t.Helper()
	definitions := []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "a-regex", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Selector: testKnowledgeSelector("index", "main"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: `(?P<rex_out>x)`, OutputFields: []string{"rex_out"},
				}},
			}},
		},
		{
			AppId: "app", Name: "b-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
					Path: "payload.id", OutputField: "json_out",
				}},
			}},
		},
		{
			AppId: "app", Name: "alias", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Selector: testKnowledgeSelector("host", "web*"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunkv1.FieldAliasDefinition{
				SourceField: "status", DestinationField: "status_copy",
			}},
		},
		{
			AppId: "app", Name: "calculated", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
			Selector: testKnowledgeSelector("source", "api"),
			Body: &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
				DestinationField: "calculated_out",
				Expression:       `if(source="api", sourcetype, host)`,
			}},
		},
	}
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := map[opensplunkv1.KnowledgeSearchStage]uint32{}
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(%s): %v", definition.GetName(), err)
		}
		stage := opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
		switch normalized.ObjectType {
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
		case opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
			stage = opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "ko-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	return program
}

func testKnowledgeSelector(dimension, value string) *opensplunkv1.KnowledgeSelector {
	pattern := []*opensplunkv1.KnowledgeSelectorPattern{{Value: value}}
	selector := &opensplunkv1.KnowledgeSelector{}
	switch dimension {
	case "index":
		selector.IndexPatterns = pattern
	case "host":
		selector.HostPatterns = pattern
	case "source":
		selector.SourcePatterns = pattern
	}
	return selector
}
