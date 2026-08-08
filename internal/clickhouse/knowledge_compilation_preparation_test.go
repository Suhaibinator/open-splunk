package clickhouse

import (
	"errors"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
)

func TestPrepareKnowledgeCompilationDistinguishesLegacyAndPresentEmpty(t *testing.T) {
	t.Parallel()

	logical := buildPlan(t,
		`index=gradethis | rex field=_raw "(?<word>[a-z]+)" `+
			`| spath input=_raw output=selected path=payload.value `+
			`| eval status=if(isnull(selected), "missing", "present") `+
			`| where status="present"`,
	)
	legacy, err := prepareKnowledgeCompilation(logical)
	if err != nil {
		t.Fatalf("prepareKnowledgeCompilation(legacy): %v", err)
	}
	pattern, err := splregex.CompileExtractionPattern(`(?<word>[a-z]+)`)
	if err != nil {
		t.Fatalf("CompileExtractionPattern: %v", err)
	}
	steps, err := splpath.ParseJSON("payload.value")
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	if legacy.present || legacy.prefixLength != 0 || len(legacy.operatorKinds) != 0 ||
		!legacy.program.IsZero() || legacy.programCharges != (knowledgeprogram.Charges{}) ||
		legacy.authored.regexPrograms != 1 ||
		legacy.authored.regexWorkUnits != uint64(pattern.ProgramWorkUnits) ||
		legacy.authored.extractionOutputs != 2 ||
		legacy.authored.jsonEvaluationWork != uint32(splpath.EvaluationWorkUnits(steps)) ||
		!legacy.authoredScalarPredicatesExact || legacy.authoredScalarPredicates != 2 {
		t.Fatalf("legacy preparation = %#v", legacy)
	}

	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare(empty): %v", err)
	}
	sealed, err := plan.InjectKnowledgePrelude(logical, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude(empty): %v", err)
	}
	presentEmpty, err := prepareKnowledgeCompilation(sealed)
	if err != nil {
		t.Fatalf("prepareKnowledgeCompilation(present empty): %v", err)
	}
	wantCommitment, ok := empty.Commitment()
	if !ok {
		t.Fatal("empty program has no commitment")
	}
	if !presentEmpty.present || presentEmpty.prefixLength != 0 ||
		len(presentEmpty.operatorKinds) != 0 || !presentEmpty.program.IsEmpty() ||
		!presentEmpty.program.Equal(empty) || presentEmpty.programCommitment != wantCommitment ||
		presentEmpty.programCharges != empty.Charges() || presentEmpty.authored != legacy.authored ||
		presentEmpty.authoredScalarPredicates != legacy.authoredScalarPredicates ||
		!presentEmpty.authoredScalarPredicatesExact {
		t.Fatalf("present-empty preparation = %#v", presentEmpty)
	}
}

func TestPrepareKnowledgeCompilationReturnsExactPrefixAndAuthoredSuffix(t *testing.T) {
	t.Parallel()

	program := knowledgePreparationProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "a-regex", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: `(?P<knowledge_word>[a-z]+)`, OutputFields: []string{"knowledge_word"},
				}},
			}},
		},
		{
			AppId: "app", Name: "b-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
					Path: "knowledge.value", OutputField: "knowledge_value",
				}},
			}},
		},
	})
	logical := buildPlan(t,
		`index=gradethis | rex field=_raw "(?<authored_word>[0-9]+)" `+
			`| spath input=_raw output=authored_value path=payload.value `+
			`| where authored_value="selected"`,
	)
	logical, err := plan.InjectKnowledgePrelude(logical, program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}

	preparation, err := prepareKnowledgeCompilation(logical)
	if err != nil {
		t.Fatalf("prepareKnowledgeCompilation: %v", err)
	}
	wantKinds := program.OperatorKinds()
	wantCommitment, _ := program.Commitment()
	if !preparation.present || preparation.prefixLength != len(wantKinds) ||
		!slices.Equal(preparation.operatorKinds, wantKinds) ||
		!preparation.program.Equal(program) || preparation.programCharges != program.Charges() ||
		preparation.programCommitment != wantCommitment ||
		preparation.authored.regexPrograms != 1 || preparation.authored.extractionOutputs != 2 ||
		preparation.authored.jsonEvaluationWork == 0 ||
		!preparation.authoredScalarPredicatesExact || preparation.authoredScalarPredicates != 1 {
		t.Fatalf("preparation = %#v", preparation)
	}
	if _, ok := logical.Operators[1].(*plan.ConditionalExtract); !ok {
		t.Fatalf("operator 1 = %T, want *plan.ConditionalExtract", logical.Operators[1])
	}
	if _, ok := logical.Operators[2].(*plan.ConditionalExtractJSON); !ok {
		t.Fatalf("operator 2 = %T, want *plan.ConditionalExtractJSON", logical.Operators[2])
	}

	preparation.operatorKinds[0] = knowledgeprogram.OperatorInvalid
	if !slices.Equal(program.OperatorKinds(), wantKinds) {
		t.Fatal("prepared operator sequence aliases the program")
	}
	fresh, err := prepareKnowledgeCompilation(logical)
	if err != nil || !slices.Equal(fresh.operatorKinds, wantKinds) {
		t.Fatalf("fresh preparation after caller mutation = (%#v, %v)", fresh, err)
	}
}

func TestPrepareKnowledgeCompilationRejectsMalformedPrefixes(t *testing.T) {
	t.Parallel()

	program := knowledgePreparationProgram(t, []*opensplunkv1.KnowledgeObjectDefinition{
		{
			AppId: "app", Name: "a-regex", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: `(?P<first>x)`, OutputFields: []string{"first"},
				}},
			}},
		},
		{
			AppId: "app", Name: "b-json", SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			Body: &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
				InputField: "_raw",
				Extraction: &opensplunkv1.FieldExtractionDefinition_Json{Json: &opensplunkv1.JsonFieldExtractionDefinition{
					Path: "payload.value", OutputField: "second",
				}},
			}},
		},
	})
	base, err := plan.InjectKnowledgePrelude(buildPlan(t, `index=gradethis | where status=200`), program)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	clone := func() *plan.Query {
		result := *base
		result.Operators = slices.Clone(base.Operators)
		return &result
	}
	tests := []struct {
		name   string
		mutate func(*plan.Query)
	}{
		{
			name: "reordered",
			mutate: func(query *plan.Query) {
				query.Operators[1], query.Operators[2] = query.Operators[2], query.Operators[1]
			},
		},
		{
			name: "substituted",
			mutate: func(query *plan.Query) {
				query.Operators[1] = &plan.Limit{Count: 1}
			},
		},
		{
			name: "dropped",
			mutate: func(query *plan.Query) {
				query.Operators = append(query.Operators[:1:1], query.Operators[2:]...)
			},
		},
		{
			name: "knowledge operator after prefix",
			mutate: func(query *plan.Query) {
				query.Operators = append(query.Operators, query.Operators[1])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query := clone()
			test.mutate(query)
			if _, err := prepareKnowledgeCompilation(query); err == nil {
				t.Fatal("prepareKnowledgeCompilation(mutated) succeeded")
			}
		})
	}

	legacyWithForgedPrefix := &plan.Query{Operators: []plan.Operator{
		&plan.Scan{},
		&plan.ConditionalExtract{},
	}}
	if _, err := prepareKnowledgeCompilation(legacyWithForgedPrefix); err == nil {
		t.Fatal("prepareKnowledgeCompilation(legacy forged prefix) succeeded")
	}
}

func TestValidateSharedKnowledgeCompilationBudgets(t *testing.T) {
	t.Parallel()

	if err := validateSharedKnowledgeCompilationBudgets(
		knowledgeprogram.Charges{
			RegexPrograms:      knowledgeprogram.MaximumRegexPrograms - 1,
			RegexWorkUnits:     knowledgeprogram.MaximumRegexWorkUnits - 1,
			ExtractionOutputs:  knowledgeprogram.MaximumExtractionOutputs - 1,
			JSONEvaluationWork: knowledgeprogram.MaximumJSONEvaluationWork - 1,
			ScalarPredicates:   knowledgeprogram.MaximumScalarPredicates - 1,
		},
		authoredKnowledgeCompilation{
			regexPrograms:      1,
			regexWorkUnits:     1,
			extractionOutputs:  1,
			jsonEvaluationWork: 1,
		},
		1,
	); err != nil {
		t.Fatalf("exact shared ceilings: %v", err)
	}

	tests := []struct {
		name       string
		program    knowledgeprogram.Charges
		authored   authoredKnowledgeCompilation
		predicates uint32
	}{
		{
			name:     "regex programs",
			program:  knowledgeprogram.Charges{RegexPrograms: knowledgeprogram.MaximumRegexPrograms},
			authored: authoredKnowledgeCompilation{regexPrograms: 1},
		},
		{
			name:     "regex work",
			program:  knowledgeprogram.Charges{RegexWorkUnits: knowledgeprogram.MaximumRegexWorkUnits},
			authored: authoredKnowledgeCompilation{regexWorkUnits: 1},
		},
		{
			name:     "extraction outputs",
			program:  knowledgeprogram.Charges{ExtractionOutputs: knowledgeprogram.MaximumExtractionOutputs},
			authored: authoredKnowledgeCompilation{extractionOutputs: 1},
		},
		{
			name:     "JSON work",
			program:  knowledgeprogram.Charges{JSONEvaluationWork: knowledgeprogram.MaximumJSONEvaluationWork},
			authored: authoredKnowledgeCompilation{jsonEvaluationWork: 1},
		},
		{
			name:       "predicate leaves",
			program:    knowledgeprogram.Charges{ScalarPredicates: knowledgeprogram.MaximumScalarPredicates},
			predicates: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSharedKnowledgeCompilationBudgets(test.program, test.authored, test.predicates)
			var diagnostic *plan.Diagnostic
			if !errors.As(err, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
				t.Fatalf("overflow error = %v, want SPL_QUERY_TOO_COMPLEX", err)
			}
		})
	}
}

func TestPrepareKnowledgeCompilationRequiresExactAuthoredPredicateEvidence(t *testing.T) {
	t.Parallel()

	empty, err := knowledgeprogram.Prepare(knowledgeprogram.Input{})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare(empty): %v", err)
	}
	manual, err := plan.InjectKnowledgePrelude(&plan.Query{Operators: []plan.Operator{&plan.Scan{}}}, empty)
	if err != nil {
		t.Fatalf("InjectKnowledgePrelude: %v", err)
	}
	if _, err := prepareKnowledgeCompilation(manual); err == nil {
		t.Fatal("prepareKnowledgeCompilation(manual prelude) succeeded")
	}
}

func knowledgePreparationProgram(
	t *testing.T,
	definitions []*opensplunkv1.KnowledgeObjectDefinition,
) knowledgeprogram.Program {
	t.Helper()
	objects := make([]*opensplunkv1.KnowledgeSnapshotObject, len(definitions))
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("Normalize(%d): %v", index, err)
		}
		objects[index] = &opensplunkv1.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
			StageOrdinal:      uint32(index),
			KnowledgeObjectId: "object-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  slices.Clone(normalized.Digest[:]),
		}
	}
	program, err := knowledgeprogram.Prepare(knowledgeprogram.Input{Objects: objects})
	if err != nil {
		t.Fatalf("knowledgeprogram.Prepare: %v", err)
	}
	return program
}
