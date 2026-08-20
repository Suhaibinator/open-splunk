package clickhouse

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/plan"
)

func TestCompileKnowledgeRelation(t *testing.T) {
	program := knowledgePreludeProgram(t, []*opensplunk.KnowledgeObjectDefinition{
		knowledgeJSONStageDefinition("a-json", "json_value", "payload.value", "east"),
		knowledgeRegexStageDefinition(
			"b-regex",
			`(?P<regex_value>[a-z]+)`,
			[]string{"regex_value"},
			"west",
		),
		knowledgePreludeAliasDefinition("c-alias", "host", "alias_value"),
		knowledgePreludeCalculatedDefinition("d-calculated", "calculated_value", "lower(source)"),
	})
	preparation := knowledgePreludePreparationForTest(program)
	input := compiledRelation{sql: `SELECT ? AS "tenant", ? AS "seed"`, depth: 3}
	seed := []string{"original"}
	existingArgs := []any{
		compiledReadScopeArgument{ordinal: 0, value: "tenant"},
		seed,
	}
	compiled, err := compileKnowledgeRelation(
		input,
		knowledgeExtractionStageState(),
		existingArgs,
		preparation,
	)
	if err != nil {
		t.Fatalf("compile knowledge relation: %v", err)
	}
	if len(compiled.prelude.stages) != 3 ||
		compiled.relation.depth != input.depth+2*len(compiled.prelude.stages)+2 ||
		compiled.relation.ownerRange != input.ownerRange ||
		strings.Count(compiled.relation.sql, " ARRAY JOIN ") != len(compiled.prelude.stages) ||
		strings.Count(compiled.relation.sql, "?") != len(compiled.args) ||
		!strings.HasSuffix(compiled.relation.sql, materializedCTESettingsSQL) ||
		len(compiled.relation.sql) > maxCompiledQueryBytes {
		t.Fatalf("compiled relation shape = %#v\n%s", compiled.relation, compiled.relation.sql)
	}
	if !reflect.DeepEqual(compiled.state, compiled.prelude.state) ||
		compiled.state.rexCapturedBytesSQL == "" {
		t.Fatalf("compiled relation state = %#v", compiled.state)
	}

	wantArgs := []any{
		compiledReadScopeArgument{ordinal: 0, value: "tenant"},
		[]string{"original"},
	}
	for _, stage := range compiled.prelude.stages {
		wantArgs = append(wantArgs, stage.suffixArgs...)
	}
	if !reflect.DeepEqual(compiled.args, wantArgs) {
		t.Fatalf("compiled args = %#v, want %#v", compiled.args, wantArgs)
	}

	expected := input
	for _, stage := range compiled.prelude.stages {
		inputAlias := quoteIdentifier("__os_ko_stage_input_" +
			strconv.Itoa(stage.operatorOffset))
		boundAlias := quoteIdentifier("__os_ko_stage_bound_" +
			strconv.Itoa(stage.operatorOffset))
		bindingSQL := "SELECT " + strings.Join(stage.bindingProjection, ", ") +
			" FROM (" + expected.sql + ") AS " + inputAlias + " ARRAY JOIN " +
			strings.Join(stage.arrayJoinBindings, ", ")
		expected = expected.selectFrom(bindingSQL, input.ownerRange)
		projectionSQL := "SELECT " + strings.Join(stage.projection, ", ") +
			" FROM (" + expected.sql + ") AS " + boundAlias
		expected = expected.selectFrom(projectionSQL, input.ownerRange)
	}
	expectedGuard, err := compileKnowledgeRuntimeGuard(expected, compiled.prelude, preparation)
	if err != nil {
		t.Fatalf("compile expected guard: %v", err)
	}
	if compiled.relation != expectedGuard.relation {
		t.Fatalf("physical stages differ:\n got: %s\nwant: %s", compiled.relation.sql, expectedGuard.relation.sql)
	}

	seed[0] = "mutated-input"
	if compiled.args[1].([]string)[0] != "original" {
		t.Fatal("compiled arguments alias the input arguments")
	}
	mutatedSuffix := false
	argumentOffset := len(existingArgs)
	for stageIndex := range compiled.prelude.stages {
		stage := &compiled.prelude.stages[stageIndex]
		for argumentIndex, argument := range stage.suffixArgs {
			values, ok := argument.([]string)
			if !ok || len(values) == 0 {
				argumentOffset++
				continue
			}
			compiledValues := compiled.args[argumentOffset].([]string)
			compiledValues[0] = "mutated-result"
			if stage.suffixArgs[argumentIndex].([]string)[0] == "mutated-result" {
				t.Fatal("result arguments alias retained prelude authority")
			}
			mutatedSuffix = true
			break
		}
		if mutatedSuffix {
			break
		}
	}
	if !mutatedSuffix {
		t.Fatal("fixture has no mutable stage argument")
	}
	delete(compiled.state.visible, "host")
	if _, retained := compiled.prelude.state.visible["host"]; !retained {
		t.Fatal("result state aliases retained prelude state")
	}

	t.Run("absent and empty identity", func(t *testing.T) {
		identityInput := compiledRelation{sql: "SELECT 1", depth: 1}
		absent, compileErr := compileKnowledgeRelation(
			identityInput,
			compileState{},
			nil,
			preparedKnowledgeCompilation{},
		)
		if compileErr != nil || absent.relation != identityInput || absent.args != nil ||
			absent.prelude.present {
			t.Fatalf("absent identity = %#v, %v", absent, compileErr)
		}
		emptyProgram, prepareErr := knowledgeprogram.Prepare(knowledgeprogram.Input{})
		if prepareErr != nil {
			t.Fatalf("prepare empty program: %v", prepareErr)
		}
		emptyPreparation := knowledgePreludePreparationForTest(emptyProgram)
		emptyArgs := make([]any, 0)
		empty, compileErr := compileKnowledgeRelation(
			identityInput,
			knowledgeExtractionStageState(),
			emptyArgs,
			emptyPreparation,
		)
		if compileErr != nil || empty.relation != identityInput || !empty.prelude.present ||
			empty.prelude.proof.charges != (knowledgeprogram.Charges{}) ||
			empty.args == nil || len(empty.args) != 0 {
			t.Fatalf("present-empty identity = %#v, %v", empty, compileErr)
		}
	})

	t.Run("rejects malformed input before lowering", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			relation compiledRelation
			args     []any
		}{
			{"empty relation", compiledRelation{}, nil},
			{"placeholder mismatch", compiledRelation{sql: "SELECT ?", depth: 1}, nil},
			{"unsupported argument", compiledRelation{sql: "SELECT ?", depth: 1}, []any{map[string]string{"x": "y"}}},
			{"SQL bound", compiledRelation{sql: strings.Repeat("x", maxCompiledQueryBytes+1), depth: 1}, nil},
		} {
			t.Run(test.name, func(t *testing.T) {
				if _, compileErr := compileKnowledgeRelation(
					test.relation,
					knowledgeExtractionStageState(),
					test.args,
					preparation,
				); compileErr == nil {
					t.Fatal("malformed input compiled")
				}
			})
		}
	})

	t.Run("all optional stage combinations", func(t *testing.T) {
		type dependency struct {
			source int
			target int
			depth  uint32
		}
		extractionJSON := func(name, output string) *opensplunk.KnowledgeObjectDefinition {
			return knowledgeJSONStageDefinition(name, output, "payload.value", "east")
		}
		extractionRegex := func(name, output string) *opensplunk.KnowledgeObjectDefinition {
			return knowledgeRegexStageDefinition(
				name,
				`(?P<`+output+`>[a-z]+)`,
				[]string{output},
				"west",
			)
		}
		alias := func(name, source, destination string) *opensplunk.KnowledgeObjectDefinition {
			return knowledgePreludeAliasDefinition(name, source, destination)
		}
		calculated := func(name, input, destination string) *opensplunk.KnowledgeObjectDefinition {
			return knowledgePreludeCalculatedDefinition(
				name,
				destination,
				"lower("+input+")",
			)
		}
		selected := func(
			definition *opensplunk.KnowledgeObjectDefinition,
			index string,
		) *opensplunk.KnowledgeObjectDefinition {
			definition.Selector = &opensplunk.KnowledgeSelector{
				IndexPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: index}},
			}
			return definition
		}
		tests := []struct {
			name         string
			definitions  []*opensplunk.KnowledgeObjectDefinition
			dependencies []dependency
			wantKinds    []compiledKnowledgePreludeStageKind
			wantOffsets  []int
			wantRex      bool
		}{
			{
				name:        "extraction only",
				definitions: []*opensplunk.KnowledgeObjectDefinition{extractionJSON("e-json", "e_value")},
				wantKinds:   []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageExtraction},
				wantOffsets: []int{0},
			},
			{
				name:        "alias only",
				definitions: []*opensplunk.KnowledgeObjectDefinition{alias("a-only", "host", "a_value")},
				wantKinds:   []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageAlias},
				wantOffsets: []int{0},
			},
			{
				name:        "calculated only",
				definitions: []*opensplunk.KnowledgeObjectDefinition{calculated("c-only", "source", "c_value")},
				wantKinds:   []compiledKnowledgePreludeStageKind{compiledKnowledgePreludeStageCalculated},
				wantOffsets: []int{0},
			},
			{
				name: "extraction alias",
				definitions: []*opensplunk.KnowledgeObjectDefinition{
					extractionJSON("ea-extract", "ea_extracted"),
					selected(alias("ea-alias", "ea_extracted", "ea_alias"), "east"),
				},
				dependencies: []dependency{{source: 1, target: 0, depth: 1}},
				wantKinds: []compiledKnowledgePreludeStageKind{
					compiledKnowledgePreludeStageExtraction,
					compiledKnowledgePreludeStageAlias,
				},
				wantOffsets: []int{0, 1},
			},
			{
				name: "extraction calculated",
				definitions: []*opensplunk.KnowledgeObjectDefinition{
					extractionRegex("ec-extract", "ec_extracted"),
					selected(calculated("ec-calculated", "ec_extracted", "ec_calculated"), "west"),
				},
				dependencies: []dependency{{source: 1, target: 0, depth: 1}},
				wantKinds: []compiledKnowledgePreludeStageKind{
					compiledKnowledgePreludeStageExtraction,
					compiledKnowledgePreludeStageCalculated,
				},
				wantOffsets: []int{0, 1},
				wantRex:     true,
			},
			{
				name: "alias calculated",
				definitions: []*opensplunk.KnowledgeObjectDefinition{
					alias("ac-alias", "host", "ac_alias"),
					calculated("ac-calculated", "ac_alias", "ac_calculated"),
				},
				dependencies: []dependency{{source: 1, target: 0, depth: 1}},
				wantKinds: []compiledKnowledgePreludeStageKind{
					compiledKnowledgePreludeStageAlias,
					compiledKnowledgePreludeStageCalculated,
				},
				wantOffsets: []int{0, 1},
			},
			{
				name: "extraction alias calculated chain",
				definitions: []*opensplunk.KnowledgeObjectDefinition{
					extractionRegex("eac-extract", "eac_extracted"),
					selected(alias("eac-alias", "eac_extracted", "eac_alias"), "west"),
					selected(calculated("eac-calculated", "eac_alias", "eac_calculated"), "west"),
				},
				dependencies: []dependency{
					{source: 1, target: 0, depth: 1},
					{source: 2, target: 1, depth: 2},
				},
				wantKinds: []compiledKnowledgePreludeStageKind{
					compiledKnowledgePreludeStageExtraction,
					compiledKnowledgePreludeStageAlias,
					compiledKnowledgePreludeStageCalculated,
				},
				wantOffsets: []int{0, 1, 2},
				wantRex:     true,
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				objects := knowledgeRelationSnapshotObjects(t, test.definitions)
				dependencies := make([]*opensplunk.KnowledgeObjectDependency, len(test.dependencies))
				for index, edge := range test.dependencies {
					dependencies[index] = knowledgeRelationDependency(
						objects[edge.source],
						objects[edge.target],
						edge.depth,
						uint32(index),
					)
				}
				program, prepareErr := knowledgeprogram.Prepare(knowledgeprogram.Input{
					Objects:      objects,
					Dependencies: dependencies,
				})
				if prepareErr != nil {
					t.Fatalf("prepare stage combination: %v", prepareErr)
				}
				stagePreparation := knowledgePreludePreparationForTest(program)
				input := compiledRelation{sql: "SELECT ?", depth: 2}
				existing := []any{"existing-first"}
				result, compileErr := compileKnowledgeRelation(
					input,
					knowledgeExtractionStageState(),
					existing,
					stagePreparation,
				)
				if compileErr != nil {
					t.Fatalf("compile stage combination: %v", compileErr)
				}
				if len(result.prelude.stages) != len(test.wantKinds) ||
					result.relation.depth != input.depth+2*len(test.wantKinds)+2 {
					t.Fatalf("stage/depth shape = %#v / %d", result.prelude.stages, result.relation.depth)
				}
				for index, stage := range result.prelude.stages {
					if stage.kind != test.wantKinds[index] || stage.operatorOffset != test.wantOffsets[index] {
						t.Fatalf("stage %d = %#v", index, stage)
					}
				}
				wantArgs := []any{"existing-first"}
				for _, stage := range result.prelude.stages {
					wantArgs = append(wantArgs, stage.suffixArgs...)
				}
				if !reflect.DeepEqual(result.args, wantArgs) || result.args[0] != existing[0] {
					t.Fatalf("argument order = %#v, want %#v", result.args, wantArgs)
				}
				if !strings.Contains(result.relation.sql, KnowledgeSelectorEventLimitMarker) ||
					!strings.Contains(result.relation.sql, KnowledgeSelectorQueryLimitMarker) ||
					!strings.Contains(result.relation.sql, `"__os_ko_guard_input" AS MATERIALIZED (`) {
					t.Fatalf("selector guard is absent:\n%s", result.relation.sql)
				}
				hasRexMarker := strings.Contains(result.relation.sql, RexCaptureLimitMarker)
				hasRexColumn := result.prelude.capturedBytes != "" &&
					result.state.rexCapturedBytesSQL == result.prelude.capturedBytes &&
					strings.Contains(result.relation.sql, `"__os_ko_rex_captured_bytes_`)
				if hasRexMarker != test.wantRex || hasRexColumn != test.wantRex {
					t.Fatalf("rex guard/capture = marker:%t column:%t, want %t\n%s", hasRexMarker, hasRexColumn, test.wantRex, result.relation.sql)
				}
			})
		}
	})

	t.Run("depth boundary and forged preparation", func(t *testing.T) {
		boundary, compileErr := compileKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 88},
			knowledgeExtractionStageState(),
			nil,
			preparation,
		)
		if compileErr != nil || boundary.relation.depth != maximumCompiledRelationalDepth {
			t.Fatalf("depth boundary = %d, %v", boundary.relation.depth, compileErr)
		}
		_, depthErr := compileKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 89},
			knowledgeExtractionStageState(),
			nil,
			preparation,
		)
		var diagnostic *plan.Diagnostic
		if !errors.As(depthErr, &diagnostic) || diagnostic.Code != "SPL_QUERY_TOO_COMPLEX" {
			t.Fatalf("depth error = %#v", depthErr)
		}
		forged := preparation
		forged.programCommitment[0] ^= 1
		if _, compileErr := compileKnowledgeRelation(
			compiledRelation{sql: "SELECT 1", depth: 1},
			knowledgeExtractionStageState(),
			nil,
			forged,
		); compileErr == nil {
			t.Fatal("forged preparation compiled")
		}
	})
}

func knowledgeRelationSnapshotObjects(
	t *testing.T,
	definitions []*opensplunk.KnowledgeObjectDefinition,
) []*opensplunk.KnowledgeSnapshotObject {
	t.Helper()
	objects := make([]*opensplunk.KnowledgeSnapshotObject, len(definitions))
	stageOrdinals := make(map[opensplunk.KnowledgeSearchStage]uint32)
	for index, definition := range definitions {
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatalf("normalize definition %d: %v", index, err)
		}
		var stage opensplunk.KnowledgeSearchStage
		switch normalized.ObjectType {
		case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION:
			stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION
		case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS:
			stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
		case opensplunk.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD:
			stage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
		default:
			t.Fatalf("unexpected object type %v", normalized.ObjectType)
		}
		objects[index] = &opensplunk.KnowledgeSnapshotObject{
			ResolutionOrdinal: uint32(index),
			Stage:             stage,
			StageOrdinal:      stageOrdinals[stage],
			KnowledgeObjectId: "relation-" + normalized.Name,
			Version:           1,
			ObjectType:        normalized.ObjectType,
			Name:              normalized.Name,
			AppId:             normalized.AppID,
			OwnerId:           "owner",
			SharingScope:      normalized.SharingScope,
			Definition:        normalized.Definition,
			DefinitionSha256:  bytes.Clone(normalized.Digest[:]),
		}
		stageOrdinals[stage]++
	}
	return objects
}

func knowledgeRelationDependency(
	source, target *opensplunk.KnowledgeSnapshotObject,
	depth, ordinal uint32,
) *opensplunk.KnowledgeObjectDependency {
	return &opensplunk.KnowledgeObjectDependency{
		Source: &opensplunk.KnowledgeObjectVersionReference{
			KnowledgeObjectId: source.GetKnowledgeObjectId(),
			Version:           source.GetVersion(),
			DefinitionSha256:  bytes.Clone(source.GetDefinitionSha256()),
		},
		Target: &opensplunk.KnowledgeDependencyTarget{
			Target: &opensplunk.KnowledgeDependencyTarget_Object{
				Object: &opensplunk.KnowledgeObjectVersionReference{
					KnowledgeObjectId: target.GetKnowledgeObjectId(),
					Version:           target.GetVersion(),
					DefinitionSha256:  bytes.Clone(target.GetDefinitionSha256()),
				},
			},
		},
		Role:             opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
		SourceStage:      source.GetStage(),
		TargetStage:      target.GetStage(),
		TopologicalDepth: depth,
		CanonicalOrdinal: ordinal,
	}
}
