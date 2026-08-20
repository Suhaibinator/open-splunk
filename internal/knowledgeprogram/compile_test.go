package knowledgeprogram

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"google.golang.org/protobuf/proto"
)

func TestCompileDerivesCanonicalVersionPinnedDependencies(t *testing.T) {
	definitions := []*opensplunk.KnowledgeObjectDefinition{
		regexDefinition("extract-a", "extracted_a", nil, opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
		regexDefinition("extract-b", "extracted_b", nil, opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
		aliasDefinition("alias-a", "extracted_a", "alias_a", nil, opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
		aliasDefinition("alias-b", "extracted_b", "alias_b", nil, opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
		calculatedDefinition("calculated-a", "calculated_a", "lower(alias_b)"),
		calculatedDefinition("calculated-b", "calculated_b", "coalesce(alias_a, extracted_b)"),
	}
	input := inputFromDefinitions(t, definitions)
	for index := range input.Objects {
		input.Objects[index].Version = uint64(index*4 + 3)
	}
	owned := cloneProgramInput(input)

	compiled, err := Compile(input.Objects)
	if err != nil {
		t.Fatalf("Compile(chain): %v", err)
	}
	dependencies := compiled.Dependencies()
	want := []struct {
		source string
		target string
		depth  uint32
	}{
		{source: "object-alias-a", target: "object-extract-a", depth: 1},
		{source: "object-alias-b", target: "object-extract-b", depth: 1},
		{source: "object-calculated-a", target: "object-alias-b", depth: 2},
		{source: "object-calculated-b", target: "object-alias-a", depth: 2},
		{source: "object-calculated-b", target: "object-extract-b", depth: 2},
	}
	if len(dependencies) != len(want) {
		t.Fatalf("derived dependency count = %d, want %d", len(dependencies), len(want))
	}
	objects := make(map[string]*opensplunk.KnowledgeSnapshotObject, len(input.Objects))
	for _, object := range input.Objects {
		objects[object.GetKnowledgeObjectId()] = object
	}
	for index, expected := range want {
		dependency := dependencies[index]
		source, target := dependency.GetSource(), dependency.GetTarget().GetObject()
		sourceObject, targetObject := objects[expected.source], objects[expected.target]
		if source.GetKnowledgeObjectId() != expected.source || target.GetKnowledgeObjectId() != expected.target ||
			dependency.GetTopologicalDepth() != expected.depth || dependency.GetCanonicalOrdinal() != uint32(index) ||
			dependency.GetRole() != opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT ||
			dependency.GetSourceStage() != sourceObject.GetStage() || dependency.GetTargetStage() != targetObject.GetStage() ||
			source.GetVersion() != sourceObject.GetVersion() || target.GetVersion() != targetObject.GetVersion() ||
			!bytes.Equal(source.GetDefinitionSha256(), sourceObject.GetDefinitionSha256()) ||
			!bytes.Equal(target.GetDefinitionSha256(), targetObject.GetDefinitionSha256()) {
			t.Fatalf("dependency %d = %v, want %s -> %s at depth %d", index, dependency, expected.source, expected.target, expected.depth)
		}
	}

	prepared, err := Prepare(Input{Objects: owned.Objects, Dependencies: cloneDependencies(dependencies)})
	if err != nil {
		t.Fatalf("Prepare(Compile dependencies): %v", err)
	}
	recompiled, err := Compile(owned.Objects)
	if err != nil {
		t.Fatalf("Compile(clone): %v", err)
	}
	if !compiled.Equal(prepared) || !compiled.Equal(recompiled) {
		t.Fatal("Compile and Prepare did not produce the same deterministic program")
	}

	wantDetached := cloneDependencies(dependencies)
	input.Objects[0].KnowledgeObjectId = "mutated-object"
	input.Objects[0].DefinitionSha256[0] ^= 0xff
	dependencies[0].Source.KnowledgeObjectId = "mutated-source"
	dependencies[0].Source.DefinitionSha256[0] ^= 0xff
	gotDetached := compiled.Dependencies()
	for index := range wantDetached {
		if !proto.Equal(gotDetached[index], wantDetached[index]) {
			t.Fatalf("compiled dependency %d aliases caller/accessor storage", index)
		}
	}
}

func TestCompiledProgramAccessorsAreConcurrentAndDetached(t *testing.T) {
	program, err := Compile(mixedProgramInput(t).Objects)
	if err != nil {
		t.Fatalf("Compile(mixed): %v", err)
	}
	baseline := program.Clone()
	wantCommitment, ok := program.Commitment()
	if !ok {
		t.Fatal("compiled program has no commitment")
	}
	wantDependencies := program.Dependencies()
	wantKinds := program.OperatorKinds()
	wantRegex := program.RegexExtractions()
	wantJSON := program.JSONExtractions()
	wantAliases := program.Aliases()
	wantCalculated := program.CalculatedFields()
	if len(wantDependencies) == 0 || len(wantKinds) == 0 || len(wantRegex) == 0 ||
		len(wantJSON) == 0 || len(wantAliases) == 0 || len(wantCalculated) == 0 {
		t.Fatal("mixed program does not exercise every accessor family")
	}
	wantSelectorBytes := wantRegex[0].Selector().CanonicalBytes()

	const workers = 8
	const iterations = 64
	results := make(chan error, workers)
	for worker := range workers {
		go func(worker int) {
			for iteration := range iterations {
				commitment, present := program.Commitment()
				if program.IsZero() || program.IsEmpty() || program.ObjectCount() != baseline.ObjectCount() ||
					!present || commitment != wantCommitment || program.Charges() != baseline.Charges() ||
					program.RetainedBytes() != baseline.RetainedBytes() || !program.Equal(baseline) {
					results <- fmt.Errorf("worker %d iteration %d observed unstable scalar authority", worker, iteration)
					return
				}
				cloned := program.Clone()
				if cloned.IsZero() || !cloned.Equal(program) {
					results <- fmt.Errorf("worker %d iteration %d observed unstable clone", worker, iteration)
					return
				}

				dependencies := program.Dependencies()
				if len(dependencies) != len(wantDependencies) || !proto.Equal(dependencies[0], wantDependencies[0]) {
					results <- fmt.Errorf("worker %d iteration %d dependency count = %d", worker, iteration, len(dependencies))
					return
				}
				dependencies[0].Source.KnowledgeObjectId = "mutated-source"
				dependencies[0].Source.DefinitionSha256[0] ^= 0xff
				dependencies[0].Target.GetObject().DefinitionSha256[0] ^= 0xff

				kinds := program.OperatorKinds()
				kinds[0] = OperatorInvalid

				regex := program.RegexExtractions()
				captures := regex[0].Captures()
				origin := regex[0].Origin()
				selector := regex[0].Selector()
				selectorBytes := selector.CanonicalBytes()
				runtime, constrained := selector.RuntimeProgram(knowledge.DimensionIndex)
				if !constrained || len(selectorBytes) == 0 || len(captures) == 0 {
					results <- fmt.Errorf("worker %d iteration %d selector authority changed", worker, iteration)
					return
				}
				_ = regex[0].Overwrite()
				_ = regex[0].Input()
				_ = regex[0].Pattern()
				_ = regex[0].ProgramWorkUnits()
				_ = captures[0].Name()
				_ = captures[0].Group()
				_ = captures[0].DefinitionLocation()
				_ = origin.ResolutionOrdinal()
				_ = origin.StageOrdinal()
				_ = origin.ObjectID()
				_ = origin.Version()
				_ = origin.ObjectType()
				_ = origin.Name()
				_ = origin.AppID()
				_ = origin.OwnerID()
				_ = origin.SharingScope()
				_ = origin.Stage()
				_ = origin.DefinitionDigest()
				_ = origin.DefinitionLocation()
				_ = selector.IsUnrestricted()
				_ = selector.ProvablyDisjoint(regex[0].Selector())
				selectorBytes[0] ^= 0xff
				runtime.ExactLiterals = append(runtime.ExactLiterals, "mutated")
				captures[0].name = "mutated"
				regex[0].captures[0].name = "mutated"

				json := program.JSONExtractions()
				steps := json[0].Steps()
				_ = json[0].Origin()
				_ = json[0].Selector()
				_ = json[0].Overwrite()
				_ = json[0].Input()
				_ = json[0].Path()
				_ = json[0].Output()
				_ = json[0].OutputDefinitionLocation()
				_ = json[0].EvaluationWorkUnits()
				steps[0].Key = "mutated"
				json[0].steps[0].Key = "mutated"

				aliases := program.Aliases()
				_ = aliases[0].Origin()
				_ = aliases[0].Selector()
				_ = aliases[0].Overwrite()
				_ = aliases[0].Source()
				_ = aliases[0].Destination()
				aliases[0].source = "mutated"

				calculated := program.CalculatedFields()
				inputs := calculated[0].InputFields()
				_ = calculated[0].Origin()
				_ = calculated[0].Selector()
				_ = calculated[0].Overwrite()
				_ = calculated[0].Destination()
				_ = calculated[0].Expression()
				_ = calculated[0].Nodes()
				_ = calculated[0].Predicates()
				inputs[0] = "mutated"
				calculated[0].inputFields[0] = "mutated"

				cloneDependencies := cloned.Dependencies()
				cloneDependencies[0].Source.KnowledgeObjectId = "mutated-clone-accessor"
			}
			results <- nil
		}(worker)
	}
	for range workers {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}

	gotCommitment, present := program.Commitment()
	if !program.Equal(baseline) || !present || gotCommitment != wantCommitment {
		t.Fatal("concurrent detached mutations changed the compiled program")
	}
	gotDependencies := program.Dependencies()
	for index := range wantDependencies {
		if !proto.Equal(gotDependencies[index], wantDependencies[index]) {
			t.Fatalf("dependency %d changed after concurrent detached mutations", index)
		}
	}
	if program.RegexExtractions()[0].Captures()[0].Name() != wantRegex[0].Captures()[0].Name() ||
		program.JSONExtractions()[0].Steps()[0].Key != wantJSON[0].Steps()[0].Key ||
		program.Aliases()[0].Source() != wantAliases[0].Source() ||
		program.CalculatedFields()[0].InputFields()[0] != wantCalculated[0].InputFields()[0] ||
		program.OperatorKinds()[0] != wantKinds[0] ||
		!bytes.Equal(program.RegexExtractions()[0].Selector().CanonicalBytes(), wantSelectorBytes) {
		t.Fatal("operation accessors aliased concurrent caller mutation")
	}
}

func TestCompileEmptyAndNoEdgePrograms(t *testing.T) {
	empty, err := Compile(nil)
	if err != nil {
		t.Fatalf("Compile(empty): %v", err)
	}
	preparedEmpty, err := Prepare(Input{})
	if err != nil {
		t.Fatalf("Prepare(empty): %v", err)
	}
	if empty.IsZero() || !empty.IsEmpty() || len(empty.Dependencies()) != 0 || !empty.Equal(preparedEmpty) {
		t.Fatal("Compile(empty) did not return the canonical present empty program")
	}

	tests := []struct {
		name        string
		definitions []*opensplunk.KnowledgeObjectDefinition
	}{
		{
			name: "stored input",
			definitions: []*opensplunk.KnowledgeObjectDefinition{
				aliasDefinition("alias-a", "stored_field", "alias_value", nil, opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
			},
		},
		{
			name: "provably disjoint producer",
			definitions: []*opensplunk.KnowledgeObjectDefinition{
				regexDefinition("extract-a", "derived_input", hostSelector("api"), opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
				aliasDefinition("alias-a", "derived_input", "alias_value", hostSelector("web"), opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			program, err := Compile(inputFromDefinitions(t, test.definitions).Objects)
			if err != nil {
				t.Fatalf("Compile(): %v", err)
			}
			if len(program.Dependencies()) != 0 {
				t.Fatalf("dependencies = %v, want none", program.Dependencies())
			}
		})
	}
}

func TestCompileEnforcesSelectorAndSharingAuthority(t *testing.T) {
	private := opensplunk.SharingScope_SHARING_SCOPE_PRIVATE
	app := opensplunk.SharingScope_SHARING_SCOPE_APP
	global := opensplunk.SharingScope_SHARING_SCOPE_GLOBAL
	tests := []struct {
		name                           string
		sourceSelector, targetSelector string
		sourceScope, targetScope       opensplunk.SharingScope
		sourceApp, targetApp           string
		targetOwner                    string
		wantDependencies               int
		wantError                      bool
	}{
		{name: "literal implies wildcard", sourceSelector: "api-01", targetSelector: "api-??", sourceScope: app, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantDependencies: 1},
		{name: "identical wildcard", sourceSelector: "api-*", targetSelector: "api-*", sourceScope: app, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantDependencies: 1},
		{name: "disjoint selectors omit edge", sourceSelector: "worker-01", targetSelector: "api-*", sourceScope: app, targetScope: app, sourceApp: "app-a", targetApp: "app-a"},
		{name: "overlap without implication", sourceSelector: "api-?", targetSelector: "api-*", sourceScope: app, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantError: true},
		{name: "unrestricted does not imply present universal", targetSelector: "*", sourceScope: app, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantError: true},
		{name: "private reads global", sourceScope: private, targetScope: global, sourceApp: "app-a", targetApp: "app-b", wantDependencies: 1},
		{name: "private reads same app", sourceScope: private, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantDependencies: 1},
		{name: "private reads same owner private", sourceScope: private, targetScope: private, sourceApp: "app-a", targetApp: "app-a", targetOwner: "owner-a", wantDependencies: 1},
		{name: "private cannot read other owner private", sourceScope: private, targetScope: private, sourceApp: "app-a", targetApp: "app-a", targetOwner: "owner-b", wantError: true},
		{name: "app cannot read private", sourceScope: app, targetScope: private, sourceApp: "app-a", targetApp: "app-a", wantError: true},
		{name: "global cannot read app", sourceScope: global, targetScope: app, sourceApp: "app-a", targetApp: "app-a", wantError: true},
		{name: "global reads global", sourceScope: global, targetScope: global, sourceApp: "app-a", targetApp: "app-b", wantDependencies: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := inputFromDefinitions(t, []*opensplunk.KnowledgeObjectDefinition{
				regexDefinition("extract-a", "derived_input", hostSelector(test.targetSelector), test.targetScope, test.targetApp),
				aliasDefinition("alias-a", "derived_input", "alias_value", hostSelector(test.sourceSelector), test.sourceScope, test.sourceApp),
			})
			if test.targetOwner != "" {
				input.Objects[0].OwnerId = test.targetOwner
			}
			program, err := Compile(input.Objects)
			if test.wantError {
				if !errors.Is(err, ErrInvalidProgram) {
					t.Fatalf("Compile() error = %v, want ErrInvalidProgram", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Compile(): %v", err)
			}
			if got := len(program.Dependencies()); got != test.wantDependencies {
				t.Fatalf("dependency count = %d, want %d", got, test.wantDependencies)
			}
		})
	}
}

func TestSemanticDepthsRejectsCyclesAndEnforcesCeiling(t *testing.T) {
	role := opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT
	cycleObjects := []semanticObject{{key: semanticObjectKey{id: "a", version: 1}}, {key: semanticObjectKey{id: "b", version: 1}}}
	cycle := map[semanticEdgeKey]struct{}{
		{source: cycleObjects[0].key, target: cycleObjects[1].key, role: role}: {},
		{source: cycleObjects[1].key, target: cycleObjects[0].key, role: role}: {},
	}
	if _, err := semanticDepths(cycleObjects, cycle); !errors.Is(err, ErrInvalidProgram) {
		t.Fatalf("semanticDepths(cycle) error = %v, want ErrInvalidProgram", err)
	}

	objects := make([]semanticObject, MaximumDependencyDepth+2)
	for index := range objects {
		objects[index].key = semanticObjectKey{id: fmt.Sprintf("object-%02d", index), version: 1}
	}
	edges := make(map[semanticEdgeKey]struct{}, MaximumDependencyDepth+1)
	for index := range MaximumDependencyDepth {
		edges[semanticEdgeKey{source: objects[index].key, target: objects[index+1].key, role: role}] = struct{}{}
	}
	depths, err := semanticDepths(objects, edges)
	if err != nil || depths[objects[0].key] != MaximumDependencyDepth {
		t.Fatalf("semanticDepths(exact ceiling) = %d/%v", depths[objects[0].key], err)
	}
	edges[semanticEdgeKey{source: objects[MaximumDependencyDepth].key, target: objects[MaximumDependencyDepth+1].key, role: role}] = struct{}{}
	if _, err := semanticDepths(objects, edges); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("semanticDepths(over ceiling) error = %v, want ErrResourceLimit", err)
	}
}

func TestCompileEnforcesDerivedDependencyCeiling(t *testing.T) {
	atLimit, err := Compile(inputFromDefinitions(t, denseDependencyDefinitions(32)).Objects)
	if err != nil {
		t.Fatalf("Compile(exact dependency ceiling): %v", err)
	}
	if got := len(atLimit.Dependencies()); got != MaximumDependencies {
		t.Fatalf("derived dependencies = %d, want %d", got, MaximumDependencies)
	}
	if _, err := Compile(inputFromDefinitions(t, denseDependencyDefinitions(33)).Objects); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("Compile(over dependency ceiling) error = %v, want ErrResourceLimit", err)
	}
}

func regexDefinition(
	name, output string,
	selector *opensplunk.KnowledgeSelector,
	scope opensplunk.SharingScope,
	appID string,
) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId: appID, Name: name, SharingScope: scope, Selector: selector,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: &opensplunk.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunk.FieldExtractionDefinition_Regex{Regex: &opensplunk.RegexFieldExtractionDefinition{
				Pattern: "(?P<" + output + ">x)", OutputFields: []string{output},
			}},
		}},
	}
}

func aliasDefinition(
	name, source, destination string,
	selector *opensplunk.KnowledgeSelector,
	scope opensplunk.SharingScope,
	appID string,
) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId: appID, Name: name, SharingScope: scope, Selector: selector,
		Body: &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: &opensplunk.FieldAliasDefinition{
			SourceField: source, DestinationField: destination,
		}},
	}
}

func calculatedDefinition(
	name, destination, expression string,
) *opensplunk.KnowledgeObjectDefinition {
	return &opensplunk.KnowledgeObjectDefinition{
		AppId: "app-a", Name: name, SharingScope: opensplunk.SharingScope_SHARING_SCOPE_APP,
		Body: &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: &opensplunk.CalculatedFieldDefinition{
			DestinationField: destination, Expression: expression,
		}},
	}
}

func hostSelector(pattern string) *opensplunk.KnowledgeSelector {
	if pattern == "" {
		return nil
	}
	return &opensplunk.KnowledgeSelector{
		HostPatterns: []*opensplunk.KnowledgeSelectorPattern{{Value: pattern}},
	}
}

func denseDependencyDefinitions(targets int) []*opensplunk.KnowledgeObjectDefinition {
	definitions := make([]*opensplunk.KnowledgeObjectDefinition, 0, targets+MaximumScalarExpressions)
	fields := make([]string, targets)
	for index := range fields {
		fields[index] = fmt.Sprintf("input_%02d", index)
		definitions = append(definitions, regexDefinition(
			fmt.Sprintf("extract-%02d", index), fields[index], nil,
			opensplunk.SharingScope_SHARING_SCOPE_APP, "app-a",
		))
	}
	expression := denseDependencyExpression(fields)
	for index := range MaximumScalarExpressions {
		definitions = append(definitions, calculatedDefinition(
			fmt.Sprintf("calculated-%02d", index), fmt.Sprintf("calculated_%02d", index), expression,
		))
	}
	return definitions
}

func denseDependencyExpression(fields []string) string {
	parts := append([]string(nil), fields...)
	for len(parts) > 32 {
		parts = append([]string{"coalesce(" + strings.Join(parts[:32], ",") + ")"}, parts[32:]...)
	}
	return "coalesce(" + strings.Join(parts, ",") + ")"
}
