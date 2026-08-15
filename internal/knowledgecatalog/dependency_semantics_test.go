package knowledgecatalog

import (
	"errors"
	"slices"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

func calculatedDependencyInputFields(expression string) ([]string, error) {
	analysis, err := calculatedDependencyAnalysis(expression)
	if err != nil {
		return nil, err
	}
	return analysis.InputFields, nil
}

func TestCalculatedDependencyInputFieldsAreBinarySortedAndDeduplicated(t *testing.T) {
	fields, err := calculatedDependencyInputFields(
		`if(isnull(host), coalesce(service,host), case(status=200,lower(Host),isnull(service),host))`,
	)
	if err != nil {
		t.Fatalf("calculatedDependencyInputFields: %v", err)
	}
	want := []string{"Host", "host", "service", "status"}
	if !slices.Equal(fields, want) {
		t.Fatalf("calculated fields = %v, want %v", fields, want)
	}
}

func TestDependencySemanticValidatorAggregateWorkBudgetsFailClosed(t *testing.T) {
	budget := dependencySemanticWorkBudget{nodes: 1, edges: 1, definitionBytes: 1, queries: 1}
	tests := []struct {
		name string
		work dependencySemanticWork
	}{
		{name: "nodes", work: dependencySemanticWork{nodes: 2}},
		{name: "edges", work: dependencySemanticWork{edges: 2}},
		{name: "definition bytes", work: dependencySemanticWork{definitionBytes: 2}},
		{name: "queries", work: dependencySemanticWork{queries: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := dependencySemanticValidator{budget: budget, work: test.work}
			if err := validator.validateAggregateWork(); !errors.Is(err, control.ErrCapacityExceeded) {
				t.Fatalf("validateAggregateWork(%s) error = %v, want ErrCapacityExceeded", test.name, err)
			}
		})
	}
}

func TestInactiveDependencyRootsStillEnforceStructuralGraphBounds(t *testing.T) {
	root := versionRecord{
		TenantID: testTenant, KnowledgeObjectID: "ko-opaque-root", ObjectVersion: 1,
		State: StateDraft,
	}
	target := versionRecord{
		TenantID: testTenant, KnowledgeObjectID: "ko-opaque-target", ObjectVersion: 1,
		State: StateDisabled,
	}
	rootRecords := []dependencyRecord{{
		TenantID: testTenant, SourceObjectID: root.KnowledgeObjectID, SourceObjectVersion: 1,
		TargetObjectID: target.KnowledgeObjectID, TargetObjectVersion: 1,
	}}
	targetRecords := []dependencyRecord{{
		TenantID: testTenant, SourceObjectID: target.KnowledgeObjectID, SourceObjectVersion: 1,
		TargetObjectID: root.KnowledgeObjectID, TargetObjectVersion: 1,
	}}
	tests := []struct {
		name    string
		decoded decodedDefinition
	}{
		{name: "opaque future body", decoded: decodedDefinition{ObjectTypeKnown: false}},
		{
			name: "recognized invalid draft body",
			decoded: decodedDefinition{
				ObjectTypeKnown: true,
				Definition: dependencyCalculatedDefinition(
					testApp, "opaque-root", SharingScopePrivate, nil, "", "lower(", "result",
				),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := newDependencySemanticValidator(nil, testTenant, dependencySemanticWorkBudget{
				nodes: 2, edges: 2, definitionBytes: 0, queries: 0,
			})
			if err := validator.seedStructuralNode(target, targetRecords); err != nil {
				t.Fatalf("seed inactive target: %v", err)
			}
			err := validator.validateDecodedVersionDependencies(root, test.decoded, rootRecords)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("validate inactive structural cycle error = %v, want ErrCorrupt", err)
			}
			if validator.work.definitionBytes != 0 || validator.work.queries != 0 {
				t.Fatalf("inactive structural validation decoded/query-read a body: work = %#v", validator.work)
			}
		})
	}
}

func TestDependencySemanticRootStateFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		version versionRecord
		decoded decodedDefinition
	}{
		{
			name:    "opaque active body",
			version: versionRecord{TenantID: testTenant, KnowledgeObjectID: "ko-active", ObjectVersion: 1, State: StateActive},
			decoded: decodedDefinition{ObjectTypeKnown: false},
		},
		{
			name:    "quarantined root bypasses redaction",
			version: versionRecord{TenantID: testTenant, KnowledgeObjectID: "ko-quarantined", ObjectVersion: 1, State: StateQuarantined},
			decoded: decodedDefinition{ObjectTypeKnown: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := newDependencySemanticValidator(nil, testTenant, dependencySemanticWorkBudget{})
			if err := validator.validateDecodedVersionDependencies(test.version, test.decoded, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("validate dependency semantic root error = %v, want ErrCorrupt", err)
			}
			if len(validator.nodes) != 0 {
				t.Fatalf("invalid root populated semantic cache: %#v", validator.nodes)
			}
		})
	}
}

func TestDependencySemanticsDeriveTierOneStagesAndExactFields(t *testing.T) {
	tests := []struct {
		name        string
		definition  *opensplunkv1.KnowledgeObjectDefinition
		wantStage   dependencyStage
		wantInputs  []string
		wantOutputs []string
	}{
		{
			name: "extraction",
			definition: dependencyExtractionDefinition(
				testApp, "extract", SharingScopePrivate, nil, "", "raw_value",
			),
			wantStage: dependencyStageExtraction, wantOutputs: []string{"raw_value"},
		},
		{
			name: "alias",
			definition: dependencyAliasDefinition(
				testApp, "alias", SharingScopePrivate, nil, "", "raw_value", "alias_value",
			),
			wantStage: dependencyStageAlias, wantInputs: []string{"raw_value"}, wantOutputs: []string{"alias_value"},
		},
		{
			name: "calculated",
			definition: dependencyCalculatedDefinition(
				testApp, "calculated", SharingScopePrivate, nil, "", "lower(alias_value)", "calculated_value",
			),
			wantStage: dependencyStageCalculated, wantInputs: []string{"alias_value"}, wantOutputs: []string{"calculated_value"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := dependencySemanticsFromDefinition(test.definition)
			if err != nil {
				t.Fatalf("dependencySemanticsFromDefinition: %v", err)
			}
			if got.stage != test.wantStage || !slices.Equal(got.inputFields, test.wantInputs) ||
				!slices.Equal(got.outputFields, test.wantOutputs) {
				t.Fatalf("semantics = %#v, want stage=%d inputs=%v outputs=%v", got, test.wantStage, test.wantInputs, test.wantOutputs)
			}
		})
	}
}
