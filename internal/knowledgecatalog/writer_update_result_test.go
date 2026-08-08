package knowledgecatalog

import (
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestUpdateResultMatchesRequestBindsEveryCanonicalMaskPath(t *testing.T) {
	t.Parallel()

	baseDescription := "base description"
	updatedDescription := "updated description"
	baseAlias := dependencyAliasDefinition(
		testApp,
		"update-result-base",
		SharingScopePrivate,
		&baseDescription,
		"base-host",
		"source_base",
		"destination_base",
	)
	baseAlias = mustNormalizeUpdateResultDefinition(t, baseAlias)
	changedAlias := proto.Clone(baseAlias).(*opensplunkv1.KnowledgeObjectDefinition)
	changedAlias.AppId = testAppTwo
	changedAlias.Name = "update-result-changed"
	changedAlias.Description = &updatedDescription
	changedAlias.SharingScope = opensplunkv1.SharingScope_SHARING_SCOPE_APP
	changedAlias.Selector = &opensplunkv1.KnowledgeSelector{
		HostPatterns: []*opensplunkv1.KnowledgeSelectorPattern{
			{Value: "zeta*"},
			{Value: "alpha"},
		},
	}
	changedAlias.GetFieldAlias().SourceField = "source_changed"
	changedAlias.GetFieldAlias().DestinationField = "destination_changed"
	changedAlias.GetFieldAlias().OverwriteBehavior =
		opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED

	baseExtraction := mustNormalizeUpdateResultDefinition(t, dependencyExtractionDefinition(
		testApp,
		"update-result-extraction",
		SharingScopePrivate,
		&baseDescription,
		"base-host",
		"base_output",
	))
	changedExtraction := proto.Clone(baseExtraction).(*opensplunkv1.KnowledgeObjectDefinition)
	changedExtraction.GetFieldExtraction().Extraction =
		&opensplunkv1.FieldExtractionDefinition_Json{
			Json: &opensplunkv1.JsonFieldExtractionDefinition{
				Path:        "payload.changed",
				OutputField: "changed_output",
			},
		}

	baseCalculated := mustNormalizeUpdateResultDefinition(t, dependencyCalculatedDefinition(
		testApp,
		"update-result-calculated",
		SharingScopePrivate,
		&baseDescription,
		"base-host",
		"coalesce(value, 0)",
		"calculated_base",
	))
	changedCalculated := proto.Clone(baseCalculated).(*opensplunkv1.KnowledgeObjectDefinition)
	changedCalculated.GetCalculatedField().Expression = "coalesce(value, 1)"
	changedCalculated.GetCalculatedField().DestinationField = "calculated_changed"

	tests := []struct {
		path      string
		current   *opensplunkv1.KnowledgeObjectDefinition
		submitted *opensplunkv1.KnowledgeObjectDefinition
		apply     func(*opensplunkv1.KnowledgeObjectDefinition, *opensplunkv1.KnowledgeObjectDefinition)
	}{
		{path: "app_id", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) { result.AppId = submitted.GetAppId() }},
		{path: "name", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) { result.Name = submitted.GetName() }},
		{path: "description", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.Description = cloneString(submitted.Description)
		}},
		{path: "sharing_scope", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.SharingScope = submitted.GetSharingScope()
		}},
		{path: "selector", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.Selector = proto.Clone(submitted.GetSelector()).(*opensplunkv1.KnowledgeSelector)
		}},
		{path: "field_alias", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{FieldAlias: proto.Clone(submitted.GetFieldAlias()).(*opensplunkv1.FieldAliasDefinition)}
		}},
		{path: "field_extraction", current: baseExtraction, submitted: changedExtraction, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: proto.Clone(submitted.GetFieldExtraction()).(*opensplunkv1.FieldExtractionDefinition)}
		}},
		{path: "calculated_field", current: baseCalculated, submitted: changedCalculated, apply: func(result, submitted *opensplunkv1.KnowledgeObjectDefinition) {
			result.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{CalculatedField: proto.Clone(submitted.GetCalculatedField()).(*opensplunkv1.CalculatedFieldDefinition)}
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			result := proto.Clone(test.current).(*opensplunkv1.KnowledgeObjectDefinition)
			test.apply(result, test.submitted)
			result = mustNormalizeUpdateResultDefinition(t, result)
			if !UpdateResultMatchesRequest(
				result,
				test.submitted,
				&fieldmaskpb.FieldMask{Paths: []string{test.path}},
				opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			) {
				t.Fatalf("canonical %s result did not match submitted request", test.path)
			}
		})
	}

	t.Run("selected mismatch", func(t *testing.T) {
		result := proto.Clone(baseAlias).(*opensplunkv1.KnowledgeObjectDefinition)
		result.Name = "wrong-selected-name"
		result = mustNormalizeUpdateResultDefinition(t, result)
		if UpdateResultMatchesRequest(
			result,
			changedAlias,
			&fieldmaskpb.FieldMask{Paths: []string{"name"}},
			opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		) {
			t.Fatal("mismatched selected name was accepted")
		}
	})

	t.Run("body type mismatch", func(t *testing.T) {
		if UpdateResultMatchesRequest(
			baseAlias,
			changedCalculated,
			&fieldmaskpb.FieldMask{Paths: []string{"calculated_field"}},
			opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		) {
			t.Fatal("body-type-changing update was accepted")
		}
	})
}

func TestUpdateResultMatchesRequestPreservesInactiveOpaqueBodyOnlyForMetadata(
	t *testing.T,
) {
	t.Parallel()

	metadata := &opensplunkv1.KnowledgeObjectDefinition{
		AppId:        testApp,
		Name:         "update-result-opaque",
		SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
	}
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal opaque metadata: %v", err)
	}
	encoded := protowire.AppendBytes(
		protowire.AppendTag(known, 13, protowire.BytesType),
		[]byte{0x08, 0x01},
	)
	current := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, current); err != nil {
		t.Fatalf("unmarshal opaque definition: %v", err)
	}
	updatedDescription := "opaque metadata update"
	submitted := proto.Clone(current).(*opensplunkv1.KnowledgeObjectDefinition)
	submitted.Description = &updatedDescription
	result := proto.Clone(current).(*opensplunkv1.KnowledgeObjectDefinition)
	result.Description = &updatedDescription
	if !UpdateResultMatchesRequest(
		result,
		submitted,
		&fieldmaskpb.FieldMask{Paths: []string{"description"}},
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	) {
		t.Fatal("metadata-only inactive opaque update did not match")
	}
	if UpdateResultMatchesRequest(
		result,
		submitted,
		&fieldmaskpb.FieldMask{Paths: []string{"description"}},
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
	) {
		t.Fatal("active opaque update was accepted")
	}
	recognizedBody := proto.Clone(submitted).(*opensplunkv1.KnowledgeObjectDefinition)
	recognizedBody.ProtoReflect().SetUnknown(nil)
	recognizedBody.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunkv1.FieldAliasDefinition{
			SourceField:      "source",
			DestinationField: "destination",
		},
	}
	if UpdateResultMatchesRequest(
		result,
		recognizedBody,
		&fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	) {
		t.Fatal("opaque body edit was accepted")
	}
}

func mustNormalizeUpdateResultDefinition(
	t *testing.T,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) *opensplunkv1.KnowledgeObjectDefinition {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("normalize update result fixture: %v", err)
	}
	return normalized.Definition
}
