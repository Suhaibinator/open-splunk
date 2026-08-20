package knowledgecatalog

import (
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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
	changedAlias := proto.Clone(baseAlias).(*opensplunk.KnowledgeObjectDefinition)
	changedAlias.AppId = testAppTwo
	changedAlias.Name = "update-result-changed"
	changedAlias.Description = &updatedDescription
	changedAlias.SharingScope = opensplunk.SharingScope_SHARING_SCOPE_APP
	changedAlias.Selector = &opensplunk.KnowledgeSelector{
		HostPatterns: []*opensplunk.KnowledgeSelectorPattern{
			{Value: "zeta*"},
			{Value: "alpha"},
		},
	}
	changedAlias.GetFieldAlias().SourceField = "source_changed"
	changedAlias.GetFieldAlias().DestinationField = "destination_changed"
	changedAlias.GetFieldAlias().OverwriteBehavior =
		opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_UNSPECIFIED

	baseExtraction := mustNormalizeUpdateResultDefinition(t, dependencyExtractionDefinition(
		testApp,
		"update-result-extraction",
		SharingScopePrivate,
		&baseDescription,
		"base-host",
		"base_output",
	))
	changedExtraction := proto.Clone(baseExtraction).(*opensplunk.KnowledgeObjectDefinition)
	changedExtraction.GetFieldExtraction().Extraction =
		&opensplunk.FieldExtractionDefinition_Json{
			Json: &opensplunk.JsonFieldExtractionDefinition{
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
	changedCalculated := proto.Clone(baseCalculated).(*opensplunk.KnowledgeObjectDefinition)
	changedCalculated.GetCalculatedField().Expression = "coalesce(value, 1)"
	changedCalculated.GetCalculatedField().DestinationField = "calculated_changed"

	tests := []struct {
		path      string
		current   *opensplunk.KnowledgeObjectDefinition
		submitted *opensplunk.KnowledgeObjectDefinition
		apply     func(*opensplunk.KnowledgeObjectDefinition, *opensplunk.KnowledgeObjectDefinition)
	}{
		{path: "app_id", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) { result.AppId = submitted.GetAppId() }},
		{path: "name", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) { result.Name = submitted.GetName() }},
		{path: "description", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.Description = cloneString(submitted.Description)
		}},
		{path: "sharing_scope", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.SharingScope = submitted.GetSharingScope()
		}},
		{path: "selector", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.Selector = proto.Clone(submitted.GetSelector()).(*opensplunk.KnowledgeSelector)
		}},
		{path: "field_alias", current: baseAlias, submitted: changedAlias, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{FieldAlias: proto.Clone(submitted.GetFieldAlias()).(*opensplunk.FieldAliasDefinition)}
		}},
		{path: "field_extraction", current: baseExtraction, submitted: changedExtraction, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_FieldExtraction{FieldExtraction: proto.Clone(submitted.GetFieldExtraction()).(*opensplunk.FieldExtractionDefinition)}
		}},
		{path: "calculated_field", current: baseCalculated, submitted: changedCalculated, apply: func(result, submitted *opensplunk.KnowledgeObjectDefinition) {
			result.Body = &opensplunk.KnowledgeObjectDefinition_CalculatedField{CalculatedField: proto.Clone(submitted.GetCalculatedField()).(*opensplunk.CalculatedFieldDefinition)}
		}},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			result := proto.Clone(test.current).(*opensplunk.KnowledgeObjectDefinition)
			test.apply(result, test.submitted)
			result = mustNormalizeUpdateResultDefinition(t, result)
			if !UpdateResultMatchesRequest(
				result,
				test.submitted,
				&fieldmaskpb.FieldMask{Paths: []string{test.path}},
				opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
			) {
				t.Fatalf("canonical %s result did not match submitted request", test.path)
			}
		})
	}

	t.Run("selected mismatch", func(t *testing.T) {
		result := proto.Clone(baseAlias).(*opensplunk.KnowledgeObjectDefinition)
		result.Name = "wrong-selected-name"
		result = mustNormalizeUpdateResultDefinition(t, result)
		if UpdateResultMatchesRequest(
			result,
			changedAlias,
			&fieldmaskpb.FieldMask{Paths: []string{"name"}},
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		) {
			t.Fatal("mismatched selected name was accepted")
		}
	})

	t.Run("body type mismatch", func(t *testing.T) {
		if UpdateResultMatchesRequest(
			baseAlias,
			changedCalculated,
			&fieldmaskpb.FieldMask{Paths: []string{"calculated_field"}},
			opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		) {
			t.Fatal("body-type-changing update was accepted")
		}
	})
}

func TestUpdateResultMatchesRequestPreservesInactiveOpaqueBodyOnlyForMetadata(
	t *testing.T,
) {
	t.Parallel()

	metadata := &opensplunk.KnowledgeObjectDefinition{
		AppId:        testApp,
		Name:         "update-result-opaque",
		SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE,
	}
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal opaque metadata: %v", err)
	}
	encoded := protowire.AppendBytes(
		protowire.AppendTag(known, 13, protowire.BytesType),
		[]byte{0x08, 0x01},
	)
	current := &opensplunk.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, current); err != nil {
		t.Fatalf("unmarshal opaque definition: %v", err)
	}
	updatedDescription := "opaque metadata update"
	submitted := proto.Clone(current).(*opensplunk.KnowledgeObjectDefinition)
	submitted.Description = &updatedDescription
	result := proto.Clone(current).(*opensplunk.KnowledgeObjectDefinition)
	result.Description = &updatedDescription
	if !UpdateResultMatchesRequest(
		result,
		submitted,
		&fieldmaskpb.FieldMask{Paths: []string{"description"}},
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	) {
		t.Fatal("metadata-only inactive opaque update did not match")
	}
	if UpdateResultMatchesRequest(
		result,
		submitted,
		&fieldmaskpb.FieldMask{Paths: []string{"description"}},
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
	) {
		t.Fatal("active opaque update was accepted")
	}
	recognizedBody := proto.Clone(submitted).(*opensplunk.KnowledgeObjectDefinition)
	recognizedBody.ProtoReflect().SetUnknown(nil)
	recognizedBody.Body = &opensplunk.KnowledgeObjectDefinition_FieldAlias{
		FieldAlias: &opensplunk.FieldAliasDefinition{
			SourceField:      "source",
			DestinationField: "destination",
		},
	}
	if UpdateResultMatchesRequest(
		result,
		recognizedBody,
		&fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED,
	) {
		t.Fatal("opaque body edit was accepted")
	}
}

func mustNormalizeUpdateResultDefinition(
	t *testing.T,
	definition *opensplunk.KnowledgeObjectDefinition,
) *opensplunk.KnowledgeObjectDefinition {
	t.Helper()
	normalized, err := knowledgedefinition.Normalize(definition)
	if err != nil {
		t.Fatalf("normalize update result fixture: %v", err)
	}
	return normalized.Definition
}
