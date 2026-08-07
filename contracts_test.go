package opensplunk_test

import (
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestTypedValueRoundTripPreservesDistinctKinds(t *testing.T) {
	t.Parallel()

	values := map[string]*opensplunkv1.TypedValue{
		"minimum signed integer": {
			Kind: &opensplunkv1.TypedValue_Sint64Value{Sint64Value: int64(-1 << 63)},
		},
		"maximum unsigned integer": {
			Kind: &opensplunkv1.TypedValue_Uint64Value{Uint64Value: ^uint64(0)},
		},
		"decimal": {
			Kind: &opensplunkv1.TypedValue_DecimalValue{
				DecimalValue: &opensplunkv1.DecimalValue{Value: "12345678901234567890.000000001"},
			},
		},
		"explicit null": {
			Kind: &opensplunkv1.TypedValue_NullValue{NullValue: opensplunkv1.NullValue_NULL_VALUE_NULL},
		},
		"missing field": {
			Kind: &opensplunkv1.TypedValue_MissingValue{MissingValue: opensplunkv1.MissingValue_MISSING_VALUE_MISSING},
		},
		"nested object": {
			Kind: &opensplunkv1.TypedValue_ObjectValue{
				ObjectValue: &opensplunkv1.TypedObject{Fields: []*opensplunkv1.TypedObjectField{
					{
						Name: "items",
						Value: &opensplunkv1.TypedValue{Kind: &opensplunkv1.TypedValue_ListValue{
							ListValue: &opensplunkv1.TypedValueList{Values: []*opensplunkv1.TypedValue{
								{Kind: &opensplunkv1.TypedValue_BoolValue{BoolValue: true}},
								{Kind: &opensplunkv1.TypedValue_BytesValue{BytesValue: []byte{0x00, 0xff}}},
							}},
						}},
					},
				}},
			},
		},
	}

	for name, value := range values {
		value := value
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			wire, err := proto.Marshal(value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}

			var decoded opensplunkv1.TypedValue
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !proto.Equal(value, &decoded) {
				t.Fatalf("round trip changed value: want %v, got %v", value, &decoded)
			}
		})
	}
}

func TestGeneratedGoMessagesRetainKnownFieldsFromFuturePeers(t *testing.T) {
	t.Parallel()

	wire, err := proto.Marshal(&opensplunkv1.GetSystemBootstrapResponse{
		ApiVersion:              "v1",
		SplCompatibilityVersion: "open-splunk-v0.1",
	})
	if err != nil {
		t.Fatalf("marshal current response: %v", err)
	}
	wire = protowire.AppendString(
		protowire.AppendTag(wire, 2_047, protowire.BytesType),
		"future-response-field",
	)

	var decoded opensplunkv1.GetSystemBootstrapResponse
	if err := proto.Unmarshal(wire, &decoded); err != nil {
		t.Fatalf("unmarshal future response: %v", err)
	}
	if decoded.GetApiVersion() != "v1" ||
		decoded.GetSplCompatibilityVersion() != "open-splunk-v0.1" {
		t.Fatalf("known response fields = %+v", &decoded)
	}
	if len(decoded.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("Go runtime did not retain the future response field")
	}
}

func TestCollectorServiceDescriptorIsBidirectionalStreaming(t *testing.T) {
	t.Parallel()

	service := opensplunkv1.File_open_splunk_v1_collector_proto.Services().ByName(protoreflect.Name("CollectorIngestService"))
	if service == nil {
		t.Fatal("CollectorIngestService descriptor is missing")
	}
	method := service.Methods().ByName(protoreflect.Name("Collect"))
	if method == nil {
		t.Fatal("Collect method descriptor is missing")
	}
	if !method.IsStreamingClient() || !method.IsStreamingServer() {
		t.Fatalf("Collect must be bidirectional streaming: client=%t server=%t", method.IsStreamingClient(), method.IsStreamingServer())
	}
	if got := method.Input().FullName(); got != "open_splunk.v1.CollectRequest" {
		t.Fatalf("unexpected request type: %s", got)
	}
	if got := method.Output().FullName(); got != "open_splunk.v1.CollectResponse" {
		t.Fatalf("unexpected response type: %s", got)
	}
}

func TestEventRejectionAuthorizationCodesKeepStableWireNumbers(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		code opensplunkv1.EventRejectionCode
		want int32
	}{
		"host": {
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST,
			want: 11,
		},
		"source": {
			code: opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE,
			want: 12,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := int32(test.code); got != test.want {
				t.Fatalf("wire number = %d, want %d", got, test.want)
			}
		})
	}
}

func TestKnowledgeSnapshotContractIsCanonicalIntegerOnly(t *testing.T) {
	t.Parallel()

	file := opensplunkv1.File_open_splunk_v1_knowledge_proto
	var inspectMessages func(protoreflect.MessageDescriptors)
	inspectMessages = func(messages protoreflect.MessageDescriptors) {
		for index := 0; index < messages.Len(); index++ {
			message := messages.Get(index)
			fields := message.Fields()
			for fieldIndex := 0; fieldIndex < fields.Len(); fieldIndex++ {
				field := fields.Get(fieldIndex)
				if field.IsMap() {
					t.Errorf("%s must not use protobuf maps", field.FullName())
				}
				if field.Kind() == protoreflect.FloatKind || field.Kind() == protoreflect.DoubleKind {
					t.Errorf("%s must not use floating-point values", field.FullName())
				}
			}
			inspectMessages(message.Messages())
		}
	}
	inspectMessages(file.Messages())

	snapshot := file.Messages().ByName("KnowledgeSnapshot")
	if snapshot == nil {
		t.Fatal("KnowledgeSnapshot descriptor is missing")
	}
	for name, wantNumber := range map[protoreflect.Name]protoreflect.FieldNumber{
		"objects":                    8,
		"dependencies":               9,
		"lookup_assets":              10,
		"snapshot_sha256":            11,
		"tenant_catalog_state_token": 16,
	} {
		field := snapshot.Fields().ByName(name)
		if field == nil {
			t.Errorf("KnowledgeSnapshot.%s is missing", name)
			continue
		}
		if field.Number() != wantNumber {
			t.Errorf("KnowledgeSnapshot.%s wire number = %d, want %d", name, field.Number(), wantNumber)
		}
		if name == "tenant_catalog_state_token" && field.Kind() != protoreflect.BytesKind {
			t.Errorf("KnowledgeSnapshot.%s kind = %s, want bytes", name, field.Kind())
		}
	}
	visited := make(map[protoreflect.FullName]bool)
	var inspectDigestTree func(protoreflect.MessageDescriptor)
	inspectDigestTree = func(message protoreflect.MessageDescriptor) {
		if visited[message.FullName()] {
			return
		}
		visited[message.FullName()] = true
		fields := message.Fields()
		for index := 0; index < fields.Len(); index++ {
			field := fields.Get(index)
			if field.IsMap() {
				t.Errorf("digest field %s must not be a map", field.FullName())
			}
			if field.Kind() == protoreflect.FloatKind || field.Kind() == protoreflect.DoubleKind {
				t.Errorf("digest field %s must not be floating point", field.FullName())
			}
			if field.Kind() != protoreflect.MessageKind {
				continue
			}
			if field.Message().FullName() == "google.protobuf.Timestamp" {
				t.Errorf("digest field %s must not contain a timestamp", field.FullName())
				continue
			}
			inspectDigestTree(field.Message())
		}
	}
	inspectDigestTree(snapshot)
	if unknown := (&opensplunkv1.KnowledgeSnapshot{}).ProtoReflect().GetUnknown(); len(unknown) != 0 {
		t.Fatalf("new KnowledgeSnapshot has %d unknown bytes, want none", len(unknown))
	}

	snapshotObject := file.Messages().ByName("KnowledgeSnapshotObject")
	if snapshotObject == nil {
		t.Fatal("KnowledgeSnapshotObject descriptor is missing")
	}
	for name, wantNumber := range map[protoreflect.Name]protoreflect.FieldNumber{
		"resolution_ordinal": 1,
		"stage":              2,
		"stage_ordinal":      3,
	} {
		field := snapshotObject.Fields().ByName(name)
		if field == nil {
			t.Errorf("KnowledgeSnapshotObject.%s is missing", name)
			continue
		}
		if field.Number() != wantNumber {
			t.Errorf("KnowledgeSnapshotObject.%s wire number = %d, want %d", name, field.Number(), wantNumber)
		}
	}

	dependency := file.Messages().ByName("KnowledgeObjectDependency")
	if dependency == nil {
		t.Fatal("KnowledgeObjectDependency descriptor is missing")
	}
	for name, wantNumber := range map[protoreflect.Name]protoreflect.FieldNumber{
		"topological_depth": 6,
		"canonical_ordinal": 7,
	} {
		field := dependency.Fields().ByName(name)
		if field == nil {
			t.Errorf("KnowledgeObjectDependency.%s is missing", name)
			continue
		}
		if field.Number() != wantNumber {
			t.Errorf("KnowledgeObjectDependency.%s wire number = %d, want %d", name, field.Number(), wantNumber)
		}
	}

	lookupAsset := file.Messages().ByName("KnowledgeSnapshotLookupAsset")
	if lookupAsset == nil {
		t.Fatal("KnowledgeSnapshotLookupAsset descriptor is missing")
	}
	assetOrdinal := lookupAsset.Fields().ByName("asset_ordinal")
	if assetOrdinal == nil || assetOrdinal.Number() != 1 {
		t.Errorf("KnowledgeSnapshotLookupAsset.asset_ordinal = %v, want wire field 1", assetOrdinal)
	}
}

func TestTierOneKnowledgeDefinitionBodiesKeepStableWireNumbers(t *testing.T) {
	t.Parallel()

	descriptor := opensplunkv1.File_open_splunk_v1_knowledge_proto.Messages().ByName("KnowledgeObjectDefinition")
	if descriptor == nil {
		t.Fatal("KnowledgeObjectDefinition descriptor is missing")
	}
	body := descriptor.Oneofs().ByName("body")
	if body == nil {
		t.Fatal("KnowledgeObjectDefinition.body oneof is missing")
	}
	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"field_extraction": 10,
		"field_alias":      11,
		"calculated_field": 12,
	}
	calculated := opensplunkv1.File_open_splunk_v1_knowledge_proto.Messages().ByName("CalculatedFieldDefinition")
	if calculated == nil {
		t.Fatal("CalculatedFieldDefinition is missing")
	}
	if field := calculated.Fields().ByName("overwrite_behavior"); field == nil || field.Number() != 3 {
		t.Errorf("CalculatedFieldDefinition.overwrite_behavior = %v, want wire field 3", field)
	}
	if body.Fields().Len() != len(want) {
		t.Fatalf("Tier-1 body count = %d, want %d", body.Fields().Len(), len(want))
	}
	for name, wantNumber := range want {
		field := body.Fields().ByName(name)
		if field == nil {
			t.Errorf("KnowledgeObjectDefinition.%s is missing", name)
			continue
		}
		if field.Number() != wantNumber {
			t.Errorf("KnowledgeObjectDefinition.%s wire number = %d, want %d", name, field.Number(), wantNumber)
		}
	}
	if field := descriptor.Fields().ByName("stage_order"); field != nil {
		t.Fatalf("client-authored KnowledgeObjectDefinition.stage_order is present at wire field %d", field.Number())
	}

	object := opensplunkv1.File_open_splunk_v1_knowledge_proto.Messages().ByName("KnowledgeObject")
	if object == nil {
		t.Fatal("KnowledgeObject descriptor is missing")
	}
	for _, name := range []protoreflect.Name{"app_id", "name", "sharing_scope", "object_type", "definition"} {
		if field := object.Fields().ByName(name); field == nil {
			t.Errorf("KnowledgeObject.%s indexed-agreement field is missing", name)
		}
	}
	for name, contract := range map[protoreflect.Name]struct {
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		optional bool
	}{
		"disabled_at":       {number: 14, kind: protoreflect.MessageKind, optional: true},
		"quarantined_at":    {number: 15, kind: protoreflect.MessageKind, optional: true},
		"deleted_at":        {number: 16, kind: protoreflect.MessageKind, optional: true},
		"quarantine_reason": {number: 17, kind: protoreflect.StringKind, optional: true},
	} {
		field := object.Fields().ByName(name)
		if field == nil {
			t.Errorf("KnowledgeObject.%s lifecycle field is missing", name)
			continue
		}
		if field.Number() != contract.number || field.Kind() != contract.kind {
			t.Errorf("KnowledgeObject.%s = wire field %d kind %s, want %d %s", name, field.Number(), field.Kind(), contract.number, contract.kind)
		}
		if field.HasOptionalKeyword() != contract.optional {
			t.Errorf("KnowledgeObject.%s optional = %t, want %t", name, field.HasOptionalKeyword(), contract.optional)
		}
		if contract.kind == protoreflect.MessageKind && field.Message().FullName() != "google.protobuf.Timestamp" {
			t.Errorf("KnowledgeObject.%s message = %s, want google.protobuf.Timestamp", name, field.Message().FullName())
		}
	}
	for _, name := range []protoreflect.Name{"app_id", "name", "sharing_scope", "body"} {
		if name == "body" {
			if descriptor.Oneofs().ByName(name) == nil {
				t.Errorf("KnowledgeObjectDefinition.%s indexed-agreement oneof is missing", name)
			}
			continue
		}
		if field := descriptor.Fields().ByName(name); field == nil {
			t.Errorf("KnowledgeObjectDefinition.%s indexed-agreement field is missing", name)
		}
	}
}

func TestKnowledgeFeatureWireNumberIsReservedButNotImplied(t *testing.T) {
	t.Parallel()

	if got := int32(opensplunkv1.ServerFeature_SERVER_FEATURE_KNOWLEDGE_FIELD_OBJECTS); got != 15 {
		t.Fatalf("knowledge field objects feature wire number = %d, want 15", got)
	}
}

func TestKnowledgeRecoveryAndMutationAuthorityKeepStableWireContracts(t *testing.T) {
	t.Parallel()

	if got := int32(opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_QUARANTINED); got != 4 {
		t.Fatalf("quarantined knowledge state wire number = %d, want 4", got)
	}
	if got := int32(opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DELETED); got != 5 {
		t.Fatalf("deleted knowledge state wire number = %d, want 5", got)
	}

	apiFile := opensplunkv1.File_open_splunk_v1_knowledge_api_proto
	for _, messageName := range []protoreflect.Name{
		"CreateKnowledgeObjectRequest",
		"UpdateKnowledgeObjectRequest",
		"SetKnowledgeObjectStateRequest",
		"DeleteKnowledgeObjectRequest",
		"QuarantineKnowledgeObjectRequest",
	} {
		message := apiFile.Messages().ByName(messageName)
		if message == nil {
			t.Errorf("%s descriptor is missing", messageName)
			continue
		}
		field := message.Fields().ByName("client_request_id")
		if field == nil {
			t.Errorf("%s.client_request_id is missing", messageName)
			continue
		}
		if field.HasOptionalKeyword() {
			t.Errorf("%s.client_request_id must be a validation-required scalar, not optional", messageName)
		}
	}

	prepare := apiFile.Messages().ByName("PrepareKnowledgeObjectQuarantineRequest")
	quarantine := apiFile.Messages().ByName("QuarantineKnowledgeObjectRequest")
	if prepare == nil || quarantine == nil {
		t.Fatalf("quarantine prepare/mutation descriptors missing: prepare=%v quarantine=%v", prepare, quarantine)
	}
	if field := quarantine.Fields().ByName("recovery_token"); field == nil || field.Number() != 1 {
		t.Errorf("QuarantineKnowledgeObjectRequest.recovery_token = %v, want wire field 1", field)
	}
}

func TestKnowledgeProvenanceVariantsCannotMixIdentityAndRedaction(t *testing.T) {
	t.Parallel()

	file := opensplunkv1.File_open_splunk_v1_knowledge_proto
	provenance := file.Messages().ByName("KnowledgeProvenance")
	if provenance == nil {
		t.Fatal("KnowledgeProvenance descriptor is missing")
	}
	source := provenance.Oneofs().ByName("source")
	if source == nil {
		t.Fatal("KnowledgeProvenance.source oneof is missing")
	}
	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"authored":          1,
		"authorized_object": 2,
		"redacted_object":   3,
	}
	if source.Fields().Len() != len(want) {
		t.Fatalf("KnowledgeProvenance source variants = %d, want %d", source.Fields().Len(), len(want))
	}
	for name, number := range want {
		field := source.Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Errorf("KnowledgeProvenance.%s = %v, want wire field %d", name, field, number)
		}
	}
	redacted := file.Messages().ByName("KnowledgeRedactedObjectProvenance")
	if redacted == nil {
		t.Fatal("KnowledgeRedactedObjectProvenance descriptor is missing")
	}
	for _, forbidden := range []protoreflect.Name{
		"knowledge_object_id", "knowledge_object_version", "object_name", "definition_location",
	} {
		if field := redacted.Fields().ByName(forbidden); field != nil {
			t.Errorf("redacted provenance unexpectedly exposes %s", forbidden)
		}
	}
	for name, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"redacted_object_ordinal": 1,
		"object_type":             2,
		"stage":                   3,
	} {
		field := redacted.Fields().ByName(name)
		if field == nil || field.Number() != number {
			t.Errorf("KnowledgeRedactedObjectProvenance.%s = %v, want wire field %d", name, field, number)
		}
	}
}

func TestKnowledgeApiFamiliesExposeOptimisticAndBoundedContracts(t *testing.T) {
	t.Parallel()

	file := opensplunkv1.File_open_splunk_v1_knowledge_api_proto
	tests := map[protoreflect.Name][]protoreflect.Name{
		"ListKnowledgeObjectsRequest": {
			"page",
			"app_id_filter",
			"owner_id_filter",
			"text_filter",
			"object_type_filters",
			"state_filters",
			"sharing_scope_filters",
			"selector_text_filter",
			"sort_by",
			"sort_direction",
		},
		"UpdateKnowledgeObjectRequest": {
			"expected_version",
			"update_mask",
			"client_request_id",
		},
		"ValidateKnowledgeObjectRequest": {
			"expected_version",
			"update_mask",
		},
		"ListKnowledgeObjectDependenciesRequest": {
			"version",
			"page",
		},
		"ListKnowledgeObjectDependentsRequest": {
			"version",
			"page",
		},
		"PreviewKnowledgeObjectRequest": {
			"retained_search_job_id",
			"expected_version",
			"update_mask",
			"maximum_rows",
		},
	}
	for messageName, fieldNames := range tests {
		message := file.Messages().ByName(messageName)
		if message == nil {
			t.Errorf("%s descriptor is missing", messageName)
			continue
		}
		for _, fieldName := range fieldNames {
			if field := message.Fields().ByName(fieldName); field == nil {
				t.Errorf("%s.%s is missing", messageName, fieldName)
			}
		}
	}
}
