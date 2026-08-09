package opensplunk_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
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

func TestKnowledgeSnapshotReferenceAndSummaryKeepExactWireContracts(t *testing.T) {
	t.Parallel()

	type fieldContract struct {
		name            protoreflect.Name
		number          protoreflect.FieldNumber
		kind            protoreflect.Kind
		cardinality     protoreflect.Cardinality
		hasPresence     bool
		optionalKeyword bool
		message         protoreflect.FullName
		enum            protoreflect.FullName
		oneof           protoreflect.Name
	}
	type messageContract struct {
		name   protoreflect.Name
		fields []fieldContract
	}

	file := opensplunkv1.File_open_splunk_v1_knowledge_proto
	contracts := []messageContract{
		{
			name: "KnowledgeSnapshotRef",
			fields: []fieldContract{
				{name: "snapshot_sha256", number: 1, kind: protoreflect.BytesKind, cardinality: protoreflect.Optional},
				{name: "tenant_catalog_revision", number: 2, kind: protoreflect.Uint64Kind, cardinality: protoreflect.Optional},
				{name: "tenant_catalog_state_token", number: 3, kind: protoreflect.BytesKind, cardinality: protoreflect.Optional},
				{name: "object_count", number: 4, kind: protoreflect.Uint32Kind, cardinality: protoreflect.Optional},
				{name: "compiler_compatibility_version", number: 5, kind: protoreflect.StringKind, cardinality: protoreflect.Optional},
			},
		},
		{
			name: "KnowledgeSnapshotAuthorizedObjectSummary",
			fields: []fieldContract{
				{name: "knowledge_object_id", number: 1, kind: protoreflect.StringKind, cardinality: protoreflect.Optional},
				{name: "version", number: 2, kind: protoreflect.Uint64Kind, cardinality: protoreflect.Optional},
				{name: "name", number: 3, kind: protoreflect.StringKind, cardinality: protoreflect.Optional},
			},
		},
		{
			name: "KnowledgeSnapshotObjectSummary",
			fields: []fieldContract{
				{name: "resolution_ordinal", number: 1, kind: protoreflect.Uint32Kind, cardinality: protoreflect.Optional},
				{
					name:        "object_type",
					number:      2,
					kind:        protoreflect.EnumKind,
					cardinality: protoreflect.Optional,
					enum:        "open_splunk.v1.KnowledgeObjectType",
				},
				{
					name:        "stage",
					number:      3,
					kind:        protoreflect.EnumKind,
					cardinality: protoreflect.Optional,
					enum:        "open_splunk.v1.KnowledgeSearchStage",
				},
				{
					name:        "authorized_object",
					number:      4,
					kind:        protoreflect.MessageKind,
					cardinality: protoreflect.Optional,
					hasPresence: true,
					message:     "open_splunk.v1.KnowledgeSnapshotAuthorizedObjectSummary",
					oneof:       "disclosure",
				},
				{
					name:        "redacted",
					number:      5,
					kind:        protoreflect.BoolKind,
					cardinality: protoreflect.Optional,
					hasPresence: true,
					oneof:       "disclosure",
				},
			},
		},
		{
			name: "KnowledgeSnapshotSummary",
			fields: []fieldContract{
				{
					name:        "ref",
					number:      1,
					kind:        protoreflect.MessageKind,
					cardinality: protoreflect.Optional,
					hasPresence: true,
					message:     "open_splunk.v1.KnowledgeSnapshotRef",
				},
				{
					name:        "objects",
					number:      2,
					kind:        protoreflect.MessageKind,
					cardinality: protoreflect.Repeated,
					message:     "open_splunk.v1.KnowledgeSnapshotObjectSummary",
				},
				{name: "objects_truncated", number: 3, kind: protoreflect.BoolKind, cardinality: protoreflect.Optional},
			},
		},
	}

	for _, contract := range contracts {
		contract := contract
		t.Run(string(contract.name), func(t *testing.T) {
			descriptor := file.Messages().ByName(contract.name)
			if descriptor == nil {
				t.Fatalf("%s descriptor is missing", contract.name)
			}
			if got := descriptor.Fields().Len(); got != len(contract.fields) {
				t.Fatalf("%s field count = %d, want exact append-only count %d", contract.name, got, len(contract.fields))
			}
			for index, want := range contract.fields {
				field := descriptor.Fields().Get(index)
				if field.Name() != want.name || field.Number() != want.number {
					t.Errorf(
						"%s declaration %d = %s/%d, want %s/%d",
						contract.name,
						index,
						field.Name(),
						field.Number(),
						want.name,
						want.number,
					)
				}
				if field.Kind() != want.kind || field.Cardinality() != want.cardinality {
					t.Errorf(
						"%s.%s = %s/%s, want %s/%s",
						contract.name,
						want.name,
						field.Cardinality(),
						field.Kind(),
						want.cardinality,
						want.kind,
					)
				}
				if field.HasPresence() != want.hasPresence || field.HasOptionalKeyword() != want.optionalKeyword {
					t.Errorf(
						"%s.%s presence = %t/optional-keyword=%t, want %t/%t",
						contract.name,
						want.name,
						field.HasPresence(),
						field.HasOptionalKeyword(),
						want.hasPresence,
						want.optionalKeyword,
					)
				}
				if want.message != "" && (field.Message() == nil || field.Message().FullName() != want.message) {
					t.Errorf("%s.%s message = %v, want %s", contract.name, want.name, field.Message(), want.message)
				}
				if want.enum != "" && (field.Enum() == nil || field.Enum().FullName() != want.enum) {
					t.Errorf("%s.%s enum = %v, want %s", contract.name, want.name, field.Enum(), want.enum)
				}
				containingOneof := field.ContainingOneof()
				if want.oneof == "" {
					if containingOneof != nil {
						t.Errorf("%s.%s unexpectedly belongs to oneof %s", contract.name, want.name, containingOneof.Name())
					}
					continue
				}
				if containingOneof == nil || containingOneof.Name() != want.oneof || containingOneof.IsSynthetic() {
					t.Errorf("%s.%s oneof = %v, want non-synthetic %s", contract.name, want.name, containingOneof, want.oneof)
				}
			}
		})
	}

	objectSummary := file.Messages().ByName("KnowledgeSnapshotObjectSummary")
	if objectSummary == nil {
		t.Fatal("KnowledgeSnapshotObjectSummary descriptor is missing")
	}
	if objectSummary.Oneofs().Len() != 1 {
		t.Fatalf("KnowledgeSnapshotObjectSummary oneof count = %d, want exactly 1", objectSummary.Oneofs().Len())
	}
	disclosure := objectSummary.Oneofs().ByName("disclosure")
	if disclosure == nil || disclosure.IsSynthetic() || disclosure.Fields().Len() != 2 {
		t.Fatalf("KnowledgeSnapshotObjectSummary.disclosure = %v, want non-synthetic two-variant oneof", disclosure)
	}
	if disclosure.Fields().Get(0).Name() != "authorized_object" || disclosure.Fields().Get(1).Name() != "redacted" {
		t.Fatalf("KnowledgeSnapshotObjectSummary.disclosure variants = %s/%s, want authorized_object/redacted", disclosure.Fields().Get(0).Name(), disclosure.Fields().Get(1).Name())
	}

	attachments := []struct {
		name    string
		file    protoreflect.FileDescriptor
		message protoreflect.Name
		number  protoreflect.FieldNumber
		value   protoreflect.FullName
	}{
		{
			name:    "search job",
			file:    opensplunkv1.File_open_splunk_v1_search_proto,
			message: "SearchJob",
			number:  23,
			value:   "open_splunk.v1.KnowledgeSnapshotSummary",
		},
		{
			name:    "history entry",
			file:    opensplunkv1.File_open_splunk_v1_history_proto,
			message: "SearchHistoryEntry",
			number:  18,
			value:   "open_splunk.v1.KnowledgeSnapshotSummary",
		},
		{
			name:    "attempt audit",
			file:    opensplunkv1.File_open_splunk_v1_search_attempt_audit_proto,
			message: "SearchAttemptAuditEvent",
			number:  8,
			value:   "open_splunk.v1.KnowledgeSnapshotRef",
		},
		{
			name:    "inspection response",
			file:    opensplunkv1.File_open_splunk_v1_search_inspection_api_proto,
			message: "InspectSearchJobResponse",
			number:  7,
			value:   "open_splunk.v1.KnowledgeSnapshotSummary",
		},
		{
			name:    "export job",
			file:    opensplunkv1.File_open_splunk_v1_export_proto,
			message: "ExportJob",
			number:  13,
			value:   "open_splunk.v1.KnowledgeSnapshotSummary",
		},
	}
	for _, attachment := range attachments {
		attachment := attachment
		t.Run(attachment.name+" attachment", func(t *testing.T) {
			message := attachment.file.Messages().ByName(attachment.message)
			if message == nil {
				t.Fatalf("%s descriptor is missing", attachment.message)
			}
			field := message.Fields().ByName("knowledge_snapshot")
			if field == nil {
				t.Fatalf("%s.knowledge_snapshot is missing", attachment.message)
			}
			if field.Number() != attachment.number || field.Kind() != protoreflect.MessageKind ||
				field.Cardinality() != protoreflect.Optional || field.Message() == nil || field.Message().FullName() != attachment.value {
				t.Errorf(
					"%s.knowledge_snapshot = wire %d/%s/%s/%v, want %d/optional/message/%s",
					attachment.message,
					field.Number(),
					field.Cardinality(),
					field.Kind(),
					field.Message(),
					attachment.number,
					attachment.value,
				)
			}
			presence := field.ContainingOneof()
			if !field.HasPresence() || !field.HasOptionalKeyword() || presence == nil ||
				!presence.IsSynthetic() || presence.Name() != "_knowledge_snapshot" || presence.Fields().Len() != 1 {
				t.Errorf(
					"%s.knowledge_snapshot presence = has:%t optional:%t oneof:%v, want optional synthetic _knowledge_snapshot",
					attachment.message,
					field.HasPresence(),
					field.HasOptionalKeyword(),
					presence,
				)
			}
		})
	}
}

func TestFieldExtractionDefinitionDeterministicWireMatchesCrossLanguageGolden(t *testing.T) {
	t.Parallel()

	descriptor := opensplunkv1.File_open_splunk_v1_knowledge_proto.Messages().ByName("FieldExtractionDefinition")
	if descriptor == nil {
		t.Fatal("FieldExtractionDefinition descriptor is missing")
	}
	wantDeclarationOrder := []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
	}{
		{name: "input_field", number: 1},
		{name: "overwrite_behavior", number: 4},
		{name: "regex", number: 2},
		{name: "json", number: 3},
	}
	if got := descriptor.Fields().Len(); got != len(wantDeclarationOrder) {
		t.Fatalf("FieldExtractionDefinition field count = %d, want %d", got, len(wantDeclarationOrder))
	}
	for index, want := range wantDeclarationOrder {
		field := descriptor.Fields().Get(index)
		if field.Name() != want.name || field.Number() != want.number {
			t.Fatalf(
				"FieldExtractionDefinition declaration %d = %s/%d, want %s/%d",
				index,
				field.Name(),
				field.Number(),
				want.name,
				want.number,
			)
		}
	}

	type regexFixture struct {
		Pattern      string   `json:"pattern"`
		OutputFields []string `json:"outputFields"`
	}
	type jsonFixture struct {
		Path        string `json:"path"`
		OutputField string `json:"outputField"`
	}
	var fixture struct {
		Version int `json:"version"`
		Cases   []struct {
			Name              string        `json:"name"`
			InputField        string        `json:"inputField"`
			OverwriteBehavior int32         `json:"overwriteBehavior"`
			Regex             *regexFixture `json:"regex"`
			JSON              *jsonFixture  `json:"json"`
			WireHex           string        `json:"wireHex"`
		} `json:"cases"`
	}
	encodedFixture, err := os.ReadFile("testdata/knowledge-field-extraction-wire.json")
	if err != nil {
		t.Fatalf("read cross-language field-extraction fixture: %v", err)
	}
	if err := json.Unmarshal(encodedFixture, &fixture); err != nil {
		t.Fatalf("decode cross-language field-extraction fixture: %v", err)
	}
	if fixture.Version != 1 || len(fixture.Cases) != 2 {
		t.Fatalf("cross-language field-extraction fixture = version %d with %d cases, want version 1 with 2 cases", fixture.Version, len(fixture.Cases))
	}

	seen := make(map[string]bool, len(fixture.Cases))
	for _, contract := range fixture.Cases {
		contract := contract
		t.Run(contract.Name, func(t *testing.T) {
			if contract.Name != "regex" && contract.Name != "json" {
				t.Fatalf("unexpected cross-language fixture case %q", contract.Name)
			}
			if seen[contract.Name] {
				t.Fatalf("duplicate cross-language fixture case %q", contract.Name)
			}
			seen[contract.Name] = true

			message := &opensplunkv1.FieldExtractionDefinition{
				InputField:        contract.InputField,
				OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior(contract.OverwriteBehavior),
			}
			switch {
			case contract.Regex != nil && contract.JSON == nil:
				message.Extraction = &opensplunkv1.FieldExtractionDefinition_Regex{
					Regex: &opensplunkv1.RegexFieldExtractionDefinition{
						Pattern:      contract.Regex.Pattern,
						OutputFields: append([]string(nil), contract.Regex.OutputFields...),
					},
				}
			case contract.JSON != nil && contract.Regex == nil:
				message.Extraction = &opensplunkv1.FieldExtractionDefinition_Json{
					Json: &opensplunkv1.JsonFieldExtractionDefinition{
						Path:        contract.JSON.Path,
						OutputField: contract.JSON.OutputField,
					},
				}
			default:
				t.Fatal("cross-language fixture must contain exactly one extraction body")
			}

			wantWire, err := hex.DecodeString(contract.WireHex)
			if err != nil {
				t.Fatalf("decode golden wire: %v", err)
			}
			marshal := proto.MarshalOptions{Deterministic: true}
			first, err := marshal.Marshal(message)
			if err != nil {
				t.Fatalf("marshal FieldExtractionDefinition: %v", err)
			}
			second, err := marshal.Marshal(message)
			if err != nil {
				t.Fatalf("marshal FieldExtractionDefinition again: %v", err)
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("deterministic Go wire changed between runs: first=%x second=%x", first, second)
			}
			if !bytes.Equal(first, wantWire) {
				t.Fatalf("Go wire = %x, want shared Go/TypeScript golden %x", first, wantWire)
			}
		})
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

func TestKnowledgeMutationResponsesPairRevisionAndStateToken(t *testing.T) {
	t.Parallel()

	token := make([]byte, 32)
	for index := range token {
		token[index] = byte(index)
	}
	sharedWire := append([]byte{0x10, 0x07, 0x1a, 0x20}, token...)
	deleteWire := append([]byte{0x18, 0x07, 0x22, 0x20}, token...)

	tests := []struct {
		name           protoreflect.Name
		fieldCount     int
		revisionNumber protoreflect.FieldNumber
		tokenNumber    protoreflect.FieldNumber
		message        proto.Message
		wantWire       []byte
	}{
		{
			name:           "CreateKnowledgeObjectResponse",
			fieldCount:     3,
			revisionNumber: 2,
			tokenNumber:    3,
			message: &opensplunkv1.CreateKnowledgeObjectResponse{
				TenantCatalogRevision:   7,
				TenantCatalogStateToken: token,
			},
			wantWire: sharedWire,
		},
		{
			name:           "UpdateKnowledgeObjectResponse",
			fieldCount:     3,
			revisionNumber: 2,
			tokenNumber:    3,
			message: &opensplunkv1.UpdateKnowledgeObjectResponse{
				TenantCatalogRevision:   7,
				TenantCatalogStateToken: token,
			},
			wantWire: sharedWire,
		},
		{
			name:           "SetKnowledgeObjectStateResponse",
			fieldCount:     3,
			revisionNumber: 2,
			tokenNumber:    3,
			message: &opensplunkv1.SetKnowledgeObjectStateResponse{
				TenantCatalogRevision:   7,
				TenantCatalogStateToken: token,
			},
			wantWire: sharedWire,
		},
		{
			name:           "DeleteKnowledgeObjectResponse",
			fieldCount:     4,
			revisionNumber: 3,
			tokenNumber:    4,
			message: &opensplunkv1.DeleteKnowledgeObjectResponse{
				TenantCatalogRevision:   7,
				TenantCatalogStateToken: token,
			},
			wantWire: deleteWire,
		},
	}

	file := opensplunkv1.File_open_splunk_v1_knowledge_api_proto
	for _, test := range tests {
		test := test
		t.Run(string(test.name), func(t *testing.T) {
			t.Parallel()

			descriptor := file.Messages().ByName(test.name)
			if descriptor == nil {
				t.Fatalf("%s descriptor is missing", test.name)
			}
			if got := descriptor.Fields().Len(); got != test.fieldCount {
				t.Fatalf("%s field count = %d, want exact append-only count %d", test.name, got, test.fieldCount)
			}

			revision := descriptor.Fields().ByName("tenant_catalog_revision")
			if revision == nil || revision.Number() != test.revisionNumber || revision.Kind() != protoreflect.Uint64Kind {
				t.Errorf("%s.tenant_catalog_revision = %v, want uint64 field %d", test.name, revision, test.revisionNumber)
			}
			stateToken := descriptor.Fields().ByName("tenant_catalog_state_token")
			if stateToken == nil || stateToken.Number() != test.tokenNumber || stateToken.Kind() != protoreflect.BytesKind {
				t.Errorf("%s.tenant_catalog_state_token = %v, want bytes field %d", test.name, stateToken, test.tokenNumber)
			} else if stateToken.IsList() || stateToken.IsMap() || stateToken.HasOptionalKeyword() {
				t.Errorf("%s.tenant_catalog_state_token must be one validation-required bytes scalar", test.name)
			}

			first, err := (proto.MarshalOptions{Deterministic: true}).Marshal(test.message)
			if err != nil {
				t.Fatalf("marshal response: %v", err)
			}
			second, err := (proto.MarshalOptions{Deterministic: true}).Marshal(test.message)
			if err != nil {
				t.Fatalf("marshal response again: %v", err)
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("deterministic response encoding changed between runs: first=%x second=%x", first, second)
			}
			if !bytes.Equal(first, test.wantWire) {
				t.Fatalf("response wire encoding = %x, want stable fields %x", first, test.wantWire)
			}
		})
	}
}

func TestKnowledgeMutationOutcomeRecordPinsCanonicalAuthorityWire(t *testing.T) {
	t.Parallel()

	descriptor := opensplunkv1.File_open_splunk_v1_knowledge_api_proto.Messages().ByName("KnowledgeMutationOutcomeRecord")
	if descriptor == nil {
		t.Fatal("KnowledgeMutationOutcomeRecord descriptor is missing")
	}
	fields := []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
		kind   protoreflect.Kind
	}{
		{"route", 1, protoreflect.StringKind},
		{"mutation_kind", 2, protoreflect.StringKind},
		{"object", 3, protoreflect.MessageKind},
		{"tenant_catalog_revision", 4, protoreflect.Uint64Kind},
		{"tenant_catalog_state_token", 5, protoreflect.BytesKind},
		{"successful_audit_sequence", 6, protoreflect.Uint64Kind},
		{"recovery_audit_sequence", 7, protoreflect.Uint64Kind},
		{"occurred_at_unix_micro", 8, protoreflect.Int64Kind},
		{"retention_anchor_unix_micro", 9, protoreflect.Int64Kind},
		{"retain_until_unix_micro", 10, protoreflect.Int64Kind},
	}
	if descriptor.Fields().Len() != len(fields) || descriptor.Oneofs().Len() != 1 {
		t.Fatalf("outcome descriptor shape = %d fields/%d oneofs, want %d/1",
			descriptor.Fields().Len(), descriptor.Oneofs().Len(), len(fields))
	}
	auditAuthority := descriptor.Oneofs().ByName("audit_authority")
	for _, want := range fields {
		field := descriptor.Fields().ByName(want.name)
		if field == nil || field.Number() != want.number || field.Kind() != want.kind || field.IsList() || field.IsMap() {
			t.Errorf("KnowledgeMutationOutcomeRecord.%s = %v, want singular %s field %d",
				want.name, field, want.kind, want.number)
			continue
		}
		if (want.number == 6 || want.number == 7) && field.ContainingOneof() != auditAuthority {
			t.Errorf("KnowledgeMutationOutcomeRecord.%s is not in audit_authority", want.name)
		}
	}

	message := &opensplunkv1.KnowledgeMutationOutcomeRecord{
		Route:        "objects.update",
		MutationKind: "scope_change",
		Object: &opensplunkv1.KnowledgeObjectVersionReference{
			KnowledgeObjectId: "ko-1",
			Version:           7,
			DefinitionSha256:  []byte{1, 2},
		},
		TenantCatalogRevision:   9,
		TenantCatalogStateToken: []byte{0xaa, 0xbb},
		AuditAuthority: &opensplunkv1.KnowledgeMutationOutcomeRecord_SuccessfulAuditSequence{
			SuccessfulAuditSequence: 11,
		},
		OccurredAtUnixMicro:      13,
		RetentionAnchorUnixMicro: 17,
		RetainUntilUnixMicro:     19,
	}
	wantWire := append([]byte{0x0a, 0x0e}, []byte("objects.update")...)
	wantWire = append(wantWire, 0x12, 0x0c)
	wantWire = append(wantWire, []byte("scope_change")...)
	wantWire = append(wantWire,
		0x1a, 0x0c, 0x0a, 0x04, 'k', 'o', '-', '1', 0x10, 0x07, 0x1a, 0x02, 0x01, 0x02,
		0x20, 0x09, 0x2a, 0x02, 0xaa, 0xbb,
		0x40, 0x0d, 0x48, 0x11, 0x50, 0x13, 0x30, 0x0b,
	)
	first, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal outcome authority: %v", err)
	}
	second, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal outcome authority again: %v", err)
	}
	if !bytes.Equal(first, second) || !bytes.Equal(first, wantWire) {
		t.Fatalf("outcome authority wire = %x then %x, want %x", first, second, wantWire)
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

func TestSearchInspectionProvenanceKeepsAppendOnlyWireContract(t *testing.T) {
	t.Parallel()

	file := opensplunkv1.File_open_splunk_v1_search_inspection_api_proto
	stage := file.Messages().ByName("SearchInspectionLogicalStage")
	if stage == nil {
		t.Fatal("SearchInspectionLogicalStage descriptor is missing")
	}
	for name, contract := range map[protoreflect.Name]struct {
		number  protoreflect.FieldNumber
		message protoreflect.FullName
	}{
		"operator_provenance": {6, "open_splunk.v1.KnowledgeProvenance"},
		"output_provenance":   {7, "open_splunk.v1.SearchInspectionOutputProvenance"},
	} {
		field := stage.Fields().ByName(name)
		if field == nil || field.Number() != contract.number ||
			field.Kind() != protoreflect.MessageKind ||
			field.Cardinality() != protoreflect.Repeated ||
			field.HasPresence() || field.HasOptionalKeyword() ||
			field.Message() == nil || field.Message().FullName() != contract.message {
			t.Errorf(
				"SearchInspectionLogicalStage.%s = %+v, want repeated message field %d of %s",
				name,
				field,
				contract.number,
				contract.message,
			)
		}
	}

	output := file.Messages().ByName("SearchInspectionOutputProvenance")
	if output == nil || output.Fields().Len() != 2 {
		t.Fatalf("SearchInspectionOutputProvenance descriptor = %+v, want two fields", output)
	}
	outputField := output.Fields().ByName("output_field")
	if outputField == nil || outputField.Number() != 1 ||
		outputField.Kind() != protoreflect.StringKind ||
		outputField.Cardinality() != protoreflect.Optional ||
		outputField.HasPresence() || outputField.HasOptionalKeyword() {
		t.Errorf("SearchInspectionOutputProvenance.output_field = %+v, want scalar string field 1", outputField)
	}
	provenance := output.Fields().ByName("provenance")
	if provenance == nil || provenance.Number() != 2 ||
		provenance.Kind() != protoreflect.MessageKind ||
		provenance.Cardinality() != protoreflect.Optional ||
		!provenance.HasPresence() || provenance.HasOptionalKeyword() ||
		provenance.Message() == nil ||
		provenance.Message().FullName() != "open_splunk.v1.KnowledgeProvenance" {
		t.Errorf("SearchInspectionOutputProvenance.provenance = %+v, want present message field 2", provenance)
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
