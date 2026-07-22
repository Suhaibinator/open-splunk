package opensplunk_test

import (
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
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
