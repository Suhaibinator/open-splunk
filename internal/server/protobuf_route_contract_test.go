package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const updateProtobufRouteFixturesEnvironment = "UPDATE_PROTOBUF_ROUTE_FIXTURES"

type protobufHTTPRouteContractFixture struct {
	Version           int                               `json:"version"`
	FutureFieldNumber int32                             `json:"futureFieldNumber"`
	Routes            []protobufHTTPRouteContractRecord `json:"routes"`
}

type protobufHTTPRouteContractRecord struct {
	Path               string `json:"path"`
	RequestType        string `json:"requestType"`
	ResponseType       string `json:"responseType"`
	RequestKnownWire   string `json:"requestKnownWire"`
	RequestFutureWire  string `json:"requestFutureWire"`
	ResponseKnownWire  string `json:"responseKnownWire"`
	ResponseFutureWire string `json:"responseFutureWire"`
}

// TestEveryProtobufHTTPRouteHasCrossRuntimeForwardCompatibility proves every
// protobuf HTTP route decodes a future (unknown) field on both its request
// and response types without error and without disturbing the known fields. The fixture is the route inventory: a new protobuf
// route needs a record (path, request and response type) added by hand, then
// UPDATE_PROTOBUF_ROUTE_FIXTURES=1 regenerates its wire bytes. The same file
// drives lib/api/protobuf-contracts.test.ts on the TypeScript side.
func TestEveryProtobufHTTPRouteHasCrossRuntimeForwardCompatibility(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "testdata", "protobuf-http-route-contracts.json")
	encoded, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read route fixture: %v", err)
	}
	var fixture protobufHTTPRouteContractFixture
	if err := json.Unmarshal(encoded, &fixture); err != nil {
		t.Fatalf("decode route fixture: %v", err)
	}
	update := os.Getenv(updateProtobufRouteFixturesEnvironment) == "1"
	if fixture.Version != 1 || len(fixture.Routes) != 97 {
		t.Fatalf("route fixture version/count = %d/%d, want 1/97", fixture.Version, len(fixture.Routes))
	}
	futureFieldNumber := protowire.Number(fixture.FutureFieldNumber)
	if futureFieldNumber <= 0 || futureFieldNumber > protowire.MaxValidNumber {
		t.Fatalf("future field number = %d", futureFieldNumber)
	}

	seenPaths := make(map[string]struct{}, len(fixture.Routes))
	for index := range fixture.Routes {
		route := &fixture.Routes[index]
		if _, duplicate := seenPaths[route.Path]; duplicate {
			t.Fatalf("duplicate route fixture %q", route.Path)
		}
		seenPaths[route.Path] = struct{}{}

		t.Run(route.Path, func(t *testing.T) {
			requestKnown, requestFuture := protobufRouteFixtureWire(
				t,
				route.RequestType,
				route.Path+":request",
				futureFieldNumber,
			)
			responseKnown, responseFuture := protobufRouteFixtureWire(
				t,
				route.ResponseType,
				route.Path+":response",
				futureFieldNumber,
			)
			if update {
				route.RequestKnownWire = base64.StdEncoding.EncodeToString(requestKnown)
				route.RequestFutureWire = base64.StdEncoding.EncodeToString(requestFuture)
				route.ResponseKnownWire = base64.StdEncoding.EncodeToString(responseKnown)
				route.ResponseFutureWire = base64.StdEncoding.EncodeToString(responseFuture)
				return
			}

			assertProtobufRouteFixtureBytes(t, "request known", route.RequestKnownWire, requestKnown)
			assertProtobufRouteFixtureBytes(t, "request future", route.RequestFutureWire, requestFuture)
			assertProtobufRouteFixtureBytes(t, "response known", route.ResponseKnownWire, responseKnown)
			assertProtobufRouteFixtureBytes(t, "response future", route.ResponseFutureWire, responseFuture)
			assertGoRequestAcceptsFutureWire(t, route.RequestType, requestKnown, requestFuture)
			assertGoResponseAcceptsFutureWire(t, route.ResponseType, responseKnown, responseFuture)
		})
	}

	if update {
		updated, err := json.MarshalIndent(&fixture, "", "  ")
		if err != nil {
			t.Fatalf("encode updated route fixture: %v", err)
		}
		updated = append(updated, '\n')
		if err := os.WriteFile(fixturePath, updated, 0o600); err != nil {
			t.Fatalf("write updated route fixture: %v", err)
		}
	}
}
func protobufRouteFixtureWire(
	t *testing.T,
	typeName string,
	seed string,
	futureFieldNumber protowire.Number,
) ([]byte, []byte) {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName("open_splunk." + typeName),
	)
	if err != nil {
		t.Fatalf("find %s: %v", typeName, err)
	}
	message := messageType.New()
	emptyMessage := message.Descriptor().Fields().Len() == 0
	if message.Descriptor().Fields().ByNumber(futureFieldNumber) != nil {
		t.Fatalf("%s already defines future field %d", typeName, futureFieldNumber)
	}
	if err := populateProtobufRouteFixture(message, seed, 0); err != nil {
		t.Fatalf("populate %s: %v", typeName, err)
	}
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message.Interface())
	if err != nil {
		t.Fatalf("marshal %s: %v", typeName, err)
	}
	if len(known) == 0 && !emptyMessage {
		t.Fatalf("%s fixture has no non-default known field", typeName)
	}
	future := bytes.Clone(known)
	future = protowire.AppendString(
		protowire.AppendTag(future, futureFieldNumber, protowire.BytesType),
		"future:"+seed,
	)
	return known, future
}

func populateProtobufRouteFixture(
	message protoreflect.Message,
	seed string,
	depth int,
) error {
	if depth >= 32 {
		return errors.New("fixture message nesting exceeds 32 levels")
	}
	fields := message.Descriptor().Fields()
	if fields.Len() == 0 {
		return nil
	}
	field := fields.Get(0)
	switch {
	case field.IsMap():
		return populateProtobufRouteFixtureMap(message.Mutable(field).Map(), field, seed, depth)
	case field.IsList():
		return populateProtobufRouteFixtureList(message.Mutable(field).List(), field, seed, depth)
	case field.Message() != nil:
		return populateProtobufRouteFixture(message.Mutable(field).Message(), seed, depth+1)
	default:
		value, err := protobufRouteFixtureScalar(field, seed)
		if err != nil {
			return err
		}
		message.Set(field, value)
		return nil
	}
}

func populateProtobufRouteFixtureList(
	list protoreflect.List,
	field protoreflect.FieldDescriptor,
	seed string,
	depth int,
) error {
	element := list.NewElement()
	if field.Message() != nil {
		if err := populateProtobufRouteFixture(element.Message(), seed, depth+1); err != nil {
			return err
		}
	} else {
		value, err := protobufRouteFixtureScalar(field, seed)
		if err != nil {
			return err
		}
		element = value
	}
	list.Append(element)
	return nil
}

func populateProtobufRouteFixtureMap(
	protobufMap protoreflect.Map,
	field protoreflect.FieldDescriptor,
	seed string,
	depth int,
) error {
	key, err := protobufRouteFixtureScalar(field.MapKey(), seed+":key")
	if err != nil {
		return err
	}
	valueDescriptor := field.MapValue()
	value := protobufMap.NewValue()
	if valueDescriptor.Message() != nil {
		if err := populateProtobufRouteFixture(value.Message(), seed+":value", depth+1); err != nil {
			return err
		}
	} else {
		value, err = protobufRouteFixtureScalar(valueDescriptor, seed+":value")
		if err != nil {
			return err
		}
	}
	protobufMap.Set(key.MapKey(), value)
	return nil
}

func protobufRouteFixtureScalar(
	field protoreflect.FieldDescriptor,
	seed string,
) (protoreflect.Value, error) {
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true), nil
	case protoreflect.EnumKind:
		values := field.Enum().Values()
		for index := 0; index < values.Len(); index++ {
			if number := values.Get(index).Number(); number != 0 {
				return protoreflect.ValueOfEnum(number), nil
			}
		}
		return protoreflect.Value{}, errors.New("fixture enum has no nonzero value")
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1), nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1), nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1), nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1), nil
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.25), nil
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.25), nil
	case protoreflect.StringKind:
		return protoreflect.ValueOfString(seed), nil
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte(seed)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported fixture field kind %s", field.Kind())
	}
}

func assertProtobufRouteFixtureBytes(
	t *testing.T,
	name string,
	encoded string,
	want []byte,
) {
	t.Helper()
	got, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode %s fixture: %v", name, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s fixture is stale; run %s=1 go test ./internal/server -run TestEveryProtobufHTTPRouteHasCrossRuntimeForwardCompatibility", name, updateProtobufRouteFixturesEnvironment)
	}
}

func assertGoRequestAcceptsFutureWire(
	t *testing.T,
	typeName string,
	wantKnown []byte,
	future []byte,
) {
	t.Helper()
	message := newProtobufRouteFixtureMessage(t, typeName)
	if err := proto.Unmarshal(future, message); err != nil {
		t.Fatalf("unmarshal future request: %v", err)
	}
	if len(message.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("future request field was not decoded as unknown")
	}
	message.ProtoReflect().SetUnknown(nil)
	assertProtobufRouteKnownWire(t, message, wantKnown)
}

func assertGoResponseAcceptsFutureWire(
	t *testing.T,
	typeName string,
	wantKnown []byte,
	future []byte,
) {
	t.Helper()
	message := newProtobufRouteFixtureMessage(t, typeName)
	if err := proto.Unmarshal(future, message); err != nil {
		t.Fatalf("unmarshal future response: %v", err)
	}
	if len(message.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("future response field was not retained")
	}
	message.ProtoReflect().SetUnknown(nil)
	assertProtobufRouteKnownWire(t, message, wantKnown)
}

func newProtobufRouteFixtureMessage(t *testing.T, typeName string) proto.Message {
	t.Helper()
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(
		protoreflect.FullName("open_splunk." + typeName),
	)
	if err != nil {
		t.Fatalf("find %s: %v", typeName, err)
	}
	return messageType.New().Interface()
}

func assertProtobufRouteKnownWire(t *testing.T, message proto.Message, want []byte) {
	t.Helper()
	got, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatalf("marshal known fields: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("known fields changed: got %x, want %x", got, want)
	}
}
