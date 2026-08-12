package server

import (
	"github.com/Suhaibinator/SRouter/pkg/codec"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// The catalog limits the canonical definitions detached into one response
	// to four MiB. Keep a separate transport envelope for object metadata,
	// timestamps, digests, list paging, and protobuf framing.
	maximumKnowledgeObjectResponseBytes = 8 << 20
	maximumKnowledgeListResponseBytes   = 8 << 20
	maximumKnowledgeGraphResponseBytes  = 128 << 10
	maximumKnowledgeUnknownDepth        = 32
)

type serializedCreateKnowledgeObjectResponse = boundedProtoResponse[*opensplunkv1.CreateKnowledgeObjectResponse]
type serializedGetKnowledgeObjectResponse = boundedProtoResponse[*opensplunkv1.GetKnowledgeObjectResponse]
type serializedListKnowledgeObjectsResponse = boundedProtoResponse[*opensplunkv1.ListKnowledgeObjectsResponse]
type serializedListKnowledgeObjectDependenciesResponse = boundedProtoResponse[*opensplunkv1.ListKnowledgeObjectDependenciesResponse]
type serializedListKnowledgeObjectDependentsResponse = boundedProtoResponse[*opensplunkv1.ListKnowledgeObjectDependentsResponse]
type serializedUpdateKnowledgeObjectResponse = boundedProtoResponse[*opensplunkv1.UpdateKnowledgeObjectResponse]
type serializedSetKnowledgeObjectStateResponse = boundedProtoResponse[*opensplunkv1.SetKnowledgeObjectStateResponse]
type serializedDeleteKnowledgeObjectResponse = boundedProtoResponse[*opensplunkv1.DeleteKnowledgeObjectResponse]

type serializedCreateKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunkv1.CreateKnowledgeObjectRequest,
	*opensplunkv1.CreateKnowledgeObjectResponse,
]
type serializedGetKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunkv1.GetKnowledgeObjectRequest,
	*opensplunkv1.GetKnowledgeObjectResponse,
]
type serializedListKnowledgeObjectsCodec = boundedProtoCodec[
	*opensplunkv1.ListKnowledgeObjectsRequest,
	*opensplunkv1.ListKnowledgeObjectsResponse,
]
type serializedListKnowledgeObjectDependenciesCodec = boundedProtoCodec[
	*opensplunkv1.ListKnowledgeObjectDependenciesRequest,
	*opensplunkv1.ListKnowledgeObjectDependenciesResponse,
]
type serializedListKnowledgeObjectDependentsCodec = boundedProtoCodec[
	*opensplunkv1.ListKnowledgeObjectDependentsRequest,
	*opensplunkv1.ListKnowledgeObjectDependentsResponse,
]
type serializedUpdateKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunkv1.UpdateKnowledgeObjectRequest,
	*opensplunkv1.UpdateKnowledgeObjectResponse,
]
type serializedSetKnowledgeObjectStateCodec = boundedProtoCodec[
	*opensplunkv1.SetKnowledgeObjectStateRequest,
	*opensplunkv1.SetKnowledgeObjectStateResponse,
]
type serializedDeleteKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunkv1.DeleteKnowledgeObjectRequest,
	*opensplunkv1.DeleteKnowledgeObjectResponse,
]

func newSerializedCreateKnowledgeObjectCodec() *serializedCreateKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.CreateKnowledgeObjectRequest,
			*opensplunkv1.CreateKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"create",
	)
}

func newSerializedGetKnowledgeObjectCodec() *serializedGetKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.GetKnowledgeObjectRequest,
			*opensplunkv1.GetKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"get",
	)
}

func newSerializedListKnowledgeObjectsCodec() *serializedListKnowledgeObjectsCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.ListKnowledgeObjectsRequest,
			*opensplunkv1.ListKnowledgeObjectsResponse,
		](),
		maximumKnowledgeListResponseBytes,
		"list",
	)
}

func newSerializedListKnowledgeObjectDependenciesCodec() *serializedListKnowledgeObjectDependenciesCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.ListKnowledgeObjectDependenciesRequest,
			*opensplunkv1.ListKnowledgeObjectDependenciesResponse,
		](),
		maximumKnowledgeGraphResponseBytes,
		"dependencies",
	)
}

func newSerializedListKnowledgeObjectDependentsCodec() *serializedListKnowledgeObjectDependentsCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.ListKnowledgeObjectDependentsRequest,
			*opensplunkv1.ListKnowledgeObjectDependentsResponse,
		](),
		maximumKnowledgeGraphResponseBytes,
		"dependents",
	)
}

func newSerializedUpdateKnowledgeObjectCodec() *serializedUpdateKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.UpdateKnowledgeObjectRequest,
			*opensplunkv1.UpdateKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"update",
	)
}

func newSerializedSetKnowledgeObjectStateCodec() *serializedSetKnowledgeObjectStateCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.SetKnowledgeObjectStateRequest,
			*opensplunkv1.SetKnowledgeObjectStateResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"set-state",
	)
}

func newSerializedDeleteKnowledgeObjectCodec() *serializedDeleteKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunkv1.DeleteKnowledgeObjectRequest,
			*opensplunkv1.DeleteKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"delete",
	)
}

func newKnowledgeBoundedProtoCodec[Request any, Message proto.Message](
	inner codec.Codec[Request, Message],
	maximumBytes int,
	operation string,
) *boundedProtoCodec[Request, Message] {
	return newBoundedProtoCodec(
		inner,
		boundedProtoCodecOptions{
			stateError:   "knowledge " + operation + " response state is invalid",
			messageError: "knowledge " + operation + " response is absent",
			maximumBytes: maximumBytes,
			sizeError:    "knowledge " + operation + " response exceeds its byte limit",
		},
	)
}

func rejectUnknownKnowledgeDefinition(
	definition *opensplunkv1.KnowledgeObjectDefinition,
) error {
	if definition == nil {
		return nil
	}
	if !boundedKnowledgeDefinitionRepeatedShape(definition) {
		return badRequestError("knowledge mutation definition exceeds its entry limit")
	}
	type pendingMessage struct {
		message protoreflect.Message
		depth   int
	}
	pending := []pendingMessage{{message: definition.ProtoReflect()}}
	for len(pending) != 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		if current.depth > maximumKnowledgeUnknownDepth {
			return badRequestError("knowledge mutation definition exceeds its recursion limit")
		}
		if !current.message.IsValid() {
			continue
		}
		if len(current.message.GetUnknown()) != 0 {
			return badRequestError("knowledge mutation definition contains unsupported fields")
		}
		current.message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			switch {
			case field.IsMap():
				if field.MapValue().Kind() != protoreflect.MessageKind {
					return true
				}
				value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
					pending = append(pending, pendingMessage{
						message: item.Message(),
						depth:   current.depth + 1,
					})
					return true
				})
			case field.IsList():
				if field.Kind() != protoreflect.MessageKind {
					return true
				}
				list := value.List()
				for index := 0; index < list.Len(); index++ {
					pending = append(pending, pendingMessage{
						message: list.Get(index).Message(),
						depth:   current.depth + 1,
					})
				}
			case field.Kind() == protoreflect.MessageKind:
				pending = append(pending, pendingMessage{
					message: value.Message(),
					depth:   current.depth + 1,
				})
			}
			return true
		})
	}
	return nil
}
