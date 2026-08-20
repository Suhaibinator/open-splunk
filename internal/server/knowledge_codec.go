package server

import (
	"github.com/Suhaibinator/SRouter/pkg/codec"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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

type serializedCreateKnowledgeObjectResponse = boundedProtoResponse[*opensplunk.CreateKnowledgeObjectResponse]
type serializedGetKnowledgeObjectResponse = boundedProtoResponse[*opensplunk.GetKnowledgeObjectResponse]
type serializedListKnowledgeObjectsResponse = boundedProtoResponse[*opensplunk.ListKnowledgeObjectsResponse]
type serializedListKnowledgeObjectDependenciesResponse = boundedProtoResponse[*opensplunk.ListKnowledgeObjectDependenciesResponse]
type serializedListKnowledgeObjectDependentsResponse = boundedProtoResponse[*opensplunk.ListKnowledgeObjectDependentsResponse]
type serializedUpdateKnowledgeObjectResponse = boundedProtoResponse[*opensplunk.UpdateKnowledgeObjectResponse]
type serializedSetKnowledgeObjectStateResponse = boundedProtoResponse[*opensplunk.SetKnowledgeObjectStateResponse]
type serializedDeleteKnowledgeObjectResponse = boundedProtoResponse[*opensplunk.DeleteKnowledgeObjectResponse]

type serializedCreateKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunk.CreateKnowledgeObjectRequest,
	*opensplunk.CreateKnowledgeObjectResponse,
]
type serializedGetKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunk.GetKnowledgeObjectRequest,
	*opensplunk.GetKnowledgeObjectResponse,
]
type serializedListKnowledgeObjectsCodec = boundedProtoCodec[
	*opensplunk.ListKnowledgeObjectsRequest,
	*opensplunk.ListKnowledgeObjectsResponse,
]
type serializedListKnowledgeObjectDependenciesCodec = boundedProtoCodec[
	*opensplunk.ListKnowledgeObjectDependenciesRequest,
	*opensplunk.ListKnowledgeObjectDependenciesResponse,
]
type serializedListKnowledgeObjectDependentsCodec = boundedProtoCodec[
	*opensplunk.ListKnowledgeObjectDependentsRequest,
	*opensplunk.ListKnowledgeObjectDependentsResponse,
]
type serializedUpdateKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunk.UpdateKnowledgeObjectRequest,
	*opensplunk.UpdateKnowledgeObjectResponse,
]
type serializedSetKnowledgeObjectStateCodec = boundedProtoCodec[
	*opensplunk.SetKnowledgeObjectStateRequest,
	*opensplunk.SetKnowledgeObjectStateResponse,
]
type serializedDeleteKnowledgeObjectCodec = boundedProtoCodec[
	*opensplunk.DeleteKnowledgeObjectRequest,
	*opensplunk.DeleteKnowledgeObjectResponse,
]

func newSerializedCreateKnowledgeObjectCodec() *serializedCreateKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.CreateKnowledgeObjectRequest,
			*opensplunk.CreateKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"create",
	)
}

func newSerializedGetKnowledgeObjectCodec() *serializedGetKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.GetKnowledgeObjectRequest,
			*opensplunk.GetKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"get",
	)
}

func newSerializedListKnowledgeObjectsCodec() *serializedListKnowledgeObjectsCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.ListKnowledgeObjectsRequest,
			*opensplunk.ListKnowledgeObjectsResponse,
		](),
		maximumKnowledgeListResponseBytes,
		"list",
	)
}

func newSerializedListKnowledgeObjectDependenciesCodec() *serializedListKnowledgeObjectDependenciesCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.ListKnowledgeObjectDependenciesRequest,
			*opensplunk.ListKnowledgeObjectDependenciesResponse,
		](),
		maximumKnowledgeGraphResponseBytes,
		"dependencies",
	)
}

func newSerializedListKnowledgeObjectDependentsCodec() *serializedListKnowledgeObjectDependentsCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.ListKnowledgeObjectDependentsRequest,
			*opensplunk.ListKnowledgeObjectDependentsResponse,
		](),
		maximumKnowledgeGraphResponseBytes,
		"dependents",
	)
}

func newSerializedUpdateKnowledgeObjectCodec() *serializedUpdateKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.UpdateKnowledgeObjectRequest,
			*opensplunk.UpdateKnowledgeObjectResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"update",
	)
}

func newSerializedSetKnowledgeObjectStateCodec() *serializedSetKnowledgeObjectStateCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.SetKnowledgeObjectStateRequest,
			*opensplunk.SetKnowledgeObjectStateResponse,
		](),
		maximumKnowledgeObjectResponseBytes,
		"set-state",
	)
}

func newSerializedDeleteKnowledgeObjectCodec() *serializedDeleteKnowledgeObjectCodec {
	return newKnowledgeBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.DeleteKnowledgeObjectRequest,
			*opensplunk.DeleteKnowledgeObjectResponse,
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
	definition *opensplunk.KnowledgeObjectDefinition,
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
