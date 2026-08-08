package knowledgecatalog

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	mutationRequestDigestDomain = "open-splunk/knowledge-mutation-request/v1\x00"
	maximumMutationRequestBytes = knowledgedefinition.MaximumCanonicalBytes + 64<<10
	minimumClientRequestIDBytes = 16
	maximumClientRequestIDBytes = 128
)

const (
	mutationRouteCreate     = "objects.create"
	mutationRouteUpdate     = "objects.update"
	mutationRouteSetState   = "objects.set_state"
	mutationRouteDelete     = "objects.delete"
	mutationRouteQuarantine = "objects.quarantine"
)

type preparedMutation struct {
	scope           normalizedWriteScope
	actor           audit.Actor
	clientRequestID string
	requestDigest   [sha256.Size]byte
	requestBytes    []byte
	updatePaths     []string
	createRequest   *opensplunkv1.CreateKnowledgeObjectRequest
	updateRequest   *opensplunkv1.UpdateKnowledgeObjectRequest
	setStateRequest *opensplunkv1.SetKnowledgeObjectStateRequest
	deleteRequest   *opensplunkv1.DeleteKnowledgeObjectRequest
}

func prepareCreateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunkv1.CreateKnowledgeObjectRequest,
) (preparedMutation, error) {
	if request == nil || request.GetDefinition() == nil {
		return preparedMutation{}, invalidMutation("create request and definition are required")
	}
	switch request.GetInitialState() {
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
	default:
		return preparedMutation{}, invalidMutation("initial state must be draft or active")
	}
	return prepareMutationRequest(
		ctxScope,
		actor,
		mutationRouteCreate,
		request.GetClientRequestId(),
		request,
		func(cloned proto.Message) {
			cloned.(*opensplunkv1.CreateKnowledgeObjectRequest).ClientRequestId = ""
		},
		nil,
	)
}

func prepareUpdateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunkv1.UpdateKnowledgeObjectRequest,
) (preparedMutation, error) {
	if request == nil || request.GetDefinition() == nil ||
		!validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return preparedMutation{}, invalidMutation("update request identity, version, and definition are required")
	}
	paths, err := normalizeKnowledgeUpdateMask(request.GetUpdateMask())
	if err != nil {
		return preparedMutation{}, err
	}
	return prepareMutationRequest(
		ctxScope,
		actor,
		mutationRouteUpdate,
		request.GetClientRequestId(),
		request,
		func(cloned proto.Message) {
			cloned.(*opensplunkv1.UpdateKnowledgeObjectRequest).ClientRequestId = ""
		},
		paths,
	)
}

func prepareSetStateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunkv1.SetKnowledgeObjectStateRequest,
) (preparedMutation, error) {
	if request == nil || !validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return preparedMutation{}, invalidMutation("state request identity and version are required")
	}
	switch request.GetState() {
	case opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		opensplunkv1.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
	default:
		return preparedMutation{}, invalidMutation("state must be active or disabled")
	}
	return prepareMutationRequest(
		ctxScope,
		actor,
		mutationRouteSetState,
		request.GetClientRequestId(),
		request,
		func(cloned proto.Message) {
			cloned.(*opensplunkv1.SetKnowledgeObjectStateRequest).ClientRequestId = ""
		},
		nil,
	)
}

func prepareDeleteMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunkv1.DeleteKnowledgeObjectRequest,
) (preparedMutation, error) {
	if request == nil || !validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return preparedMutation{}, invalidMutation("delete request identity and version are required")
	}
	return prepareMutationRequest(
		ctxScope,
		actor,
		mutationRouteDelete,
		request.GetClientRequestId(),
		request,
		func(cloned proto.Message) {
			cloned.(*opensplunkv1.DeleteKnowledgeObjectRequest).ClientRequestId = ""
		},
		nil,
	)
}

func prepareMutationRequest(
	scope normalizedWriteScope,
	actor audit.Actor,
	route string,
	clientRequestID string,
	request proto.Message,
	clearClientRequestID func(proto.Message),
	updatePaths []string,
) (preparedMutation, error) {
	if !validClientRequestID(clientRequestID) {
		return preparedMutation{}, invalidMutation("client request identity is invalid")
	}
	if err := preflightMutationMessage(request); err != nil {
		return preparedMutation{}, err
	}
	if err := rejectMutationUnknownFields(request.ProtoReflect(), 0); err != nil {
		return preparedMutation{}, err
	}
	cloned := proto.Clone(request)
	if cloned == nil {
		return preparedMutation{}, invalidMutation("request could not be cloned")
	}
	clearClientRequestID(cloned)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cloned)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumMutationRequestBytes {
		return preparedMutation{}, invalidMutation("request cannot be canonically encoded")
	}
	digest := digestMutationRequest(route, scope.ownerID, encoded)
	prepared := preparedMutation{
		scope:           scope,
		actor:           actor,
		clientRequestID: strings.Clone(clientRequestID),
		requestDigest:   digest,
		requestBytes:    encoded,
		updatePaths:     append([]string(nil), updatePaths...),
	}
	// Retain the exact detached clone that produced the canonical digest. Route
	// execution must never read the caller-owned protobuf again after this
	// point: callers may reuse or mutate their message as soon as preparation
	// completes, including while a writer hook or a slow transaction runs.
	switch route {
	case mutationRouteCreate:
		var ok bool
		prepared.createRequest, ok = cloned.(*opensplunkv1.CreateKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("create request type is invalid")
		}
	case mutationRouteUpdate:
		var ok bool
		prepared.updateRequest, ok = cloned.(*opensplunkv1.UpdateKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("update request type is invalid")
		}
	case mutationRouteSetState:
		var ok bool
		prepared.setStateRequest, ok = cloned.(*opensplunkv1.SetKnowledgeObjectStateRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("state request type is invalid")
		}
	case mutationRouteDelete:
		var ok bool
		prepared.deleteRequest, ok = cloned.(*opensplunkv1.DeleteKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("delete request type is invalid")
		}
	default:
		return preparedMutation{}, invalidMutation("mutation route is invalid")
	}
	return prepared, nil
}

func digestMutationRequest(route, ownerID string, request []byte) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(mutationRequestDigestDomain))
	writeDigestFrame(hasher, []byte(route))
	writeDigestFrame(hasher, []byte(ownerID))
	writeDigestFrame(hasher, request)
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestFrame(writer digestWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func preflightMutationMessage(message proto.Message) error {
	if message == nil {
		return invalidMutation("request is required")
	}
	var definition *opensplunkv1.KnowledgeObjectDefinition
	switch request := message.(type) {
	case *opensplunkv1.CreateKnowledgeObjectRequest:
		definition = request.GetDefinition()
	case *opensplunkv1.UpdateKnowledgeObjectRequest:
		definition = request.GetDefinition()
	}
	if definition != nil {
		selector := definition.GetSelector()
		if selector != nil && (len(selector.GetIndexPatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetHostPatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetSourcePatterns()) > knowledge.MaximumSelectorPatternsPerDimension ||
			len(selector.GetSourcetypePatterns()) > knowledge.MaximumSelectorPatternsPerDimension) {
			return fmt.Errorf("%w: submitted selector exceeds its entry limit", control.ErrCapacityExceeded)
		}
		if extraction := definition.GetFieldExtraction(); extraction != nil && extraction.GetRegex() != nil &&
			len(extraction.GetRegex().GetOutputFields()) > knowledgedefinition.MaximumFieldExtractionOutputs {
			return fmt.Errorf("%w: submitted extraction outputs exceed their entry limit", control.ErrCapacityExceeded)
		}
	}
	if size := proto.Size(message); size <= 0 || size > maximumMutationRequestBytes {
		return fmt.Errorf("%w: submitted mutation exceeds its byte limit", control.ErrCapacityExceeded)
	}
	return nil
}

func rejectMutationUnknownFields(message protoreflect.Message, depth int) error {
	if depth > 32 {
		return fmt.Errorf("%w: submitted mutation exceeds its recursion limit", control.ErrCapacityExceeded)
	}
	if len(message.GetUnknown()) != 0 {
		return invalidMutation("submitted mutation contains unknown protobuf fields")
	}
	var result error
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			if field.MapValue().Kind() != protoreflect.MessageKind {
				return true
			}
			value.Map().Range(func(_ protoreflect.MapKey, item protoreflect.Value) bool {
				result = rejectMutationUnknownFields(item.Message(), depth+1)
				return result == nil
			})
			return result == nil
		}
		if field.IsList() {
			if field.Kind() != protoreflect.MessageKind {
				return true
			}
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if result = rejectMutationUnknownFields(list.Get(index).Message(), depth+1); result != nil {
					return false
				}
			}
			return true
		}
		if field.Kind() == protoreflect.MessageKind {
			result = rejectMutationUnknownFields(value.Message(), depth+1)
			return result == nil
		}
		return true
	})
	return result
}

func validClientRequestID(value string) bool {
	if len(value) < minimumClientRequestIDBytes || len(value) > maximumClientRequestIDBytes {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func invalidMutation(message string) error {
	return fmt.Errorf("%w: %s", control.ErrInvalidArgument, strings.TrimSpace(message))
}
