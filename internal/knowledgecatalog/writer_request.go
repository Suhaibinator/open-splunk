package knowledgecatalog

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
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
	scope             normalizedWriteScope
	actor             audit.Actor
	clientRequestID   string
	requestDigest     [sha256.Size]byte
	requestBytes      []byte
	updatePaths       []string
	createRequest     *opensplunk.CreateKnowledgeObjectRequest
	updateRequest     *opensplunk.UpdateKnowledgeObjectRequest
	setStateRequest   *opensplunk.SetKnowledgeObjectStateRequest
	deleteRequest     *opensplunk.DeleteKnowledgeObjectRequest
	quarantineRequest *opensplunk.QuarantineKnowledgeObjectRequest
}

type validatedMutationRequest struct {
	clientRequestID string
	requestBytes    []byte
	request         proto.Message
}

// ValidateCreateKnowledgeObjectRequest validates the complete detached wire
// shape that Writer requires before it consults idempotency or catalog state.
// Definition semantics and app authorization deliberately remain later Writer
// concerns, preserving exact-retry and ACTIVE-publication precedence.
func ValidateCreateKnowledgeObjectRequest(
	request *opensplunk.CreateKnowledgeObjectRequest,
) error {
	return validateCreateKnowledgeObjectRequestShape(request)
}

func validateCreateKnowledgeObjectRequestShape(
	request *opensplunk.CreateKnowledgeObjectRequest,
) error {
	if request == nil || request.GetDefinition() == nil {
		return invalidMutation("create request and definition are required")
	}
	switch request.GetInitialState() {
	case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DRAFT,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE:
	default:
		return invalidMutation("initial state must be draft or active")
	}
	return validateMutationRequestShape(
		request.GetClientRequestId(),
		request,
	)
}

// clientRequestMessage is any mutation request carrying a caller-chosen
// idempotency key that must be cleared before deterministic marshaling.
type clientRequestMessage interface {
	proto.Message
	GetClientRequestId() string
}

// prepareTypedMutation detaches an already shape-validated request and prepares
// it for one mutation route. Callers keep their own shape validation so the
// rejection names the concrete request type.
func prepareTypedMutation[T clientRequestMessage](
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	route string,
	request T,
	clearClientRequestID func(T),
	updatePaths []string,
) (preparedMutation, error) {
	validated, err := detachMutationRequest(
		request.GetClientRequestId(),
		request,
		func(cloned proto.Message) { clearClientRequestID(cloned.(T)) },
	)
	if err != nil {
		return preparedMutation{}, err
	}
	return prepareMutationRequest(ctxScope, actor, route, validated, updatePaths)
}

func prepareCreateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunk.CreateKnowledgeObjectRequest,
) (preparedMutation, error) {
	if err := validateCreateKnowledgeObjectRequestShape(request); err != nil {
		return preparedMutation{}, err
	}
	return prepareTypedMutation(
		ctxScope,
		actor,
		mutationRouteCreate,
		request,
		func(cloned *opensplunk.CreateKnowledgeObjectRequest) { cloned.ClientRequestId = "" },
		nil,
	)
}

// ValidateUpdateKnowledgeObjectRequest validates the complete detached wire
// shape that Writer requires before it consults idempotency or catalog state.
func ValidateUpdateKnowledgeObjectRequest(
	request *opensplunk.UpdateKnowledgeObjectRequest,
) error {
	_, err := validateUpdateKnowledgeObjectRequestShape(request)
	return err
}

func validateUpdateKnowledgeObjectRequestShape(
	request *opensplunk.UpdateKnowledgeObjectRequest,
) ([]string, error) {
	if request == nil || request.GetDefinition() == nil ||
		!validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return nil, invalidMutation("update request identity, version, and definition are required")
	}
	paths, err := normalizeKnowledgeUpdateMask(request.GetUpdateMask())
	if err != nil {
		return nil, err
	}
	err = validateMutationRequestShape(
		request.GetClientRequestId(),
		request,
	)
	return paths, err
}

func prepareUpdateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunk.UpdateKnowledgeObjectRequest,
) (preparedMutation, error) {
	paths, err := validateUpdateKnowledgeObjectRequestShape(request)
	if err != nil {
		return preparedMutation{}, err
	}
	return prepareTypedMutation(
		ctxScope,
		actor,
		mutationRouteUpdate,
		request,
		func(cloned *opensplunk.UpdateKnowledgeObjectRequest) { cloned.ClientRequestId = "" },
		paths,
	)
}

// ValidateSetKnowledgeObjectStateRequest validates the complete detached wire
// shape that Writer requires before it consults idempotency or catalog state.
func ValidateSetKnowledgeObjectStateRequest(
	request *opensplunk.SetKnowledgeObjectStateRequest,
) error {
	return validateSetKnowledgeObjectStateRequestShape(request)
}

func validateSetKnowledgeObjectStateRequestShape(
	request *opensplunk.SetKnowledgeObjectStateRequest,
) error {
	if request == nil || !validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return invalidMutation("state request identity and version are required")
	}
	switch request.GetState() {
	case opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_ACTIVE,
		opensplunk.KnowledgeObjectState_KNOWLEDGE_OBJECT_STATE_DISABLED:
	default:
		return invalidMutation("state must be active or disabled")
	}
	return validateMutationRequestShape(
		request.GetClientRequestId(),
		request,
	)
}

func prepareSetStateMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunk.SetKnowledgeObjectStateRequest,
) (preparedMutation, error) {
	if err := validateSetKnowledgeObjectStateRequestShape(request); err != nil {
		return preparedMutation{}, err
	}
	return prepareTypedMutation(
		ctxScope,
		actor,
		mutationRouteSetState,
		request,
		func(cloned *opensplunk.SetKnowledgeObjectStateRequest) { cloned.ClientRequestId = "" },
		nil,
	)
}

// ValidateDeleteKnowledgeObjectRequest validates the complete detached wire
// shape that Writer requires before it consults idempotency or catalog state.
func ValidateDeleteKnowledgeObjectRequest(
	request *opensplunk.DeleteKnowledgeObjectRequest,
) error {
	return validateDeleteKnowledgeObjectRequestShape(request)
}

func validateDeleteKnowledgeObjectRequestShape(
	request *opensplunk.DeleteKnowledgeObjectRequest,
) error {
	if request == nil || !validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		request.GetExpectedVersion() == 0 || request.GetExpectedVersion() > math.MaxInt64 {
		return invalidMutation("delete request identity and version are required")
	}
	return validateMutationRequestShape(
		request.GetClientRequestId(),
		request,
	)
}

func prepareDeleteMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunk.DeleteKnowledgeObjectRequest,
) (preparedMutation, error) {
	if err := validateDeleteKnowledgeObjectRequestShape(request); err != nil {
		return preparedMutation{}, err
	}
	return prepareTypedMutation(
		ctxScope,
		actor,
		mutationRouteDelete,
		request,
		func(cloned *opensplunk.DeleteKnowledgeObjectRequest) { cloned.ClientRequestId = "" },
		nil,
	)
}

// ValidateQuarantineKnowledgeObjectRequest validates the complete detached
// execute envelope before token parsing or receipt lookup. Recovery tokens are
// deliberately opaque here; signature, expiry, actor, tenant, revision, and
// catalog bindings are verified by Writer inside the transaction.
func ValidateQuarantineKnowledgeObjectRequest(
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) error {
	return validateQuarantineKnowledgeObjectRequestShape(request)
}

func validateQuarantineKnowledgeObjectRequestShape(
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) error {
	if request == nil || !validOpaqueRecoveryToken(request.GetRecoveryToken()) {
		return invalidMutation("quarantine recovery token is invalid")
	}
	return validateMutationRequestShape(request.GetClientRequestId(), request)
}

func prepareQuarantineMutation(
	ctxScope normalizedWriteScope,
	actor audit.Actor,
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) (preparedMutation, error) {
	if err := validateQuarantineKnowledgeObjectRequestShape(request); err != nil {
		return preparedMutation{}, err
	}
	return prepareTypedMutation(
		ctxScope,
		actor,
		mutationRouteQuarantine,
		request,
		func(cloned *opensplunk.QuarantineKnowledgeObjectRequest) { cloned.ClientRequestId = "" },
		nil,
	)
}

func validOpaqueRecoveryToken(value string) bool {
	if len(value) < 64 || len(value) > 1<<10 {
		return false
	}
	for index := range len(value) {
		character := value[index]
		switch {
		case character >= 'A' && character <= 'Z':
		case character >= 'a' && character <= 'z':
		case character >= '0' && character <= '9':
		case character == '-' || character == '_':
		default:
			return false
		}
	}
	return true
}

func validateMutationRequestShape(
	clientRequestID string,
	request proto.Message,
) error {
	if !validClientRequestID(clientRequestID) {
		return invalidMutation("client request identity is invalid")
	}
	if err := preflightMutationMessage(request); err != nil {
		return err
	}
	if err := rejectMutationUnknownFields(request.ProtoReflect(), 0); err != nil {
		return err
	}
	if !mutationMessageStringsAreUTF8(request.ProtoReflect(), 0) {
		return invalidMutation("request cannot be canonically encoded")
	}
	return nil
}

func detachMutationRequest(
	clientRequestID string,
	request proto.Message,
	clearClientRequestID func(proto.Message),
) (validatedMutationRequest, error) {
	cloned := proto.Clone(request)
	if cloned == nil {
		return validatedMutationRequest{}, invalidMutation("request could not be cloned")
	}
	clearClientRequestID(cloned)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(cloned)
	if err != nil || len(encoded) == 0 || len(encoded) > maximumMutationRequestBytes {
		return validatedMutationRequest{}, invalidMutation("request cannot be canonically encoded")
	}
	return validatedMutationRequest{
		clientRequestID: strings.Clone(clientRequestID),
		requestBytes:    encoded,
		request:         cloned,
	}, nil
}

func prepareMutationRequest(
	scope normalizedWriteScope,
	actor audit.Actor,
	route string,
	validated validatedMutationRequest,
	updatePaths []string,
) (preparedMutation, error) {
	digest := digestMutationRequest(route, scope.ownerID, validated.requestBytes)
	prepared := preparedMutation{
		scope:           scope,
		actor:           actor,
		clientRequestID: validated.clientRequestID,
		requestDigest:   digest,
		requestBytes:    validated.requestBytes,
		updatePaths:     append([]string(nil), updatePaths...),
	}
	// Retain the exact detached clone that produced the canonical digest. Route
	// execution must never read the caller-owned protobuf again after this
	// point: callers may reuse or mutate their message as soon as preparation
	// completes, including while a writer hook or a slow transaction runs.
	switch route {
	case mutationRouteCreate:
		var ok bool
		prepared.createRequest, ok = validated.request.(*opensplunk.CreateKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("create request type is invalid")
		}
	case mutationRouteUpdate:
		var ok bool
		prepared.updateRequest, ok = validated.request.(*opensplunk.UpdateKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("update request type is invalid")
		}
	case mutationRouteSetState:
		var ok bool
		prepared.setStateRequest, ok = validated.request.(*opensplunk.SetKnowledgeObjectStateRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("state request type is invalid")
		}
	case mutationRouteDelete:
		var ok bool
		prepared.deleteRequest, ok = validated.request.(*opensplunk.DeleteKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("delete request type is invalid")
		}
	case mutationRouteQuarantine:
		var ok bool
		prepared.quarantineRequest, ok = validated.request.(*opensplunk.QuarantineKnowledgeObjectRequest)
		if !ok {
			return preparedMutation{}, invalidMutation("quarantine request type is invalid")
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
	var definition *opensplunk.KnowledgeObjectDefinition
	switch request := message.(type) {
	case *opensplunk.CreateKnowledgeObjectRequest:
		definition = request.GetDefinition()
	case *opensplunk.UpdateKnowledgeObjectRequest:
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

func mutationMessageStringsAreUTF8(message protoreflect.Message, depth int) bool {
	if depth > 32 {
		return false
	}
	valid := true
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if field.IsMap() {
			value.Map().Range(func(key protoreflect.MapKey, item protoreflect.Value) bool {
				if field.MapKey().Kind() == protoreflect.StringKind &&
					!utf8.ValidString(key.String()) {
					valid = false
					return false
				}
				valid = mutationValueStringsAreUTF8(
					field.MapValue().Kind(),
					item,
					depth,
				)
				return valid
			})
			return valid
		}
		if field.IsList() {
			list := value.List()
			for index := 0; index < list.Len(); index++ {
				if !mutationValueStringsAreUTF8(field.Kind(), list.Get(index), depth) {
					valid = false
					return false
				}
			}
			return true
		}
		valid = mutationValueStringsAreUTF8(field.Kind(), value, depth)
		return valid
	})
	return valid
}

func mutationValueStringsAreUTF8(
	kind protoreflect.Kind,
	value protoreflect.Value,
	depth int,
) bool {
	switch kind {
	case protoreflect.StringKind:
		return utf8.ValidString(value.String())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return mutationMessageStringsAreUTF8(value.Message(), depth+1)
	default:
		return true
	}
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
