package server

import (
	"context"
	"errors"
	"io"
	"net/http"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgevalidation"
)

var validateKnowledgeCandidateRequestWireLayout = knowledgeCandidateRequestWireLayout{
	definition:      1,
	objectID:        2,
	expectedVersion: 3,
	updateMask:      4,
}

// validateKnowledgeObjectCodec owns the Validate-specific allocation boundary
// which the general protobuf codec cannot provide for attacker-controlled
// repetitions.
type validateKnowledgeObjectCodec struct{}

func newValidateKnowledgeObjectCodec() *validateKnowledgeObjectCodec {
	return &validateKnowledgeObjectCodec{}
}

// sanitizeValidateKnowledgeObjectRequest deliberately keeps unknown fields.
// Validate distinguishes unknown request and mask fields (envelope errors)
// from unknown applied-definition fields (in-band candidate invalidity). The
// codec above has already projected updates and bounded dangerous
// repetitions, so no generic clearing is safe here.
func sanitizeValidateKnowledgeObjectRequest(
	_ context.Context,
	request *opensplunk.ValidateKnowledgeObjectRequest,
) (*opensplunk.ValidateKnowledgeObjectRequest, error) {
	return request, nil
}

func (*validateKnowledgeObjectCodec) NewRequest() *opensplunk.ValidateKnowledgeObjectRequest {
	return &opensplunk.ValidateKnowledgeObjectRequest{}
}

func (codec *validateKnowledgeObjectCodec) Decode(
	request *http.Request,
) (*opensplunk.ValidateKnowledgeObjectRequest, error) {
	if request == nil || request.Body == nil {
		return nil, errors.New("knowledge validation request body is unavailable")
	}
	defer request.Body.Close()
	data, err := io.ReadAll(io.LimitReader(
		request.Body,
		maximumKnowledgeMutationRequestBytes+1,
	))
	if err != nil {
		return nil, err
	}
	return codec.DecodeBytes(data)
}

func (*validateKnowledgeObjectCodec) DecodeBytes(
	data []byte,
) (*opensplunk.ValidateKnowledgeObjectRequest, error) {
	if int64(len(data)) > maximumKnowledgeMutationRequestBytes {
		return nil, &http.MaxBytesError{Limit: maximumKnowledgeMutationRequestBytes}
	}
	var intent uint64
	candidate, err := decodeKnowledgeCandidateRequestWire(
		data,
		validateKnowledgeCandidateRequestWireLayout,
		func(field validateWireField) (bool, error) {
			if field.number == 5 && field.wireType == protowire.VarintType {
				intent = field.varint
				return true, nil
			}
			return false, nil
		},
	)
	if err != nil {
		return nil, err
	}
	result := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition:        candidate.candidateDefinition,
		KnowledgeObjectId: candidate.objectID,
		ExpectedVersion:   candidate.expectedVersion,
		UpdateMask:        candidate.updateMask,
		Intent:            opensplunk.KnowledgeValidationIntent(protobufInt32(intent)),
	}
	setValidateUnknown(result, candidate.unknown)
	return result, nil
}

// sealedValidateKnowledgeObjectResponse is the only transport value which may
// carry a service seal and one serialization permit into Encode. The phantom
// protobuf parameter keeps the registered response contract statically
// discoverable without retaining a second mutable response representation.
type sealedValidateKnowledgeObjectResponse[Message proto.Message] struct {
	sealed  knowledgevalidation.SealedValidateResponse
	ctx     context.Context
	release func()
}

type serializedValidateKnowledgeObjectResponse = sealedValidateKnowledgeObjectResponse[*opensplunk.ValidateKnowledgeObjectResponse]

func newSerializedValidateKnowledgeObjectResponse(
	ctx context.Context,
	sealed knowledgevalidation.SealedValidateResponse,
	release func(),
) *serializedValidateKnowledgeObjectResponse {
	return &serializedValidateKnowledgeObjectResponse{
		sealed:  sealed,
		ctx:     ctx,
		release: release,
	}
}

func (*validateKnowledgeObjectCodec) Encode(
	response http.ResponseWriter,
	result *serializedValidateKnowledgeObjectResponse,
) error {
	if result == nil || result.release == nil {
		return errors.New("knowledge validation response serialization state is invalid")
	}
	defer result.release()
	if response == nil {
		return errors.New("knowledge validation response writer is unavailable")
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	validated, err := result.sealed.Proto(result.ctx)
	if err != nil {
		return err
	}
	if validated == nil {
		return knowledgevalidation.ErrInvariant
	}
	payload := result.sealed.DeterministicBytes()
	if len(payload) == 0 || len(payload) > knowledgevalidation.MaximumResponseBytes {
		return knowledgevalidation.ErrInvariant
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}

func validateTransportContextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("knowledge validation response context is unavailable")
	}
	return ctx.Err()
}
