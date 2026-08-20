package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledgepreview"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const maximumPreviewRetainedSearchJobIDBytes = searchjobs.MaximumJobIDBytes + 1

var (
	previewKnowledgeCandidateRequestWireLayout = knowledgeCandidateRequestWireLayout{
		definition:      2,
		objectID:        3,
		expectedVersion: 4,
		updateMask:      5,
	}
	previewOversizedSearchJobIDWitness = strings.Repeat(
		"x",
		maximumPreviewRetainedSearchJobIDBytes,
	)
)

// previewKnowledgeObjectRequestCodec owns both bounded request decoding and
// exact sealed response encoding. Registration remains conditional on the
// complete Preview service family.
type previewKnowledgeObjectRequestCodec struct{}

func newPreviewKnowledgeObjectRequestCodec() *previewKnowledgeObjectRequestCodec {
	return &previewKnowledgeObjectRequestCodec{}
}

func (*previewKnowledgeObjectRequestCodec) NewRequest() *opensplunk.PreviewKnowledgeObjectRequest {
	return &opensplunk.PreviewKnowledgeObjectRequest{}
}

func (codec *previewKnowledgeObjectRequestCodec) Decode(
	request *http.Request,
) (*opensplunk.PreviewKnowledgeObjectRequest, error) {
	if request == nil || request.Body == nil {
		return nil, errors.New("knowledge preview request body is unavailable")
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

func (*previewKnowledgeObjectRequestCodec) DecodeBytes(
	data []byte,
) (*opensplunk.PreviewKnowledgeObjectRequest, error) {
	if int64(len(data)) > maximumKnowledgeMutationRequestBytes {
		return nil, &http.MaxBytesError{Limit: maximumKnowledgeMutationRequestBytes}
	}
	envelope := previewKnowledgeObjectEnvelopeWireBuilder{}
	candidate, err := decodeKnowledgeCandidateRequestWire(
		data,
		previewKnowledgeCandidateRequestWireLayout,
		envelope.consume,
	)
	if err != nil {
		return nil, err
	}
	result := &opensplunk.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: envelope.retainedSearchJobID(),
		Definition:          candidate.candidateDefinition,
		KnowledgeObjectId:   candidate.objectID,
		ExpectedVersion:     candidate.expectedVersion,
		UpdateMask:          candidate.updateMask,
	}
	if envelope.maximumRowsPresent {
		value := uint32(envelope.maximumRows) // #nosec G115 -- protobuf varints intentionally retain generated unmarshal truncation semantics.
		result.MaximumRows = &value
	}
	setValidateUnknown(result, candidate.unknown)
	return result, nil
}

type previewKnowledgeObjectEnvelopeWireBuilder struct {
	searchJobID          []byte
	searchJobIDOversized bool
	maximumRows          uint64
	maximumRowsPresent   bool
}

func (builder *previewKnowledgeObjectEnvelopeWireBuilder) consume(
	field validateWireField,
) (bool, error) {
	switch {
	case field.number == 1 && field.wireType == protowire.BytesType:
		value, err := validateWireString(field)
		if err != nil {
			return true, err
		}
		if len(value) > searchjobs.MaximumJobIDBytes {
			builder.searchJobID = nil
			builder.searchJobIDOversized = true
		} else {
			builder.searchJobID = value
			builder.searchJobIDOversized = false
		}
		return true, nil
	case field.number == 6 && field.wireType == protowire.VarintType:
		builder.maximumRows = field.varint
		builder.maximumRowsPresent = true
		return true, nil
	default:
		return false, nil
	}
}

func (builder *previewKnowledgeObjectEnvelopeWireBuilder) retainedSearchJobID() string {
	if builder.searchJobIDOversized {
		return previewOversizedSearchJobIDWitness
	}
	return validateWireStringValue(builder.searchJobID)
}

type sealedPreviewKnowledgeObjectResponse[Message proto.Message] struct {
	sealed  knowledgepreview.SealedResponse
	ctx     context.Context
	release func()
}

type serializedPreviewKnowledgeObjectResponse = sealedPreviewKnowledgeObjectResponse[*opensplunk.PreviewKnowledgeObjectResponse]

func newSerializedPreviewKnowledgeObjectResponse(
	ctx context.Context,
	sealed knowledgepreview.SealedResponse,
	release func(),
) *serializedPreviewKnowledgeObjectResponse {
	return &serializedPreviewKnowledgeObjectResponse{
		sealed: sealed, ctx: ctx, release: release,
	}
}

func (*previewKnowledgeObjectRequestCodec) Encode(
	response http.ResponseWriter,
	result *serializedPreviewKnowledgeObjectResponse,
) error {
	if result == nil || result.release == nil {
		return errors.New("knowledge preview response serialization state is invalid")
	}
	defer result.release()
	if response == nil {
		return errors.New("knowledge preview response writer is unavailable")
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	validated, err := result.sealed.Proto(result.ctx)
	if err != nil || validated == nil {
		return knowledgepreview.ErrInvariant
	}
	payload := result.sealed.DeterministicBytes()
	if len(payload) == 0 || len(payload) > knowledgepreview.MaximumResponseBytes {
		return knowledgepreview.ErrInvariant
	}
	if err := validateTransportContextError(result.ctx); err != nil {
		return err
	}
	response.Header().Set("Content-Type", "application/x-protobuf")
	_, err = response.Write(payload)
	return err
}
