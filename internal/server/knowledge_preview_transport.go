package server

import (
	"errors"
	"io"
	"net/http"
	"strings"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/encoding/protowire"
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

// previewKnowledgeObjectRequestCodec is intentionally request-only and
// unregistered. It establishes Preview's allocation boundary without adding a
// handler, response codec, route, or feature exposure.
type previewKnowledgeObjectRequestCodec struct{}

func newPreviewKnowledgeObjectRequestCodec() *previewKnowledgeObjectRequestCodec {
	return &previewKnowledgeObjectRequestCodec{}
}

func (*previewKnowledgeObjectRequestCodec) NewRequest() *opensplunkv1.PreviewKnowledgeObjectRequest {
	return &opensplunkv1.PreviewKnowledgeObjectRequest{}
}

func (codec *previewKnowledgeObjectRequestCodec) Decode(
	request *http.Request,
) (*opensplunkv1.PreviewKnowledgeObjectRequest, error) {
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
) (*opensplunkv1.PreviewKnowledgeObjectRequest, error) {
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
	result := &opensplunkv1.PreviewKnowledgeObjectRequest{
		RetainedSearchJobId: envelope.retainedSearchJobID(),
		Definition:          candidate.definition,
		KnowledgeObjectId:   candidate.objectID,
		ExpectedVersion:     candidate.expectedVersion,
		UpdateMask:          candidate.updateMask,
	}
	if envelope.maximumRowsPresent {
		value := uint32(envelope.maximumRows)
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
