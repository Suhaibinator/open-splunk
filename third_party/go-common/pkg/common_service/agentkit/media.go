package agentkit

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/mediaimage"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ImageDetail selects a provider image-detail hint. Zero leaves the hint absent.
type ImageDetail int32

const (
	ImageDetailUnspecified ImageDetail = iota
	ImageDetailAuto
	ImageDetailLow
	ImageDetailHigh
	ImageDetailOriginal
)

// ImagePartOptions controls representation, not image preparation or transport limits.
type ImagePartOptions struct{ Detail ImageDetail }

var (
	ErrInvalidImageDetail = errors.New("invalid image detail")
	ErrNilContentPart     = errors.New("nil content part")
)

// TextPart constructs a multipart text value, preserving text verbatim.
func TextPart(text string) *proto_models.MessagePart {
	return &proto_models.MessagePart{Part: &proto_models.MessagePart_Text{Text: &proto_models.TextPart{Text: text}}}
}

// InlineImagePart adapts a prepared image to protobuf without base64 encoding.
// The result owns its bytes; name is caller-owned identity and is preserved verbatim.
func InlineImagePart(name string, image *mediaimage.PreparedImage, options ImagePartOptions) (*proto_models.MessagePart, error) {
	if err := image.Validate(); err != nil {
		return nil, fmt.Errorf("inline image: %w", err)
	}
	var detail string
	switch options.Detail {
	case ImageDetailUnspecified:
	case ImageDetailAuto:
		detail = "auto"
	case ImageDetailLow:
		detail = "low"
	case ImageDetailHigh:
		detail = "high"
	case ImageDetailOriginal:
		detail = "original"
	default:
		return nil, fmt.Errorf("%w: %d", ErrInvalidImageDetail, options.Detail)
	}
	return &proto_models.MessagePart{Part: &proto_models.MessagePart_Media{Media: &proto_models.MediaPart{
		ContentType: image.ContentType, Name: name, Detail: detail,
		Source: &proto_models.MediaPart_InlineData{InlineData: append([]byte(nil), image.Data...)},
	}}}, nil
}

// SuccessResultWithParts supplements SuccessResult's JSON (including null for a
// nil payload) with ordered parts. Ownership of each part transfers to the result:
// callers must not mutate parts afterward. The input slice itself is copied.
// Runtime aggregate-byte and provider-specific budgets still apply.
func SuccessResultWithParts(payload any, parts ...*proto_models.MessagePart) (*ports.AgentToolResult, error) {
	if err := ports.ValidateToolResultContentPartCount(len(parts)); err != nil {
		return nil, err
	}
	for i, part := range parts {
		if part == nil {
			return nil, fmt.Errorf("%w at index %d", ErrNilContentPart, i)
		}
	}
	result, err := SuccessResult(payload)
	if err != nil {
		return nil, err
	}
	result.ContentParts = append([]*proto_models.MessagePart(nil), parts...)
	return result, nil
}

// NormalizedCropSchema returns a fresh JSON-schema object describing a rectangle
// on the original image. Runtime preparation also enforces finite coordinates
// and the cross-field constraints that JSON schema cannot express here.
func NormalizedCropSchema() map[string]any {
	return map[string]any{
		"type":        "object",
		"description": "Normalized rectangle on the original image: origin at the top-left, x increases rightward and y downward. Positive width and height; x + width <= 1 and y + height <= 1.",
		"properties": map[string]any{
			"x":      map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"y":      map[string]any{"type": "number", "minimum": 0, "maximum": 1},
			"width":  map[string]any{"type": "number", "exclusiveMinimum": 0, "maximum": 1},
			"height": map[string]any{"type": "number", "exclusiveMinimum": 0, "maximum": 1},
		},
		"required":             []string{"x", "y", "width", "height"},
		"additionalProperties": false,
	}
}
