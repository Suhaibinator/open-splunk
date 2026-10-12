package ai

import (
	"errors"
	"fmt"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"mime"
	"strings"

	"github.com/Suhaibinator/go-common/pkg/genai/openaiinput"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ErrUnsupportedToolResultContent means a durable multipart tool result cannot be represented by
// the provider selected for the next model request. The result is already committed and replaying
// reconstruction cannot change its type, so the owning run must fail terminally and visibly.
var ErrUnsupportedToolResultContent = errors.New("ai context: tool result content is unsupported by the selected provider")

// toolResultContentKind names the multipart tool-result part being validated so call sites and
// error messages say which durable representation the provider must be able to carry.
type toolResultContentKind string

const (
	toolResultContentData  toolResultContentKind = "data"
	toolResultContentMedia toolResultContentKind = "media"
)

// validateProviderToolResultContent fails closed: only providers whose adapter is known to carry
// multipart tool-response content pass. Every other provider, including UNSPECIFIED and any
// value added later, is rejected so a new adapter cannot silently drop attachments until it has
// been checked and listed here.
func validateProviderToolResultContent(provider string, contentType, filename, detail string, kind toolResultContentKind, apis ...proto_models.InferenceAPI) error {
	api := apishape.Default
	if len(apis) > 0 {
		api = apis[0]
	}
	switch api {
	case apishape.GoogleNative:
		return nil
	case apishape.AnthropicMessages:
		if kind == toolResultContentMedia && (strings.HasPrefix(contentType, "image/") || contentType == "application/pdf") {
			return nil
		}
		return fmt.Errorf("%w: unsupported Anthropic tool-result content type", ErrUnsupportedToolResultContent)
	case apishape.Responses:
	default:
		return fmt.Errorf("%w: %s adapter cannot serialize multipart %s tool-response content", ErrUnsupportedToolResultContent, provider, kind)
	}
	canonical, _, err := mime.ParseMediaType(strings.TrimSpace(contentType))
	if err != nil || canonical == "" {
		return fmt.Errorf("%w: OpenAI Responses requires a valid content type, got %q", ErrUnsupportedToolResultContent, contentType)
	}
	canonical = strings.ToLower(canonical)

	if strings.HasPrefix(canonical, "image/") {
		if kind != toolResultContentMedia {
			return fmt.Errorf("%w: OpenAI Responses data output cannot encode image content type %q as input_file", ErrUnsupportedToolResultContent, canonical)
		}
		if err := openaiinput.ValidateImageType(canonical); err != nil {
			return fmt.Errorf("%w: %w", ErrUnsupportedToolResultContent, err)
		}
		if err := validateMediaDetail(canonical, strings.TrimSpace(detail)); err != nil {
			return fmt.Errorf("%w: %w", ErrUnsupportedToolResultContent, err)
		}
		return nil
	}

	if err := openaiinput.ValidateFileType(canonical, filename); err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedToolResultContent, err)
	}
	if err := validateMediaDetail(canonical, strings.TrimSpace(detail)); err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedToolResultContent, err)
	}
	return nil
}
