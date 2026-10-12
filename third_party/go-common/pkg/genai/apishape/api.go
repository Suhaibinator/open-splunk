// Package apishape defines wire API identity independently of destination providers.
package apishape

import (
	"fmt"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	Default           = pb.InferenceAPI_INFERENCE_API_UNSPECIFIED
	ChatCompletions   = pb.InferenceAPI_INFERENCE_API_CHAT_COMPLETIONS
	AnthropicMessages = pb.InferenceAPI_INFERENCE_API_ANTHROPIC_MESSAGES
	Responses         = pb.InferenceAPI_INFERENCE_API_RESPONSES
	GoogleNative      = pb.InferenceAPI_INFERENCE_API_GOOGLE_NATIVE
	AzureNative       = pb.InferenceAPI_INFERENCE_API_AZURE_NATIVE
)

func ValidateSelection(api pb.InferenceAPI) error {
	if api < Default || api > AzureNative {
		return fmt.Errorf("unsupported inference API selection %d", api)
	}
	return nil
}

func Key(api pb.InferenceAPI) string {
	switch api {
	case ChatCompletions:
		return "chat_completions"
	case AnthropicMessages:
		return "anthropic_messages"
	case Responses:
		return "responses"
	default:
		return ""
	}
}
