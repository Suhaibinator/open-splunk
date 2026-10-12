package config

import "github.com/Suhaibinator/go-common/pkg/proto_models"

// Built-in identifiers are conveniences; custom providers use any valid ID.
const (
	ProviderIDOpenAI             = "openai"
	CredentialNameOpenAI         = "openai"
	ProviderIDOpenRouter         = "openrouter"
	CredentialNameOpenRouter     = "openrouter"
	ProviderIDAnthropic          = "anthropic"
	CredentialNameAnthropic      = "anthropic"
	ProviderIDBaseten            = "baseten"
	CredentialNameBaseten        = "baseten"
	ProviderIDOllamaCloud        = "ollama-cloud"
	CredentialNameOllamaCloud    = "ollama_cloud"
	ProviderIDGoogleAIStudio     = "google-ai-studio"
	CredentialNameGoogleAIStudio = "google_ai_studio"
	ProviderIDVertexAI           = "vertex-ai"
	CredentialNameVertexAI       = "vertex_ai"
	ProviderIDAzureOpenAI        = "azure-openai"
	CredentialNameAzureOpenAI    = "azure_openai"
	ProviderIDAWSBedrock         = "aws-bedrock"
	CredentialNameAWSBedrock     = "aws_bedrock"
)

// OpenAIProvider returns fresh settings which applications may customize.
func OpenAIProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDOpenAI, Name: "OpenAI", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_RESPONSES, CredentialRef: CredentialNameOpenAI, Endpoints: InferenceEndpoints{ChatCompletions: "https://api.openai.com/v1/chat/completions", AnthropicMessages: "https://api.openai.com/v1/messages", Responses: "https://api.openai.com/v1/responses"}}
}

// OpenRouterProvider returns fresh settings which applications may customize.
func OpenRouterProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDOpenRouter, Name: "OpenRouter", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_RESPONSES, CredentialRef: CredentialNameOpenRouter, Endpoints: InferenceEndpoints{ChatCompletions: "https://openrouter.ai/api/v1/chat/completions", AnthropicMessages: "https://openrouter.ai/api/v1/messages", Responses: "https://openrouter.ai/api/v1/responses"}, HTTP: HTTPProviderOptions{OpenRouterRouting: true}}
}

// AnthropicProvider returns fresh settings which applications may customize.
func AnthropicProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDAnthropic, Name: "Anthropic", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_RESPONSES, CredentialRef: CredentialNameAnthropic, Endpoints: InferenceEndpoints{ChatCompletions: "https://api.anthropic.com/v1/chat/completions", AnthropicMessages: "https://api.anthropic.com/v1/messages", Responses: "https://api.anthropic.com/v1/responses"}, HTTP: HTTPProviderOptions{Headers: []Header{{Name: "anthropic-version", Value: "2023-06-01"}}}}
}

// BasetenProvider returns fresh settings which applications may customize. It
// exposes only Baseten's documented Chat Completions endpoint.
func BasetenProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDBaseten, Name: "Baseten", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_CHAT_COMPLETIONS, CredentialRef: CredentialNameBaseten, Endpoints: InferenceEndpoints{ChatCompletions: "https://inference.baseten.co/v1/chat/completions"}}
}

// OllamaCloudProvider returns fresh settings which applications may customize.
func OllamaCloudProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDOllamaCloud, Name: "Ollama Cloud", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_RESPONSES, CredentialRef: CredentialNameOllamaCloud, Endpoints: InferenceEndpoints{ChatCompletions: "https://ollama.com/v1/chat/completions", AnthropicMessages: "https://ollama.com/v1/messages", Responses: "https://ollama.com/v1/responses"}}
}

// GoogleAIStudioProvider returns fresh settings which applications may customize.
func GoogleAIStudioProvider() ProviderConfig {
	return ProviderConfig{ID: ProviderIDGoogleAIStudio, Name: "Google AI Studio", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_GOOGLE_GENAI, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_GOOGLE_NATIVE, CredentialRef: CredentialNameGoogleAIStudio, Google: GoogleProviderOptions{Backend: proto_models.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_AI_STUDIO, Auth: proto_models.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_API_KEY}}
}

// VertexAIProvider returns native Vertex settings. Supply either API_KEY or APPLICATION_DEFAULT authentication.
func VertexAIProvider(options GoogleProviderOptions) ProviderConfig {
	options.Backend = proto_models.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_VERTEX_AI
	credential := CredentialNameVertexAI
	if options.Auth == proto_models.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_APPLICATION_DEFAULT {
		credential = ""
	}
	return ProviderConfig{ID: ProviderIDVertexAI, Name: "Vertex AI", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_GOOGLE_GENAI, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_GOOGLE_NATIVE, CredentialRef: credential, Google: options}
}

// AzureOpenAIProvider requires an account resource endpoint; API version may be empty for the SDK default.
func AzureOpenAIProvider(endpoint, apiVersion string) ProviderConfig {
	return ProviderConfig{ID: ProviderIDAzureOpenAI, Name: "Azure OpenAI", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_AZURE_FOUNDRY, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_AZURE_NATIVE, CredentialRef: CredentialNameAzureOpenAI, Azure: AzureProviderOptions{Endpoint: endpoint, APIVersion: apiVersion}}
}

// AWSBedrockProvider requires the complete Chat Completions URL of the application's gateway.
func AWSBedrockProvider(chatCompletionsURL string) ProviderConfig {
	return ProviderConfig{ID: ProviderIDAWSBedrock, Name: "AWS Bedrock", Adapter: proto_models.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP, DefaultAPI: proto_models.InferenceAPI_INFERENCE_API_CHAT_COMPLETIONS, CredentialRef: CredentialNameAWSBedrock, Endpoints: InferenceEndpoints{ChatCompletions: chatCompletionsURL}}
}
