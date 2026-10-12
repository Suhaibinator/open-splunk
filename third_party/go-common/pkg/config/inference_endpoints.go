package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ProviderConfig describes an integration independently of its display name or upstream brand.
type ProviderConfig struct {
	ID            string                      `json:"id" yaml:"id" toml:"id"`
	Name          string                      `json:"name" yaml:"name" toml:"name"`
	Adapter       pb.InferenceProviderAdapter `json:"adapter" yaml:"adapter" toml:"adapter"`
	DefaultAPI    pb.InferenceAPI             `json:"default_api" yaml:"default_api" toml:"default_api"`
	CredentialRef string                      `json:"credential_ref" yaml:"credential_ref" toml:"credential_ref"`
	Endpoints     InferenceEndpoints          `json:"endpoints" yaml:"endpoints" toml:"endpoints"`
	HTTP          HTTPProviderOptions         `json:"http" yaml:"http" toml:"http"`
	Google        GoogleProviderOptions       `json:"google" yaml:"google" toml:"google"`
	Azure         AzureProviderOptions        `json:"azure" yaml:"azure" toml:"azure"`
}

type InferenceEndpoints struct {
	ChatCompletions   string `json:"chat_completions" yaml:"chat_completions" toml:"chat_completions"`
	AnthropicMessages string `json:"anthropic_messages" yaml:"anthropic_messages" toml:"anthropic_messages"`
	Responses         string `json:"responses" yaml:"responses" toml:"responses"`
}

type Header struct {
	Name  string `json:"name" yaml:"name" toml:"name"`
	Value string `json:"value" yaml:"value" toml:"value"`
}

type HTTPProviderOptions struct {
	Headers           []Header `json:"headers" yaml:"headers" toml:"headers"`
	OpenRouterRouting bool     `json:"openrouter_routing" yaml:"openrouter_routing" toml:"openrouter_routing"`
	// UNSPECIFIED Auth uses X-API-Key for Messages and bearer for other HTTP
	// APIs. API_KEY uses AuthHeader (default X-API-Key); NONE sends no credential.
	Auth       pb.InferenceHTTPAuth `json:"auth" yaml:"auth" toml:"auth"`
	AuthHeader string               `json:"auth_header" yaml:"auth_header" toml:"auth_header"`
}

type GoogleProviderOptions struct {
	Backend  pb.InferenceGoogleBackend `json:"backend" yaml:"backend" toml:"backend"`
	Auth     pb.InferenceGoogleAuth    `json:"auth" yaml:"auth" toml:"auth"`
	Project  string                    `json:"project" yaml:"project" toml:"project"`
	Location string                    `json:"location" yaml:"location" toml:"location"`
}

type AzureProviderOptions struct {
	Endpoint   string `json:"endpoint" yaml:"endpoint" toml:"endpoint"`
	APIVersion string `json:"api_version" yaml:"api_version" toml:"api_version"`
}

// CredentialBinding is a runtime projection of an application-owned fixed secret.
// It must not be declared as a managed KMS configuration field.
type CredentialBinding struct {
	Name   string
	Secret Secret
}

func (e InferenceEndpoints) Endpoint(api pb.InferenceAPI) string {
	switch api {
	case pb.InferenceAPI_INFERENCE_API_CHAT_COMPLETIONS:
		return e.ChatCompletions
	case pb.InferenceAPI_INFERENCE_API_ANTHROPIC_MESSAGES:
		return e.AnthropicMessages
	case pb.InferenceAPI_INFERENCE_API_RESPONSES:
		return e.Responses
	default:
		return ""
	}
}

func (p ProviderConfig) SupportsAPI(api pb.InferenceAPI) bool {
	if p.Endpoints.Endpoint(api) != "" {
		return true
	}
	return (api == pb.InferenceAPI_INFERENCE_API_GOOGLE_NATIVE && p.Adapter == pb.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_GOOGLE_GENAI) || (api == pb.InferenceAPI_INFERENCE_API_AZURE_NATIVE && p.Adapter == pb.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_AZURE_FOUNDRY)
}

func (p ProviderConfig) SupportedAPIs() []pb.InferenceAPI {
	var result []pb.InferenceAPI
	for _, api := range []pb.InferenceAPI{pb.InferenceAPI_INFERENCE_API_CHAT_COMPLETIONS, pb.InferenceAPI_INFERENCE_API_ANTHROPIC_MESSAGES, pb.InferenceAPI_INFERENCE_API_RESPONSES, pb.InferenceAPI_INFERENCE_API_GOOGLE_NATIVE, pb.InferenceAPI_INFERENCE_API_AZURE_NATIVE} {
		if p.SupportsAPI(api) {
			result = append(result, api)
		}
	}
	return result
}

func CloneProviders(providers []ProviderConfig) []ProviderConfig {
	if providers == nil {
		return nil
	}
	result := append([]ProviderConfig{}, providers...)
	for i := range result {
		result[i].HTTP.Headers = append([]Header(nil), result[i].HTTP.Headers...)
	}

	return result
}

var providerIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
var headerNamePattern = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// maxProviderIDLength bounds a provider ID wherever one is accepted.
const maxProviderIDLength = 128

// ValidProviderID reports whether id is a well-formed inference provider ID: lowercase
// alphanumeric segments joined by single hyphens, at most 128 bytes. It is the one rule
// shared by provider configuration and request sanitization.
func ValidProviderID(id string) bool {
	return len(id) <= maxProviderIDLength && providerIDPattern.MatchString(id)
}

// ValidateProviders checks settings without loading or inspecting credentials.
func ValidateProviders(providers []ProviderConfig) error {
	seen := make(map[string]bool, len(providers))
	for _, p := range providers {
		if !ValidProviderID(p.ID) {
			return fmt.Errorf("invalid provider ID")
		}
		if seen[p.ID] {
			return fmt.Errorf("duplicate provider ID %q", p.ID)
		}
		seen[p.ID] = true
		fail := func(reason string) error { return fmt.Errorf("provider %q: %s", p.ID, reason) }
		if strings.TrimSpace(p.Name) == "" {
			return fail("name is required")
		}
		if strings.TrimSpace(p.CredentialRef) != p.CredentialRef {
			return fail("credential reference must not contain surrounding whitespace")
		}
		for _, endpoint := range []string{p.Endpoints.ChatCompletions, p.Endpoints.AnthropicMessages, p.Endpoints.Responses} {
			if endpoint != "" && !validInferenceURL(endpoint) {
				return fail("endpoint must be an absolute HTTP(S) URL without userinfo or fragment")
			}
		}
		{
			switch p.HTTP.Auth {
			case pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_UNSPECIFIED, pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_BEARER, pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_API_KEY, pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_NONE:
			default:
				return fail("unknown HTTP authentication")
			}
			if p.HTTP.AuthHeader != "" && (p.HTTP.Auth != pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_API_KEY || !headerNamePattern.MatchString(p.HTTP.AuthHeader)) {
				return fail("auth_header requires api_key authentication and a valid HTTP header name")
			}
			headers := map[string]bool{}
			for _, h := range p.HTTP.Headers {
				name := strings.ToLower(h.Name)
				if !headerNamePattern.MatchString(h.Name) || strings.ContainsAny(h.Value, "\r\n") {
					return fail("invalid HTTP header")
				}
				if headers[name] {
					return fail("duplicate HTTP header")
				}
				headers[name] = true
				if name == "authorization" || name == "x-api-key" || name == "api-key" || name == "x-goog-api-key" || (p.HTTP.AuthHeader != "" && name == strings.ToLower(p.HTTP.AuthHeader)) {
					return fail("authentication headers must use credential bindings")
				}
			}
		}
		switch p.Adapter {
		case pb.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_HTTP:
			if p.Google != (GoogleProviderOptions{}) || p.Azure != (AzureProviderOptions{}) {
				return fail("HTTP adapter cannot have native options")
			}
			if p.CredentialRef == "" && p.HTTP.Auth != pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_NONE {
				return fail("credential reference is required")
			}
		case pb.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_GOOGLE_GENAI:
			if p.Google == (GoogleProviderOptions{}) || p.Azure != (AzureProviderOptions{}) {
				return fail("Google options are required and Azure options are forbidden")
			}
			g := p.Google
			if g.Backend != pb.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_AI_STUDIO && g.Backend != pb.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_VERTEX_AI {
				return fail("unknown Google backend")
			}
			switch g.Auth {
			case pb.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_API_KEY:
				if p.CredentialRef == "" {
					return fail("credential reference is required")
				}
			case pb.InferenceGoogleAuth_INFERENCE_GOOGLE_AUTH_APPLICATION_DEFAULT:
				if g.Backend != pb.InferenceGoogleBackend_INFERENCE_GOOGLE_BACKEND_VERTEX_AI || strings.TrimSpace(g.Project) == "" || strings.TrimSpace(g.Location) == "" {
					return fail("Vertex application-default authentication requires project and location")
				}
			default:
				return fail("unknown Google authentication")
			}
		case pb.InferenceProviderAdapter_INFERENCE_PROVIDER_ADAPTER_AZURE_FOUNDRY:
			if p.Azure == (AzureProviderOptions{}) || p.Google != (GoogleProviderOptions{}) {
				return fail("Azure options are required and Google options are forbidden")
			}
			if !validInferenceURL(p.Azure.Endpoint) {
				return fail("Azure requires an absolute HTTP(S) endpoint without userinfo or fragment")
			}
			if p.CredentialRef == "" {
				return fail("credential reference is required")
			}
		default:
			return fail("unknown adapter")
		}
		if p.Endpoints != (InferenceEndpoints{}) && p.HTTP.Auth != pb.InferenceHTTPAuth_INFERENCE_HTTP_AUTH_NONE && p.CredentialRef == "" {
			return fail("HTTP endpoints require a credential reference or explicit none authentication")
		}
		if !p.SupportsAPI(p.DefaultAPI) {
			return fail("default API is not supported")
		}
	}
	return nil
}

func validInferenceURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil && u.Fragment == "" && !strings.Contains(value, "#")
}

// ValidateCredentialBindings checks membership, allowing unresolved plaintext in source defaults.
func ValidateCredentialBindings(providers []ProviderConfig, bindings []CredentialBinding) error {
	if err := ValidateProviders(providers); err != nil {
		return err
	}
	seen := make(map[string]bool, len(bindings))
	for _, b := range bindings {
		if strings.TrimSpace(b.Name) == "" || strings.TrimSpace(b.Name) != b.Name {
			return fmt.Errorf("invalid credential binding name")
		}
		if seen[b.Name] {
			return fmt.Errorf("duplicate credential binding %q", b.Name)
		}
		seen[b.Name] = true
	}
	for _, p := range providers {
		if p.CredentialRef != "" && !seen[p.CredentialRef] {
			return fmt.Errorf("provider %q references missing credential binding %q", p.ID, p.CredentialRef)
		}
	}
	return nil
}
