package genai

import "github.com/Suhaibinator/go-common/pkg/genai/inference"

func NormalizeProviderModelID(provider, model string) (string, error) {
	return inference.NormalizeProviderModelID(provider, model)
}
