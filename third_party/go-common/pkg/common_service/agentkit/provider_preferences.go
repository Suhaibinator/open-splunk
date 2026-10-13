package agentkit

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
	"github.com/Suhaibinator/go-common/pkg/genai/apishape"
	"github.com/Suhaibinator/go-common/pkg/genai/modelconfig"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
)

// ProviderPreferences declares optional provider behavior without exposing SDK
// request types to applications. Preferences apply only when the resolved route
// advertises OpenRouter routing; other providers retain their policy defaults.
type ProviderPreferences struct {
	// PromptCacheNamespace enables stable per-run cache affinity. The key is
	// namespace:AgentRunID across all drives and recoveries of the same run.
	PromptCacheNamespace string
	// ProviderSort asks OpenRouter to order upstream providers, keeping any
	// other routing restrictions from the policy's OpenRouter provider config.
	ProviderSort pb.OpenRouterProviderSort
}

// BuildConfig adds preferences to the selected API's typed config. Empty
// preferences leave configuration untouched. Session endpoint pinning is
// independent of prompt-cache affinity and is never enabled by this builder.
func (p ProviderPreferences) BuildConfig(_ context.Context, run ports.AgentRunInfo) *modelconfig.Config {
	if !run.ModelPolicy.OpenRouterRouting || (p.PromptCacheNamespace == "" && p.ProviderSort == pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_UNSPECIFIED) {
		return nil
	}
	cfg := modelconfig.Config{}
	if run.ModelPolicy.ProviderConfig != nil {
		cfg = *run.ModelPolicy.ProviderConfig
		// Preserve invalid configurations for the runtime's validation boundary.
		if cfg.Validate(run.ModelPolicy.API) != nil {
			return &cfg
		}
	}
	cacheKey := fmt.Sprintf("%s:%d", p.PromptCacheNamespace, run.AgentRunID)
	switch run.ModelPolicy.API {
	case apishape.ChatCompletions:
		params := openai.ChatCompletionNewParams{}
		if cfg.ChatCompletions != nil {
			params = *cfg.ChatCompletions
		}
		if p.PromptCacheNamespace != "" {
			if encoded, overridden := overridePromptCacheKey(params, cacheKey); !overridden {
				params.PromptCacheKey = param.NewOpt(cacheKey)
			} else if encoded != nil {
				param.SetJSON(encoded, &params)
			}
		}
		cfg.ChatCompletions = &params
	case apishape.Responses:
		params := modelconfig.ResponsesConfig{}
		if cfg.Responses != nil {
			params = *cfg.Responses
		}
		if p.PromptCacheNamespace != "" {
			if encoded, overridden := overridePromptCacheKey(params.Params, cacheKey); !overridden {
				params.Params.PromptCacheKey = param.NewOpt(cacheKey)
			} else if encoded != nil {
				param.SetJSON(encoded, &params.Params)
			}
		}
		cfg.Responses = &params
	default:
		return nil
	}
	options := modelconfig.OpenRouterConfig{}
	if cfg.OpenRouter != nil {
		options = *cfg.OpenRouter
	}
	if p.ProviderSort != pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_UNSPECIFIED {
		provider := modelconfig.OpenRouterProvider{}
		if options.Provider != nil {
			provider = *options.Provider
		}
		provider.Sort = p.ProviderSort
		options.Provider = &provider
	}
	cfg.OpenRouter = &options
	return &cfg
}

// overridePromptCacheKey returns params' whole-object override with its
// prompt_cache_key set, since the SDK ignores typed fields beside an override.
// overridden reports whether params has one; encoded is nil only when the
// override does not decode, which leaves it for the runtime's validation.
func overridePromptCacheKey[T interface{ Overrides() (any, bool) }](params T, cacheKey string) (encoded []byte, overridden bool) {
	members, overridden, err := modelconfig.OverrideMembers(params)
	if !overridden || err != nil {
		return nil, overridden
	}
	if err := modelconfig.SetOverrideMember(members, "prompt_cache_key", cacheKey); err != nil {
		return nil, true
	}
	encoded, err = modelconfig.EncodeOverrideMembers(members)
	if err != nil {
		return nil, true
	}
	return encoded, true
}
