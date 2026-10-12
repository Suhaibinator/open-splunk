package modelconfig

import (
	"fmt"
	"maps"
	"slices"

	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/firebase/genkit/go/core"
)

// OpenRouterConfig contains supported extensions to the OpenAI wire APIs. It is
// the only owner of the provider and session_id request fields; Validate rejects
// either key in the SDK params' raw extra fields.
type OpenRouterConfig struct {
	Provider  *OpenRouterProvider
	SessionID string
}

// OpenRouterProvider is OpenRouter's provider routing object, sent as the
// request's provider field. Zero fields are omitted and keep OpenRouter defaults.
type OpenRouterProvider struct {
	Sort              pb.OpenRouterProviderSort
	Order             []string
	Only              []string
	Ignore            []string
	AllowFallbacks    *bool
	RequireParameters *bool
	DataCollection    pb.OpenRouterDataCollection
	ZDR               *bool
	Quantizations     []string
	MaxPrice          *OpenRouterMaxPrice
}

// OpenRouterMaxPrice caps upstream prices in USD: per million tokens for
// Prompt and Completion, per request, and per image.
type OpenRouterMaxPrice struct {
	Prompt     *float64 `json:"prompt,omitzero"`
	Completion *float64 `json:"completion,omitzero"`
	Request    *float64 `json:"request,omitzero"`
	Image      *float64 `json:"image,omitzero"`
}

// openRouterSorts and openRouterDataCollections map proto enums to OpenRouter
// wire values. They are the single source for the request and its schema.
var openRouterSorts = map[pb.OpenRouterProviderSort]string{
	pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_PRICE:      "price",
	pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_LATENCY:    "latency",
	pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_THROUGHPUT: "throughput",
}

var openRouterDataCollections = map[pb.OpenRouterDataCollection]string{
	pb.OpenRouterDataCollection_OPEN_ROUTER_DATA_COLLECTION_ALLOW: "allow",
	pb.OpenRouterDataCollection_OPEN_ROUTER_DATA_COLLECTION_DENY:  "deny",
}

// openRouterProviderWire is the provider object as OpenRouter receives it.
type openRouterProviderWire struct {
	Sort              string              `json:"sort,omitzero"`
	Order             []string            `json:"order,omitzero"`
	Only              []string            `json:"only,omitzero"`
	Ignore            []string            `json:"ignore,omitzero"`
	AllowFallbacks    *bool               `json:"allow_fallbacks,omitzero"`
	RequireParameters *bool               `json:"require_parameters,omitzero"`
	DataCollection    string              `json:"data_collection,omitzero"`
	ZDR               *bool               `json:"zdr,omitzero"`
	Quantizations     []string            `json:"quantizations,omitzero"`
	MaxPrice          *OpenRouterMaxPrice `json:"max_price,omitzero"`
}

// openRouterOwnedFields are written only from OpenRouterConfig.
var openRouterOwnedFields = []string{"provider", "session_id"}

func (o *OpenRouterConfig) validate() error {
	if o == nil || o.Provider == nil {
		return nil
	}
	if _, ok := openRouterSorts[o.Provider.Sort]; !ok && o.Provider.Sort != pb.OpenRouterProviderSort_OPEN_ROUTER_PROVIDER_SORT_UNSPECIFIED {
		return fmt.Errorf("unsupported OpenRouter provider sort %s", o.Provider.Sort)
	}
	if _, ok := openRouterDataCollections[o.Provider.DataCollection]; !ok && o.Provider.DataCollection != pb.OpenRouterDataCollection_OPEN_ROUTER_DATA_COLLECTION_UNSPECIFIED {
		return fmt.Errorf("unsupported OpenRouter data collection policy %s", o.Provider.DataCollection)
	}
	return nil
}

// wire projects the typed routing object onto OpenRouter's JSON spelling;
// unspecified enums map to empty strings, which are omitted.
func (p *OpenRouterProvider) wire() openRouterProviderWire {
	return openRouterProviderWire{
		Sort:              openRouterSorts[p.Sort],
		Order:             p.Order,
		Only:              p.Only,
		Ignore:            p.Ignore,
		AllowFallbacks:    p.AllowFallbacks,
		RequireParameters: p.RequireParameters,
		DataCollection:    openRouterDataCollections[p.DataCollection],
		ZDR:               p.ZDR,
		Quantizations:     p.Quantizations,
		MaxPrice:          p.MaxPrice,
	}
}

// extraFields adds the OpenRouter options to a copy of the SDK extra fields,
// whose map[string]any type is fixed by openai-go's SetExtraFields.
func (o *OpenRouterConfig) extraFields(original map[string]any) map[string]any {
	extra := maps.Clone(original)
	if extra == nil {
		extra = make(map[string]any, len(openRouterOwnedFields))
	}
	if o.Provider != nil {
		extra["provider"] = o.Provider.wire()
	}
	if o.SessionID != "" {
		extra["session_id"] = o.SessionID
	}
	return extra
}

// openRouterOverride returns params' whole-object override with the OpenRouter
// options added to its members, since SetExtraFields would replace it.
// overridden reports whether params has an override; Validate has decoded it,
// so encoded is nil only for an unvalidated override, which is left unchanged
// rather than lost.
func openRouterOverride[T overridable](o *OpenRouterConfig, params T) (encoded []byte, overridden bool) {
	members, overridden, err := OverrideMembers(params)
	if !overridden || err != nil {
		return nil, overridden
	}
	if o.Provider != nil {
		if err := SetOverrideMember(members, "provider", o.Provider.wire()); err != nil {
			return nil, true
		}
	}
	if o.SessionID != "" {
		if err := SetOverrideMember(members, "session_id", o.SessionID); err != nil {
			return nil, true
		}
	}
	encoded, err = EncodeOverrideMembers(members)
	if err != nil {
		return nil, true
	}
	return encoded, true
}

// OpenRouterProviderSchema returns the JSON schema of the provider object,
// with enum lists taken from the wire tables. The map[string]any form is
// Genkit's config-schema type.
func OpenRouterProviderSchema() map[string]any {
	schema := core.InferSchemaMap(openRouterProviderWire{})
	properties := schema["properties"].(map[string]any)
	properties["sort"].(map[string]any)["enum"] = slices.Sorted(maps.Values(openRouterSorts))
	properties["data_collection"].(map[string]any)["enum"] = slices.Sorted(maps.Values(openRouterDataCollections))
	return schema
}
