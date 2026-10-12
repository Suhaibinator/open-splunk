// Package inference contains shared inference requests and model identity values.
// It deliberately has no runtime, service, or persistence dependencies.
package inference

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// RequestAuthority authorizes one inference request against the caller's current standing.
type RequestAuthority interface {
	AuthorizeInferenceRequest(context.Context, GenerationRequest) error
}

// ModelSelection selects an actual persisted InferenceModel primary key.
// A nil API inherits the operation route; explicit zero selects the provider default.
type ModelSelection struct {
	InferenceModelID uint64                                `json:"inference_model_id"`
	API              *proto_models.InferenceAPI            `json:"api,omitempty"`
	SelectionSource  proto_models.InferenceSelectionSource `json:"selection_source,omitempty"`
}

// GenerationRequest retains operation policy while optionally replacing its model.
type GenerationRequest struct {
	Operation          int32
	ModelSelection     *ModelSelection
	OrganizationID     uint64
	ActorUserID        uint64
	SiteOverrideUserID uint64
	Pinned             bool
	Authority          RequestAuthority
}

// Validate rejects malformed selections without looking up a catalog or runtime.
func (s *ModelSelection) Validate() error {
	if s == nil {
		return nil
	}
	if s.InferenceModelID == 0 {
		return fmt.Errorf("inference model selection requires a nonzero catalog ID")
	}
	if _, ok := proto_models.InferenceSelectionSource_name[int32(s.SelectionSource)]; !ok {
		return fmt.Errorf("unknown inference selection source %d", s.SelectionSource)
	}
	if s.API != nil {
		if _, ok := proto_models.InferenceAPI_name[int32(*s.API)]; !ok {
			return fmt.Errorf("unknown inference API %d", *s.API)
		}
	}
	return nil
}

// Clone returns an independent selection, including its optional API.
func (s *ModelSelection) Clone() *ModelSelection {
	if s == nil {
		return nil
	}
	c := *s
	if s.API != nil {
		api := *s.API
		c.API = &api
	}
	return &c
}
