package config

import (
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

const (
	DefaultInferenceMaxOperationModels              = 64
	MaxInferenceMaxOperationModels                  = 256
	DefaultInferenceMaxOrganizationProviders        = 32
	MaxInferenceMaxOrganizationProviders            = 256
	DefaultInferencePreviewSessionLimit             = 1000
	MaxInferencePreviewSessionLimit                 = 10000
	DefaultInferenceRetargetBatchSize               = 100
	MaxInferenceRetargetBatchSize                   = 1000
	DefaultInferenceMaxModelSteps            uint32 = 128
	MaxInferenceMaxModelSteps                uint32 = 10000
)

func (c *Config) validateInferenceRouting() error {
	for _, field := range []struct {
		name    string
		value   *int
		ceiling int
	}{
		{"InferenceMaxOperationModels", c.InferenceMaxOperationModels, MaxInferenceMaxOperationModels},
		{"InferenceMaxOrganizationProviders", c.InferenceMaxOrganizationProviders, MaxInferenceMaxOrganizationProviders},
		{"InferencePreviewSessionLimit", c.InferencePreviewSessionLimit, MaxInferencePreviewSessionLimit},
		{"InferenceRetargetBatchSize", c.InferenceRetargetBatchSize, MaxInferenceRetargetBatchSize},
	} {
		if field.value != nil && (*field.value < 1 || *field.value > field.ceiling) {
			return fmt.Errorf("%s must be between 1 and %d", field.name, field.ceiling)
		}
	}
	if c.InferenceMaxModelSteps != nil && (*c.InferenceMaxModelSteps < 1 || *c.InferenceMaxModelSteps > MaxInferenceMaxModelSteps) {
		return fmt.Errorf("InferenceMaxModelSteps must be between 1 and %d", MaxInferenceMaxModelSteps)
	}
	// Per-depth length is checked with every other organization role slice in ValidateConfig;
	// these thresholds have no user-type bypass, so an absent slice is a configuration error.
	for _, field := range []struct {
		name  string
		roles []proto_models.OrganizationRole
	}{
		{"OrgMinRoleViewInferencePolicy", c.OrgMinRoleViewInferencePolicy},
		{"OrgMinRoleManageInferencePolicy", c.OrgMinRoleManageInferencePolicy},
		{"OrgMinRoleChooseInferenceModel", c.OrgMinRoleChooseInferenceModel},
	} {
		if len(field.roles) == 0 {
			return fmt.Errorf("%s is required", field.name)
		}
	}
	return nil
}
