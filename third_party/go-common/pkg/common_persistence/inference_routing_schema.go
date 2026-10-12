package common_persistence

import (
	"fmt"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"gorm.io/gorm"
)

// inferenceRoutingConstraints are final-schema additions. Existing conflicting
// constraints require the application's explicit migration; startup never
// rewrites or deletes saved policy to make a constraint validate.
func inferenceRoutingConstraints() []sharedConstraint {
	options := cpm.TableNameInferenceOperationModelOptions
	policies := cpm.TableNameOrganizationInferencePolicies
	operations := cpm.TableNameOrganizationInferenceOperationPolicies
	models := cpm.TableNameOrganizationInferenceModelOptions
	providers := cpm.TableNameOrganizationInferenceProviders
	return []sharedConstraint{
		{cpm.TableNameInferenceOperationRoutes, "chk_inference_routes_model_list_version", "CHECK (model_list_version >= 0)"},
		{options, "fk_inference_model_options_route", "FOREIGN KEY (operation) REFERENCES " + cpm.TableNameInferenceOperationRoutes + "(operation) ON DELETE CASCADE"},
		{options, "fk_inference_model_options_model", "FOREIGN KEY (inference_model_id) REFERENCES " + cpm.TableNameInferenceModels + "(id) ON DELETE RESTRICT"},
		{options, "chk_inference_model_options_payer", "CHECK (payer_eligibility >= 1 AND payer_eligibility <= 3)"},
		{policies, "fk_org_inference_policies_org", "FOREIGN KEY (organization_id) REFERENCES " + cpm.TableNameOrganizations + "(id) ON DELETE CASCADE"},
		{policies, "chk_org_inference_policies_org", "CHECK (organization_id > 0)"},
		{policies, "chk_org_inference_policies_version", "CHECK (version > 0)"},
		{policies, "chk_org_inference_policies_updater", "CHECK (updater_user_id > 0)"},
		{operations, "fk_org_inference_operations_org", "FOREIGN KEY (organization_id) REFERENCES " + cpm.TableNameOrganizations + "(id) ON DELETE CASCADE"},
		{operations, "fk_org_inference_operations_route", "FOREIGN KEY (operation) REFERENCES " + cpm.TableNameInferenceOperationRoutes + "(operation) ON DELETE RESTRICT"},
		{operations, "fk_org_inference_operations_default", "FOREIGN KEY (default_model_id) REFERENCES " + cpm.TableNameInferenceModels + "(id) ON DELETE RESTRICT"},
		{operations, "chk_org_inference_operations_org", "CHECK (organization_id > 0)"},
		{operations, "chk_org_inference_operations_version", "CHECK (version > 0)"},
		{operations, "chk_org_inference_operations_updater", "CHECK (updater_user_id > 0)"},
		{models, "fk_org_inference_options_policy", "FOREIGN KEY (organization_id, operation) REFERENCES " + operations + "(organization_id, operation) ON DELETE CASCADE"},
		{models, "fk_org_inference_options_model", "FOREIGN KEY (inference_model_id) REFERENCES " + cpm.TableNameInferenceModels + "(id) ON DELETE RESTRICT"},
		{providers, "fk_org_inference_providers_policy", "FOREIGN KEY (organization_id) REFERENCES " + policies + "(organization_id) ON DELETE CASCADE"},
		{providers, "chk_org_inference_providers_slug", "CHECK (char_length(provider_id) > 0 AND provider_id = btrim(provider_id))"},
	}
}

// ensureInferenceRoutingConstraints runs after the five routing models and the
// route version column have been migrated. Repeated calls validate the installed
// definitions and never replace a conflicting constraint.
func (h *CommonPersistenceHandler[U]) ensureInferenceRoutingConstraints(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, constraint := range inferenceRoutingConstraints() {
			if err := executeSharedConstraint(tx, constraint); err != nil {
				return fmt.Errorf("install inference routing constraint %s: %w", constraint.name, err)
			}
		}
		return nil
	})
}
