package common_persistence

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// validBigintID reports whether id is a positive PostgreSQL bigint.
func validBigintID(id uint64) bool { return id != 0 && id <= math.MaxInt64 }

// validInferencePolicyVersion reports whether version can still be incremented as a bigint.
func validInferencePolicyVersion(version uint64) bool { return version < math.MaxInt64 }

func validInferenceBlockReason(reason proto_models.InferenceBlockReason) bool {
	_, defined := proto_models.InferenceBlockReason_name[int32(reason)]
	return defined && reason != proto_models.InferenceBlockReason_INFERENCE_BLOCK_REASON_UNSPECIFIED
}

// routeDefaultAllowed reports whether the platform may pay for a route default under the
// operation's model list. An empty list does not narrow the default.
func routeDefaultAllowed(options []cpm.InferenceOperationModelOption, modelID uint64) bool {
	if len(options) == 0 {
		return true
	}
	for _, option := range options {
		if option.InferenceModelID == modelID {
			return option.PayerEligibility == proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY ||
				option.PayerEligibility == proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_OR_CONNECTION
		}
	}
	return false
}

func lockInferenceRoutingOrganization(tx *gorm.DB, orgID int64) error {
	var org cpm.Organization
	err := tx.Select("id").Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", orgID).First(&org).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrInferenceRoutingOrganizationNotFound
	}
	return err
}

// distinctInferenceRoutingRows converts each item to its child row, rejecting invalid items and
// repeated keys. It returns the rows in input order and the items by key.
func distinctInferenceRoutingRows[T any, K comparable, R any](items []T, toRow func(T) (K, R, bool)) ([]R, map[K]T, error) {
	rows := make([]R, len(items))
	seen := make(map[K]T, len(items))
	for position, item := range items {
		key, row, ok := toRow(item)
		if _, duplicate := seen[key]; !ok || duplicate {
			return nil, nil, ErrInferenceRoutingInvalidPolicy
		}
		seen[key] = item
		rows[position] = row
	}
	return rows, seen, nil
}

// replaceInferenceRoutingRows replaces every child row in scope with rows.
func replaceInferenceRoutingRows[R any](tx *gorm.DB, rows []R, scope string, args ...any) error {
	if err := tx.Where(scope, args...).Delete(new(R)).Error; err != nil {
		return err
	}
	if len(rows) > 0 {
		return tx.Create(&rows).Error
	}
	return nil
}

// validOrganizationPolicyWrite checks the fields every organization policy save shares.
func validOrganizationPolicyWrite(organizationID int64, updaterUserID, expectedVersion uint64) bool {
	return organizationID > 0 && validBigintID(updaterUserID) && validInferencePolicyVersion(expectedVersion)
}

// saveOrganizationPolicyTx writes one organization policy row under its CAS version, then
// replaces the child rows it owns. An expected version of zero creates an absent row; competing
// creations and stale updates fail rather than overwrite newer policy. saved already carries
// the next version; scope selects the row and its children by key.
func saveOrganizationPolicyTx[P, R any](tx *gorm.DB, organizationID int64, saved *P, expectedVersion uint64, updateColumns []string, rows []R, scope string, args ...any) error {
	if err := lockInferenceRoutingOrganization(tx, organizationID); err != nil {
		return err
	}
	var result *gorm.DB
	if expectedVersion == 0 {
		result = tx.Clauses(clause.OnConflict{DoNothing: true}).Create(saved)
	} else {
		result = tx.Model(saved).Clauses(clause.Returning{}).
			Where(scope+" AND version = ?", append(slices.Clone(args), expectedVersion)...).
			Select(updateColumns).Updates(saved)
	}
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrInferencePolicyVersionConflict
	}
	return replaceInferenceRoutingRows(tx, rows, scope, args...)
}

// SaveInferenceOperationModels atomically replaces the operation's entire model
// list. The route owns the CAS version, including when the list is empty.
// Semantic validation against app registration and sampled provider settings is
// performed by the service before this persistence operation.
func (s *ControlPlaneStore) SaveInferenceOperationModels(ctx context.Context, operation int32, options []cpm.InferenceOperationModelOption, expectedVersion, actor uint64) error {
	if !validInferencePolicyVersion(expectedVersion) || !validBigintID(actor) {
		return ErrInferenceRoutingInvalidPolicy
	}
	rows, _, err := distinctInferenceRoutingRows(options, func(option cpm.InferenceOperationModelOption) (uint64, cpm.InferenceOperationModelOption, bool) {
		switch option.PayerEligibility {
		case proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_OR_CONNECTION,
			proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_PLATFORM_ONLY,
			proto_models.InferenceModelPayerEligibility_INFERENCE_MODEL_PAYER_ELIGIBILITY_CONNECTION_ONLY:
		default:
			return 0, cpm.InferenceOperationModelOption{}, false
		}
		return option.InferenceModelID, cpm.InferenceOperationModelOption{Operation: operation, InferenceModelID: option.InferenceModelID, PayerEligibility: option.PayerEligibility}, validBigintID(option.InferenceModelID)
	})
	if err != nil {
		return err
	}
	err = s.routingSession(ctx).Transaction(func(tx *gorm.DB) error {
		var route cpm.InferenceOperationRoute
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("operation = ?", operation).First(&route).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrControlPlaneRouteNotFound
		}
		if err != nil {
			return err
		}
		if route.ModelListVersion != expectedVersion {
			return ErrInferencePolicyVersionConflict
		}
		if !routeDefaultAllowed(rows, route.InferenceModelID) {
			return ErrInferenceRoutingInvalidPolicy
		}
		result := tx.Model(&cpm.InferenceOperationRoute{}).Where("operation = ? AND model_list_version = ?", operation, expectedVersion).
			Select("model_list_version", "updater_user_id", "updated_at").
			Updates(&cpm.InferenceOperationRoute{ModelListVersion: expectedVersion + 1, UpdaterUserID: new(actor), UpdatedAt: time.Now()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrInferencePolicyVersionConflict
		}
		return replaceInferenceRoutingRows(tx, rows, "operation = ?", operation)
	})
	if err != nil {
		return fmt.Errorf("save inference operation models: %w", err)
	}
	return nil
}

// SaveOrganizationInferenceOperationPolicy replaces one same-org policy and its
// narrowed list. An expected version of zero creates an absent row; competing
// creations and stale updates fail rather than overwrite newer policy.
func (s *ControlPlaneStore) SaveOrganizationInferenceOperationPolicy(ctx context.Context, policy *cpm.OrganizationInferenceOperationPolicy, options []cpm.OrganizationInferenceModelOption, expectedVersion uint64) error {
	if policy == nil || !validOrganizationPolicyWrite(policy.OrganizationID, policy.UpdaterUserID, expectedVersion) {
		return ErrInferenceRoutingInvalidPolicy
	}
	if policy.DefaultModelID != nil && !validBigintID(*policy.DefaultModelID) {
		return ErrInferenceRoutingInvalidPolicy
	}
	rows, seen, err := distinctInferenceRoutingRows(options, func(option cpm.OrganizationInferenceModelOption) (uint64, cpm.OrganizationInferenceModelOption, bool) {
		return option.InferenceModelID, cpm.OrganizationInferenceModelOption{OrganizationID: policy.OrganizationID, Operation: policy.Operation, InferenceModelID: option.InferenceModelID}, validBigintID(option.InferenceModelID)
	})
	if err != nil {
		return err
	}
	if policy.DefaultModelID != nil && len(rows) > 0 {
		if _, listed := seen[*policy.DefaultModelID]; !listed {
			return ErrInferenceRoutingInvalidPolicy
		}
	}
	saved := *policy
	saved.Version = expectedVersion + 1
	if expectedVersion == 0 {
		saved.CreatedAt, saved.UpdatedAt = time.Time{}, time.Time{}
	}
	err = s.routingSession(ctx).Transaction(func(tx *gorm.DB) error {
		return saveOrganizationPolicyTx(tx, policy.OrganizationID, &saved, expectedVersion,
			[]string{"default_model_id", "member_choice", "member_retarget", "version", "updater_user_id", "updated_at"},
			rows, "organization_id = ? AND operation = ?", policy.OrganizationID, policy.Operation)
	})
	if err != nil {
		return fmt.Errorf("save organization inference operation policy: %w", err)
	}
	*policy = saved
	return nil
}

// SaveOrganizationInferencePolicy atomically replaces the org-wide provider
// restriction. Empty provider rows remove this org's restriction while retaining
// its version history; ancestor restrictions continue to apply.
func (s *ControlPlaneStore) SaveOrganizationInferencePolicy(ctx context.Context, policy *cpm.OrganizationInferencePolicy, providers []cpm.OrganizationInferenceProvider, expectedVersion uint64) error {
	if policy == nil || !validOrganizationPolicyWrite(policy.OrganizationID, policy.UpdaterUserID, expectedVersion) {
		return ErrInferenceRoutingInvalidPolicy
	}
	rows, _, err := distinctInferenceRoutingRows(providers, func(provider cpm.OrganizationInferenceProvider) (string, cpm.OrganizationInferenceProvider, bool) {
		valid := provider.ProviderID != "" && len(provider.ProviderID) <= 128 && provider.ProviderID == strings.TrimSpace(provider.ProviderID)
		return provider.ProviderID, cpm.OrganizationInferenceProvider{OrganizationID: policy.OrganizationID, ProviderID: provider.ProviderID}, valid
	})
	if err != nil {
		return err
	}
	saved := *policy
	saved.Version = expectedVersion + 1
	if expectedVersion == 0 {
		saved.CreatedAt, saved.UpdatedAt = time.Time{}, time.Time{}
	}
	err = s.routingSession(ctx).Transaction(func(tx *gorm.DB) error {
		return saveOrganizationPolicyTx(tx, policy.OrganizationID, &saved, expectedVersion,
			[]string{"version", "updater_user_id", "updated_at"}, rows, "organization_id = ?", policy.OrganizationID)
	})
	if err != nil {
		return fmt.Errorf("save organization inference policy: %w", err)
	}
	*policy = saved
	return nil
}
