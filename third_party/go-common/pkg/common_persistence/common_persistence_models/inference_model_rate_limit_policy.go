// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the AI control-plane model-level rate-limit policy: the single authoritative
// home for request and token rate limits, keyed by inference model. Each bucket is optional and
// all-or-none. See the AI control-plane migration plan, "Model rate-limit policy".
package common_persistence_models

import (
	"time"
)

// InferenceModelRateLimitPolicy holds the optional request and token buckets shared by every route
// that targets one inference model. Its model reference uses a migration-installed ON DELETE
// CASCADE constraint. A bucket is present only when its Has* flag is true; a present bucket must be
// fully specified. Refill interval and wait cap are int64 nanoseconds (native time.Duration) so
// they round-trip losslessly into the kernel's BucketRateLimitPolicy.
type InferenceModelRateLimitPolicy struct {
	// InferenceModelID is the model these limits apply to and the primary key.
	InferenceModelID uint64 `json:"inference_model_id" gorm:"primaryKey"`

	// Request bucket (all-or-none): charged once before each provider call.
	HasRequestLimit       bool          `json:"has_request_limit" gorm:"not null;default:false"`
	RequestCapacity       int64         `json:"request_capacity" gorm:"not null;default:0"`
	RequestRefillInterval time.Duration `json:"request_refill_interval" gorm:"type:bigint;not null;default:0"`
	RequestRefillUnits    int64         `json:"request_refill_units" gorm:"not null;default:0"`
	RequestWaitCap        time.Duration `json:"request_wait_cap" gorm:"type:bigint;not null;default:0"`

	// Token bucket (all-or-none): consumes actual reported tokens after each attempt.
	HasTokenLimit       bool          `json:"has_token_limit" gorm:"not null;default:false"`
	TokenCapacity       int64         `json:"token_capacity" gorm:"not null;default:0"`
	TokenRefillInterval time.Duration `json:"token_refill_interval" gorm:"type:bigint;not null;default:0"`
	TokenRefillUnits    int64         `json:"token_refill_units" gorm:"not null;default:0"`
	TokenWaitCap        time.Duration `json:"token_wait_cap" gorm:"type:bigint;not null;default:0"`

	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName returns the model rate-limit policy table name.
func (InferenceModelRateLimitPolicy) TableName() string {
	return TableNameInferenceModelRateLimitPolicies
}
