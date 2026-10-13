// Package common_persistence_models contains the database models used by the persistence layer.
// This file defines the AI control-plane operation route: the per-operation resolution of a model
// plus retry/timeout/backoff parameters. Request and token rate limits are intentionally NOT stored
// here; they have one authoritative home on the model-level rate-limit policy. See the AI
// control-plane migration plan, "Operation route".
package common_persistence_models

import (
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"time"
)

// InferenceOperationRoute is one operation's persisted route. Its primary key is the owning
// application's numeric operation, so there is at most one route per operation. Durations are stored as int64
// nanoseconds (the native time.Duration representation) so route parameters round-trip losslessly
// into the execution kernel's RetryPolicy.
//
// InferenceModelID references InferenceModel.ID through a migration-installed ON DELETE RESTRICT constraint, ensuring a model
// cannot be removed while an operation routes to it.
type InferenceOperationRoute struct {
	API proto_models.InferenceAPI `json:"api" gorm:"type:smallint;not null;default:0"`
	// Operation is the owning application's generated enum value and the primary key.
	Operation int32 `json:"operation" gorm:"type:integer;primaryKey;autoIncrement:false"`

	// InferenceModelID is the selected model's registry id.
	InferenceModelID uint64 `json:"inference_model_id" gorm:"not null;index:idx_inference_operation_routes_model"`

	// TotalTimeout bounds logical execution alongside any earlier caller deadline.
	TotalTimeout time.Duration `json:"total_timeout" gorm:"type:bigint;not null;default:0"`

	// MaxAttempts is the maximum number of provider attempts (one-based).
	MaxAttempts int32 `json:"max_attempts" gorm:"type:integer;not null;default:1"`

	// InitialBackoff is the first retry backoff before jitter.
	InitialBackoff time.Duration `json:"initial_backoff" gorm:"type:bigint;not null;default:0"`

	// MaxBackoff caps the jittered backoff between attempts.
	MaxBackoff time.Duration `json:"max_backoff" gorm:"type:bigint;not null;default:0"`

	// Temperature is the optional generation temperature applied by the attempt.
	Temperature *float64 `json:"temperature,omitempty" gorm:"type:double precision"`

	// ConfigVersion is an optional admin configuration version. New column: seeded routes leave it
	// unset.
	ConfigVersion    *uint64 `json:"config_version,omitempty"`
	ModelListVersion uint64  `json:"model_list_version" gorm:"not null;default:0"`

	// UpdaterUserID is the optional last admin editor. New column: seeded routes leave it unset.
	UpdaterUserID *uint64 `json:"updater_user_id,omitempty"`

	CreatedAt time.Time `json:"created_at" gorm:"type:timestamptz;autoCreateTime"`
	UpdatedAt time.Time `json:"updated_at" gorm:"type:timestamptz;autoUpdateTime"`
}

// TableName returns the operation-route table name.
func (InferenceOperationRoute) TableName() string {
	return TableNameInferenceOperationRoutes
}
