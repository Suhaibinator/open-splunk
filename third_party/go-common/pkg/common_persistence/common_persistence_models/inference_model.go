package common_persistence_models

import (
	"time"
)

// InferenceModel defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type InferenceModel struct {
	ID              uint64    `json:"id" gorm:"primaryKey;autoIncrement"`
	ProviderID      string    `json:"provider_id" gorm:"type:varchar(128);not null;index:idx_inference_models_provider_active,priority:1;uniqueIndex:idx_inference_models_provider_model,priority:1"`
	ProviderModelID string    `json:"provider_model_id" gorm:"type:varchar(255);not null;default:'';uniqueIndex:idx_inference_models_provider_model,priority:2"`
	DisplayName     string    `json:"display_name" gorm:"type:varchar(255);not null;default:''"`
	Active          bool      `json:"active" gorm:"not null;default:true;index:idx_inference_models_provider_active,priority:2"`
	CreatedAt       time.Time `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt       time.Time `json:"updated_at" gorm:"autoUpdateTime"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (InferenceModel) TableName() string {
	return TableNameInferenceModels
}
