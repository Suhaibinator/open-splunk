package common_persistence_models

import (
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"gorm.io/gorm"
)

// UserAuthMethod defines the data structure used in the persistence model flow for database reads, writes, and ORM mappings.
// It carries state across layer boundaries handled by this package.
type UserAuthMethod struct {
	gorm.Model
	// (UserID, MethodType) is unique among live rows only, so a soft-deleted
	// method does not block adding it again.
	UserID uint64 `gorm:"not null;uniqueIndex:idx_user_method,where:deleted_at IS NULL" json:"user_id"`
	User   User   `gorm:"foreignKey:UserID;constraint:OnDelete:CASCADE" json:"-"`
	// MethodType is stored as the proto enum's integer value.
	MethodType proto_models.AuthProvider `gorm:"type:int;not null;uniqueIndex:idx_user_method" json:"method_type"`
	Enabled    bool                      `gorm:"default:true" json:"enabled"`
}

// TableName executes this method as part of the persistence model flow for database reads, writes, and ORM mappings.
func (UserAuthMethod) TableName() string {
	return TableNameUserAuthMethods
}
