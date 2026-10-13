package savedobjects

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

// GetInTransaction applies Store.Get's same organization and sharing policy
// using an existing metadata transaction, so schedule claims seal the version
// they authorized without opening a separate snapshot.
func GetInTransaction(ctx context.Context, transaction *gorm.DB, scope AccessScope, id string) (*opensplunk.SavedSearch, error) {
	if transaction == nil {
		return nil, control.ErrInvalidArgument
	}
	return (&Store{orm: transaction}).Get(ctx, scope, id)
}

// ValidateAccessScope exposes the same canonical scope contract used by Store
// to repositories that compose saved-query source reads.
func ValidateAccessScope(scope AccessScope) error { _, err := normalizeScope(scope); return err }
