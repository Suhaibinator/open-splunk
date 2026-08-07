package knowledgecatalog

import (
	"context"
	"fmt"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

const (
	minimumCursorKeyBytes = 32
	maximumCursorKeyBytes = 4 << 10
)

type Options struct {
	CursorKey []byte
}

// Store reads migration-owned catalog tables without changing schema.
type Store struct {
	orm       *gorm.DB
	cursorKey []byte
}

func New(database *control.DB, options Options) (*Store, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf("%w: knowledge catalog control database is required", control.ErrInvalidArgument)
	}
	if len(options.CursorKey) < minimumCursorKeyBytes || len(options.CursorKey) > maximumCursorKeyBytes {
		return nil, fmt.Errorf("%w: knowledge catalog cursor key must contain between %d and %d bytes", control.ErrInvalidArgument, minimumCursorKeyBytes, maximumCursorKeyBytes)
	}
	return &Store{orm: database.GORMDB(), cursorKey: slices.Clone(options.CursorKey)}, nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: knowledge catalog context is nil", control.ErrInvalidArgument)
	}
	return ctx.Err()
}
