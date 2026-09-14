package main

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
)

const patternCursorKeyPurpose = "retained-search-pattern-cursors-v1"

// Keep the concrete store so Patterns can acquire its bounded retained-row
// lease. A re-execution adapter would change the immutable source relation.
func newRuntimePatternService(ctx context.Context, db *control.DB, masterKeyPath string, store *searchartifacts.Store) (*patterns.Service, error) {
	masterKey, err := loadVerifiedMasterKey(ctx, db, masterKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load pattern cursor master key: %w", err)
	}
	defer clear(masterKey)
	cursorKey, err := deriveServerKey(masterKey, patternCursorKeyPurpose)
	if err != nil {
		return nil, fmt.Errorf("derive pattern cursor key: %w", err)
	}
	defer clear(cursorKey)
	service, err := patterns.New(patterns.Config{Source: store, CursorKey: cursorKey})
	if err != nil {
		return nil, fmt.Errorf("create retained pattern service: %w", err)
	}
	return service, nil
}
