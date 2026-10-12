package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"gorm.io/gorm"
)

// transactionStartPool intercepts the GORM transaction boundary, including
// repository helpers and workers that call Transaction directly. It shares the
// application's pool; only transactions opened through this handler run the hook.
type transactionStartPool struct {
	gorm.ConnPool
	db       *sql.DB
	template *gorm.DB
	start    func(context.Context, *gorm.DB) error
}

func (p *transactionStartPool) GetDBConn() (*sql.DB, error) { return p.db, nil }
func (p *transactionStartPool) BeginTx(ctx context.Context, options *sql.TxOptions) (gorm.ConnPool, error) {
	tx, err := p.db.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	scoped := p.template.Session(&gorm.Session{NewDB: true, Context: ctx})
	scoped.Statement.ConnPool, scoped.ConnPool = tx, tx
	if err := p.start(ctx, scoped); err != nil {
		rollbackErr := tx.Rollback()
		if errors.Is(rollbackErr, sql.ErrTxDone) {
			rollbackErr = nil
		}
		return nil, errors.Join(fmt.Errorf("transaction start hook: %w", err), rollbackErr)
	}
	return tx, nil
}

func (h *CommonPersistenceHandler[U]) installTransactionStartHook() error {
	if h.transactionStartHook == nil {
		return nil
	}
	db, err := h.db.DB()
	if err != nil {
		return err
	}
	// Session clones the GORM configuration and statement; the caller-owned GORM
	// handle must retain its own transaction behavior.
	scoped := h.db.Session(&gorm.Session{NewDB: true, Initialized: true})
	pool := &transactionStartPool{ConnPool: scoped.Statement.ConnPool, db: db, template: scoped, start: h.transactionStartHook}
	scoped.Statement.ConnPool, scoped.ConnPool = pool, pool
	h.db = scoped
	return nil
}
