package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"gorm.io/gorm"
)

var projectionPortalSequence atomic.Uint64

// A PostgreSQL portal prevents an eager query result and Rows.Close from doing
// all physical work after an aggregate byte budget has already been exceeded.
// It borrows an ambient metadata transaction or owns one short transaction.
type projectionPortal struct {
	query          *gorm.DB
	db             *gorm.DB
	name           string
	parent         context.Context
	cancel         context.CancelFunc
	ownTransaction bool
}

func newProjectionPortal(query *gorm.DB, limit int) (*projectionPortal, error) {
	if query.Statement == nil || query.Statement.Context == nil || query.Name() != "postgres" {
		return nil, fmt.Errorf("%w: projection portal requires PostgreSQL query context", ErrCorrupt)
	}
	ctx, cancel := context.WithCancel(query.Statement.Context)
	readQuery := query.WithContext(ctx).Session(&gorm.Session{}).Limit(limit)
	// Build the existing GORM query, including its bound arguments and row
	// callbacks, without opening an eager result stream.
	dry := readQuery.Session(&gorm.Session{DryRun: true}).Set("rows", true)
	dry = dry.Callback().Row().Execute(dry)
	buildErr := dry.Error
	if buildErr != nil && !errors.Is(buildErr, gorm.ErrDryRunModeUnsupported) {
		cancel()
		return nil, buildErr
	}
	if dry.Statement.SQL.Len() == 0 {
		cancel()
		return nil, fmt.Errorf("%w: projection portal query is empty", ErrCorrupt)
	}
	portal := &projectionPortal{query: readQuery, db: readQuery, name: fmt.Sprintf(`"os_projection_%d"`, projectionPortalSequence.Add(1)), parent: query.Statement.Context, cancel: cancel}
	if _, ambient := readQuery.Statement.ConnPool.(gorm.TxCommitter); !ambient {
		portal.db = readQuery.Begin()
		if portal.db.Error != nil {
			cancel()
			return nil, portal.db.Error
		}
		portal.ownTransaction = true
	}
	if err := portal.db.Session(&gorm.Session{NewDB: true}).Exec("DECLARE "+portal.name+" NO SCROLL CURSOR FOR "+dry.Statement.SQL.String(), dry.Statement.Vars...).Error; err != nil {
		portal.Close()
		return nil, err
	}
	return portal, nil
}

func (portal *projectionPortal) Fetch(count int) (*sql.Rows, error) {
	return portal.db.Session(&gorm.Session{NewDB: true}).Raw(fmt.Sprintf("FETCH FORWARD %d FROM %s", count, portal.name)).Rows()
}

func (portal *projectionPortal) Close() {
	if portal.ownTransaction {
		_ = portal.db.Rollback().Error // releases the portal and borrowed pool connection
	} else {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(portal.parent), 3*time.Second)
		defer cancel()
		_ = portal.db.WithContext(ctx).Session(&gorm.Session{NewDB: true}).Exec("CLOSE " + portal.name).Error
	}
	portal.cancel()
}
