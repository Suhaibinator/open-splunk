package collectoradmission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const maximumTenantIDBytes = 255

var (
	// ErrLeaseNotCurrent means an operation did not present the exact enabled
	// durable lease currently authoritative for this tenant and collector.
	// Disabled, disconnected, and superseded leases deliberately share one
	// externally safe result.
	ErrLeaseNotCurrent = errors.New(
		"collector admission: durable lease is not current",
	)
)

// Store owns the atomic collector Hello admission boundary for one exact
// server-controlled tenant.
type Store struct {
	orm    *gorm.DB
	tokens *auth.Store
	fleet  *collectorfleet.Store
	scope  collectorfleet.Scope
}

// Request contains untrusted Hello identity/metadata plus server-owned lease
// identity and receive time. Hello.AuthorizedIndexes is ignored; Admit uses
// only the fresh transactional credential scope.
type Request struct {
	CollectorID string
	BootEpoch   string
	StreamID    string
	AcceptedAt  time.Time
	Hello       collectorfleet.Hello
}

// Result is the fresh credential and durable fleet state committed by one
// admission transaction.
type Result struct {
	Authentication auth.Authentication
	Collector      collectorfleet.Collector
	Lease          collectorfleet.Lease
}

// New constructs one tenant-pinned admission coordinator without changing the
// migrated schema. tokens and database must refer to the same control plane.
func New(
	database *control.DB,
	tokens *auth.Store,
	tenantID string,
) (*Store, error) {
	if database == nil || database.GORMDB() == nil {
		return nil, fmt.Errorf(
			"%w: control database is required",
			control.ErrInvalidArgument,
		)
	}
	if tokens == nil {
		return nil, fmt.Errorf(
			"%w: collector token store is required",
			control.ErrInvalidArgument,
		)
	}
	if strings.TrimSpace(tenantID) != tenantID ||
		len(tenantID) == 0 ||
		len(tenantID) > maximumTenantIDBytes ||
		!utf8.ValidString(tenantID) ||
		strings.IndexByte(tenantID, 0) >= 0 {
		return nil, fmt.Errorf(
			"%w: tenant ID must be exact, valid UTF-8 without NUL bytes, and contain between 1 and %d bytes",
			control.ErrInvalidArgument,
			maximumTenantIDBytes,
		)
	}
	fleet, err := collectorfleet.New(database)
	if err != nil {
		return nil, fmt.Errorf("create collector fleet store: %w", err)
	}
	return &Store{
		orm:    database.GORMDB(),
		tokens: tokens,
		fleet:  fleet,
		scope:  collectorfleet.Scope{TenantID: tenantID},
	}, nil
}

// AuthorizeLease revalidates bearer at checkedAt and verifies its exact
// collector binding plus the enabled current durable lease in one consistent
// read transaction. It returns freshly derived index scope and deliberately
// does not record token-use telemetry.
//
// A concurrent administrative or credential change that commits before the
// snapshot is established takes effect for this operation. A change that
// commits afterward fences the next operation; the already admitted operation
// may finish. A verified identity accompanies ErrNoActiveIndexAuthority or
// ErrInvalidIndexAuthority only after the exact lease check also succeeds, so
// native ingestion can recover a previously durable batch before applying the
// mutable index result.
func (store *Store) AuthorizeLease(
	ctx context.Context,
	bearer string,
	lease collectorfleet.Lease,
	checkedAt time.Time,
) (
	authentication auth.Authentication,
	returnedErr error,
) {
	if ctx == nil {
		return auth.Authentication{}, fmt.Errorf(
			"%w: nil context",
			control.ErrInvalidArgument,
		)
	}
	if lease.TenantID != store.scope.TenantID {
		return auth.Authentication{}, ErrLeaseNotCurrent
	}
	tx := store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return auth.Authentication{}, fmt.Errorf(
			"begin collector lease authorization: %w",
			preferContextCancellation(ctx, tx.Error),
		)
	}
	finished := false
	defer finishTransaction(tx, &finished, &returnedErr)

	authentication, err := store.tokens.RevalidateCollectorInTransaction(
		ctx,
		tx,
		bearer,
		checkedAt,
	)
	deferredIndexAuthority := errors.Is(err, auth.ErrNoActiveIndexAuthority) ||
		errors.Is(err, auth.ErrInvalidIndexAuthority)
	if err != nil && !deferredIndexAuthority {
		return auth.Authentication{}, preferContextCancellation(ctx, err)
	}
	indexAuthorityErr := err
	if authentication.BoundCollectorID != lease.CollectorID {
		return auth.Authentication{}, auth.ErrUnauthorized
	}
	current, err := store.fleet.IsCurrentLeaseInTransaction(
		ctx,
		tx,
		lease,
	)
	if err != nil {
		return auth.Authentication{}, preferContextCancellation(ctx, err)
	}
	if !current {
		return auth.Authentication{}, ErrLeaseNotCurrent
	}
	if err := tx.Commit().Error; err != nil {
		return auth.Authentication{}, fmt.Errorf(
			"commit collector lease authorization: %w",
			preferContextCancellation(ctx, err),
		)
	}
	finished = true
	return authentication, indexAuthorityErr
}

// Admit atomically revalidates bearer at AcceptedAt, records token use,
// verifies the bound collector and enabled fleet state, and allocates a
// durable lease. Any failure rolls back every write.
func (store *Store) Admit(
	ctx context.Context,
	bearer string,
	request Request,
) (
	result Result,
	returnedErr error,
) {
	if ctx == nil {
		return Result{}, fmt.Errorf("%w: nil context", control.ErrInvalidArgument)
	}
	prepared, err := collectorfleet.PrepareClaim(collectorfleet.ClaimRequest{
		Scope:       store.scope,
		CollectorID: request.CollectorID,
		BootEpoch:   request.BootEpoch,
		StreamID:    request.StreamID,
		ReceivedAt:  request.AcceptedAt,
		Hello:       request.Hello,
	})
	if err != nil {
		return Result{}, err
	}

	tx := store.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return Result{}, fmt.Errorf(
			"begin collector admission: %w",
			preferContextCancellation(ctx, tx.Error),
		)
	}
	finished := false
	defer finishTransaction(tx, &finished, &returnedErr)

	authentication, err := store.tokens.
		RevalidateAndRecordCollectorUseInTransaction(
			ctx,
			tx,
			bearer,
			request.AcceptedAt,
		)
	if err != nil {
		return Result{}, preferContextCancellation(ctx, err)
	}
	if authentication.BoundCollectorID != request.CollectorID {
		return Result{}, auth.ErrUnauthorized
	}

	collector, lease, err := store.fleet.ClaimPreparedInTransaction(
		ctx,
		tx,
		prepared,
		authentication.AuthorizedIndexNames(),
	)
	if err != nil {
		return Result{}, preferContextCancellation(ctx, err)
	}
	if err := tx.Commit().Error; err != nil {
		return Result{}, fmt.Errorf(
			"commit collector admission: %w",
			preferContextCancellation(ctx, err),
		)
	}
	finished = true
	return Result{
		Authentication: authentication,
		Collector:      collector,
		Lease:          lease,
	}, nil
}

func preferContextCancellation(ctx context.Context, err error) error {
	if err == nil || ctx == nil {
		return err
	}
	contextErr := ctx.Err()
	if contextErr == nil {
		return err
	}
	if errors.Is(err, sql.ErrTxDone) {
		// database/sql rolls a context-bound transaction back asynchronously.
		// If that rollback wins a race with our next operation, ErrTxDone is the
		// transport symptom of the already-observed context cancellation. These
		// transactions are private to this operation, so they cannot otherwise
		// have been completed by another caller.
		return contextErr
	}
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == sqlite3.SQLITE_INTERRUPT {
		// modernc cancels in-flight work with sqlite3_interrupt and may return
		// its native code rather than wrapping ctx.Err().
		// Prefer the causal context sentinel only for that exact base code; an
		// unrelated storage failure racing with cancellation remains visible.
		return contextErr
	}
	return err
}

func finishTransaction(
	tx *gorm.DB,
	finished *bool,
	returnedErr *error,
) {
	if tx == nil || finished == nil || *finished {
		return
	}
	rollbackErr := tx.Rollback().Error
	*finished = true
	if rollbackErr == nil {
		return
	}
	wrapped := fmt.Errorf("roll back collector admission: %w", rollbackErr)
	if returnedErr != nil && *returnedErr != nil {
		*returnedErr = errors.Join(*returnedErr, wrapped)
		return
	}
	if returnedErr != nil {
		*returnedErr = wrapped
	}
}
