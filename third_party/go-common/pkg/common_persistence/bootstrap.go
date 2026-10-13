package common_persistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/go-common/migrations"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/pressly/goose/v3"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// SchemaSetupFunc performs repeatable, application-owned schema reconciliation.
// It runs on every persistence bootstrap and therefore must be idempotent.
type SchemaSetupFunc func(context.Context, *gorm.DB) error

// Option configures persistence bootstrap behavior.
type Option func(*bootstrapOptions) error

type bootstrapOptions struct {
	applicationSchemaSetups []applicationSchemaSetup
}

type applicationSchemaSetup struct {
	name string
	run  func(context.Context, *gorm.DB) error
}

// WithApplicationSchemaSetup registers repeatable application schema setup.
// Repeating this option registers multiple steps in declaration order. Names
// must be non-empty and unique.
func WithApplicationSchemaSetup(name string, setup SchemaSetupFunc) Option {
	return func(options *bootstrapOptions) error {
		if setup == nil {
			return fmt.Errorf("application schema setup %q has a nil callback", strings.TrimSpace(name))
		}
		return options.appendApplicationSchemaSetup(applicationSchemaSetup{
			name: name,
			run:  setup,
		})
	}
}

func (options *bootstrapOptions) appendApplicationSchemaSetup(setup applicationSchemaSetup) error {
	setup.name = strings.TrimSpace(setup.name)
	if setup.name == "" {
		return errors.New("application schema setup has an empty name")
	}
	for _, existing := range options.applicationSchemaSetups {
		if existing.name == setup.name {
			return fmt.Errorf("application schema setup name %q is duplicated", setup.name)
		}
	}
	options.applicationSchemaSetups = append(options.applicationSchemaSetups, setup)
	return nil
}

// DatabaseProvider exposes the two database-access modes needed by application
// repositories without expanding CommonPersistenceInterface with GORM details.
type DatabaseProvider interface {
	// DB returns the ambient transaction from ctx when present, otherwise the
	// base pool. The returned GORM session is bound to ctx.
	DB(ctx context.Context) *gorm.DB
	// BaseDB deliberately bypasses any ambient transaction. Use it only for
	// work whose durability must be independent from a request transaction.
	BaseDB(ctx context.Context) *gorm.DB
}

type databaseOwnership uint8

const (
	borrowedDatabase databaseOwnership = iota
	ownedDatabase
)

type handlerLifecycle struct {
	wait      sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

const (
	schemaBootstrapLockName      = "go-common:persistence-bootstrap"
	schemaBootstrapLockSQL       = "SELECT pg_advisory_lock(hashtext(current_database()), hashtext(?))"
	schemaBootstrapUnlockSQL     = "SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(?))"
	schemaBootstrapUnlockTimeout = 5 * time.Second
)

// Open creates and owns the PostgreSQL connection pool from one immutable
// startup snapshot. Close stops all persistence background work
// and closes the pool exactly once.
func Open[U common_service_models.UserLike](
	ctx context.Context,
	provider config.PersistenceProvider,
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	options ...Option,
) (*CommonPersistenceHandler[U], error) {
	resolved, err := resolveBootstrapOptions(options)
	if err != nil {
		return nil, err
	}
	startup, err := validateBootstrapInputs(ctx, provider, logger)
	if err != nil {
		return nil, err
	}

	db, err := initializeDatabase(ctx, startup, logger.Named(loggerName))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return initializeHandler[U](ctx, db, ownedDatabase, provider, startup, logger, metricsHandler, resolved)
}

// Borrow initializes persistence around a caller-owned base connection pool
// from an immutable startup snapshot. Close stops handler-owned
// background work but never closes the pool. The caller must keep the pool
// alive until Close returns and close it afterward.
func Borrow[U common_service_models.UserLike](
	ctx context.Context,
	db *gorm.DB,
	provider config.PersistenceProvider,
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	options ...Option,
) (*CommonPersistenceHandler[U], error) {
	return initializeWithDatabase[U](ctx, db, borrowedDatabase, provider, logger, metricsHandler, options)
}

// Adopt initializes persistence around a caller-created base connection pool
// from an immutable startup snapshot. Ownership transfers only
// after all arguments, options, and the base-pool requirement validate. Later
// initialization failure and Close both close the transferred pool exactly
// once.
func Adopt[U common_service_models.UserLike](
	ctx context.Context,
	db *gorm.DB,
	provider config.PersistenceProvider,
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	options ...Option,
) (*CommonPersistenceHandler[U], error) {
	return initializeWithDatabase[U](ctx, db, ownedDatabase, provider, logger, metricsHandler, options)
}

func initializeWithDatabase[U common_service_models.UserLike](
	ctx context.Context,
	db *gorm.DB,
	ownership databaseOwnership,
	provider config.PersistenceProvider,
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	options []Option,
) (*CommonPersistenceHandler[U], error) {
	resolved, err := resolveBootstrapOptions(options)
	if err != nil {
		return nil, err
	}
	startup, err := validateBootstrapInputs(ctx, provider, logger)
	if err != nil {
		return nil, err
	}
	if err := validateBaseDatabase(db); err != nil {
		return nil, err
	}

	return initializeHandler[U](ctx, db, ownership, provider, startup, logger, metricsHandler, resolved)
}

func validateBaseDatabase(db *gorm.DB) error {
	if db == nil {
		return errors.New("database is required")
	}

	connPool := db.ConnPool
	if db.Statement != nil && db.Statement.ConnPool != nil {
		connPool = db.Statement.ConnPool
	}
	if isTransactionConnPool(connPool) {
		return errors.New("database must be a base connection pool, not a transaction")
	}
	if _, err := db.DB(); err != nil {
		return fmt.Errorf("database must be a base connection pool: %w", err)
	}
	return nil
}

func isTransactionConnPool(connPool gorm.ConnPool) bool {
	if _, isTransaction := connPool.(gorm.TxCommitter); isTransaction {
		return true
	}
	if prepared, ok := connPool.(*gorm.PreparedStmtDB); ok {
		return isTransactionConnPool(prepared.ConnPool)
	}
	return false
}

func initializeHandler[U common_service_models.UserLike](
	parent context.Context,
	db *gorm.DB,
	ownership databaseOwnership,
	provider config.PersistenceProvider,
	startup config.PersistenceView,
	logger *zap.Logger,
	metricsHandler *common_metrics.CommonMetricsHandler,
	options bootstrapOptions,
) (*CommonPersistenceHandler[U], error) {
	ctx, cancel := context.WithCancel(parent)
	handler := &CommonPersistenceHandler[U]{
		runtime:                     provider,
		transactionTimeout:          startup.DBTransactionTimeout(),
		objectHashSalt:              startup.ObjectHashSalt(),
		db:                          db,
		databaseOwnership:           ownership,
		logger:                      logger,
		metricsHandler:              metricsHandler,
		ctx:                         ctx,
		cancel:                      cancel,
		ledgerInferenceModelIDCache: newLedgerInferenceModelIDCache(),
	}

	if err := handler.runSchemaBootstrap(ctx, options.applicationSchemaSetups); err != nil {
		return nil, handler.failInitialization(err)
	}
	if err := handler.runSQLMigrations(ctx); err != nil {
		return nil, handler.failInitialization(err)
	}
	if err := handler.initMinioClient(startup); err != nil {
		return nil, handler.failInitialization(err)
	}
	if err := handler.startBackgroundMonitors(); err != nil {
		return nil, handler.failInitialization(err)
	}

	appLogger.For(ctx, handler.logger, loggerName).Info("Persistence handler initialized",
		zap.String(logkeys.Operation, "initialize_persistence_handler"),
		zap.String(appLogger.KeyOutcome, "initialized"),
		zap.Bool(appLogger.KeyOwnsDatabasePool, ownership == ownedDatabase),
		zap.Int(appLogger.KeyApplicationSchemaSetupCount, len(options.applicationSchemaSetups)),
		zap.Bool(appLogger.KeyDatabaseHealthCheckerEnabled, startup.DBHealthCheckInterval() > 0),
		zap.Bool(appLogger.KeyDatabaseMetricsCollectorEnabled, metricsHandler != nil && startup.DBMetricsInterval() > 0))

	return handler, nil
}

// validateBootstrapInputs samples the one startup generation used for
// restart-bound resources.
func validateBootstrapInputs(ctx context.Context, provider config.PersistenceProvider, logger *zap.Logger) (config.PersistenceView, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if provider == nil {
		return nil, errors.New("config is required")
	}
	if logger == nil {
		return nil, errors.New("logger is required")
	}
	startup := provider()
	if startup == nil || startup == config.PersistenceView(config.RootView{}) {
		return nil, errors.New("config view is required")
	}
	return startup, nil
}

func resolveBootstrapOptions(options []Option) (bootstrapOptions, error) {
	var resolved bootstrapOptions
	for index, option := range options {
		if option == nil {
			return bootstrapOptions{}, fmt.Errorf("persistence option at index %d is nil", index)
		}
		if err := option(&resolved); err != nil {
			return bootstrapOptions{}, fmt.Errorf("invalid persistence option at index %d: %w", index, err)
		}
	}
	return resolved, nil
}

func (h *CommonPersistenceHandler[U]) runSchemaBootstrap(ctx context.Context, setups []applicationSchemaSetup) error {
	return withSchemaBootstrapLock(ctx, h.db, func(db *gorm.DB) error {
		if err := requireCurrentSchema(db); err != nil {
			return err
		}
		if err := h.runAutoMigrations(ctx, db); err != nil {
			return err
		}
		if err := ensureNotificationFanoutIndexes(db); err != nil {
			return fmt.Errorf("ensure notification fan-out indexes: %w", err)
		}
		identity, err := initializeNotificationTransactionIdentity(ctx, db, h.logger)
		if err != nil {
			return fmt.Errorf("initialize notification transaction identity: %w", err)
		}
		h.notificationTransactionIdentity = identity
		return h.runApplicationSchemaSetups(ctx, db, setups)
	})
}

// runSQLMigrations applies the versioned SQL upgrades in migrations/ after the
// bootstrap lock is released: goose pins its own connection, so it cannot share
// the bootstrap's pinned session on a one-connection pool.
// TODO(multi-instance-v2): concurrent startups need goose's session locker.
func (h *CommonPersistenceHandler[U]) runSQLMigrations(ctx context.Context) error {
	sqlDB, err := h.db.DB()
	if err != nil {
		return err
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, sqlDB, migrations.FS,
		goose.WithTableName("gocommon_schema_migrations"), goose.WithDisableGlobalRegistry(true))
	if err == nil {
		_, err = provider.Up(ctx)
	}
	if err != nil {
		return fmt.Errorf("apply SQL migrations: %w", err)
	}
	return nil
}

func withSchemaBootstrapLock(ctx context.Context, baseDB *gorm.DB, run func(*gorm.DB) error) error {
	return baseDB.WithContext(ctx).Connection(func(db *gorm.DB) (resultErr error) {
		// Resolve the namespace on the pinned physical session. Retain the exact
		// name for release even if an application setup changes search_path.
		lockName, err := scopedSchemaBootstrapLockName(db.Session(&gorm.Session{NewDB: true}))
		if err != nil {
			return err
		}
		if err := db.Exec(schemaBootstrapLockSQL, lockName).Error; err != nil {
			// A cancellation or lost response can make acquisition outcome
			// unknowable. Never return that session to a borrowed pool.
			return errors.Join(
				fmt.Errorf("acquire schema bootstrap advisory lock: %w", err),
				discardSchemaBootstrapConnection(db),
			)
		}
		defer func() {
			// The bootstrap context can be canceled while the callback runs. Unlock
			// with an independent, bounded context so cancellation cannot return a
			// session-scoped advisory lock to the connection pool.
			unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), schemaBootstrapUnlockTimeout)
			defer cancel()

			unlockErr := releaseSchemaBootstrapLock(db.WithContext(unlockCtx), lockName)
			if unlockErr != nil {
				// If PostgreSQL did not confirm the unlock, discard the physical
				// connection. Reusing an uncertain session could serialize every
				// future bootstrap behind a lock that no application owns.
				unlockErr = errors.Join(unlockErr, discardSchemaBootstrapConnection(db))
			}
			resultErr = errors.Join(resultErr, unlockErr)
		}()

		// Connection supplies an initialized GORM handle whose chain operations
		// reuse one mutable Statement. Use it only to pin the physical connection;
		// migrations need a fresh-session parent so predicates from one operation
		// cannot leak into later tables or the advisory-lock release.
		return run(db.Session(&gorm.Session{NewDB: true}))
	})
}

func scopedSchemaBootstrapLockName(db *gorm.DB) (string, error) {
	var schema sql.NullString
	if err := db.Raw("SELECT current_schema()").Scan(&schema).Error; err != nil {
		return "", fmt.Errorf("resolve schema bootstrap lock namespace: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return "", errors.New("schema bootstrap requires a current PostgreSQL schema")
	}
	return schema.String + ":" + schemaBootstrapLockName, nil
}

func discardSchemaBootstrapConnection(db *gorm.DB) error {
	if db == nil || db.Statement == nil {
		return appLogger.InvariantError("schema_bootstrap_connection_has_statement", nil,
			zap.String(logkeys.Operation, "discard_schema_bootstrap_connection"),
			zap.String(logkeys.Expected, "database statement with physical connection"),
			zap.String(logkeys.Actual, "nil database or statement"))
	}
	conn, ok := db.Statement.ConnPool.(*sql.Conn)
	if !ok {
		return appLogger.InvariantError("schema_bootstrap_uses_physical_sql_connection", nil,
			zap.String(logkeys.Operation, "discard_schema_bootstrap_connection"),
			zap.String(logkeys.Expected, "*sql.Conn connection pool"),
			zap.String(logkeys.Actual, fmt.Sprintf("%T", db.Statement.ConnPool)))
	}
	if err := conn.Raw(func(any) error { return driver.ErrBadConn }); err != nil && !errors.Is(err, driver.ErrBadConn) {
		return fmt.Errorf("discard schema bootstrap connection: %w", err)
	}
	return nil
}

func releaseSchemaBootstrapLock(db *gorm.DB, lockName string) error {
	var unlocked bool
	if err := db.Raw(schemaBootstrapUnlockSQL, lockName).Scan(&unlocked).Error; err != nil {
		return fmt.Errorf("release schema bootstrap advisory lock: %w", err)
	}
	if !unlocked {
		return appLogger.InvariantError("schema_bootstrap_lock_is_held_at_release",
			errors.New("release schema bootstrap advisory lock: lock was not held"),
			zap.String(logkeys.Operation, "release_schema_bootstrap_lock"),
			zap.String(logkeys.Expected, "advisory lock held by pinned connection"),
			zap.String(logkeys.Actual, "advisory lock was not held"))
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) runApplicationSchemaSetups(ctx context.Context, db *gorm.DB, setups []applicationSchemaSetup) error {
	logger := appLogger.For(ctx, h.logger, loggerName)
	for _, setup := range setups {
		if setup.run == nil {
			return appLogger.InvariantError("application_schema_setup_has_callback", nil,
				zap.String(logkeys.Operation, "run_application_schema_setups"),
				zap.String(appLogger.KeySetup, setup.name),
				zap.String(logkeys.Expected, "non-nil schema setup callback"),
				zap.String(logkeys.Actual, "nil schema setup callback"))
		}
		logger.Info(
			"Running application schema setup",
			zap.String(logkeys.Operation, "run_application_schema_setups"),
			zap.String(appLogger.KeySetup, setup.name),
		)
		if err := setup.run(ctx, db.WithContext(ctx)); err != nil {
			return appLogger.WithErrorFields(fmt.Errorf("application schema setup %q failed: %w", setup.name, err),
				zap.String(logkeys.Operation, "run_application_schema_setups"),
				zap.String(appLogger.KeySetup, setup.name))
		}
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) startBackgroundMonitors() error {
	if h.metricsHandler != nil {
		h.lifecycle.wait.Go(func() {
			h.collectDBMetrics(h.ctx)
		})
	}

	sqlDB, err := h.db.DB()
	if err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "start_background_monitors"),
			zap.String(logkeys.Stage, "database_health_check"))
	}
	h.lifecycle.wait.Go(func() {
		h.startDBHealthCheck(h.ctx, sqlDB)
	})
	return nil
}

func (h *CommonPersistenceHandler[U]) failInitialization(cause error) error {
	return errors.Join(cause, h.Close())
}

func (h *CommonPersistenceHandler[U]) closeDatabase() error {
	return closeDatabasePool(h.db)
}

func closeDatabasePool(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("get database pool during close: %w", err)
	}
	if err := sqlDB.Close(); err != nil {
		return fmt.Errorf("close database pool: %w", err)
	}
	return nil
}
