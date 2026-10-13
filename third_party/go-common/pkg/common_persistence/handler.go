package common_persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_metrics"
	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

//	Check be/api/core/setup.go or where the router is initialized.

// Common error variables returned by persistence operations.
// These errors provide consistent error handling across the persistence layer.
var (
	// ErrUserNotFound is returned when a user lookup fails to find a matching user.
	ErrUserNotFound = errors.New("user not found")

	// ErrSessionTokenNotFound is returned when a session token lookup fails.
	ErrSessionTokenNotFound = errors.New("session token not found")

	// ErrDatabaseError is returned for general database operation failures.
	ErrDatabaseError = errors.New("database error")

	// ErrUserAlreadyExists is returned when attempting to create a user with an email
	// or username that already exists in the system.
	ErrUserAlreadyExists = errors.New("user already exists")

	// ErrUsernameAvailabilityCheckFailed is returned when username existence
	// cannot be checked due to a persistence failure.
	ErrUsernameAvailabilityCheckFailed = errors.New("username availability check failed")

	// ErrEmailVerificationNotFound is returned when an email verification record
	// cannot be found for a user or email.
	ErrEmailVerificationNotFound = errors.New("email verification not found")

	// ErrPhoneVerificationNotFound is returned when a phone verification record
	// cannot be found for a user.
	ErrPhoneVerificationNotFound = errors.New("phone verification not found")

	// ErrTokenExpired is returned when attempting to use an expired token.
	ErrTokenExpired = errors.New("token expired")

	// ErrTokenRevoked is returned when attempting to use a revoked token.
	ErrTokenRevoked = errors.New("token revoked")

	// ErrTokenAlreadyUsed is returned when attempting to use a single-use token
	// that has already been consumed.
	ErrTokenAlreadyUsed = errors.New("token already used")

	// ErrInvalidToken is returned when a token format is invalid or corrupted.
	ErrInvalidToken = errors.New("invalid token")

	// ErrContactMessageNotFound is returned when a contact message lookup fails.
	ErrContactMessageNotFound = errors.New("contact message not found")

	// ErrCareerJobPostingNotFound is returned when a career job posting lookup fails.
	ErrCareerJobPostingNotFound = errors.New("career job posting not found")

	// ErrCareerJobPostingAlreadyExists is returned when a career job posting write hits a unique constraint.
	ErrCareerJobPostingAlreadyExists = errors.New("career job posting already exists")

	// ErrCareerApplicationNotFound is returned when a career application lookup fails.
	ErrCareerApplicationNotFound = errors.New("career application not found")

	// ErrInferenceModelNotFound is returned when an inference model lookup fails.
	ErrInferenceModelNotFound = errors.New("inference model not found")

	// ErrInferenceModelAlreadyExists is returned when an inference model conflicts
	// with an existing provider/model pair.
	ErrInferenceModelAlreadyExists = errors.New("inference model already exists")

	// ErrInferenceModelInUse is returned when an operation route still references a model.
	ErrInferenceModelInUse = errors.New("inference model is in use")

	// ErrOAuthIdentityNotFound is returned when no linked OAuth identity matches.
	ErrOAuthIdentityNotFound = errors.New("oauth identity not found")

	// ErrOAuthIdentityConflict is returned when linking an OAuth identity would
	// give a user a second identity for one provider, or give one provider
	// account to a second user.
	ErrOAuthIdentityConflict = errors.New("oauth identity conflict")
)

var _ CommonPersistenceInterface = (*CommonPersistenceHandler[common_service_models.User])(nil)
var _ DatabaseProvider = (*CommonPersistenceHandler[common_service_models.User])(nil)
var _ PersonalWorkspacePersistence = (*CommonPersistenceHandler[common_service_models.User])(nil)

const (
	loggerName = "common_persistence"
)

// CommonPersistenceHandler implements the CommonPersistenceInterface using GORM and MinIO.
// It provides comprehensive database and object storage operations with support for
// generic user types through type parameters.
// Type parameters:
//   - T: The comparable type used for user IDs (typically uint64)
//   - U: The user type that implements common_service_models.UserLike
//
// The handler manages:
//   - Database connections and transactions
//   - MinIO client for object storage
//   - Background goroutines for metrics collection
//   - Attachment lifecycle management
type CommonPersistenceHandler[U common_service_models.UserLike] struct {
	// runtime supplies immutable hot policy; remaining fields are startup resource identity.
	runtime            config.PersistenceProvider
	transactionTimeout time.Duration
	objectHashSalt     config.Secret
	minioAccessKey     string
	minioSecretKey     config.Secret
	poolPolicyMu       sync.Mutex
	poolPolicy         databasePoolPolicy
	poolPolicySet      bool

	// db is the main GORM database connection
	db *gorm.DB

	// minioClient handles all object storage operations
	minioClient *minio.Client

	// minioBucket is the bucket name for storing objects
	minioBucket string

	// minioPublicBucket is the bucket name for anonymously readable objects.
	// An empty value means public object storage is disabled.
	minioPublicBucket string

	// logger is the application base logger; operations derive the persistence name.
	logger *zap.Logger

	// metricsHandler records persistence layer metrics
	metricsHandler *common_metrics.CommonMetricsHandler

	// ctx is used to manage background goroutines lifecycle
	ctx context.Context

	// cancel allows graceful shutdown of background goroutines
	cancel context.CancelFunc

	// databaseOwnership determines whether Close is responsible for the SQL pool.
	databaseOwnership databaseOwnership

	// lifecycle makes shutdown idempotent and waits for handler-owned monitors.
	lifecycle handlerLifecycle

	// userCacheInvalidator is an optional callback for invalidating user cache
	// This is called after successful user updates to ensure cache consistency
	userCacheInvalidator func(userID uint64) error

	// ledgerInferenceModelIDCache backs the best-effort ChatInferenceCall.InferenceModelID stamping
	// (ResolveLedgerInferenceModelID). nil disables caching (and stamping) for a handler built without
	// the constructor.
	ledgerInferenceModelIDCache *ledgerInferenceModelIDCache

	// notificationTransactionIdentity is reconciled under the bootstrap lock
	// before requests or workers start and remains immutable for this process.
	notificationTransactionIdentity common_persistence_models.NotificationTransactionIdentity

	// agentRunBudgetCeilings supplies work-budget ceilings for the runs persistence mints itself
	// (delegation wakes and drained follow-ups). nil until the service installs a provider.
	agentRunBudgetCeilings atomic.Pointer[ChatAgentRunBudgetCeilingsProvider]

	// agentMessageRecorder records delivered input and delegation notices in their source
	// transaction. The chat service installs its notification sink; nil records none.
	agentMessageRecorder atomic.Pointer[ChatAgentMessageRecorder]
	childWakeLimits      atomic.Pointer[ChatAgentChildWakeLimitsProvider]
	// graphEventNotifier is the post-commit fast path for parked graphs whose resume revision
	// advanced. The service installs it; nil notifies nobody.
	graphEventNotifier atomic.Pointer[ChatAgentGraphEventNotifier]
	// workflowAgentTypes lists the owner-driven agent types for the wake guard and the recovery
	// exclusion. nil until the service installs a provider.
	workflowAgentTypes atomic.Pointer[ChatAgentWorkflowTypesProvider]
}

// GetMetricsHandler executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetMetricsHandler() *common_metrics.CommonMetricsHandler {
	return h.metricsHandler
}

// recordAfterCommit records metrics once ctx's transaction commits, or at once when ctx carries no
// hook-capable transaction. Nothing runs while metrics are disabled.
func (h *CommonPersistenceHandler[U]) recordAfterCommit(ctx context.Context, record func(*common_metrics.CommonMetricsHandler)) {
	metrics := h.metricsHandler
	if metrics == nil || !metrics.IsEnabled() {
		return
	}
	if !AddPostCommitHookFromContext(ctx, func(context.Context) error {
		record(metrics)
		return nil
	}) {
		record(metrics)
	}
}

// BeginTransaction executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) BeginTransaction(ctx context.Context) (*gorm.DB, error) {
	h.applyPoolPolicy()
	txTimeout := h.transactionTimeout

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Starting new transaction",
			zap.String(logkeys.Operation, "begin_transaction"),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, txTimeout))
	}

	// Begin transaction with the timeout context
	tx := h.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, databaseErrorWithFields(tx.Error,
			zap.String(logkeys.Operation, "begin_transaction"),
			appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, txTimeout))
	}
	return tx, nil
}

// collectDBMetrics executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) collectDBMetrics(ctx context.Context) {
	logger := appLogger.For(ctx, h.logger, loggerName)
	interval := h.runtime().DBMetricsInterval()

	ticker := time.NewTicker(monitorPollInterval(interval))
	lastRun := time.Now()
	defer ticker.Stop()

	logger.Info("Starting database metrics collector",
		zap.String(logkeys.Operation, "collect_database_metrics"),
		appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval))
	for {
		select {
		case <-ctx.Done():
			if ce := logger.Check(zap.DebugLevel, "Stopping database metrics collector"); ce != nil {
				ce.Write(
					zap.String(logkeys.Operation, "collect_database_metrics"),
					zap.Error(ctx.Err()),
				)
			}
			return
		case <-ticker.C:
			interval = h.runtime().DBMetricsInterval()
			ticker.Reset(monitorPollInterval(interval))
			if interval <= 0 || time.Since(lastRun) < interval {
				continue
			}
			lastRun = time.Now()
			h.applyPoolPolicy()
			if h.db != nil && h.metricsHandler != nil {
				// Create a timeout context for metrics collection
				metricsCtx, cancel := context.WithTimeout(ctx, interval/2)

				// Get database stats with the timeout context
				sqlDB, err := h.db.DB()
				if err == nil {
					// Execute the stats collection with context timeout
					select {
					case <-metricsCtx.Done():
						logger.Warn("Metrics collection timed out",
							zap.String(logkeys.Operation, "collect_database_metrics"),
							appLogger.DurationSeconds(appLogger.KeyTimeoutSeconds, interval/2),
							zap.String(logkeys.Fallback, "skip_metrics_sample"),
							zap.Error(metricsCtx.Err()))
					default:
						stats := sqlDB.Stats()
						h.metricsHandler.RecordDBStats(stats.OpenConnections, stats.Idle)
						if ce := logger.Check(zap.DebugLevel, "DB metrics recorded"); ce != nil {
							ce.Write(
								zap.String(logkeys.Operation, "collect_database_metrics"),
								zap.Int(appLogger.KeyOpenConnections, stats.OpenConnections),
								zap.Int(appLogger.KeyIdleConnections, stats.Idle),
							)
						}
					}
				} else {
					// Metrics collection is best-effort and has no caller to receive
					// the failure, so a recoverable warning is the owning signal.
					logger.Warn("Failed to access database pool for metrics",
						zap.String(logkeys.Operation, "collect_database_metrics"),
						zap.String(logkeys.Fallback, "skip_metrics_sample"),
						zap.Error(err))
				}

				// Cancel the context after use (not deferred in loop)
				cancel()
			}
		}
	}
}

// initializeDatabase executes its operation in the persistence flow between service handlers and the database layer.
// Parameters:
//   - ctx: context.Context controlling the initial connectivity check.
//   - cfg: immutable startup view used by initializeDatabase.
//   - logger: *zap.Logger input used by initializeDatabase.
func initializeDatabase(ctx context.Context, cfg config.PersistenceView, logger *zap.Logger) (*gorm.DB, error) {
	diagnosticFields := []zap.Field{
		zap.String(logkeys.Operation, "initialize_database"),
		zap.String(appLogger.KeyDatabaseHost, cfg.DBHost()),
		zap.Int(appLogger.KeyDatabasePort, cfg.DBPort()),
		zap.String(appLogger.KeyDatabaseName, cfg.DBName()),
		zap.String(appLogger.KeySSLMode, cfg.DBSSLMode()),
	}
	// Set up database connection
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
		cfg.DBHost(),
		cfg.DBPort(),
		cfg.DBUser(),
		cfg.DBPassword().StringValue(),
		cfg.DBName(),
		cfg.DBSSLMode(),
	)

	// Set log level
	gormConfig := &gorm.Config{
		Logger:               databaseGormLogger(logger, cfg.DBLogLevel()),
		DisableAutomaticPing: true,
	}

	// Connect to database
	db, err := gorm.Open(postgres.Open(dsn), gormConfig)
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			append(diagnosticFields, zap.String(logkeys.Stage, "open"))...)
	}

	// Set connection pool settings
	sqlDB, err := db.DB()
	if err != nil {
		return nil, appLogger.WithErrorFields(err,
			append(diagnosticFields, zap.String(logkeys.Stage, "get_connection_pool"))...)
	}
	sqlDB.SetMaxOpenConns(cfg.DBMaxOpenConns())
	sqlDB.SetMaxIdleConns(cfg.DBMaxIdleConns())
	if cfg.DBConnMaxLifetime() > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(cfg.DBConnMaxLifetime()) * time.Second)
	}
	if cfg.DBConnMaxIdleTime() > 0 {
		sqlDB.SetConnMaxIdleTime(time.Duration(cfg.DBConnMaxIdleTime()) * time.Second)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		return nil, appLogger.WithErrorFields(errors.Join(
			fmt.Errorf("database ping failed: %w", err),
			closeDatabasePool(db),
		), append(diagnosticFields, zap.String(logkeys.Stage, "ping"))...)
	}

	return db, nil
}

// databaseGormLogger maps the persistence setting onto the logger instance
// installed in GORM. LogMode returns a configured copy, so its return value
// must be used rather than discarded.
func databaseGormLogger(logger *zap.Logger, configuredLevel config.DBLogLevel) gormlogger.Interface {
	var logLevel gormlogger.LogLevel
	switch configuredLevel {
	case config.DBLogLevelSilent:
		logLevel = gormlogger.Silent
	case config.DBLogLevelError:
		logLevel = gormlogger.Error
	case config.DBLogLevelWarn:
		logLevel = gormlogger.Warn
	case config.DBLogLevelInfo:
		logLevel = gormlogger.Info
	default:
		// Match config parsing/defaults: an unknown value should not silently
		// enable per-query logging.
		logLevel = gormlogger.Warn
		appLogger.LogInvariantViolation(logger, "database_log_level_is_valid", nil,
			zap.String(logkeys.Operation, "configure_gorm_logger"),
			zap.String(logkeys.Expected, "silent, error, warn, or info"),
			zap.String(logkeys.Actual, fmt.Sprint(configuredLevel)),
			zap.String(logkeys.Fallback, "use_warn_level"))
	}
	return appLogger.NewGormLogger(logger).LogMode(logLevel)
}

// runAutoMigrations executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) runAutoMigrations(ctx context.Context, db *gorm.DB) error {
	logger := appLogger.For(ctx, h.logger, loggerName)
	logger.Info("Starting database migration", zap.String(logkeys.Operation, "run_auto_migrations"))

	// Define all models to migrate
	models := []any{
		&common_persistence_models.User{},
		&common_persistence_models.Attachment{},
		&common_persistence_models.UserSettings{},
		&common_persistence_models.SessionToken{},
		&common_persistence_models.OAuthLogin{},
		&common_persistence_models.UserPassword{},
		&common_persistence_models.UserAuthMethod{},
		&common_persistence_models.EmailVerification{},
		&common_persistence_models.PhoneVerification{},
		&common_persistence_models.ContactMessage{},
		&common_persistence_models.CareerJobPosting{},
		&common_persistence_models.CareerApplication{},
		&common_persistence_models.Organization{},
		&common_persistence_models.UserOrganization{},
		&common_persistence_models.OrganizationEmailDomain{},
		&common_persistence_models.OrganizationInvitation{},
		&common_persistence_models.UsernameChangeHistory{},
		&common_persistence_models.InferenceModel{},
		// Chat models
		&common_persistence_models.ChatChannel{},
		&common_persistence_models.ChatChannelParticipant{},
		&common_persistence_models.ChatAgentSession{},
		&common_persistence_models.ChatAgentMessageReceipt{},
		&common_persistence_models.ChatAgentGraphBudget{},
		&common_persistence_models.ChatAgentSessionConfigSnapshot{},
		&common_persistence_models.ChatAgentSessionToolConfig{},
		&common_persistence_models.ChatAgentRun{},
		&common_persistence_models.ChatAgentContext{},
		&common_persistence_models.ChatContextOperation{},
		&common_persistence_models.ChatMessage{},
		&common_persistence_models.ChatAgentQueuedInput{},
		&common_persistence_models.ChatMessageToolCall{},
		&common_persistence_models.ChatMessageEditHistory{},
		&common_persistence_models.ChatMessageEmbed{},
		&common_persistence_models.ChatMessageReaction{},
		&common_persistence_models.ChatMessageMention{},
		&common_persistence_models.ChatInferenceCall{},
		&common_persistence_models.ChatInferenceTotals{},
		// Durable notifications
		&common_persistence_models.Notification{},
		&common_persistence_models.NotificationOccurrence{},
		&common_persistence_models.NotificationDelivery{},
		&common_persistence_models.NotificationPreference{},
		&common_persistence_models.NotificationEmission{},
		&common_persistence_models.NotificationEmissionRecipient{},
		&common_persistence_models.NotificationTransactionEpoch{},
		&common_persistence_models.NotificationGroupState{},
		&common_persistence_models.NotificationRetirementCutoff{},
		&common_persistence_models.NotificationChatRetirement{},
		&common_persistence_models.NotificationScopeRetirementFence{},
		&common_persistence_models.NotificationScopeRetirement{},
		&common_persistence_models.NotificationRecipientRetirement{},
		&common_persistence_models.NotificationSourceResolution{},
		// AI control-plane models
		&common_persistence_models.InferenceOperationRoute{},
		&common_persistence_models.InferenceOperationModelOption{},
		&common_persistence_models.OrganizationInferencePolicy{},
		&common_persistence_models.OrganizationInferenceOperationPolicy{},
		&common_persistence_models.OrganizationInferenceModelOption{},
		&common_persistence_models.OrganizationInferenceProvider{},
		&common_persistence_models.ChatAgentSessionInferenceRetarget{},
		&common_persistence_models.InferenceModelRateLimitPolicy{},
		&common_persistence_models.InferenceAttempt{},
	}

	// Pass 1: Create tables without foreign key constraints
	// This handles circular dependencies between User and Attachment
	if ce := logger.Check(zap.DebugLevel, "Creating database tables without foreign key constraints"); ce != nil {
		ce.Write(
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "create_tables_without_foreign_keys"),
		)
	}

	// Create a new session with a clean connection pool
	dbNoFK := db.Session(&gorm.Session{NewDB: true})
	// Toggle the flag on the cloned DB's config
	dbNoFK.DisableForeignKeyConstraintWhenMigrating = true

	if err := dbNoFK.AutoMigrate(models...); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "create_tables_without_foreign_keys"))
	}

	// Pass 2: Add foreign key constraints
	// All tables now exist, so circular references are no longer a problem
	if ce := logger.Check(zap.DebugLevel, "Reconciling database foreign key constraints"); ce != nil {
		ce.Write(
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "reconcile_foreign_keys"),
		)
	}

	// Use the original db which has foreign keys enabled
	if err := db.AutoMigrate(models...); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "reconcile_foreign_keys"))
	}

	if err := ensureNotificationIndexes(db); err != nil {
		return fmt.Errorf("ensure notification indexes: %w", err)
	}

	if err := h.ensureUserOrganizationsCoveringIndex(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_user_organizations_covering_index"))
	}
	if err := h.ensurePersonalWorkspaceIndexes(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_personal_workspace_indexes"))
	}
	if err := h.ensurePerformanceIndexes(db, logger); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_performance_indexes"))
	}
	if err := h.ensureChatAgenticIndexes(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_chat_agentic_indexes"))
	}
	if err := h.ensureChatAgentInteractionSchema(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_chat_agent_interaction_schema"))
	}
	if err := h.ensureInferenceControlPlaneIndexes(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_inference_control_plane_indexes"))
	}
	if err := h.ensureInferenceControlPlaneModelConstraints(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_inference_control_plane_model_constraints"))
	}
	if err := h.ensureChatAgentOrganizationConstraints(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_chat_agent_organization_constraints"))
	}
	if err := h.ensureInferenceRoutingConstraints(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_inference_routing_constraints"))
	}
	if err := h.ensureInferenceAttemptOrganizationConstraints(db); err != nil {
		return databaseErrorWithFields(err,
			zap.String(logkeys.Operation, "run_auto_migrations"),
			zap.String(logkeys.Stage, "ensure_inference_attempt_organization_constraints"))
	}
	logger.Info("Successfully completed database migration",
		zap.String(logkeys.Operation, "run_auto_migrations"),
		zap.String(appLogger.KeyOutcome, "completed"))
	return nil
}

// ensurePersonalWorkspaceIndexes creates the partial unique index that GORM
// struct tags cannot express. Deliberately omitting a deleted_at predicate makes
// the one-root-per-owner invariant span the root's full soft-deletion lifetime.
func (h *CommonPersistenceHandler[U]) ensurePersonalWorkspaceIndexes(db *gorm.DB) error {
	const indexName = "idx_organizations_personal_owner_root_unique"
	statement := fmt.Sprintf(
		"CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (personal_owner_user_id) WHERE parent_organization_id IS NULL AND personal_owner_user_id IS NOT NULL",
		indexName,
		common_persistence_models.TableNameOrganizations,
	)
	if err := db.Exec(statement).Error; err != nil {
		return err
	}

	// IF NOT EXISTS does not verify that a pre-existing same-named index has
	// the required definition. Fail startup instead of silently running without
	// the lifetime-root invariant after an interrupted or prerelease migration.
	var valid bool
	err := db.Raw(`
SELECT COALESCE((
	SELECT i.indisunique
	   AND i.indisvalid
	   AND i.indisready
	   AND i.indislive
       AND i.indnkeyatts = 1
       AND i.indnatts = 1
       AND (
           SELECT array_agg(a.attname ORDER BY index_key.ordinality)
           FROM unnest(i.indkey) WITH ORDINALITY AS index_key(attnum, ordinality)
           JOIN pg_attribute a
             ON a.attrelid = i.indrelid
            AND a.attnum = index_key.attnum
       ) = ARRAY['personal_owner_user_id']::name[]
       AND regexp_replace(lower(pg_get_expr(i.indpred, i.indrelid)), '[[:space:]()]', '', 'g') IN (
           'parent_organization_idisnullandpersonal_owner_user_idisnotnull',
           'personal_owner_user_idisnotnullandparent_organization_idisnull'
       )
    FROM pg_index i
    JOIN pg_class index_class ON index_class.oid = i.indexrelid
    JOIN pg_class table_class ON table_class.oid = i.indrelid
    JOIN pg_namespace namespace ON namespace.oid = table_class.relnamespace
    WHERE namespace.nspname = current_schema()
      AND table_class.relname = ?
      AND index_class.relname = ?
), FALSE)`, common_persistence_models.TableNameOrganizations, indexName).Scan(&valid).Error
	if err != nil {
		return err
	}
	if !valid {
		return fmt.Errorf("personal workspace index %s exists with an incompatible definition", indexName)
	}
	return nil
}

// ensureInferenceControlPlaneIndexes creates the AI control-plane attempt-ledger indexes that
// AutoMigrate cannot express through struct tags: the "(column, created_at DESC)" listing indexes
// used by the admin attempt list/summary queries, and the GIN jsonb_path_ops index that accelerates
// allowlisted metadata containment (@>) over the sanitized-context JSON. It is idempotent (every
// statement is IF NOT EXISTS) and additive.
func inferenceControlPlaneIndexStatements() []string {
	attempts := common_persistence_models.TableNameInferenceAttempts

	statements := []string{
		// Organization, operation, provider, model, status, and actor listings are always newest-first.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_created_id ON %s (created_at DESC, id DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_organization_created ON %s (organization_id, created_at DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_operation_created ON %s (operation, created_at DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_provider_created ON %s (provider_id, created_at DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_model_created ON %s (inference_model_id, created_at DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_status_created ON %s (status, created_at DESC)", attempts),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_actor_created ON %s (actor_user_id, created_at DESC)", attempts),
		// Metadata containment: GIN jsonb_path_ops is the compact, @>-only operator class the admin
		// metadata filters rely on.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_inference_attempts_context_gin ON %s USING gin (sanitized_context_json jsonb_path_ops)", attempts),
	}
	return statements
}

func (h *CommonPersistenceHandler[U]) ensureInferenceControlPlaneIndexes(db *gorm.DB) error {
	for _, statement := range inferenceControlPlaneIndexStatements() {
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}
	return nil
}

func (h *CommonPersistenceHandler[U]) ensureUserOrganizationsCoveringIndex(db *gorm.DB) error {
	const indexName = "idx_user_organizations_user_org_covering"

	query := fmt.Sprintf(
		"CREATE INDEX IF NOT EXISTS %s ON %s (user_id, organization_id) INCLUDE (role, joined_at, updated_at)",
		indexName,
		common_persistence_models.TableNameUserOrganizations,
	)

	return executeSharedDDL(db, query)
}

// ensurePerformanceIndexes creates the indexes that GORM struct tags cannot
// express — GIN, functional, partial, and expression indexes — so the hot
// queries they serve stay on index scans. All statements are idempotent.
func performanceIndexStatements() []string {
	return []string{
		// `? = ANY(attachment_ids)` reference checks on attachment deletion.
		// Abandoned registration cleanup scans only never-verified users in age order.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_email_verifications_unverified_cleanup ON %s (created_at, user_id) WHERE verified_at IS NULL", common_persistence_models.TableNameEmailVerifications),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_messages_attachment_ids_gin ON %s USING GIN (attachment_ids)", common_persistence_models.TableNameChatMessages),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_edit_history_attachment_ids_gin ON %s USING GIN (attachment_ids)", common_persistence_models.TableNameChatMessageEditHistory),
		// Case-insensitive username availability checks during registration;
		// the unique index on the raw column cannot serve LOWER(username) = ?.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_username_lower ON %s (LOWER(username))", common_persistence_models.TableNameUsers),
		// Cursor pagination in ListChatMessagesWithOptions: equality on channel_id and
		// parent_message_id (or IS NULL) with a range/order on id.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_messages_channel_parent_id ON %s (channel_id, parent_message_id, id)", common_persistence_models.TableNameChatMessages),
		// Main-timeline pages OR in broadcast thread replies, which the equality index above
		// cannot serve without a sort. A partial index whose predicate is the exact
		// main-timeline predicate lets Postgres walk (channel_id, id) in order; the planner
		// only picks it when the query predicate implies this one, so both share
		// chatMainTimelinePredicate.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_messages_channel_main_timeline_id ON %s (channel_id, id) WHERE %s", common_persistence_models.TableNameChatMessages, chatMainTimelinePredicate),
		// Entity-scoped attachment lookups filter on both columns together;
		// only single-column indexes exist via struct tags.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_attachments_object_type_entity_id ON %s (object_type, entity_id)", common_persistence_models.TableNameAttachments),
		// Serves the settled temporary-object sweep; deleted objects have their name cleared, so
		// only in-flight uploads and the sweep's backlog stay in it.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_attachments_temporary_object ON %s (id) WHERE temporary_object_name <> ''", common_persistence_models.TableNameAttachments),
		// Server-produced provider/tool blobs bind one immutable object to one deterministic
		// entity key. This closes the concurrent lookup-then-create race in internal storage.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_attachments_internal_object_entity ON %s (object_type, entity_id) WHERE internal_only IS TRUE AND deleted_at IS NULL", common_persistence_models.TableNameAttachments),
		// Active-session lookups predicate `revoked_at IS NULL`, which the
		// COALESCE-based idx_user_type_valid expression index cannot serve.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_auth_tokens_user_type_active ON %s (user_id, token_type, expires_at) WHERE revoked_at IS NULL", common_persistence_models.TableNameSessionTokens),
		// At most one pending invitation (and one pending auto-add entry) per
		// email+organization: concurrent creates race past the count check, so
		// the database is the arbiter. expires_at cannot appear in the predicate;
		// CreateOrganizationInvitation clears expired pending rows before insert,
		// Existing conflicting rows must be repaired manually before startup.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_org_invitations_pending_unique ON %s (organization_id, email, requires_acceptance) WHERE accepted_at IS NULL AND declined_at IS NULL", common_persistence_models.TableNameOrganizationInvitations),
	}
}

func trigramIndexStatements() []string {
	return []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_username_trgm ON %s USING GIN (username gin_trgm_ops)", common_persistence_models.TableNameUsers),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_email_trgm ON %s USING GIN (email gin_trgm_ops)", common_persistence_models.TableNameUsers),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_first_name_trgm ON %s USING GIN (first_name gin_trgm_ops)", common_persistence_models.TableNameUsers),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_last_name_trgm ON %s USING GIN (last_name gin_trgm_ops)", common_persistence_models.TableNameUsers),
		// Must match the full-name expression used by SearchPublicUsers.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_users_full_name_trgm ON %s USING GIN ((first_name || ' ' || last_name) gin_trgm_ops)", common_persistence_models.TableNameUsers),
	}
}

func (h *CommonPersistenceHandler[U]) ensurePerformanceIndexes(db *gorm.DB, logger *zap.Logger) error {

	statements := performanceIndexStatements()
	for _, statement := range statements {
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}

	// Trigram indexes serve the prefix-ILIKE clauses in SearchPublicUsers. The
	// pg_trgm extension may not be creatable by the connecting role, so this
	// block is best-effort: unavailability is logged and skipped rather than
	// failing the migration.
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm").Error; err != nil {
		logger.Warn("pg_trgm extension unavailable; skipping trigram search indexes",
			zap.String(logkeys.Operation, "ensure_performance_indexes"),
			zap.String(logkeys.Fallback, "continue_without_trigram_indexes"),
			zap.Error(err))
		return nil
	}
	trigramStatements := trigramIndexStatements()
	for _, statement := range trigramStatements {
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}
	return nil
}

// ensureChatAgenticIndexes creates the agentic-transcript indexes that GORM struct
// tags cannot express — partial and partial-unique indexes whose predicates filter on
// message kind, approval status, or execution state. Enum literals are derived from the
// proto_models constants so a value renumber cannot silently drift these predicates.
// All statements are idempotent.
func chatAgenticSchemaStatements() []string {
	messages := common_persistence_models.TableNameChatMessages
	toolCalls := common_persistence_models.TableNameChatMessageToolCalls
	sessions := common_persistence_models.TableNameChatAgentSessions
	runs := common_persistence_models.TableNameChatAgentRuns
	inferenceCalls := common_persistence_models.TableNameChatInferenceCalls

	kindStandard := int(proto_models.MessageKind_MESSAGE_KIND_STANDARD)
	kindToolCall := int(proto_models.MessageKind_MESSAGE_KIND_TOOL_CALL)
	kindToolResult := int(proto_models.MessageKind_MESSAGE_KIND_TOOL_RESULT)
	kindProviderState := int(proto_models.MessageKind_MESSAGE_KIND_PROVIDER_STATE)
	approvalPending := int(proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_PENDING)
	approvalApproved := int(proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_APPROVED)
	approvalUnspecified := int(proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_UNSPECIFIED)
	approvalNotRequired := int(proto_models.ToolCallApprovalStatus_TOOL_CALL_APPROVAL_STATUS_NOT_REQUIRED)
	execPendingExecution := int(proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_PENDING_EXECUTION)
	execExecuting := int(proto_models.ToolCallExecutionState_TOOL_CALL_EXECUTION_STATE_EXECUTING)

	sessionStatusActive := int(common_persistence_models.ChatAgentSessionStatusActive)
	runStatusRunning := int(proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RUNNING)
	runStatusRetryable := int(proto_models.ChatAgentRunStatus_CHAT_AGENT_RUN_STATUS_RETRYABLE)

	statements := []string{
		// Organization usage listings are newest-first. Keep organization_id out of metrics labels;
		// the durable ledger is the high-cardinality reporting source.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_inference_calls_organization_created ON %s (organization_id, created_at DESC)", inferenceCalls),
		// Failed/pre-message calls have no message_id. Create the sparse replacement
		// for sparse lookup coverage.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_inference_calls_message_present ON %s (message_id) WHERE message_id IS NOT NULL", inferenceCalls),

		// Compaction/thread lookup stays on chat_messages: GetLatestCompaction scans by channel +
		// thread scope + kind, ordered by id desc. kind/parent_message_id remain on the hot table.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_messages_channel_parent_kind_id ON %s (channel_id, parent_message_id, kind, id)", messages),

		// One assistant turn per (agent_run_id, agent_step_index). Run-tagged STANDARD rows are
		// always the bot's assistant text for a step (CreateTranscriptMessage rejects run-tagged
		// rows from any other sender), so two concurrent drives of the same run cannot both insert
		// a step's assistant message: the loser's insert raises a unique violation that
		// CreateTranscriptMessage resolves to the winner's row as an idempotent no-op.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_messages_run_step_assistant ON %s (agent_run_id, agent_step_index) WHERE kind = %d AND agent_run_id IS NOT NULL AND agent_step_index IS NOT NULL AND deleted_at IS NULL", messages, kindStandard),

		// The tool-call/result pairing + approval/execution indexes now live entirely on the
		// 1:1 side table (gocommon_chat_message_tool_calls), which carries the denormalized
		// kind / agent_run_id / channel_id so no join back to chat_messages is needed.
		//
		// One TOOL_CALL per (agent_run_id, tool_call_id). The side table is an append-only
		// transcript record with no soft-delete: a tool_call_id is never reused within a run, so
		// uniqueness is absolute per run (no deleted_at clause, unlike the old chat_messages index).
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_message_tool_calls_call_unique ON %s (agent_run_id, tool_call_id) WHERE kind = %d", toolCalls, kindToolCall),
		// One TOOL_RESULT per (agent_run_id, tool_call_id). Same partial-unique shape; the
		// auto-written denial/expiry result goes through the same idempotent path.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_message_tool_calls_result_unique ON %s (agent_run_id, tool_call_id) WHERE kind = %d", toolCalls, kindToolResult),
		// Exactly one opaque provider-state commit marker per generated run step.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_messages_run_step_provider_state ON %s (agent_run_id, agent_step_index) WHERE kind = %d AND agent_run_id IS NOT NULL AND agent_step_index IS NOT NULL AND deleted_at IS NULL", messages, kindProviderState),

		// Pending-approval expiry sweep. The message-id tie-breaker supports stable keyset paging past
		// failing head rows; NULL deadlines have no timed expiry.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_pending_expiry ON %s (approval_expires_at, message_id) INCLUDE (channel_id, agent_run_id, tool_call_id) WHERE kind = %d AND approval_status = %d AND approval_expires_at IS NOT NULL", toolCalls, kindToolCall, approvalPending),

		// Claimable approved work: approved + pending execution, scanned per channel.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_claimable_work ON %s (channel_id, message_id) WHERE approval_status = %d AND execution_state = %d", toolCalls, approvalApproved, execPendingExecution),
		// Retryable inline work: only inline calls that failed once on the live path are scanned.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_inline_retry_work ON %s (channel_id, message_id) WHERE kind = %d AND approval_status IN (%d, %d) AND execution_state = %d AND last_execution_error IS NOT NULL", toolCalls, kindToolCall, approvalUnspecified, approvalNotRequired, execPendingExecution),
		// Crash recovery for a fresh inline call whose live owner disappeared before it could write a
		// result or failure settlement. The query applies the moving created_at cutoff; this partial
		// index bounds that age scan to inline PENDING_EXECUTION rows.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_inline_fresh_recovery ON %s (created_at, channel_id, message_id) WHERE kind = %d AND approval_status IN (%d, %d) AND execution_state = %d AND last_execution_error IS NULL", toolCalls, kindToolCall, approvalUnspecified, approvalNotRequired, execPendingExecution),
		// Expired-lease reclaim: in-flight executing rows whose lease has lapsed.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_execution_lease ON %s (execution_lease_expires_at) WHERE execution_state = %d", toolCalls, execExecuting),
		// Fair due-work ordering across both pending retries and expired leases. The attempt-count
		// prefix serves the bounded fresh/retry population scans; execution_not_before is the durable
		// retry clock shared by every replica.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_message_tool_calls_due_work_v2 ON %s (execution_attempt_count, (COALESCE(execution_not_before, execution_lease_expires_at, created_at)), channel_id, message_id) WHERE kind = %d AND execution_state IN (%d, %d)", toolCalls, kindToolCall, execPendingExecution, execExecuting),

		// Durable agent-run recovery scans only drivable rows. The lease claim remains the atomic
		// authority check; this partial index keeps the bounded due-run scan off terminal/waiting rows.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_agent_runs_drive_recovery ON %s (drive_not_before, drive_lease_expires_at, id) WHERE status IN (%d, %d)", runs, runStatusRunning, runStatusRetryable),
		// Thread-scoped status read: newest runs of one (channel, thread) location. The id tie-breaker
		// keeps a long main timeline from sorting every run it ever had.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_agent_runs_channel_parent_run ON %s (channel_id, parent_message_id, id DESC)", runs),

		// Location uniqueness is lifetime-wide, including archived sessions.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_sessions_location ON %s (channel_id, parent_message_id)", sessions),

		// Sessions store an explicit lifecycle status.
		addCheckConstraintOnce(sessions, "chk_chat_agent_sessions_status_specified", "status <> 0"),

		// Scoped GetOrCreateByContext: one active/default reusable session per
		// (organization_id, agent_type, user_id, context_id, context_entity_id). Ordinary creates may carry the
		// same context metadata with is_default_scoped=false and are intentionally unconstrained
		// by this index. NULL context fields must not participate in broad uniqueness matching,
		// and only ACTIVE sessions are covered so stale channels can be retired and replaced.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_sessions_default_scoped_context_active_org_v2 ON %s (organization_id, agent_type, user_id, context_id, context_entity_id) WHERE organization_id IS NOT NULL AND organization_id > 0 AND is_default_scoped IS TRUE AND context_id IS NOT NULL AND context_entity_id IS NOT NULL AND status = %d", sessions, sessionStatusActive),

		// Idempotent create with initial content: one request key per organization/creator/effective-
		// user/agent type. Only non-NULL keys with positive organization ownership participate.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_sessions_create_idempotency_org ON %s (organization_id, created_by_user_id, user_id, agent_type, create_idempotency_key) WHERE organization_id IS NOT NULL AND organization_id > 0 AND create_idempotency_key IS NOT NULL", sessions),

		// Delegation: children of one parent session. Only child rows carry a parent, so the
		// partial index stays as small as the delegation graph. The trailing id lets a
		// ListDelegatedChildren keyset page stop after its LIMIT instead of sorting every child.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_agent_sessions_parent_id ON %s (parent_agent_session_id, id) WHERE parent_agent_session_id IS NOT NULL", sessions),
		// Delegation: one child per (parent, create key). The org-scoped index above skips
		// organization-less parents and includes agent_type, so it cannot back child replay alone.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_sessions_parent_create_idempotency ON %s (parent_agent_session_id, create_idempotency_key) WHERE parent_agent_session_id IS NOT NULL", sessions),
		// Delegation: parked graph roots, the only rows ListUnparkableChatAgentGraphs scans.
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_chat_agent_sessions_parked_roots ON %s (id) WHERE parked_revision IS NOT NULL", sessions),
		// Delegation: a child is replaced at most once. ReplaceDelegatedChild checks it under the
		// channel lock; this is the backstop and the lookup GetChatAgentSessionReplacing uses.
		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_sessions_replaces_child ON %s (replaces_child_session_id) WHERE replaces_child_session_id IS NOT NULL", sessions),

		// Delegation: each terminal run creates at most one wake per destination session. A child
		// can wake its parent and also drain an unconsumed instruction into its own follow-up.

		fmt.Sprintf("CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_agent_runs_woken_by_session ON %s (woken_by_agent_run_id, agent_session_id) WHERE woken_by_agent_run_id IS NOT NULL", runs),
	}
	return statements
}

func (h *CommonPersistenceHandler[U]) ensureChatAgenticIndexes(db *gorm.DB) error {
	for _, statement := range chatAgenticSchemaStatements() {
		if err := executeSharedDDL(db, statement); err != nil {
			return err
		}
	}
	return nil
}

// initMinioClient executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) initMinioClient(v config.PersistenceView) error {
	storage := h.objectStorage()
	if err := storage.initMinioClient(v); err != nil {
		return err
	}
	h.minioClient, h.minioBucket, h.minioPublicBucket = storage.minioClient, storage.minioBucket, storage.minioPublicBucket
	h.minioAccessKey, h.minioSecretKey = storage.minioAccessKey, storage.minioSecretKey
	return nil
}

// anonymousReadOnlyBucketPolicy builds the bucket policy granting unauthenticated
// s3:GetObject on every object in bucket. MinIO's canned "download" policy is
// deliberately not used because it also grants s3:ListBucket, which would expose
// the object namespace.
// Parameters:
//   - bucket: string name of the bucket the policy applies to.
//
// Returns:
//   - result1: string containing the JSON policy document.
func anonymousReadOnlyBucketPolicy(bucket string) string {
	return fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":["*"]},"Action":["s3:GetObject"],"Resource":["arn:aws:s3:::%s/*"]}]}`, bucket)
}

// Close executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) Close() error {
	h.lifecycle.closeOnce.Do(func() {
		if h.logger != nil {
			h.logger.Named(loggerName).Info("Closing persistence handler", zap.String(logkeys.Operation, "close_persistence_handler"))
		}
		if h.cancel != nil {
			h.cancel()
		}
		h.lifecycle.wait.Wait()
		if h.databaseOwnership == ownedDatabase {
			h.lifecycle.closeErr = h.closeDatabase()
		}
		if h.lifecycle.closeErr != nil {
			h.lifecycle.closeErr = appLogger.WithErrorLevel(h.lifecycle.closeErr, zapcore.WarnLevel,
				zap.String(logkeys.Operation, "close_persistence_handler"),
				zap.String(logkeys.Fallback, "return_cleanup_error_to_caller"))
		}
	})
	return h.lifecycle.closeErr
}

// DB returns a GORM session bound to ctx. It joins the ambient transaction when
// SRouter has stored one in ctx and otherwise uses the base connection pool.
func (h *CommonPersistenceHandler[U]) DB(ctx context.Context) *gorm.DB {
	h.applyPoolPolicy()
	db := h.db
	if tx, ok := scontext.GetTransaction(ctx); ok && tx != nil {
		db = tx.GetDB()
	}
	if db == nil {
		return nil
	}
	return db.WithContext(ctx)
}

// BaseDB returns a GORM session bound to ctx while deliberately bypassing any
// ambient transaction. Prefer DB for ordinary application repository work.
func (h *CommonPersistenceHandler[U]) BaseDB(ctx context.Context) *gorm.DB {
	h.applyPoolPolicy()
	if h.db == nil {
		return nil
	}
	return h.db.WithContext(ctx)
}

// transaction runs fn in one transaction of DB(ctx) through transactionWithHooks, sampling the
// post-commit hook budget from this handler's configuration.
func (h *CommonPersistenceHandler[U]) transaction(ctx context.Context, fn func(txCtx context.Context, tx *gorm.DB) error) error {
	return transactionWithHooks[U](ctx, h.DB(ctx), h.postCommitHookTimeout, h.logger, fn)
}

// getDBFromContext executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) getDBFromContext(ctx context.Context) *gorm.DB {
	return h.DB(ctx)
}

// User-related methods

// SetUserCacheInvalidator executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) SetUserCacheInvalidator(invalidator func(userID uint64) error) {
	h.userCacheInvalidator = invalidator
}

// invalidateUserCache evicts the user from the service-layer cache. When the
// context carries a transaction with post-commit hook support, the eviction is
// deferred until after the commit: evicting while the update is still
// uncommitted lets a concurrent cache miss re-read the pre-commit row and
// re-cache stale state. Without such a transaction (e.g. the caller manages
// its own tx and commits before returning), the eviction runs immediately.
func (h *CommonPersistenceHandler[U]) invalidateUserCache(ctx context.Context, userID uint64) {
	if h.userCacheInvalidator == nil {
		return
	}
	if AddPostCommitHookFromContext(ctx, func(hookCtx context.Context) error {
		h.invalidateUserCacheNow(hookCtx, userID)
		return nil
	}) {
		return
	}
	h.invalidateUserCacheNow(ctx, userID)
}

// invalidateUserCacheNow performs the actual cache eviction via the configured
// invalidator callback.
func (h *CommonPersistenceHandler[U]) invalidateUserCacheNow(ctx context.Context, userID uint64) {
	if h.userCacheInvalidator != nil {
		if err := h.userCacheInvalidator(userID); err != nil {
			// Cache eviction is best-effort and the database mutation already
			// succeeded. Surface the recoverable stale-cache risk without claiming
			// the request itself failed.
			appLogger.For(ctx, h.logger, loggerName).Warn("Failed to invalidate user cache",
				zap.String(logkeys.Operation, "invalidate_user_cache"),
				zap.Uint64(appLogger.KeyUserID, userID),
				zap.String(logkeys.Fallback, "allow_cache_entry_to_expire"),
				zap.Error(err))
		}
	}
}

// --- Attachment Methods ---

var (
	ErrAttachmentNotFound = errors.New("attachment not found")

	// ErrInvalidAttachmentStatus is returned when an unrecognized attachment status is provided.
	ErrInvalidAttachmentStatus = errors.New("invalid attachment status")
)

// startDBHealthCheck executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) startDBHealthCheck(ctx context.Context, db *sql.DB) {
	logger := appLogger.For(ctx, h.logger, loggerName)
	interval := h.runtime().DBHealthCheckInterval()

	ticker := time.NewTicker(monitorPollInterval(interval))
	lastRun := time.Now()
	defer ticker.Stop()

	logger.Info("Starting database health checker",
		zap.String(logkeys.Operation, "database_health_check"),
		appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval))
	healthy := true
	for {
		select {
		case <-ctx.Done():
			if ce := logger.Check(zap.DebugLevel, "Stopping database health checker"); ce != nil {
				ce.Write(
					zap.String(logkeys.Operation, "database_health_check"),
					zap.Error(ctx.Err()),
				)
			}
			return
		case <-ticker.C:
			interval = h.runtime().DBHealthCheckInterval()
			ticker.Reset(monitorPollInterval(interval))
			if interval <= 0 || time.Since(lastRun) < interval {
				continue
			}
			lastRun = time.Now()
			h.applyPoolPolicy()
			// Create timeout context for health check
			healthCtx, cancel := context.WithTimeout(ctx, interval/4)
			err := db.PingContext(healthCtx) // Use PingContext for cancellable ping
			if err != nil {
				if ctx.Err() != nil {
					cancel()
					if ce := logger.Check(zap.DebugLevel, "Database health check stopped"); ce != nil {
						ce.Write(
							zap.String(logkeys.Operation, "database_health_check"),
							zap.Error(ctx.Err()),
						)
					}
					return
				}
				if healthy {
					logger.Warn("Database health check failed",
						zap.String(logkeys.Operation, "database_health_check"),
						appLogger.DurationSeconds(appLogger.KeyIntervalSeconds, interval),
						zap.String(logkeys.Fallback, "continue_health_monitoring"),
						zap.Error(err))
				} else {
					if ce := logger.Check(zap.DebugLevel, "Database health check still failing"); ce != nil {
						ce.Write(
							zap.String(logkeys.Operation, "database_health_check"),
							zap.Error(err),
						)
					}
				}
				healthy = false
			} else {
				if !healthy {
					logger.Info("Database health check recovered",
						zap.String(logkeys.Operation, "database_health_check"))
				} else {
					if ce := logger.Check(zap.DebugLevel, "Database health check passed"); ce != nil {
						ce.Write(
							zap.String(logkeys.Operation, "database_health_check"),
						)
					}
				}
				healthy = true
			}
			cancel() // Cancel timeout context
		}
	}
}

// ============================================================================
// ADMIN METHODS MOVED TO admin_persistence.go
// ============================================================================

// All admin persistence methods have been moved to admin_persistence.go for
// better code organization and maintainability. This includes:
// - User management methods (AdminListUsers, AdminGetUser, AdminUpdateUserBanStatus, etc.)
// - Contact message methods (AdminListContactMessages, AdminGetContactMessageDetails, etc.)
// - Attachment management methods (AdminListAttachments, AdminGetAttachmentDetails, etc.)
// - Analytics methods (AdminGetUserAnalytics, AdminGetStorageAnalytics)
// - Token management methods (AdminListTokens, AdminRevokeToken)
// - System statistics methods (AdminGetSystemStats, AdminGetSystemHealth)
// - Helper conversion methods (convertToAdminUserDetails, convertToAdminContactMessage, etc.)

// ============================================================================
// PHONE VERIFICATION MANAGEMENT
// ============================================================================

// CreatePhoneVerification executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) CreatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error {
	if pv == nil {
		return appLogger.InvariantError("phone_verification_create_requires_model", nil,
			zap.String(logkeys.Operation, "create_phone_verification"),
			zap.String(logkeys.Expected, "non-nil phone verification"),
			zap.String(logkeys.Actual, "nil phone verification"))
	}
	db := h.getDBFromContext(ctx)
	result := db.Create(pv)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "create_phone_verification"),
			zap.Uint64(appLogger.KeyUserID, pv.UserID),
			zap.String(appLogger.KeyPhoneNumber, pv.PhoneNumber))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Phone verification created",
			zap.String(logkeys.Operation, "create_phone_verification"),
			zap.Uint64(appLogger.KeyVerificationID, pv.ID),
			zap.Uint64(appLogger.KeyUserID, pv.UserID),
			zap.String(appLogger.KeyPhoneNumber, pv.PhoneNumber))
	}
	return nil
}

// GetPhoneVerificationByUserID executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) GetPhoneVerificationByUserID(ctx context.Context, userID uint64) (*common_persistence_models.PhoneVerification, error) {
	var pv common_persistence_models.PhoneVerification
	db := h.getDBFromContext(ctx)
	result := db.Where("user_id = ? AND is_verified = ?", userID, false).Order("created_at DESC").First(&pv)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrPhoneVerificationNotFound
		}
		return nil, databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "get_phone_verification_by_user_id"),
			zap.Uint64(appLogger.KeyUserID, userID))
	}
	return &pv, nil
}

// UpdatePhoneVerification executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) UpdatePhoneVerification(ctx context.Context, pv *common_persistence_models.PhoneVerification) error {
	if pv == nil {
		return appLogger.InvariantError("phone_verification_update_requires_model", nil,
			zap.String(logkeys.Operation, "update_phone_verification"),
			zap.String(logkeys.Expected, "non-nil phone verification"),
			zap.String(logkeys.Actual, "nil phone verification"))
	}
	db := h.getDBFromContext(ctx)
	result := db.Save(pv)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "update_phone_verification"),
			zap.Uint64(appLogger.KeyVerificationID, pv.ID),
			zap.Uint64(appLogger.KeyUserID, pv.UserID))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Phone verification updated",
			zap.String(logkeys.Operation, "update_phone_verification"),
			zap.Uint64(appLogger.KeyVerificationID, pv.ID),
			zap.Uint64(appLogger.KeyUserID, pv.UserID),
			zap.Bool(appLogger.KeyValid, pv.IsVerified))
	}
	return nil
}

// DeletePhoneVerification executes its operation in the persistence flow between service handlers and the database layer.
func (h *CommonPersistenceHandler[U]) DeletePhoneVerification(ctx context.Context, id uint64) error {
	db := h.getDBFromContext(ctx)
	result := db.Delete(&common_persistence_models.PhoneVerification{}, id)
	if result.Error != nil {
		return databaseErrorWithFields(result.Error,
			zap.String(logkeys.Operation, "delete_phone_verification"),
			zap.Uint64(appLogger.KeyVerificationID, id))
	}

	if appLogger.DebugEnabled(h.logger) {
		appLogger.For(ctx, h.logger, loggerName).Debug("Phone verification deleted",
			zap.String(logkeys.Operation, "delete_phone_verification"),
			zap.Uint64(appLogger.KeyVerificationID, id))
	}
	return nil
}
