package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/Suhaibinator/open-splunk/migrations"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	defaultMaxOpenConns              = 16
	maximumControlTimestampUnixMicro = int64(253_402_300_799_999_999)
)

var ErrServerAlreadyRunning = errors.New("control: another server owns this PostgreSQL control plane")

// DB owns the PostgreSQL control-plane pool. SQLDB and GORMDB share that pool;
// consumers borrow it and must never close either handle.
type DB struct {
	sql                *sql.DB
	connectionConfig   *pgx.ConnConfig
	orm                *gorm.DB
	identity           string
	owner              *sql.Conn
	ownerPool          *sql.DB
	ownerKey           int64
	ownerCancel        context.CancelFunc
	ownershipLost      chan struct{}
	ownerWait          sync.WaitGroup
	closeOnce          sync.Once
	closeErr           error
	sharedAdmissionMu  sync.Mutex
	sharedAdmissionMap map[string]*AdmissionGate
}

// Open connects to PostgreSQL, exclusively leases this database/schema to one
// server, verifies/applies native migrations and validates the index catalog.
// dsn is a PostgreSQL URI or libpq connection string; file paths are rejected.
func Open(ctx context.Context, dsn string) (*DB, error) { return open(ctx, dsn, false) }

// OpenReadOnly verifies a PostgreSQL control plane without migrations or a
// server lease. Every transaction on its pool is read-only by default.
func OpenReadOnly(ctx context.Context, dsn string) (*DB, error) { return open(ctx, dsn, true) }

func open(ctx context.Context, dsn string, readOnly bool) (*DB, error) {
	if ctx == nil || strings.TrimSpace(dsn) == "" || (!strings.Contains(dsn, "=") && !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://")) {
		return nil, fmt.Errorf("%w: a PostgreSQL connection string and context are required", ErrInvalidArgument)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid PostgreSQL connection string", ErrInvalidArgument)
	}
	cfg.RuntimeParams["application_name"] = "open-splunk-control"
	if readOnly {
		cfg.RuntimeParams["default_transaction_read_only"] = "on"
	}
	raw := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(func(_ context.Context, conn *pgx.Conn) error {
		conn.TypeMap().RegisterType(&pgtype.Type{Name: "int8", OID: pgtype.Int8OID, Codec: strictIntegerCodec{}})
		return nil
	}))
	raw.SetMaxOpenConns(defaultMaxOpenConns)
	raw.SetMaxIdleConns(defaultMaxOpenConns)
	database := &DB{sql: raw, connectionConfig: cfg, ownershipLost: make(chan struct{})}
	fail := func(cause error) (*DB, error) { return nil, errors.Join(cause, database.Close()) }
	if err := raw.PingContext(ctx); err != nil {
		return fail(fmt.Errorf("connect to PostgreSQL control plane: %w", err))
	}
	if err := raw.QueryRowContext(ctx, "SELECT current_database() || ':' || current_schema()").Scan(&database.identity); err != nil {
		return fail(fmt.Errorf("identify PostgreSQL control plane: %w", err))
	}
	database.identity = fmt.Sprintf("%s:%d/%s", cfg.Host, cfg.Port, database.identity)
	database.orm, err = configureGORM(raw)
	if err != nil {
		return fail(fmt.Errorf("configure PostgreSQL control plane: %w", err))
	}
	if !readOnly {
		if err := database.acquireOwner(ctx); err != nil {
			return fail(err)
		}
		if err := ApplyMigrations(ctx, raw, migrations.PostgreSQL()); err != nil {
			return fail(err)
		}
		if _, err := readIndexCatalogIntegrity(database.orm.WithContext(ctx)); err != nil {
			return fail(fmt.Errorf("audit index catalog: %w", err))
		}
		database.startOwnershipMonitor()
	}
	return database, nil
}

func configureGORM(raw *sql.DB) (*gorm.DB, error) {
	return gorm.Open(postgres.New(postgres.Config{Conn: raw}), &gorm.Config{DisableAutomaticPing: true, Logger: logger.Discard, SkipDefaultTransaction: true})
}

func (db *DB) acquireOwner(ctx context.Context) error {
	db.ownerPool = stdlib.OpenDB(*db.connectionConfig)
	db.ownerPool.SetMaxOpenConns(1)
	db.ownerPool.SetMaxIdleConns(1)
	conn, err := db.ownerPool.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire server ownership connection: %w", err)
	}
	db.owner = conn
	if err := conn.QueryRowContext(ctx, "SELECT hashtextextended('open-splunk:server:' || current_database() || ':' || current_schema(), 0)").Scan(&db.ownerKey); err != nil {
		return err
	}
	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", db.ownerKey).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire PostgreSQL server lease: %w", err)
	}
	if !acquired {
		return ErrServerAlreadyRunning
	}
	return nil
}

func (db *DB) startOwnershipMonitor() {
	ctx, cancel := context.WithCancel(context.Background())
	db.ownerCancel = cancel
	db.ownerWait.Go(func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				check, cancel := context.WithTimeout(ctx, 5*time.Second)
				err := db.owner.PingContext(check)
				cancel()
				if err != nil && ctx.Err() == nil {
					close(db.ownershipLost)
					return
				}
			}
		}
	})
}

// OwnershipLost closes if the pinned PostgreSQL lease connection is lost.
// The process must stop serving and exit when this channel closes.
func (db *DB) OwnershipLost() <-chan struct{} { return db.ownershipLost }
func (db *DB) SQLDB() *sql.DB {
	if db == nil {
		return nil
	}
	return db.sql
}
func (db *DB) GORMDB() *gorm.DB {
	if db == nil {
		return nil
	}
	return db.orm
}

// SameDatabase compares the canonical database/schema identity without exposing credentials.
func (db *DB) SameDatabase(other *DB) bool {
	return db != nil && other != nil && db.identity != "" && db.identity == other.identity
}

// IsDatabaseContention identifies PostgreSQL errors that can retry the entire
// transaction; a failed statement never retries inside an aborted transaction.
func IsDatabaseContention(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && (pg.Code == "40001" || pg.Code == "40P01" || pg.Code == "55P03")
}
func IsUniqueViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}
func IsConstraintViolation(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && strings.HasPrefix(pg.Code, "23")
}

func (db *DB) Close() error {
	if db == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		if db.ownerCancel != nil {
			db.ownerCancel()
		}
		db.ownerWait.Wait()
		if db.owner != nil {
			var unlockErr error
			select {
			case <-db.ownershipLost:
				// The broken session has already released its PostgreSQL locks.
			default:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				var released bool
				unlockErr = db.owner.QueryRowContext(ctx, "SELECT pg_advisory_unlock($1)", db.ownerKey).Scan(&released)
				cancel()
			}
			ownerErr := db.owner.Close()
			// Canceling an in-flight heartbeat may have closed the pinned
			// connection before this cleanup reaches it.
			if errors.Is(unlockErr, sql.ErrConnDone) {
				unlockErr = nil
			}
			if errors.Is(ownerErr, sql.ErrConnDone) {
				ownerErr = nil
			}
			db.closeErr = errors.Join(db.closeErr, unlockErr, ownerErr)
		}
		if db.ownerPool != nil {
			db.closeErr = errors.Join(db.closeErr, db.ownerPool.Close())
		}
		if db.sql != nil {
			db.closeErr = errors.Join(db.closeErr, db.sql.Close())
		}
	})
	return db.closeErr
}

// strictIntegerCodec prevents pgx's generic numeric wrapper from truncating a
// fractional Go float before PostgreSQL can enforce a BIGINT storage contract.
// Application integer fields and integral numeric values keep their native encoding.
type strictIntegerCodec struct{ pgtype.Int8Codec }
type rejectedIntegerEncoding struct{}

func (rejectedIntegerEncoding) Encode(any, []byte) ([]byte, error) {
	return nil, errors.New("fractional value cannot be encoded as PostgreSQL BIGINT")
}
func (codec strictIntegerCodec) PlanEncode(m *pgtype.Map, oid uint32, format int16, value any) pgtype.EncodePlan {
	var number float64
	var numeric bool
	switch v := value.(type) {
	case float64:
		number = v
		numeric = true
	case float32:
		number = float64(v)
		numeric = true
	case pgtype.Float64Valuer:
		f, err := v.Float64Value()
		if err == nil && f.Valid {
			number = f.Float64
			numeric = true
		}
	}
	if numeric && (math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number) {
		return rejectedIntegerEncoding{}
	}
	return codec.Int8Codec.PlanEncode(m, oid, format, value)
}
