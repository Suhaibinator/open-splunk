package pgtest

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// SQLMockGormConfig returns the GORM configuration the scripted-SQL suites
// share. SkipDefaultTransaction matters most: without it every bare write is
// wrapped in an implicit BEGIN/COMMIT that each expectation script would then
// have to spell out. Suites whose subject *is* transaction handling pass their
// own config instead.
func SQLMockGormConfig() *gorm.Config {
	return &gorm.Config{
		Logger:                 gormlogger.Default.LogMode(gormlogger.Silent),
		SkipDefaultTransaction: true,
		DisableAutomaticPing:   true,
	}
}

// SQLMockOptions tunes a scripted-SQL connection. The zero value is the common
// case: the shared GORM config and sqlmock's own defaults.
type SQLMockOptions struct {
	// GormConfig overrides SQLMockGormConfig when non-nil. Pass a bare
	// &gorm.Config{} to keep GORM's implicit transactions observable.
	GormConfig *gorm.Config

	// QueryMatcher overrides sqlmock's default matcher when non-nil, e.g. to
	// capture every statement GORM emits rather than matching against it.
	QueryMatcher sqlmock.QueryMatcher

	// MonitorPings makes Ping() a scriptable expectation rather than a no-op.
	MonitorPings bool
}

// OpenSQLMock opens a GORM handle over a fresh sqlmock connection through the
// Postgres dialector, so the SQL under assertion is the SQL production emits.
// The underlying connection closes when the test finishes; a caller that asserts
// on unmet expectations should register that check in its own t.Cleanup, which
// runs first.
func OpenSQLMock(t *testing.T, options SQLMockOptions) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	// sqlmock's option type names an unexported struct, so the slice type is
	// reachable only through inference - hence sliceOf rather than a literal.
	sqlmockOptions := sliceOf(sqlmock.MonitorPingsOption(options.MonitorPings))
	if options.QueryMatcher != nil {
		sqlmockOptions = append(sqlmockOptions, sqlmock.QueryMatcherOption(options.QueryMatcher))
	}

	sqlDB, mock, err := sqlmock.New(sqlmockOptions...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	gormConfig := options.GormConfig
	if gormConfig == nil {
		gormConfig = SQLMockGormConfig()
	}
	gormDB, err := gorm.Open(
		postgres.New(postgres.Config{Conn: sqlDB, PreferSimpleProtocol: true}),
		gormConfig,
	)
	require.NoError(t, err)

	return gormDB, mock
}

// NewDryRunDB returns a GORM handle that builds SQL without ever contacting a
// database, for tests that assert on the generated statement.
func NewDryRunDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(
		postgres.New(postgres.Config{
			DSN:                  dryRunDSN,
			PreferSimpleProtocol: true,
		}),
		&gorm.Config{DryRun: true, DisableAutomaticPing: true},
	)
	require.NoError(t, err)

	return db
}

// dryRunDSN is never dialed - DryRun mode stops before the connection is used -
// but the dialector still requires a syntactically valid DSN.
const dryRunDSN = "host=localhost user=test password=test dbname=test sslmode=disable"

// sliceOf collects items into a slice whose element type is inferred, which is
// the only way to build one whose element type cannot be written down.
func sliceOf[T any](items ...T) []T {
	return items
}
