// Package pgtest provides the shared ephemeral-Postgres rig used by the
// real-database ("realdb") test suites across the module.
//
// Every suite provisions a uniquely named throwaway database, migrates the
// handful of models it needs, and drops the database when the test finishes.
// Connection details come from GO_COMMON_TEST_PG_DSN, falling back to the local
// Compose mapping.
//
// Availability policy: when Postgres cannot be reached the suites skip, so a
// developer machine without a database stays green. CI sets
// GO_COMMON_REQUIRE_TEST_PG=true, which turns every one of those skips into a
// hard failure - an infrastructure regression must never present itself as a
// silently passing run.
package pgtest

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const (
	// DSNEnvVar names the environment variable holding the admin DSN used to
	// create and drop the throwaway databases.
	DSNEnvVar = "GO_COMMON_TEST_PG_DSN"

	// RequireEnvVar names the environment variable that promotes an
	// unreachable-Postgres skip into a test failure.
	RequireEnvVar = "GO_COMMON_REQUIRE_TEST_PG"

	// requireEnabledValue is the only value of RequireEnvVar that enables the
	// strict policy.
	requireEnabledValue = "true"

	// DefaultAdminDSN is the local Compose mapping used when DSNEnvVar is unset.
	DefaultAdminDSN = "host=127.0.0.1 port=5442 user=postgres password=postgres dbname=postgres sslmode=disable"

	// databaseField is the key/value DSN token naming the target database.
	databaseField = "dbname="

	createDatabaseStatement    = "CREATE DATABASE "
	dropDatabaseStatement      = "DROP DATABASE IF EXISTS "
	terminateBackendsStatement = "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = ?"
)

// Reporter is the subset of *testing.T that the availability gate needs. It
// exists so the skip-versus-fail decision can be exercised by a unit test rather
// than trusted by inspection.
type Reporter interface {
	Helper()
	Skipf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// RealDBRequired reports whether an unreachable Postgres must fail the test
// instead of skipping it.
func RealDBRequired() bool {
	return os.Getenv(RequireEnvVar) == requireEnabledValue
}

// Unavailable ends the current test because Postgres is not usable: it skips by
// default and fails when RealDBRequired reports that a database was promised.
// Both branches stop the calling goroutine, so callers never resume afterwards.
func Unavailable(t Reporter, format string, args ...any) {
	t.Helper()
	if RealDBRequired() {
		t.Fatalf(format, args...)
		return
	}
	t.Skipf(format, args...)
}

// AdminDSN returns the DSN of the database used to create and drop throwaway
// databases.
func AdminDSN() string {
	if dsn := os.Getenv(DSNEnvVar); dsn != "" {
		return dsn
	}
	return DefaultAdminDSN
}

// SwapDSNDatabase points a key/value DSN at databaseName. A DSN that carries no
// dbname token gets one appended: leaving it out would silently connect to
// libpq's default database and let a suite run against - and then try to drop -
// the shared admin database instead of its own throwaway copy.
func SwapDSNDatabase(dsn, databaseName string) string {
	fields := strings.Fields(dsn)
	for index, field := range fields {
		if strings.HasPrefix(field, databaseField) {
			fields[index] = databaseField + databaseName
			return strings.Join(fields, " ")
		}
	}
	return strings.Join(append(fields, databaseField+databaseName), " ")
}

// silentGormConfig keeps the noisy default GORM logger out of test output.
func silentGormConfig() *gorm.Config {
	return &gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)}
}

// New provisions a uniquely named throwaway database whose name starts with
// namePrefix and returns a GORM handle bound to it. The handle, every lingering
// backend, and the database itself are torn down when the test finishes.
//
// The test skips (or fails, see Unavailable) when Postgres is unreachable.
// Neither branch returns, so the nil result is unreachable in practice.
func New(t *testing.T, namePrefix string) *gorm.DB {
	t.Helper()

	adminDSN := AdminDSN()
	admin, err := gorm.Open(postgres.Open(adminDSN), silentGormConfig())
	if err != nil {
		Unavailable(t, "real-DB test cannot open admin connection (%v)", err)
		return nil
	}
	adminSQL, err := admin.DB()
	if err != nil {
		Unavailable(t, "real-DB test cannot access admin connection (%v)", err)
		return nil
	}
	if err := adminSQL.Ping(); err != nil {
		_ = adminSQL.Close()
		Unavailable(t, "real-DB test postgres unreachable (%v)", err)
		return nil
	}

	databaseName := uniqueDatabaseName(namePrefix)
	if err := admin.Exec(createDatabaseStatement + databaseName).Error; err != nil {
		_ = adminSQL.Close()
		Unavailable(t, "real-DB test cannot create database (%v)", err)
		return nil
	}

	// Registered before the database is opened so a failure to connect still
	// drops the database rather than leaking it into the shared server.
	var db *gorm.DB
	t.Cleanup(func() {
		if db != nil {
			if sqlDB, dbErr := db.DB(); dbErr == nil {
				_ = sqlDB.Close()
			}
		}
		_ = admin.Exec(terminateBackendsStatement, databaseName).Error
		_ = admin.Exec(dropDatabaseStatement + databaseName).Error
		_ = adminSQL.Close()
	})

	db, err = gorm.Open(postgres.Open(SwapDSNDatabase(adminDSN, databaseName)), silentGormConfig())
	require.NoError(t, err)
	return db
}

// MigrateWithoutForeignKeys creates the tables backing models with foreign-key
// constraints disabled, mirroring pass 1 of the production auto-migration. The
// realdb suites seed bare IDs without the full referenced graph, so enforcing the
// constraints here would reject their fixtures.
func MigrateWithoutForeignKeys(t *testing.T, db *gorm.DB, models ...any) {
	t.Helper()
	noFK := db.Session(&gorm.Session{NewDB: true})
	noFK.DisableForeignKeyConstraintWhenMigrating = true
	require.NoError(t, noFK.AutoMigrate(models...))
}

// databaseSequence disambiguates databases created by parallel tests in one
// process. Wall-clock nanoseconds alone are not unique: darwin reports only
// microsecond resolution, so parallel fixtures sharing a prefix collided.
var databaseSequence atomic.Uint64

// uniqueDatabaseName combines the prefix with the process ID, the Unix second,
// and a process-wide sequence so concurrent test processes and parallel tests
// within one process never race on CREATE DATABASE. PostgreSQL truncates
// identifiers at 63 bytes, so an overlong prefix is shortened to keep the
// distinguishing suffix intact.
func uniqueDatabaseName(prefix string) string {
	suffix := fmt.Sprintf("_%d_%d_%d", os.Getpid(), time.Now().Unix(), databaseSequence.Add(1))
	const maxIdentifier = 63
	if len(prefix)+len(suffix) > maxIdentifier {
		prefix = prefix[:maxIdentifier-len(suffix)]
	}
	return prefix + suffix
}
