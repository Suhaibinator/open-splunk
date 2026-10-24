package control

import (
	"errors"
	"slices"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQL SQLSTATE values classified by the control plane. They are stable
// protocol values documented in the PostgreSQL "Error Codes" appendix.
const (
	postgresUniqueViolation      = "23505"
	postgresCheckViolation       = "23514"
	postgresForeignKeyViolation  = "23503"
	postgresRaiseException       = "P0001"
	postgresSerializationFailure = "40001"
	postgresDeadlockDetected     = "40P01"
	postgresLockNotAvailable     = "55P03"
	postgresInvalidTextEncoding  = "22021"
)

// PostgresErrorCode returns the SQLSTATE of the first PostgreSQL server error
// in err's chain. Errors produced locally by the driver, such as a refused
// connection, carry no SQLSTATE and report false.
func PostgresErrorCode(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	return pgErr.Code, true
}

func hasPostgresErrorCode(err error, codes ...string) bool {
	code, ok := PostgresErrorCode(err)
	return ok && slices.Contains(codes, code)
}

// IsUniqueViolation reports whether err is PostgreSQL's unique_violation
// (23505), raised by a UNIQUE constraint, primary key, or unique index.
func IsUniqueViolation(err error) bool {
	return hasPostgresErrorCode(err, postgresUniqueViolation)
}

// IsCheckViolation reports whether err is PostgreSQL's check_violation
// (23514), raised by a CHECK constraint or domain check.
func IsCheckViolation(err error) bool {
	return hasPostgresErrorCode(err, postgresCheckViolation)
}

// IsForeignKeyViolation reports whether err is PostgreSQL's
// foreign_key_violation (23503).
func IsForeignKeyViolation(err error) bool {
	return hasPostgresErrorCode(err, postgresForeignKeyViolation)
}

// IsInvalidTextEncoding reports whether err is PostgreSQL's
// character_not_in_repertoire (22021): a text value or bind parameter held a
// NUL byte or was not valid UTF-8. PostgreSQL raises it for reads as well as
// writes, and before any CHECK runs. SQLite stored such text, or refused NUL
// through a CHECK constraint, so callers that mapped a CHECK failure to an
// invalid-argument error map this the same way.
func IsInvalidTextEncoding(err error) bool {
	return hasPostgresErrorCode(err, postgresInvalidTextEncoding)
}

// IsRaise reports whether err is a PL/pgSQL RAISE EXCEPTION carrying the
// default raise_exception SQLSTATE (P0001). Schema triggers use it for
// invariant failures whose exact message text callers match.
func IsRaise(err error) bool {
	return hasPostgresErrorCode(err, postgresRaiseException)
}

// RaiseMessage returns the primary message of a P0001 RAISE EXCEPTION exactly
// as the trigger produced it, without the driver's severity or SQLSTATE
// decoration. It reports false for every other error.
func RaiseMessage(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != postgresRaiseException {
		return "", false
	}
	return pgErr.Message, true
}

// PostgresConstraintName returns the constraint named by a PostgreSQL
// integrity error, letting callers map one specific constraint to a domain
// error without parsing message text. It reports false when the server did
// not name a constraint.
func PostgresConstraintName(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.ConstraintName == "" {
		return "", false
	}
	return pgErr.ConstraintName, true
}

// IsRetryable reports whether err aborted a transaction that can be retried
// from the beginning without changing its meaning: serialization_failure
// (40001) or deadlock_detected (40P01). WriteTx retries exactly these.
func IsRetryable(err error) bool {
	return hasPostgresErrorCode(err, postgresSerializationFailure, postgresDeadlockDetected)
}

// IsContention reports whether err is PostgreSQL's transient concurrency
// result: a retryable transaction abort or lock_not_available (55P03) from
// NOWAIT or lock_timeout. It is the PostgreSQL counterpart of
// IsDatabaseContention, so callers can preserve retryable dependency failures
// across package boundaries without treating integrity violations,
// corruption, or connectivity loss as temporary.
func IsContention(err error) bool {
	return hasPostgresErrorCode(
		err,
		postgresSerializationFailure,
		postgresDeadlockDetected,
		postgresLockNotAvailable,
	)
}
