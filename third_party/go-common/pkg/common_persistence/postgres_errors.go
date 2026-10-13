package common_persistence

import (
	"errors"
	"slices"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

// postgresErrorState returns the SQLSTATE and constraint name of a PostgreSQL error raised by
// either supported driver (pgx, or lib/pq behind a borrowed/adopted pool), or empty strings.
//
// Constraint names survive only on untranslated driver errors: a borrowed/adopted pool opened with
// GORM's TranslateError replaces the driver error with gorm.ErrDuplicatedKey or
// gorm.ErrForeignKeyViolated, so isUniqueViolation still classifies it by code while
// isUniqueViolationOn cannot name the constraint and reports false.
func postgresErrorState(err error) (code, constraint string) {
	var pgxErr *pgconn.PgError
	if errors.As(err, &pgxErr) {
		return pgxErr.Code, pgxErr.ConstraintName
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code), pqErr.Constraint
	}
	return "", ""
}

// isUniqueViolation reports whether a database error is a unique-constraint violation on any
// constraint. It accepts GORM's translated gorm.ErrDuplicatedKey as well as raw driver errors.
func isUniqueViolation(err error) bool {
	code, _ := postgresErrorState(err)
	return code == "23505" || errors.Is(err, gorm.ErrDuplicatedKey)
}

// isLockNotAvailable reports whether a NOWAIT lock request found the row already locked.
func isLockNotAvailable(err error) bool {
	code, _ := postgresErrorState(err)
	return code == "55P03"
}

// isUniqueViolationOn reports whether err is a unique-constraint violation of exactly one of the
// named constraints.
func isUniqueViolationOn(err error, constraints ...string) bool {
	code, constraint := postgresErrorState(err)
	return code == "23505" && slices.Contains(constraints, constraint)
}
