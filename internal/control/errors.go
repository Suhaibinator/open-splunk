package control

import "errors"

var (
	// ErrInvalidArgument means an input cannot be represented safely by the
	// control-plane schema.
	ErrInvalidArgument = errors.New("control: invalid argument")
	// ErrNotFound means the requested control-plane object does not exist.
	ErrNotFound = errors.New("control: object not found")
	// ErrAlreadyExists means a unique control-plane object already exists.
	ErrAlreadyExists = errors.New("control: object already exists")
	// ErrVersionConflict means an optimistic update used a stale version.
	ErrVersionConflict = errors.New("control: version conflict")
	// ErrImmutableName means an update attempted to rename an index. Index
	// names are part of SPL and collector configuration and never change.
	ErrImmutableName = errors.New("control: index name is immutable")
	// ErrImmutableSlug means an update attempted to rename an app workspace.
	// App slugs are durable navigation identifiers and never change.
	ErrImmutableSlug = errors.New("control: app slug is immutable")
	// ErrDependencyConflict means a lifecycle change would invalidate another
	// control-plane object or a referenced object is not ready for deletion.
	ErrDependencyConflict = errors.New("control: dependent object conflict")
	// ErrCapacityExceeded means a bounded control-plane collection cannot
	// safely accept another object.
	ErrCapacityExceeded = errors.New("control: capacity exceeded")
	// ErrPageInvalidated means a catalog mutation occurred after a list cursor
	// was issued, so continuing it could omit or duplicate records.
	ErrPageInvalidated = errors.New("control: list page invalidated")
	// ErrMigrationDrift means the stored ledger is neither the exact embedded
	// sequence nor a narrowly recognized compatible release history.
	ErrMigrationDrift = errors.New("control: migration history is unsupported; provision a fresh state database")
	// ErrDatabaseTooNew means the database contains a migration beyond the
	// embedded sequence. In-place downgrade is intentionally unsupported.
	ErrDatabaseTooNew = errors.New("control: database migration history is unsupported; provision a fresh state database")
	// ErrDatabaseNotCurrent means the database is missing one or more migrations
	// from the exact migration set supplied by the caller. Read-only backup
	// tooling must not silently upgrade its source while verifying it.
	ErrDatabaseNotCurrent = errors.New("control: database migration history is not current")
)
