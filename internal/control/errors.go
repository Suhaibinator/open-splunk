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
	// ErrMigrationDrift means an applied migration's name or contents no
	// longer match the version embedded in this binary.
	ErrMigrationDrift = errors.New("control: applied migration differs from embedded migration")
	// ErrDatabaseTooNew means the database was migrated by a newer binary.
	ErrDatabaseTooNew = errors.New("control: database schema is newer than this binary")
)
