package knowledgecatalog

import (
	"database/sql"
	"errors"
	"time"

	"modernc.org/sqlite"
)

// ResolutionDiagnostics contains payload-free evidence for a failed Resolve.
// Step comes only from fixed internal labels. Pool is sampled at failure;
// its wait deltas cover all users of the shared pool during this resolution,
// not just this request. Concurrent resolutions can report overlapping deltas.
type ResolutionDiagnostics struct {
	Step                  string
	Elapsed               time.Duration
	Attempts              int
	SQLiteCode            int
	HasSQLiteCode         bool
	Pool                  sql.DBStats
	PoolWaitCountDelta    int64
	PoolWaitDurationDelta time.Duration
}

type resolutionDiagnosticsError struct {
	cause       error
	diagnostics ResolutionDiagnostics
}

func (wrapped *resolutionDiagnosticsError) Error() string { return wrapped.cause.Error() }
func (wrapped *resolutionDiagnosticsError) Unwrap() error { return wrapped.cause }

// ResolutionDiagnosticsFromError returns a detached, definition-free snapshot.
// Error messages, SQL, object identities, and catalog contents are never copied.
func ResolutionDiagnosticsFromError(err error) (ResolutionDiagnostics, bool) {
	if wrapped, ok := errors.AsType[*resolutionDiagnosticsError](err); ok {
		return wrapped.diagnostics, true
	}
	return ResolutionDiagnostics{}, false
}

func resolutionErrorDiagnostics(err error) ResolutionDiagnostics {
	diagnostics, _ := ResolutionDiagnosticsFromError(err)
	if !diagnostics.HasSQLiteCode {
		if sqliteErr, ok := errors.AsType[*sqlite.Error](err); ok {
			diagnostics.SQLiteCode = sqliteErr.Code()
			diagnostics.HasSQLiteCode = true
		}
	}
	return diagnostics
}

// mapResolutionError preserves the driver's numeric code even when mapError
// replaces its cause with the context error. Error identity is left unchanged.
func mapResolutionError(ctxErr error, operation string, err error) error {
	return &resolutionDiagnosticsError{
		cause:       mapError(ctxErr, operation, err),
		diagnostics: resolutionErrorDiagnostics(err),
	}
}

func copyResolutionErrorMetadata(cause, source error) error {
	return &resolutionDiagnosticsError{
		cause:       copyCatalogErrorMetadata(cause, source),
		diagnostics: resolutionErrorDiagnostics(source),
	}
}
