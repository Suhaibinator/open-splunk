package knowledgecatalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"modernc.org/sqlite"
)

func TestResolutionDiagnosticsIdentifyMissingAppAndRemainDetached(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	scope := testResolutionScope("main")
	scope.AppID = "private-missing-app"
	_, err := resolver.Resolve(t.Context(), scope)
	if !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("Resolve(missing app) = %v, want ErrNotFound", err)
	}
	diagnostics, found := ResolutionDiagnosticsFromError(fmt.Errorf("outer wrapper: %w", err))
	if !found || diagnostics.Step != "authorize_app" || diagnostics.Attempts != 1 ||
		diagnostics.Elapsed <= 0 || diagnostics.HasSQLiteCode || diagnostics.Pool.MaxOpenConnections != database.SQLDB().Stats().MaxOpenConnections {
		t.Fatalf("missing-app diagnostics = %+v, found %v", diagnostics, found)
	}
	if strings.Contains(fmt.Sprint(diagnostics), scope.AppID) {
		t.Fatal("diagnostics retained the app identity")
	}
	diagnostics.Step = "caller-mutated"
	again, found := ResolutionDiagnosticsFromError(errors.Join(errors.New("unrelated"), err))
	if !found || again.Step != "authorize_app" {
		t.Fatalf("diagnostics accessor retained caller memory: %+v, found %v", again, found)
	}
	if _, found := ResolutionDiagnosticsFromError(nil); found {
		t.Fatal("nil error carried resolution diagnostics")
	}
}

func TestResolutionDiagnosticsRetainSQLiteCodeAcrossDeadlineMapping(t *testing.T) {
	database, store := newCatalogTestStore(t)
	_, sqliteErr := database.SQLDB().ExecContext(t.Context(), `SELECT * FROM private_resolution_missing_table`)
	if sqliteErr == nil {
		t.Fatal("SQLite missing-table probe succeeded")
	}
	concrete, ok := errors.AsType[*sqlite.Error](sqliteErr)
	if !ok {
		t.Fatalf("missing-table error type = %T, want SQLite error", sqliteErr)
	}
	resolver := mustTestResolver(t, store)
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		cause := withAuthorizedContext(
			withErrorDisposition(sqliteErr, ErrorDispositionDefinitiveRejection),
			AuthorizedContext{AppID: testApp},
		)
		return Resolution{}, mapResolutionError(context.DeadlineExceeded, "private operation", cause)
	}
	_, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, sqliteErr) {
		t.Fatalf("deadline mapping changed error identity: %v", err)
	}
	diagnostics, found := ResolutionDiagnosticsFromError(err)
	if !found || !diagnostics.HasSQLiteCode || diagnostics.SQLiteCode != concrete.Code() || diagnostics.Attempts != 1 {
		t.Fatalf("deadline diagnostics = %+v, found %v", diagnostics, found)
	}
	if strings.Contains(fmt.Sprint(diagnostics), "private") || strings.Contains(err.Error(), "private") {
		t.Fatal("deadline diagnostics exposed private error data")
	}
	requireCatalogDisposition(t, err, ErrorDispositionDefinitiveRejection)
	if authorized, found := AuthorizedContextFromError(err); !found || authorized.AppID != testApp {
		t.Fatalf("deadline lost authorized context: %+v, found %v", authorized, found)
	}
}

func TestResolutionDiagnosticsRetainBusyCodeDuringRetryCancellation(t *testing.T) {
	database, store := newCatalogTestStore(t)
	busyErr := resolverTestSQLiteBusyError(t, database.SQLDB())
	resolver := mustTestResolver(t, store)
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		return Resolution{}, busyErr
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	resolver.backoff = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}
	_, err := resolver.Resolve(ctx, testResolutionScope("main"))
	if !errors.Is(err, context.Canceled) || errors.Is(err, busyErr) {
		t.Fatalf("retry cancellation changed error identity: %v", err)
	}
	diagnostics, found := ResolutionDiagnosticsFromError(err)
	if !found || diagnostics.Step != "retry_backoff" || diagnostics.Attempts != 1 ||
		!diagnostics.HasSQLiteCode || diagnostics.SQLiteCode&0xff != 5 {
		t.Fatalf("retry cancellation diagnostics = %+v, found %v", diagnostics, found)
	}
}

func TestResolutionDiagnosticsRejectSQLiteCodeImpostor(t *testing.T) {
	_, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	cause := &resolverRetryCodeImpostor{code: 5}
	resolver.attempt = func(context.Context, normalizedResolutionScope) (Resolution, error) {
		return Resolution{}, cause
	}
	_, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if !errors.Is(err, cause) {
		t.Fatalf("Resolve(impostor) lost cause: %v", err)
	}
	diagnostics, found := ResolutionDiagnosticsFromError(err)
	if !found || diagnostics.HasSQLiteCode || diagnostics.Attempts != 1 {
		t.Fatalf("impostor diagnostics = %+v, found %v", diagnostics, found)
	}
}
