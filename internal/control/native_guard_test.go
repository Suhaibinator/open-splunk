package control

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func requireNativeControlGuard(t *testing.T, err error, message string) {
	t.Helper()
	var postgres *pgconn.PgError
	if !errors.As(err, &postgres) || postgres.Code != "23514" || !strings.Contains(postgres.Message, message) {
		t.Fatalf("native PostgreSQL guard %q did not reject mutation: %v", message, err)
	}
}
