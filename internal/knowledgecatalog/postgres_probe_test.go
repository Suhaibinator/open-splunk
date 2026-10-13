package knowledgecatalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	pgx "github.com/jackc/pgx/v5"
)

// pgVisitCounter records physical predicate evaluations with a PostgreSQL
// sequence. Sequence advancement survives a canceled query or rollback, so the
// progress and cancellation contracts remain observable without a custom driver.
type pgVisitCounter struct {
	database *sql.DB
	sequence string
	test     testing.TB
}

func (counter *pgVisitCounter) Load() int64 {
	if counter.database == nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var value int64
	if err := counter.database.QueryRowContext(ctx, "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM "+pgx.Identifier{counter.sequence}.Sanitize()).Scan(&value); err != nil {
		counter.test.Fatalf("read native visit sequence: %v", err)
	}
	return value
}
func preparePostgresVisit(t *testing.T, database *control.DB, function, token string, counter *pgVisitCounter, delayed bool) {
	t.Helper()
	digest := sha256.Sum256([]byte(token))
	name := fmt.Sprintf("ko_probe_%x", digest[:16])
	if _, err := database.SQLDB().ExecContext(t.Context(), "CREATE SEQUENCE "+pgx.Identifier{name}.Sanitize()+" AS BIGINT"); err != nil {
		t.Fatal(err)
	}
	delay := ""
	if delayed {
		delay = "PERFORM pg_sleep(0.003);"
	}
	statement := "CREATE OR REPLACE FUNCTION " + pgx.Identifier{function}.Sanitize() + `(token TEXT, value ANYELEMENT) RETURNS BIGINT LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM nextval(quote_ident('ko_probe_' || substr(encode(sha256(convert_to(token,'UTF8')),'hex'),1,32))); ` + delay + ` RETURN 1; END $$`
	if _, err := database.SQLDB().ExecContext(t.Context(), statement); err != nil {
		t.Fatal(err)
	}
	counter.database, counter.sequence, counter.test = database.SQLDB(), name, t
}
func prepareBoundedPostgresProbe(t *testing.T, database *control.DB, token string, probe *boundedPreflightProbe) {
	preparePostgresVisit(t, database, boundedPreflightVisitFunction, token, &probe.visits, probe.cancel != nil)
	if probe.cancel == nil {
		return
	}
	done := make(chan struct{})
	stopped := make(chan struct{})
	t.Cleanup(func() { close(done); <-stopped })
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if probe.visits.Load() >= probe.cancelAt {
					probe.cancel()
					return
				}
			}
		}
	}()
}
