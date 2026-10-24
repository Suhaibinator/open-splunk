package control_test

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

type postgresClassification struct {
	unique, check, foreignKey, raise, retryable, contention bool
}

func classifyPostgresError(err error) postgresClassification {
	return postgresClassification{
		unique:     control.IsUniqueViolation(err),
		check:      control.IsCheckViolation(err),
		foreignKey: control.IsForeignKeyViolation(err),
		raise:      control.IsRaise(err),
		retryable:  control.IsRetryable(err),
		contention: control.IsContention(err),
	}
}

func TestPostgresErrorClassificationMatchesServerSQLStates(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	if _, err := db.SQLDB().ExecContext(t.Context(), `INSERT INTO probes (id, label) VALUES (1, 'existing')`); err != nil {
		t.Fatalf("seed probe: %v", err)
	}

	cases := []struct {
		name       string
		statement  string
		code       string
		constraint string
		want       postgresClassification
	}{
		{
			name:       "unique",
			statement:  `INSERT INTO probes (id, label) VALUES (2, 'existing')`,
			code:       "23505",
			constraint: "probes_label_unique",
			want:       postgresClassification{unique: true},
		},
		{
			name:       "check",
			statement:  `INSERT INTO probes (id, label) VALUES (3, 'forbidden')`,
			code:       "23514",
			constraint: "probes_label_allowed",
			want:       postgresClassification{check: true},
		},
		{
			name:       "foreign key",
			statement:  `INSERT INTO probe_children (id, probe_id) VALUES (1, 999)`,
			code:       "23503",
			constraint: "probe_children_probe_fk",
			want:       postgresClassification{foreignKey: true},
		},
		{
			name:      "raise",
			statement: `INSERT INTO probes (id, label) VALUES (7, 'frozen')`,
			code:      "P0001",
			want:      postgresClassification{raise: true},
		},
		{
			name:      "serialization failure",
			statement: `DO $$ BEGIN RAISE EXCEPTION 'forced' USING ERRCODE = '40001'; END $$`,
			code:      "40001",
			want:      postgresClassification{retryable: true, contention: true},
		},
		{
			name:      "deadlock",
			statement: `DO $$ BEGIN RAISE EXCEPTION 'forced' USING ERRCODE = '40P01'; END $$`,
			code:      "40P01",
			want:      postgresClassification{retryable: true, contention: true},
		},
		{
			name:      "lock not available",
			statement: `DO $$ BEGIN RAISE EXCEPTION 'forced' USING ERRCODE = '55P03'; END $$`,
			code:      "55P03",
			want:      postgresClassification{contention: true},
		},
		{
			name:      "not null",
			statement: `INSERT INTO probes (id, label) VALUES (4, NULL)`,
			code:      "23502",
			want:      postgresClassification{},
		},
		{
			name:      "syntax",
			statement: `SELEC 1`,
			code:      "42601",
			want:      postgresClassification{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := db.SQLDB().ExecContext(t.Context(), tc.statement)
			if err == nil {
				t.Fatalf("%s succeeded", tc.statement)
			}
			for label, candidate := range map[string]error{
				"direct":  err,
				"wrapped": fmt.Errorf("store layer: %w", fmt.Errorf("repository: %w", err)),
				"joined":  errors.Join(errors.New("rollback also failed"), err),
			} {
				code, ok := control.PostgresErrorCode(candidate)
				if !ok || code != tc.code {
					t.Fatalf("%s PostgresErrorCode() = %q, %v; want %q", label, code, ok, tc.code)
				}
				if got := classifyPostgresError(candidate); got != tc.want {
					t.Fatalf("%s classification = %+v, want %+v (err %v)", label, got, tc.want, candidate)
				}
				constraint, named := control.PostgresConstraintName(candidate)
				if constraint != tc.constraint || named != (tc.constraint != "") {
					t.Fatalf("%s PostgresConstraintName() = %q, %v; want %q", label, constraint, named, tc.constraint)
				}
			}
		})
	}
}

func TestPostgresRaiseMessageIsTheTriggerTextUnchanged(t *testing.T) {
	t.Parallel()
	db := newPostgresFixtureDB(t)
	ctx := t.Context()

	err := db.WriteTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO probes (id, label) VALUES (7, 'frozen')`)
		return err
	})
	message, ok := control.RaiseMessage(fmt.Errorf("persist probe: %w", err))
	if want := `probe 7 is frozen: "quoted" and 100% literal`; !ok || message != want {
		t.Fatalf("RaiseMessage() = %q, %v; want %q (err %v)", message, ok, want, err)
	}
	if _, ok := control.RaiseMessage(&pgconn.PgError{Code: "23505", Message: "duplicate"}); ok {
		t.Fatal("RaiseMessage() accepted a unique violation")
	}
}

func TestPostgresErrorClassificationIgnoresNonServerErrors(t *testing.T) {
	t.Parallel()
	for _, err := range []error{
		nil,
		errors.New("40001"),
		sql.ErrNoRows,
		control.ErrInvalidArgument,
		fmt.Errorf("deadlock detected: %w", errors.New("40P01")),
	} {
		if got := classifyPostgresError(err); got != (postgresClassification{}) {
			t.Fatalf("classification(%v) = %+v, want none", err, got)
		}
		if _, ok := control.PostgresErrorCode(err); ok {
			t.Fatalf("PostgresErrorCode(%v) reported a SQLSTATE", err)
		}
		if _, ok := control.RaiseMessage(err); ok {
			t.Fatalf("RaiseMessage(%v) reported a raise", err)
		}
	}
	// IsContention is independent of the SQLite classifier and vice versa, so
	// call sites can be swapped one package at a time.
	pgErr := &pgconn.PgError{Code: "40001"}
	if control.IsDatabaseContention(pgErr) || !control.IsContention(pgErr) {
		t.Fatal("SQLite and PostgreSQL contention classifiers overlap")
	}
}
