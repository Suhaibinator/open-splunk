package pgtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	pgx "github.com/jackc/pgx/v5"
)

// SQLExecutor permits a dedicated connection or a caller-owned transaction.
type SQLExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type constraint struct{ table, name, definition string }

var corruptedConstraints = struct {
	sync.Mutex
	bySchema map[string]map[string][]constraint
}{bySchema: make(map[string]map[string][]constraint)}

// Checks toggles original CHECK constraints only in an isolated os_test_ schema.
// Restored constraints are NOT VALID so intentionally corrupt old rows remain
// readable, while every new write faces the exact original constraint.
func Checks(ctx context.Context, executor SQLExecutor, enabled bool) (sql.Result, error) {
	return toggleConstraints(ctx, executor, "c", enabled)
}
func ChecksExecutor(ctx context.Context, executor SQLExecutor, enabled bool) (sql.Result, error) {
	return Checks(ctx, executor, enabled)
}

// CheckTables suspends the exact CHECK constraints of the named test tables.
// The same table set must be supplied when restoring those constraints.
func CheckTables(ctx context.Context, executor SQLExecutor, enabled bool, tables ...string) (sql.Result, error) {
	if len(tables) == 0 {
		return nil, errors.New("test constraint table set is required")
	}
	selected := slices.Clone(tables)
	slices.Sort(selected)
	selected = slices.Compact(selected)
	for _, table := range selected {
		if table == "" || strings.ContainsAny(table, "\x00\x1f") {
			return nil, errors.New("invalid test constraint table name")
		}
	}
	return toggleSelectedConstraints(ctx, executor, "c", enabled, selected)
}
func DropForeignKeys(ctx context.Context, executor SQLExecutor) (sql.Result, error) {
	return toggleConstraints(ctx, executor, "f", false)
}
func RestoreForeignKeys(ctx context.Context, executor SQLExecutor) (sql.Result, error) {
	return toggleConstraints(ctx, executor, "f", true)
}

func forgetCorruptedConstraints(schema string) {
	corruptedConstraints.Lock()
	delete(corruptedConstraints.bySchema, schema)
	corruptedConstraints.Unlock()
}

func toggleConstraints(ctx context.Context, executor SQLExecutor, kind string, enabled bool) (sql.Result, error) {
	return toggleSelectedConstraints(ctx, executor, kind, enabled, nil)
}

func toggleSelectedConstraints(ctx context.Context, executor SQLExecutor, kind string, enabled bool, tables []string) (sql.Result, error) {
	var schema string
	if err := executor.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(schema, "os_test_") {
		return nil, errors.New("constraint corruption is restricted to an isolated PostgreSQL test schema")
	}
	corruptedConstraints.Lock()
	defer corruptedConstraints.Unlock()
	if corruptedConstraints.bySchema[schema] == nil {
		corruptedConstraints.bySchema[schema] = make(map[string][]constraint)
	}
	saved := corruptedConstraints.bySchema[schema]
	key := kind
	if len(tables) > 0 {
		key += "\x1f" + strings.Join(tables, "\x1f")
	}
	var result sql.Result
	if enabled {
		for _, c := range saved[key] {
			r, err := executor.ExecContext(ctx, "ALTER TABLE "+pgx.Identifier{schema, c.table}.Sanitize()+" ADD CONSTRAINT "+pgx.Identifier{c.name}.Sanitize()+" "+strings.TrimSuffix(c.definition, " NOT VALID")+" NOT VALID")
			if err != nil {
				return nil, err
			}
			result = r
		}
		delete(saved, key)
		return result, nil
	}
	if _, exists := saved[key]; exists {
		return nil, errors.New("test constraints are already disabled")
	}
	rows, err := executor.QueryContext(ctx, `SELECT r.relname,c.conname,pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=$1 AND c.contype::text=$2 ORDER BY r.relname,c.conname`, schema, kind)
	if err != nil {
		return nil, err
	}
	var constraints []constraint
	for rows.Next() {
		var c constraint
		if err := rows.Scan(&c.table, &c.name, &c.definition); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if len(tables) > 0 && !slices.Contains(tables, c.table) {
			continue
		}
		constraints = append(constraints, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, c := range constraints {
		r, err := executor.ExecContext(ctx, "ALTER TABLE "+pgx.Identifier{schema, c.table}.Sanitize()+" DROP CONSTRAINT "+pgx.Identifier{c.name}.Sanitize())
		if err != nil {
			return nil, fmt.Errorf("drop test constraint: %w", err)
		}
		result = r
	}
	saved[key] = constraints
	return result, nil
}
