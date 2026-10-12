package pgtest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// AllowOversizedIndexedValue removes only indexes and foreign keys containing
// the named column
// in an isolated corruption fixture. PostgreSQL's physical B-tree size bound
// would otherwise reject the forged multi-megabyte value before application
// clipping/redaction can be exercised. Dependent foreign keys are deliberately
// removed with a dropped unique index or constraint. Outgoing foreign keys
// containing the forged column are also removed; all other guards remain.
// The damaged fixture is disposable and must not be used as a healthy schema.
func AllowOversizedIndexedValue(ctx context.Context, executor SQLExecutor, table, column string) error {
	var schema string
	if err := executor.QueryRowContext(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return err
	}
	if !strings.HasPrefix(schema, "os_test_") {
		return errors.New("oversized corruption requires an isolated PostgreSQL test schema")
	}
	rows, err := executor.QueryContext(ctx, `SELECT irel.relname, coalesce(c.conname,'')
		FROM pg_index i
		JOIN pg_class r ON r.oid=i.indrelid
		JOIN pg_namespace n ON n.oid=r.relnamespace
		JOIN pg_class irel ON irel.oid=i.indexrelid
		JOIN pg_attribute a ON a.attrelid=r.oid AND a.attnum=ANY(i.indkey)
		LEFT JOIN pg_constraint c ON c.conindid=i.indexrelid AND c.contype IN ('p','u','x')
		WHERE n.nspname=$1 AND r.relname=$2 AND a.attname=$3
		UNION ALL
		SELECT '', c.conname
		FROM pg_constraint c
		JOIN pg_class r ON r.oid=c.conrelid
		JOIN pg_namespace n ON n.oid=r.relnamespace
		JOIN pg_attribute a ON a.attrelid=r.oid AND a.attnum=ANY(c.conkey)
		WHERE n.nspname=$1 AND r.relname=$2 AND a.attname=$3 AND c.contype='f'
		ORDER BY 1,2`, schema, table, column)
	if err != nil {
		return err
	}
	type indexedColumn struct{ index, constraint string }
	var indexes []indexedColumn
	for rows.Next() {
		var index indexedColumn
		if err := rows.Scan(&index.index, &index.constraint); err != nil {
			_ = rows.Close()
			return err
		}
		indexes = append(indexes, index)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(indexes) == 0 {
		return fmt.Errorf("no native fixture index covers %s.%s", table, column)
	}
	for _, index := range indexes {
		query := "DROP INDEX " + pgx.Identifier{schema, index.index}.Sanitize() + " CASCADE"
		if index.constraint != "" {
			query = "ALTER TABLE " + pgx.Identifier{schema, table}.Sanitize() +
				" DROP CONSTRAINT " + pgx.Identifier{index.constraint}.Sanitize() + " CASCADE"
		}
		if _, err := executor.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("remove oversized corruption index: %w", err)
		}
	}
	return nil
}
