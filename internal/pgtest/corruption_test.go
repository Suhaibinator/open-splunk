package pgtest

import "testing"

func TestCheckTablesKeepsUnselectedGuardsAndRestoresExactChecks(t *testing.T) {
	db := Open(t)
	ctx := t.Context()
	for _, query := range []string{
		"CREATE TABLE selected_check(value BIGINT CHECK(value>0))",
		"CREATE TABLE untouched_check(value BIGINT CHECK(value>0))",
	} {
		if _, err := db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := CheckTables(ctx, db, false, "selected_check"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO selected_check VALUES(-1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO untouched_check VALUES(-1)"); err == nil {
		t.Fatal("unselected table lost its check constraint")
	}
	if _, err := CheckTables(ctx, db, true, "selected_check"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO selected_check VALUES(-2)"); err == nil {
		t.Fatal("restored check did not guard new writes")
	}
	var old int
	if err := db.QueryRowContext(ctx, "SELECT value FROM selected_check").Scan(&old); err != nil || old != -1 {
		t.Fatalf("intentionally corrupt historical row changed: value=%d error=%v", old, err)
	}
}
