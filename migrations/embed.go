// Package migrations embeds the database migrations shipped with the server.
package migrations

import (
	"embed"
	"io/fs"
)

// sqliteFiles is embedded in a package above the individual migration
// directories so a single server build can carry every database schema.
//
//go:embed sqlite/*.sql
var sqliteFiles embed.FS

//go:embed clickhouse/*.sql
var clickHouseFiles embed.FS

// postgresFiles embeds the whole directory rather than a *.sql glob so the
// tree builds before the first PostgreSQL migration lands. The runner reads
// only *.sql entries and ignores the directory README.
//
//go:embed postgres
var postgresFiles embed.FS

// SQLite returns a filesystem rooted at the ordered SQLite migrations.
func SQLite() fs.FS {
	root, err := fs.Sub(sqliteFiles, "sqlite")
	if err != nil {
		// The path is compile-time checked by go:embed. A panic here means the
		// generated binary was built incorrectly, not a runtime data problem.
		panic("embedded SQLite migrations are unavailable: " + err.Error())
	}
	return root
}

// ClickHouse returns a filesystem rooted at the ordered ClickHouse migrations.
func ClickHouse() fs.FS {
	root, err := fs.Sub(clickHouseFiles, "clickhouse")
	if err != nil {
		// The path is compile-time checked by go:embed. A panic here means the
		// generated binary was built incorrectly, not a runtime data problem.
		panic("embedded ClickHouse migrations are unavailable: " + err.Error())
	}
	return root
}

// Postgres returns a filesystem rooted at the ordered PostgreSQL control-plane
// migrations. Entries other than *.sql files, such as the directory README,
// are documentation rather than migrations.
func Postgres() fs.FS {
	root, err := fs.Sub(postgresFiles, "postgres")
	if err != nil {
		// The path is compile-time checked by go:embed. A panic here means the
		// generated binary was built incorrectly, not a runtime data problem.
		panic("embedded PostgreSQL migrations are unavailable: " + err.Error())
	}
	return root
}
