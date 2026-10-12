// Package migrations embeds go-common's versioned SQL schema upgrades. Startup
// applies them with goose after AutoMigrate, so each script must also succeed on
// a fresh database that AutoMigrate already created in its final shape.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
