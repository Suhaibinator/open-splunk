// Package opensplunk exposes application assets shared by the server command.
package opensplunk

import (
	"embed"
	"io/fs"
)

// embeddedWebUI contains the output of the root Next.js static export.
// The supported release build runs `npm run build` before compiling Go.
//
//go:embed all:out
var embeddedWebUI embed.FS

// WebUI returns the embedded Next.js export rooted at its public directory.
func WebUI() (fs.FS, error) {
	return fs.Sub(embeddedWebUI, "out")
}
