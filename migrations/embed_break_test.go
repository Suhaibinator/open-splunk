package migrations

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestServerAppearanceMigrationDigestPinsTheExactFileBytes proves the 0011
// ledger entry is a real deployment guard: the pinned digest equals the
// SHA-256 of the file on disk (cross-checked against `shasum -a 256` when
// available), the embedded copy is byte-identical, and any edit at all —
// even a trailing newline or a reordered enum member — changes the digest so
// TestEmbeddedMigrationSetsAreContiguousAndHaveOneBaseline would fail.
func TestServerAppearanceMigrationDigestPinsTheExactFileBytes(t *testing.T) {
	t.Parallel()
	const name = "0011_server_appearance_settings.sql"
	pinned, ok := embeddedMigrationSHA256["SQLite"][name]
	if !ok {
		t.Fatalf("%s has no pinned digest", name)
	}
	onDisk, err := os.ReadFile(filepath.Join("sqlite", name))
	if err != nil {
		t.Fatal(err)
	}
	embedded, err := fs.ReadFile(SQLite(), name)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(onDisk, embedded) {
		t.Fatal("embedded 0011 differs from the file on disk")
	}
	digest := func(contents []byte) string {
		sum := sha256.Sum256(contents)
		return hex.EncodeToString(sum[:])
	}
	if got := digest(onDisk); got != pinned {
		t.Fatalf("sha256(%s) = %s, pinned %s", name, got, pinned)
	}
	if shasum, err := exec.LookPath("shasum"); err == nil {
		output, err := exec.CommandContext(t.Context(), shasum, "-a", "256", filepath.Join("sqlite", name)).Output()
		if err != nil {
			t.Fatalf("shasum -a 256: %v", err)
		}
		if fields := strings.Fields(string(output)); len(fields) < 1 || fields[0] != pinned {
			t.Fatalf("shasum -a 256 = %q, pinned %s", strings.TrimSpace(string(output)), pinned)
		}
	}
	for label, altered := range map[string][]byte{
		"trailing newline":      append(append([]byte{}, onDisk...), '\n'),
		"no trailing newline":   bytes.TrimRight(onDisk, "\n"),
		"extra palette":         bytes.Replace(onDisk, []byte("'terminal'"), []byte("'terminal', 'sepia'"), 1),
		"reordered enum":        bytes.Replace(onDisk, []byte("'classic', 'ocean'"), []byte("'ocean', 'classic'"), 1),
		"relaxed version check": bytes.Replace(onDisk, []byte("version BETWEEN 1"), []byte("version BETWEEN 0"), 1),
		"dropped strict":        bytes.Replace(onDisk, []byte("STRICT, WITHOUT ROWID"), []byte("WITHOUT ROWID"), 1),
		"tab indentation":       bytes.ReplaceAll(onDisk, []byte("    "), []byte("\t")),
	} {
		if bytes.Equal(altered, onDisk) {
			t.Fatalf("%s: alteration did not change the file; the probe is wrong", label)
		}
		if got := digest(altered); got == pinned {
			t.Fatalf("%s: altered file still matches the pinned digest", label)
		}
	}
	for _, clause := range []string{
		"palette IN ('classic', 'ocean', 'ember', 'graphite', 'glass', 'terminal')",
		"version BETWEEN 1 AND 9223372036854775807",
		"updated_at_unix_micro BETWEEN 1 AND 253402300799999999",
		"singleton_id = 1",
		"STRICT, WITHOUT ROWID",
		"COLLATE BINARY",
	} {
		if !strings.Contains(string(onDisk), clause) {
			t.Fatalf("0011 lost the clause %q", clause)
		}
	}
}
