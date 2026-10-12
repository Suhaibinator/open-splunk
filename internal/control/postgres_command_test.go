package control

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPostgreSQLCommandPinsExecutableAndKeepsArgumentsSeparate(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "pg_restore")
	if err := os.WriteFile(path, []byte("fixture executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	command, err := PostgresRestoreListCommand(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if command.Path != path || !slices.Equal(command.Args[1:], []string{"--list", "/dev/fd/3"}) {
		t.Fatalf("unexpected pinned archive inspection command: %q %q", command.Path, command.Args)
	}
	t.Setenv("PATH", t.TempDir())
	if command.Path != path {
		t.Fatal("resolved executable changed with PATH")
	}
}

func TestPostgreSQLCommandRejectsSharedWritableExecutableAndInvalidArguments(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "pg_dump")
	if err := os.WriteFile(path, []byte("fixture executable"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory)
	if err := os.Chmod(path, 0o722); err != nil {
		t.Fatal(err)
	}
	if _, err := postgresCommand(t.Context(), false); err == nil {
		t.Fatal("shared writable executable accepted")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := postgresCommand(t.Context(), false, "invalid\x00argument"); err == nil {
		t.Fatal("NUL argument accepted")
	}
}
