package control

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// PostgresRestoreListCommand inspects the archive inherited as descriptor 3.
// The executable is resolved and pinned before the caller attaches ExtraFiles.
func PostgresRestoreListCommand(ctx context.Context) (*exec.Cmd, error) {
	return postgresCommand(ctx, true, "--list", "/dev/fd/3")
}

// PostgresRestoreAuthorityCommand reads only the recovery authority tables from
// the archive inherited as descriptor 3, without connecting to a database.
func PostgresRestoreAuthorityCommand(ctx context.Context) (*exec.Cmd, error) {
	return postgresCommand(ctx, true, "--data-only", "--file=-", "--table=server_key_state", "--table=ingest_visibility_state", "--table=open_splunk_schema_migrations", "/dev/fd/3")
}

// PostgreSQL commands have fixed executable names and separate argument vectors;
// neither database names nor archive paths are interpreted by a shell. Resolve
// PATH once and reject writable shared executables before pinning Cmd.Path.
func postgresCommand(ctx context.Context, restore bool, arguments ...string) (*exec.Cmd, error) {
	if ctx == nil {
		return nil, ErrInvalidArgument
	}
	var command *exec.Cmd
	if restore {
		command = exec.CommandContext(ctx, "pg_restore")
	} else {
		command = exec.CommandContext(ctx, "pg_dump")
	}
	if command.Err != nil {
		return nil, command.Err
	}
	path, err := filepath.EvalSymlinks(command.Path)
	if err != nil {
		return nil, err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("PostgreSQL executable must be a regular executable without shared write permissions")
	}
	for _, argument := range arguments {
		if strings.ContainsRune(argument, 0) || len(argument) > 16<<10 {
			return nil, ErrInvalidArgument
		}
	}
	command.Path = path
	command.Args = append(command.Args, arguments...)
	return command, nil
}
