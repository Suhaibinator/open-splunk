package main

import (
	"path/filepath"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"go.uber.org/zap"
)

// logControlDatabaseFilesystem reports, once per start, when the SQLite
// control-plane database lives on a network or FUSE filesystem.
//
// The control plane runs in WAL mode, which SQLite documents as unsafe on
// network filesystems: the -shm index needs coherent shared memory and the
// -wal file needs reliable byte-range locks, and NFS, SMB, and FUSE bridges
// provide neither. The result is silent corruption under concurrent writers or
// after an unclean stop.
//
// This is deliberately a logged error and not a startup refusal. Unlike the
// retained-search directory, whose publications fail on every search, a
// database on NFS is otherwise functional; refusing would turn a latent risk
// into an immediate outage with no in-place migration path for an operator
// who has been running that way. The message names the path and filesystem so
// the operator can move the state directory during a planned stop.
func logControlDatabaseFilesystem(
	logger *zap.Logger,
	describe func(string) (privatefs.Filesystem, error),
	databasePath string,
) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if describe == nil {
		describe = privatefs.DescribeFilesystem
	}
	directory, err := filepath.Abs(filepath.Dir(databasePath))
	if err != nil {
		logger.Warn("inspect control database filesystem", zap.Error(err))
		return
	}
	filesystem, err := describe(directory)
	if err != nil {
		logger.Warn("inspect control database filesystem", zap.String("path", directory), zap.Error(err))
		return
	}
	if !filesystem.Remote {
		return
	}
	logger.Error(
		"control database is on a network or FUSE filesystem; SQLite WAL mode is unsafe there and can corrupt the database. Move the state directory to a local filesystem (ext4, xfs, btrfs)",
		zap.String("path", directory),
		zap.String("filesystem", filesystem.Name),
	)
}
