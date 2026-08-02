//go:build darwin || linux

package recoveryset

import (
	"context"
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/recoverycontract"
)

// DeleteAttestedArchiveOptions identifies one native archive that an operator
// explicitly attests should be destroyed. The confirmation must repeat the
// exact archive name to make the destructive boundary unambiguous.
type DeleteAttestedArchiveOptions struct {
	ArchiveRoot          string
	ArchiveName          string
	ConfirmedArchiveName string
	ArchiveOwnership     ArchiveOwnershipPolicy
}

// DeleteAttestedArchive destroys one exact canonical ClickHouse archive after
// proving the recovery root and archive still satisfy their ownership, mode,
// link-count, ACL, and pinned-path contracts. It cannot infer publication
// status: callers must stop ClickHouse and separately establish that the exact
// archive came from the failed attempt they intend to discard. An already
// absent archive is an idempotent success; an unsafe or changing object is
// never removed.
func DeleteAttestedArchive(
	ctx context.Context,
	options DeleteAttestedArchiveOptions,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("delete operator-attested ClickHouse recovery archive: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := controlbackup.ValidateExactAbsolutePath(
		"-archive-root",
		options.ArchiveRoot,
	); err != nil {
		return fmt.Errorf("delete operator-attested ClickHouse recovery archive: %w", err)
	}
	if !recoverycontract.ValidArchiveName(options.ArchiveName) {
		return errors.New(
			"delete operator-attested ClickHouse recovery archive: archive name must be exactly 32 lowercase hexadecimal characters followed by .tar.zst",
		)
	}
	if options.ConfirmedArchiveName != options.ArchiveName {
		return errors.New(
			"delete operator-attested ClickHouse recovery archive: confirmation must exactly match the archive name",
		)
	}
	if err := validateArchiveOwnershipPolicy(options.ArchiveOwnership); err != nil {
		return err
	}

	directory, err := openArchiveDirectory(
		options.ArchiveRoot,
		options.ArchiveOwnership,
	)
	if err != nil {
		return err
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, directory.close())
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	removed, err := directory.removeKnownArchive(options.ArchiveName)
	if err != nil {
		return fmt.Errorf("delete operator-attested ClickHouse recovery archive: %w", err)
	}
	// removeKnownArchive already syncs and proves absence after an unlink. An
	// absent retry still needs a directory barrier to resolve an earlier unlink
	// whose sync result was unknown to its caller.
	if !removed {
		if err := directory.sync(); err != nil {
			return fmt.Errorf("delete operator-attested ClickHouse recovery archive: %w", err)
		}
		exists, err := directory.pathExists(options.ArchiveName)
		if err != nil {
			return fmt.Errorf("delete operator-attested ClickHouse recovery archive: %w", err)
		}
		if exists {
			return errors.New(
				"delete operator-attested ClickHouse recovery archive: archive appeared during absent retry",
			)
		}
	}
	return nil
}
