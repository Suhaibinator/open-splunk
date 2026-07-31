//go:build linux

package main

import "golang.org/x/sys/unix"

func renameProvisioningTokenNoReplace(
	directoryFD int,
	from string,
	to string,
) error {
	return unix.Renameat2(
		directoryFD,
		from,
		directoryFD,
		to,
		unix.RENAME_NOREPLACE,
	)
}
