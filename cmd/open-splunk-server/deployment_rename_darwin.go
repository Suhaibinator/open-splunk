//go:build darwin

package main

import "golang.org/x/sys/unix"

func renameProvisioningTokenNoReplace(
	directoryFD int,
	from string,
	to string,
) error {
	return unix.RenameatxNp(
		directoryFD,
		from,
		directoryFD,
		to,
		unix.RENAME_EXCL,
	)
}
