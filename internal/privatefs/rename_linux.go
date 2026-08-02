//go:build linux

package privatefs

import "golang.org/x/sys/unix"

func renameNoReplaceAt(fromDirectory int, from string, toDirectory int, to string) error {
	return unix.Renameat2(
		fromDirectory,
		from,
		toDirectory,
		to,
		unix.RENAME_NOREPLACE,
	)
}
