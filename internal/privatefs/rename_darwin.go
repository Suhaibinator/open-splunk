//go:build darwin

package privatefs

import "golang.org/x/sys/unix"

func renameNoReplaceAt(fromDirectory int, from string, toDirectory int, to string) error {
	return unix.RenameatxNp(
		fromDirectory,
		from,
		toDirectory,
		to,
		unix.RENAME_EXCL,
	)
}
