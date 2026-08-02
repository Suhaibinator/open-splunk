//go:build !darwin && !linux

package privatefs

import "os"

func validateNoExtendedACL(*os.File) error {
	return nil
}
