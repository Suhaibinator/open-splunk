package privatefs

import "os"

// ValidateNoExtendedACL rejects descriptor-visible access-control metadata
// that can grant access beyond ordinary POSIX mode bits.
func ValidateNoExtendedACL(file *os.File) error {
	return validateNoExtendedACL(file)
}
