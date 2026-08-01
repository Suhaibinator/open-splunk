//go:build !darwin && !linux

package collector

import (
	"fmt"
	"os"
	"runtime"
)

func validateCollectorFileOwner(os.FileInfo) error {
	return fmt.Errorf("collector filesystem ownership validation is unsupported on %s", runtime.GOOS)
}

func validateCollectorIdentityLinkCount(os.FileInfo) error {
	return fmt.Errorf("collector filesystem link validation is unsupported on %s", runtime.GOOS)
}
