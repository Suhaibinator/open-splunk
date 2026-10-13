//go:build !darwin && !linux && !freebsd && !netbsd && !openbsd

package request_logger

import (
	"errors"
	"fmt"
	"runtime"
)

type requestLogArtifacts struct{}

var errRequestLoggerUnsupportedPlatform = errors.New(
	"request logger is unsupported on this platform because secure file permissions are not implemented",
)

func requestLoggerPlatformError() error {
	return fmt.Errorf("%w: %s", errRequestLoggerUnsupportedPlatform, runtime.GOOS)
}

func prepareRequestLogArtifacts(_ string) (*requestLogArtifacts, error) {
	return nil, requestLoggerPlatformError()
}
func (*requestLogArtifacts) databasePath() string          { return "" }
func (*requestLogArtifacts) verifyDatabasePath() error     { return requestLoggerPlatformError() }
func (*requestLogArtifacts) captureCreatedSidecars() error { return nil }
func (*requestLogArtifacts) cleanup() error                { return nil }
func (*requestLogArtifacts) commit() error                 { return nil }
func (*requestLogArtifacts) release() error                { return nil }
