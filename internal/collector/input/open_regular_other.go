//go:build !darwin && !linux

package input

import "os"

// openFileForTailing falls back to the portable Open API. The manager still
// validates the opened descriptor, but platforms without a nonblocking open
// flag cannot fully close the Stat-to-FIFO swap race.
func openFileForTailing(path string) (*os.File, error) {
	return os.Open(path)
}
