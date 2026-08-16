//go:build darwin || linux

package input

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestOpenFileForTailingDoesNotBlockOnFIFO(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "swapped.log")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("Mkfifo: %v", err)
	}

	type result struct {
		file *os.File
		err  error
	}
	opened := make(chan result, 1)
	go func() {
		file, err := openFileForTailing(path)
		opened <- result{file: file, err: err}
	}()

	select {
	case got := <-opened:
		if got.err != nil {
			t.Fatalf("open FIFO with nonblocking descriptor: %v", got.err)
		}
		defer got.file.Close()
		info, err := got.file.Stat()
		if err != nil {
			t.Fatalf("stat opened FIFO: %v", err)
		}
		if info.Mode().IsRegular() {
			t.Fatalf("opened FIFO reported regular mode %v", info.Mode())
		}
	case <-time.After(time.Second):
		t.Fatal("openFileForTailing blocked on FIFO without a writer")
	}
}
