//go:build darwin

package input

import (
	"math"
	"os"
	"syscall"
	"testing"
	"time"
)

type darwinStatFileInfo struct {
	stat *syscall.Stat_t
}

func (darwinStatFileInfo) Name() string       { return "test.log" }
func (darwinStatFileInfo) Size() int64        { return 0 }
func (darwinStatFileInfo) Mode() os.FileMode  { return 0 }
func (darwinStatFileInfo) ModTime() time.Time { return time.Time{} }
func (darwinStatFileInfo) IsDir() bool        { return false }
func (info darwinStatFileInfo) Sys() any      { return info.stat }

func TestStatDevInoPreservesHistoricalDarwinDeviceEncoding(t *testing.T) {
	t.Parallel()

	dev, ino, ok := statDevIno(darwinStatFileInfo{
		stat: &syscall.Stat_t{Dev: -1, Ino: 42},
	})
	if !ok || dev != math.MaxUint64 || ino != 42 {
		t.Fatalf("statDevIno() = (%d, %d, %t), want (%d, 42, true)", dev, ino, ok, uint64(math.MaxUint64))
	}
}
