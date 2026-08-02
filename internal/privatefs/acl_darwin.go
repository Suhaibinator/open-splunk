//go:build darwin

package privatefs

import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	darwinACLBufferBytes      = 4096
	darwinAttributeReference  = 8
	darwinFileSecurityHeader  = 44
	darwinFileSecurityMagic   = 0x012cc16d
	darwinFgetattrlistSyscall = unix.SYS_FGETATTRLIST //nolint:staticcheck
)

// validateNoExtendedACL is descriptor-based so an ACL-bearing replacement
// cannot win a path race after the caller has validated ordinary mode bits.
func validateNoExtendedACL(file *os.File) error {
	if file == nil {
		return errors.New("private filesystem ACL is unavailable")
	}
	attributes := unix.Attrlist{
		Bitmapcount: unix.ATTR_BIT_MAP_COUNT,
		Commonattr:  unix.ATTR_CMN_EXTENDED_SECURITY,
	}
	buffer := make([]byte, darwinACLBufferBytes)
	_, _, errno := unix.Syscall6(
		darwinFgetattrlistSyscall,
		file.Fd(),
		uintptr(unsafe.Pointer(&attributes)),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(len(buffer)),
		0,
		0,
	)
	runtime.KeepAlive(file)
	runtime.KeepAlive(&attributes)
	runtime.KeepAlive(buffer)
	if errno != 0 {
		return errors.New("inspect private filesystem extended ACL")
	}

	totalBytes := int(binary.NativeEndian.Uint32(buffer[:4]))
	if totalBytes < 4+darwinAttributeReference || totalBytes > len(buffer) {
		return errors.New("private filesystem extended ACL metadata is invalid")
	}
	referenceStart := 4
	rawOffset := binary.NativeEndian.Uint32(buffer[referenceStart : referenceStart+4])
	if rawOffset > math.MaxInt32 {
		return errors.New("private filesystem extended ACL metadata is invalid")
	}
	dataOffset := int(rawOffset)
	dataBytes := int(binary.NativeEndian.Uint32(
		buffer[referenceStart+4 : referenceStart+darwinAttributeReference],
	))
	if dataBytes == 0 {
		return nil
	}
	dataStart := referenceStart + dataOffset
	if dataOffset < darwinAttributeReference ||
		dataStart < referenceStart ||
		dataBytes < darwinFileSecurityHeader ||
		dataStart > totalBytes ||
		dataBytes > totalBytes-dataStart {
		return errors.New("private filesystem extended ACL metadata is invalid")
	}
	security := buffer[dataStart : dataStart+dataBytes]
	if binary.NativeEndian.Uint32(security[:4]) != darwinFileSecurityMagic {
		return errors.New("private filesystem extended ACL metadata is invalid")
	}
	entryCount := binary.NativeEndian.Uint32(security[36:40])
	if entryCount == math.MaxUint32 {
		return nil
	}
	return errors.New("private filesystem object must not have an extended ACL")
}
