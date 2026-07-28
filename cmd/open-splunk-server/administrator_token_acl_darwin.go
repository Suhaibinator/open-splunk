//go:build darwin

package main

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
	darwinAdministratorTokenACLBufferBytes = 4096
	darwinAttributeReferenceBytes          = 8
	darwinFileSecurityHeaderBytes          = 44
	darwinFileSecurityMagic                = 0x012cc16d
	// x/sys does not expose fgetattrlist. Calling the descriptor-based Darwin
	// syscall directly preserves the no-path-race guarantee without requiring
	// cgo in the release binary.
	darwinFgetattrlistSyscall = unix.SYS_FGETATTRLIST //nolint:staticcheck
)

// validateAdministratorTokenACL rejects every macOS extended ACL, including
// inherited entries. POSIX mode bits do not account for these permissions, so
// a file may otherwise appear to be 0600 while granting another identity read
// access.
func validateAdministratorTokenACL(file *os.File) error {
	if file == nil {
		return errors.New("administrator token file ACL is unavailable")
	}

	attributes := unix.Attrlist{
		Bitmapcount: unix.ATTR_BIT_MAP_COUNT,
		Commonattr:  unix.ATTR_CMN_EXTENDED_SECURITY,
	}
	buffer := make([]byte, darwinAdministratorTokenACLBufferBytes)
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
		return errors.New("inspect administrator token file ACL")
	}

	totalBytes := int(binary.NativeEndian.Uint32(buffer[:4]))
	if totalBytes < 4+darwinAttributeReferenceBytes ||
		totalBytes > len(buffer) {
		return errors.New("administrator token file ACL is invalid")
	}
	referenceStart := 4
	rawDataOffset := binary.NativeEndian.Uint32(
		buffer[referenceStart : referenceStart+4],
	)
	if rawDataOffset > math.MaxInt32 {
		return errors.New("administrator token file ACL is invalid")
	}
	dataOffset := int(rawDataOffset)
	dataBytes := int(binary.NativeEndian.Uint32(
		buffer[referenceStart+4 : referenceStart+darwinAttributeReferenceBytes],
	))
	if dataBytes == 0 {
		return nil
	}
	dataStart := referenceStart + dataOffset
	if dataOffset < darwinAttributeReferenceBytes ||
		dataStart < referenceStart ||
		dataBytes < darwinFileSecurityHeaderBytes ||
		dataStart > totalBytes ||
		dataBytes > totalBytes-dataStart {
		return errors.New("administrator token file ACL is invalid")
	}
	fileSecurity := buffer[dataStart : dataStart+dataBytes]
	if binary.NativeEndian.Uint32(fileSecurity[:4]) !=
		darwinFileSecurityMagic {
		return errors.New("administrator token file ACL is invalid")
	}
	entryCount := binary.NativeEndian.Uint32(fileSecurity[36:40])
	if entryCount == math.MaxUint32 {
		return nil
	}
	return errors.New("administrator token file must not have an extended ACL")
}
