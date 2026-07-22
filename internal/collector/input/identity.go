package input

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

// defaultFingerprintBytes is the number of leading bytes hashed into a file
// fingerprint when Config.FingerprintBytes is unset.
const defaultFingerprintBytes = 1024

// fingerprintBytesOr returns n when positive, else the package default.
func fingerprintBytesOr(n int) int {
	if n <= 0 {
		return defaultFingerprintBytes
	}
	return n
}

// computeFingerprint returns the hex-encoded SHA-256 over the first
// min(fingerprintBytes, filesize) bytes of f. It uses ReadAt so it never
// disturbs f's current read offset, which lets a tailer recompute a fingerprint
// mid-tail (for example after copy-truncate) without losing its place.
//
// A file shorter than fingerprintBytes has a fingerprint that changes as it
// grows, because fewer than fingerprintBytes bytes are available to hash. File
// identity therefore prefers the platform dev+inode when available and treats
// the fingerprint as a secondary signal used to detect copy-truncate and inode
// reuse rather than as the primary key.
func computeFingerprint(f *os.File, fingerprintBytes int) (string, error) {
	fingerprint, _, err := computeFingerprintWithLength(f, fingerprintBytes)
	return fingerprint, err
}

func computeFingerprintWithLength(f *os.File, fingerprintBytes int) (string, uint32, error) {
	buf := make([]byte, fingerprintBytesOr(fingerprintBytes))
	n, err := f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", 0, err
	}
	sum := sha256.Sum256(buf[:n])
	return hex.EncodeToString(sum[:]), uint32(n), nil
}

// computeFingerprintRange hashes exactly length bytes beginning at offset. A
// short read is reported as an error because callers use this to prove that an
// already-consumed region has not been replaced.
func computeFingerprintRange(f *os.File, offset int64, length uint32) (string, error) {
	buf := make([]byte, int(length))
	if length > 0 {
		n, err := f.ReadAt(buf, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return "", err
		}
		if n != len(buf) {
			return "", io.ErrUnexpectedEOF
		}
	}
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:]), nil
}

// identityFor builds the FileIdentity for an already-open file. dev+inode come
// from fi (zero off darwin/linux); the fingerprint hashes the file head.
func identityFor(f *os.File, fi os.FileInfo, fingerprintBytes int) (FileIdentity, error) {
	dev, ino, _ := statDevIno(fi)
	fp, n, err := computeFingerprintWithLength(f, fingerprintBytes)
	if err != nil {
		return FileIdentity{}, err
	}
	return FileIdentity{Device: dev, Inode: ino, Generation: 1, Fingerprint: fp, FingerprintLength: n}, nil
}

// NewFileIdentity opens path and computes its FileIdentity. It is a convenience
// for callers (the daemon, reconciliation, tests) that hold only a path; the
// tailer uses identityFor against its already-open file descriptor.
func NewFileIdentity(path string, fingerprintBytes int) (FileIdentity, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileIdentity{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return FileIdentity{}, err
	}
	return identityFor(f, fi, fingerprintBytes)
}
