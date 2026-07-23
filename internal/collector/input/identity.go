package input

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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

// ParseFileIdentity parses the canonical representation emitted by
// [FileIdentity.String]. It intentionally rejects legacy, reordered,
// non-canonical, and malformed values: checkpoint reconstruction from a WAL
// event must never silently reinterpret an ambiguous source identity.
//
// FingerprintLength is not encoded in the historical identity string. Callers
// reconstructing a checkpoint must restore it from EventOrigin's separate
// file_fingerprint_length field.
func ParseFileIdentity(value string) (FileIdentity, error) {
	parts := strings.Split(value, ";")
	if len(parts) != 4 {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid file identity %q: want four fields", value)
	}
	parseUint := func(part, prefix string) (uint64, error) {
		if !strings.HasPrefix(part, prefix) {
			return 0, fmt.Errorf("collector/input: invalid file identity %q: want %s field", value, strings.TrimSuffix(prefix, "="))
		}
		raw := strings.TrimPrefix(part, prefix)
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("collector/input: invalid file identity %q: parse %s: %w", value, strings.TrimSuffix(prefix, "="), err)
		}
		// FileIdentity.String emits canonical base-10 without leading zeroes.
		if strconv.FormatUint(parsed, 10) != raw {
			return 0, fmt.Errorf("collector/input: invalid file identity %q: non-canonical %s", value, strings.TrimSuffix(prefix, "="))
		}
		return parsed, nil
	}
	device, err := parseUint(parts[0], "dev=")
	if err != nil {
		return FileIdentity{}, err
	}
	inode, err := parseUint(parts[1], "ino=")
	if err != nil {
		return FileIdentity{}, err
	}
	generation, err := parseUint(parts[2], "gen=")
	if err != nil {
		return FileIdentity{}, err
	}
	if generation == 0 {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid file identity %q: generation must be positive", value)
	}
	if !strings.HasPrefix(parts[3], "fp=") {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid file identity %q: want fp field", value)
	}
	fingerprint := strings.TrimPrefix(parts[3], "fp=")
	if len(fingerprint) != sha256.Size*2 || strings.ToLower(fingerprint) != fingerprint {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid file identity %q: fingerprint must be canonical SHA-256 hex", value)
	}
	if _, err := hex.DecodeString(fingerprint); err != nil {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid file identity %q: parse fingerprint: %w", value, err)
	}
	id := FileIdentity{
		Device:      device,
		Inode:       inode,
		Generation:  generation,
		Fingerprint: fingerprint,
	}
	if id.String() != value {
		return FileIdentity{}, fmt.Errorf("collector/input: invalid non-canonical file identity %q", value)
	}
	return id, nil
}
