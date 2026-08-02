//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

var privateMode = []fs.FileMode{0o600}

type memberResult struct {
	identity FileIdentity
	contents []byte
}

// ValidateExactAbsolutePath applies the same recovery-path contract used by
// bundle execution. Command parsers call it before acquiring locks so an
// invalid final component cannot create a lock side effect first.
func ValidateExactAbsolutePath(label, path string) error {
	_, _, err := validateExactAbsolutePath(label, path)
	return err
}

func validateExactAbsolutePath(label, path string) (string, string, error) {
	if path == "" || path != strings.TrimSpace(path) || strings.IndexByte(path, 0) >= 0 {
		return "", "", fmt.Errorf("%s must be a nonempty exact absolute path", label)
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", "", fmt.Errorf("%s must be a clean absolute path", label)
	}
	base := filepath.Base(path)
	if err := privatefs.ValidateComponent(base); err != nil {
		return "", "", fmt.Errorf("%s has an invalid final component: %w", label, err)
	}
	parent := filepath.Dir(path)
	if parent == path {
		return "", "", fmt.Errorf("%s must name a child of a directory", label)
	}
	return parent, base, nil
}

func openPrivatePath(label, path string) (*privatefs.Directory, string, error) {
	parent, base, err := validateExactAbsolutePath(label, path)
	if err != nil {
		return nil, "", err
	}
	directory, err := privatefs.OpenDirectory(parent)
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", label, err)
	}
	return directory, base, nil
}

func inspectMember(
	ctx context.Context,
	directory *privatefs.Directory,
	name string,
	policy privatefs.FilePolicy,
	retainContents bool,
) (memberResult, error) {
	if ctx == nil {
		return memberResult{}, errors.New("inspect private member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return memberResult{}, err
	}
	file, err := directory.OpenRegular(name, policy)
	if err != nil {
		return memberResult{}, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	before, err := file.Stat()
	if err != nil {
		return memberResult{}, fmt.Errorf("inspect private member %q: %w", name, err)
	}
	hash := sha256.New()
	var retained bytes.Buffer
	var writer io.Writer = hash
	if retainContents {
		writer = io.MultiWriter(hash, &retained)
	}
	read, err := io.CopyBuffer(
		writer,
		contextReader{
			ctx:    ctx,
			reader: io.LimitReader(file, policy.MaximumSize+1),
		},
		make([]byte, 64<<10),
	)
	if err != nil {
		return memberResult{}, fmt.Errorf("read private member %q: %w", name, err)
	}
	if read < policy.MinimumSize || read > policy.MaximumSize || read != before.Size() {
		return memberResult{}, fmt.Errorf("private member %q changed while it was read", name)
	}
	after, err := file.Stat()
	if err != nil {
		return memberResult{}, fmt.Errorf("reinspect private member %q: %w", name, err)
	}
	if !sameFileState(before, after) {
		return memberResult{}, fmt.Errorf("private member %q changed while it was read", name)
	}
	if err := ctx.Err(); err != nil {
		return memberResult{}, err
	}
	if err := file.Close(); err != nil {
		return memberResult{}, fmt.Errorf("close private member %q: %w", name, err)
	}
	closed = true

	reopened, err := directory.OpenRegular(name, policy)
	if err != nil {
		return memberResult{}, fmt.Errorf("reopen private member %q: %w", name, err)
	}
	reopenedInfo, statErr := reopened.Stat()
	closeErr := reopened.Close()
	if statErr != nil {
		return memberResult{}, fmt.Errorf("reinspect private member %q: %w", name, statErr)
	}
	if closeErr != nil {
		return memberResult{}, fmt.Errorf("close reinspected private member %q: %w", name, closeErr)
	}
	if !sameFileState(before, reopenedInfo) {
		return memberResult{}, fmt.Errorf("private member %q changed after it was read", name)
	}
	if err := directory.Revalidate(); err != nil {
		return memberResult{}, err
	}
	// #nosec G115 -- io.CopyBuffer cannot return a negative byte count, and the
	// count was checked against the nonnegative policy bounds above.
	sizeBytes := uint64(read)
	return memberResult{
		identity: FileIdentity{
			Name:      name,
			SizeBytes: sizeBytes,
			SHA256:    hex.EncodeToString(hash.Sum(nil)),
		},
		contents: retained.Bytes(),
	}, nil
}

func sameFileState(left, right os.FileInfo) bool {
	return left != nil && right != nil &&
		os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}

func requireFileIdentity(got memberResult, want FileIdentity) error {
	if got.identity != want {
		return fmt.Errorf("control-plane backup member %q does not match its manifest", want.Name)
	}
	return nil
}

func writeMember(
	ctx context.Context,
	directory *privatefs.Directory,
	name string,
	contents []byte,
) (result memberResult, returnedErr error) {
	if ctx == nil {
		return memberResult{}, errors.New("write private member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return memberResult{}, err
	}
	temporaryName, file, err := directory.CreateTemporaryFile(fixedNameGenerator(name))
	if err != nil {
		return memberResult{}, err
	}
	exists := true
	closed := false
	defer func() {
		if !closed {
			returnedErr = errors.Join(returnedErr, file.Close())
		}
		if exists {
			if unlinkErr := directory.Unlink(temporaryName); unlinkErr != nil &&
				!errors.Is(unlinkErr, os.ErrNotExist) {
				returnedErr = errors.Join(returnedErr, unlinkErr)
			}
		}
	}()
	if err := writeAll(ctx, file, contents); err != nil {
		return memberResult{}, fmt.Errorf("write private member %q: %w", name, err)
	}
	if err := privatefs.SyncFile(file); err != nil {
		return memberResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return memberResult{}, err
	}
	if err := file.Close(); err != nil {
		return memberResult{}, fmt.Errorf("close private member %q: %w", name, err)
	}
	closed = true
	policy := privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  int64(len(contents)),
		MaximumSize:  int64(len(contents)),
	}
	result, err = inspectMember(ctx, directory, name, policy, false)
	if err != nil {
		return memberResult{}, err
	}
	exists = false
	return result, nil
}

func copyMember(
	ctx context.Context,
	source *privatefs.Directory,
	sourceName string,
	destination *privatefs.Directory,
	destinationName string,
	want FileIdentity,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("copy control-plane backup member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	policy, err := policyForIdentity(want)
	if err != nil {
		return err
	}
	sourceFile, err := source.OpenRegular(sourceName, policy)
	if err != nil {
		return err
	}
	defer sourceFile.Close()
	temporaryName, destinationFile, err := destination.CreateTemporaryFile(
		fixedNameGenerator(destinationName),
	)
	if err != nil {
		return err
	}
	exists := true
	closed := false
	defer func() {
		if !closed {
			returnedErr = errors.Join(returnedErr, destinationFile.Close())
		}
		if returnedErr != nil && exists {
			if unlinkErr := destination.Unlink(temporaryName); unlinkErr != nil &&
				!errors.Is(unlinkErr, os.ErrNotExist) {
				returnedErr = errors.Join(returnedErr, unlinkErr)
			}
		}
	}()
	hash := sha256.New()
	written, err := io.CopyBuffer(
		io.MultiWriter(destinationFile, hash),
		contextReader{
			ctx:    ctx,
			reader: io.LimitReader(sourceFile, policy.MaximumSize+1),
		},
		make([]byte, 64<<10),
	)
	if err != nil {
		return fmt.Errorf("copy control-plane backup member %q: %w", sourceName, err)
	}
	// #nosec G115 -- every accepted manifest size is capped at 1 TiB before
	// policy construction, well below MaxInt64.
	wantSize := int64(want.SizeBytes)
	if written != wantSize ||
		hex.EncodeToString(hash.Sum(nil)) != want.SHA256 {
		return fmt.Errorf("control-plane backup member %q changed while restoring", sourceName)
	}
	if err := privatefs.SyncFile(destinationFile); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := destinationFile.Close(); err != nil {
		return fmt.Errorf("close restored temporary member %q: %w", destinationName, err)
	}
	closed = true
	staged, err := inspectMember(ctx, destination, destinationName, policy, false)
	if err != nil {
		return err
	}
	staged.identity.Name = sourceName
	if err := requireFileIdentity(staged, want); err != nil {
		return err
	}
	exists = false
	return nil
}

func fixedNameGenerator(name string) privatefs.NameGenerator {
	return func() (string, error) { return name, nil }
}

func writeAll(ctx context.Context, file *os.File, contents []byte) error {
	for len(contents) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := file.Write(contents)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(contents) {
			return io.ErrShortWrite
		}
		contents = contents[written:]
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	read, err := reader.reader.Read(buffer)
	if err == nil {
		if contextErr := reader.ctx.Err(); contextErr != nil {
			return read, contextErr
		}
	}
	return read, err
}

func policyForIdentity(identity FileIdentity) (privatefs.FilePolicy, error) {
	if identity.SizeBytes > maximumDatabaseBytes {
		return privatefs.FilePolicy{}, errors.New("control-plane backup member is too large")
	}
	size := int64(identity.SizeBytes)
	return privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  size,
		MaximumSize:  size,
	}, nil
}

func cleanupKnownFiles(
	directory *privatefs.Directory,
	names []string,
) error {
	if directory == nil {
		return nil
	}
	var result error
	for _, name := range names {
		policy := privatefs.FilePolicy{
			AllowedModes: privateMode,
			MinimumSize:  0,
			MaximumSize:  int64(maximumDatabaseBytes),
		}
		file, err := directory.OpenRegular(name, policy)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		closeErr := file.Close()
		if closeErr != nil {
			result = errors.Join(result, closeErr)
			continue
		}
		if err := directory.Unlink(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return result
}
