//go:build darwin || linux

package controlbackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

var privateMode = []fs.FileMode{0o600}

var anySizePrivateFilePolicy = privatefs.FilePolicy{
	AllowedModes: privateMode,
	MinimumSize:  0,
	MaximumSize:  int64(maximumDatabaseBytes),
}

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
	inspected, err := privatefs.InspectStableRegularFile(
		ctx,
		directory,
		name,
		policy,
		retainContents,
	)
	if err != nil {
		return memberResult{}, err
	}
	return memberResult{
		identity: FileIdentity{
			Name:      name,
			SizeBytes: inspected.SizeBytes,
			SHA256:    hex.EncodeToString(inspected.SHA256[:]),
		},
		contents: inspected.Contents,
	}, nil
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
) (memberResult, error) {
	if ctx == nil {
		return memberResult{}, errors.New("write private member: context is required")
	}
	if err := ctx.Err(); err != nil {
		return memberResult{}, err
	}
	written, err := privatefs.WriteStableRegularFile(ctx, directory, name, contents)
	if err != nil {
		return memberResult{}, err
	}
	return memberResult{
		identity: FileIdentity{
			Name:      name,
			SizeBytes: written.SizeBytes,
			SHA256:    hex.EncodeToString(written.SHA256[:]),
		},
	}, nil
}

type copyMemberHooks struct {
	beforeCleanupOpen func()
	afterWriterClose  func()
}

func copyMemberWithHooks(
	ctx context.Context,
	source *privatefs.Directory,
	sourceName string,
	destination *privatefs.Directory,
	destinationName string,
	want FileIdentity,
	hooks copyMemberHooks,
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
	writerOpen := true
	var cleanupFile *os.File
	defer func() {
		if returnedErr != nil && exists {
			expected := cleanupFile
			if expected == nil && writerOpen {
				expected = destinationFile
			}
			if expected != nil {
				if unlinkErr := destination.UnlinkPinnedRegular(temporaryName, expected); unlinkErr != nil &&
					!errors.Is(unlinkErr, os.ErrNotExist) {
					returnedErr = errors.Join(returnedErr, unlinkErr)
				}
			}
		}
		if cleanupFile != nil {
			returnedErr = errors.Join(returnedErr, cleanupFile.Close())
		}
		if writerOpen {
			returnedErr = errors.Join(returnedErr, destinationFile.Close())
		}
	}()
	hash := sha256.New()
	written, err := privatefs.CopyBufferWithContext(
		ctx,
		io.MultiWriter(destinationFile, hash),
		io.LimitReader(sourceFile, policy.MaximumSize+1),
		make([]byte, 64<<10),
	)
	if err != nil {
		return fmt.Errorf("copy control-plane backup member %q: %w", sourceName, err)
	}

	wantSize := safecast.MustConv[int64](want.SizeBytes)
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
	if hooks.beforeCleanupOpen != nil {
		hooks.beforeCleanupOpen()
	}
	cleanupCandidate, err := destination.OpenRegular(destinationName, policy)
	if err != nil {
		return fmt.Errorf(
			"pin restored temporary member %q for cleanup: %w",
			destinationName,
			err,
		)
	}
	writerInfo, writerStatErr := destinationFile.Stat()
	candidateInfo, candidateStatErr := cleanupCandidate.Stat()
	identityErr := errors.Join(writerStatErr, candidateStatErr)
	if identityErr == nil && !os.SameFile(writerInfo, candidateInfo) {
		identityErr = errors.New(
			"reopened cleanup descriptor does not identify the staged file",
		)
	}
	if identityErr != nil {
		return errors.Join(
			fmt.Errorf(
				"pin restored temporary member %q for cleanup: %w",
				destinationName,
				identityErr,
			),
			cleanupCandidate.Close(),
		)
	}
	cleanupFile = cleanupCandidate
	if err := destinationFile.Close(); err != nil {
		writerOpen = false
		return fmt.Errorf("close restored temporary member %q: %w", destinationName, err)
	}
	writerOpen = false
	if hooks.afterWriterClose != nil {
		hooks.afterWriterClose()
	}
	staged, err := inspectMember(ctx, destination, destinationName, policy, false)
	if err != nil {
		return err
	}
	staged.identity.Name = sourceName
	if err := requireFileIdentity(staged, want); err != nil {
		return err
	}
	if err := destination.RequirePinnedRegular(destinationName, cleanupFile); err != nil {
		return fmt.Errorf(
			"rebind restored temporary member %q after inspection: %w",
			destinationName,
			err,
		)
	}
	exists = false
	if err := cleanupFile.Close(); err != nil {
		cleanupFile = nil
		return fmt.Errorf(
			"close restored temporary member %q cleanup descriptor: %w",
			destinationName,
			err,
		)
	}
	cleanupFile = nil
	return nil
}

func fixedNameGenerator(name string) privatefs.NameGenerator {
	return func() (string, error) { return name, nil }
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
		file, err := directory.OpenRegular(name, anySizePrivateFilePolicy)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			result = errors.Join(result, err)
			continue
		}
		unlinkErr := directory.UnlinkPinnedRegular(name, file)
		closeErr := file.Close()
		if unlinkErr != nil && !errors.Is(unlinkErr, os.ErrNotExist) {
			result = errors.Join(result, unlinkErr)
		}
		if closeErr != nil {
			result = errors.Join(result, closeErr)
		}
	}
	return result
}

type pinnedCleanupFile struct {
	name string
	file *os.File
}

func closePinnedCleanupFiles(files []pinnedCleanupFile) error {
	var result error
	for _, v := range slices.Backward(files) {
		if v.file == nil {
			continue
		}
		result = errors.Join(result, v.file.Close())
		v.file = nil
	}
	return result
}

func prepareExactBundleCleanup(
	parent *privatefs.Directory,
	name string,
	child *privatefs.Directory,
) (files []pinnedCleanupFile, artifacts *privatefs.Directory, returnedErr error) {
	if parent == nil {
		return nil, nil, errors.New("prepare created control-plane bundle removal: parent is required")
	}
	if err := privatefs.ValidateComponent(name); err != nil {
		return nil, nil, err
	}
	if child == nil {
		return nil, nil, errors.New(
			"prepare created control-plane bundle removal: pinned child is required",
		)
	}
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		return nil, nil, fmt.Errorf(
			"prepare created control-plane bundle removal: bind pinned child: %w",
			err,
		)
	}
	entries, err := child.List(len(bundleStageCleanupNames))
	if err != nil {
		return nil, nil, fmt.Errorf(
			"prepare created control-plane bundle removal: inventory child: %w",
			err,
		)
	}
	for _, entry := range entries {
		if !slices.Contains(bundleStageCleanupNames, entry) {
			return nil, nil, fmt.Errorf(
				"prepare created control-plane bundle removal: unexpected child entry %q",
				entry,
			)
		}
	}

	files = make([]pinnedCleanupFile, 0, len(entries))
	for _, entry := range entries {
		if entry == searchArtifactsFilename {
			artifactDirectory, openErr := openPinnedChild(child, entry)
			if openErr != nil {
				return nil, nil, errors.Join(openErr, closePinnedCleanupFiles(files))
			}
			if _, _, inspectErr := inspectSearchArtifactDirectory(
				context.Background(),
				artifactDirectory,
				false,
			); inspectErr != nil {
				return nil, nil, errors.Join(
					inspectErr,
					artifactDirectory.Close(),
					closePinnedCleanupFiles(files),
				)
			}
			artifacts = artifactDirectory
			continue
		}
		file, openErr := child.OpenRegular(entry, anySizePrivateFilePolicy)
		if openErr != nil {
			return nil, nil, errors.Join(
				fmt.Errorf(
					"prepare created control-plane bundle removal: open child entry %q: %w",
					entry,
					openErr,
				),
				closePinnedCleanupFiles(files),
			)
		}
		files = append(files, pinnedCleanupFile{name: entry, file: file})
	}
	for _, member := range files {
		if err := child.RequirePinnedRegular(member.name, member.file); err != nil {
			var artifactCloseErr error
			if artifacts != nil {
				artifactCloseErr = artifacts.Close()
			}
			return nil, nil, errors.Join(
				fmt.Errorf(
					"prepare created control-plane bundle removal: bind child entry %q: %w",
					member.name,
					err,
				),
				closePinnedCleanupFiles(files),
				artifactCloseErr,
			)
		}
	}
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		var artifactCloseErr error
		if artifacts != nil {
			artifactCloseErr = artifacts.Close()
		}
		return nil, nil, errors.Join(
			fmt.Errorf(
				"prepare created control-plane bundle removal: rebind pinned child: %w",
				err,
			),
			closePinnedCleanupFiles(files),
			artifactCloseErr,
		)
	}
	return files, artifacts, nil
}

// ValidateCreatedBundleForRemoval performs the complete non-mutating preflight
// used by outer recovery-set cleanup. It rejects a replaced child, an
// unexpected entry, or an unsafe known member before any outer member is
// removed.
func ValidateCreatedBundleForRemoval(
	parent *privatefs.Directory,
	name string,
	child *privatefs.Directory,
) error {
	files, artifacts, err := prepareExactBundleCleanup(parent, name, child)
	if err != nil {
		return err
	}
	var artifactCloseErr error
	if artifacts != nil {
		artifactCloseErr = artifacts.Close()
	}
	if err := errors.Join(closePinnedCleanupFiles(files), artifactCloseErr); err != nil {
		return fmt.Errorf("validate created control-plane bundle removal: %w", err)
	}
	return nil
}

// RemoveCreatedBundle removes one controlbackup-owned child from an already
// pinned owner-private parent. It knows the complete bundle and interrupted
// SQLite sidecar namespace, removes only those exact regular-file names, and
// never recursively traverses an unexpected object.
func RemoveCreatedBundle(
	parent *privatefs.Directory,
	name string,
	child *privatefs.Directory,
) (returnedErr error) {
	files, artifacts, err := prepareExactBundleCleanup(parent, name, child)
	if err != nil {
		return fmt.Errorf("remove created control-plane bundle: %w", err)
	}
	defer func() {
		var artifactCloseErr error
		if artifacts != nil {
			artifactCloseErr = artifacts.Close()
		}
		returnedErr = errors.Join(returnedErr, closePinnedCleanupFiles(files), artifactCloseErr)
	}()
	if artifacts != nil {
		if err := removeSearchArtifactDirectory(child, searchArtifactsFilename, artifacts); err != nil {
			return fmt.Errorf("remove created control-plane bundle: remove search artifacts: %w", err)
		}
	}

	for index := range files {
		member := &files[index]
		unlinkErr := child.UnlinkPinnedRegular(member.name, member.file)
		closeErr := member.file.Close()
		member.file = nil
		if err := errors.Join(unlinkErr, closeErr); err != nil {
			return fmt.Errorf(
				"remove created control-plane bundle: remove child entry %q: %w",
				member.name,
				err,
			)
		}
	}
	if err := child.Sync(); err != nil {
		return fmt.Errorf("remove created control-plane bundle: sync child: %w", err)
	}
	if err := parent.RemovePinnedEmptyDirectory(name, child); err != nil {
		return fmt.Errorf("remove created control-plane bundle: remove child: %w", err)
	}
	return nil
}
