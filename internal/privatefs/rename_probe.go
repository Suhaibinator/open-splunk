//go:build darwin || linux

package privatefs

import (
	"errors"
	"fmt"
	"os"
)

// FixedNames returns a generator that yields the supplied components in order
// and fails once they are exhausted. Stores whose directories are exact-set
// namespaces use it to keep probe files inside names their own cleanup already
// reclaims.
func FixedNames(names ...string) NameGenerator {
	index := 0
	return func() (string, error) {
		if index >= len(names) {
			return "", errors.New("private filesystem fixed names are exhausted")
		}
		name := names[index]
		index++
		if err := ValidateComponent(name); err != nil {
			return "", err
		}
		return name, nil
	}
}

// ProbeRenameNoReplace proves that the filesystem behind the pinned directory
// honors atomic no-replace rename before a store depends on it. It creates one
// owner-private temporary file under the first generated name, renames it to
// the next generated name with the same primitive every publication uses, and
// unlinks whatever remains. The caller supplies the generator so that a probe
// file orphaned by a crash between create and unlink carries a name the
// caller's own reconciliation removes instead of one it rejects. An
// unsupported filesystem is reported as an *UnsupportedFilesystemError that
// names the directory and its filesystem type; any other error is a probe
// failure and leaves the directory's contents unchanged.
func (directory *Directory) ProbeRenameNoReplace(generator NameGenerator) error {
	return directory.probeRenameNoReplace(generator, renameNoReplaceAt)
}

// RequireRenameNoReplace runs ProbeRenameNoReplace and turns an unsupported
// filesystem into operator guidance naming the directory's role, absolute
// path, and filesystem type. Other probe failures are returned unchanged.
func RequireRenameNoReplace(directory *Directory, role string, generator NameGenerator) error {
	err := directory.ProbeRenameNoReplace(generator)
	if err == nil {
		return nil
	}
	if unsupported, ok := errors.AsType[*UnsupportedFilesystemError](err); ok {
		return fmt.Errorf("%s: %w", unsupported.Guidance(role), unsupported)
	}
	return err
}

func (directory *Directory) probeRenameNoReplace(
	generator NameGenerator,
	rename renameNoReplaceOperation,
) (returnedErr error) {
	if generator == nil {
		return errors.New("probe no-replace rename: name generator is required")
	}
	sourceName, file, err := directory.CreateTemporaryFile(generator)
	if err != nil {
		return fmt.Errorf("probe no-replace rename: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, file.Close()) }()

	var (
		destinationName string
		outcome         RenameNoReplaceOutcome
		renameErr       error
	)
	for range maximumNameAttempts {
		destinationName, err = nextTemporaryName(generator)
		if err != nil {
			return errors.Join(
				fmt.Errorf("probe no-replace rename: %w", err),
				directory.UnlinkPinnedRegular(sourceName, file),
			)
		}
		outcome, renameErr = directory.renameNoReplaceWithStatus(
			sourceName,
			nil,
			directory,
			destinationName,
			rename,
		)
		if !errors.Is(renameErr, ErrDestinationExists) {
			break
		}
	}
	cleanupErr := directory.removeRenameProbe(sourceName, destinationName, file, outcome)
	if renameErr == nil {
		if cleanupErr != nil {
			return fmt.Errorf("probe no-replace rename: remove probe file: %w", cleanupErr)
		}
		return nil
	}
	probeErr := fmt.Errorf("probe no-replace rename: %w", renameErr)
	if cleanupErr != nil {
		return errors.Join(probeErr, cleanupErr)
	}
	return probeErr
}

// removeRenameProbe unlinks the probe inode under whichever name the rename
// outcome left it. The pinned descriptor guarantees that only the probe file
// itself can be removed, never an unrelated object that reused a name.
func (directory *Directory) removeRenameProbe(
	sourceName string,
	destinationName string,
	file *os.File,
	outcome RenameNoReplaceOutcome,
) error {
	switch outcome {
	case RenameNoReplaceCompleted:
		return directory.UnlinkPinnedRegular(destinationName, file)
	case RenameNoReplaceUnchanged, RenameNoReplaceNotAttempted:
		return directory.UnlinkPinnedRegular(sourceName, file)
	default:
		// Ambiguous: the inode is under exactly one of the two names.
		sourceErr := directory.UnlinkPinnedRegular(sourceName, file)
		if sourceErr == nil {
			return nil
		}
		destinationErr := directory.UnlinkPinnedRegular(destinationName, file)
		if destinationErr == nil {
			return nil
		}
		return errors.Join(sourceErr, destinationErr)
	}
}
