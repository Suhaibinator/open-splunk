//go:build darwin || linux

package controlbackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

const (
	searchArtifactLockFilename = ".open-splunk-search-artifacts.lock"
	searchArtifactPrefix       = "job-"
	searchArtifactSuffix       = ".results.json"
)

var searchArtifactPolicy = privatefs.FilePolicy{
	AllowedModes: privateMode,
	MinimumSize:  1,
	MaximumSize:  int64(maximumSearchArtifactBytes),
}

func validSearchArtifactName(name string) bool {
	if !strings.HasPrefix(name, searchArtifactPrefix) ||
		!strings.HasSuffix(name, searchArtifactSuffix) {
		return false
	}
	encoded := strings.TrimSuffix(strings.TrimPrefix(name, searchArtifactPrefix), searchArtifactSuffix)
	return validLowerHex(encoded, sha256.Size)
}

func openPinnedChild(
	parent *privatefs.Directory,
	name string,
) (*privatefs.Directory, error) {
	child, err := privatefs.OpenDirectory(filepath.Join(parent.Path(), name))
	if err != nil {
		return nil, err
	}
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		return nil, errors.Join(err, child.Close())
	}
	return child, nil
}

func pinnedChildExists(parent *privatefs.Directory, name string) (bool, error) {
	if err := parent.Revalidate(); err != nil {
		return false, err
	}
	_, err := os.Lstat(filepath.Join(parent.Path(), name))
	if errors.Is(err, os.ErrNotExist) {
		if err := parent.Revalidate(); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func inspectSearchArtifactDirectory(
	ctx context.Context,
	directory *privatefs.Directory,
	allowLock bool,
) (DirectoryIdentity, []FileIdentity, error) {
	maximumEntries := int(maximumSearchArtifactFiles)
	if allowLock {
		maximumEntries++
	}
	entries, err := directory.List(maximumEntries)
	if err != nil {
		return DirectoryIdentity{}, nil, err
	}
	identities := make([]FileIdentity, 0, len(entries))
	var sizeBytes uint64
	for _, name := range entries {
		if allowLock && name == searchArtifactLockFilename {
			lock, openErr := directory.OpenRegular(name, privatefs.FilePolicy{
				AllowedModes: privateMode,
				MinimumSize:  0,
				MaximumSize:  0,
			})
			if openErr != nil {
				return DirectoryIdentity{}, nil, openErr
			}
			if closeErr := lock.Close(); closeErr != nil {
				return DirectoryIdentity{}, nil, closeErr
			}
			continue
		}
		if !validSearchArtifactName(name) {
			return DirectoryIdentity{}, nil, fmt.Errorf("unexpected search-artifact entry %q", name)
		}
		member, inspectErr := inspectMember(ctx, directory, name, searchArtifactPolicy, false)
		if inspectErr != nil {
			return DirectoryIdentity{}, nil, inspectErr
		}
		if maximumSearchArtifactBytes-sizeBytes < member.identity.SizeBytes {
			return DirectoryIdentity{}, nil, errors.New("search-artifact directory exceeds its size limit")
		}
		sizeBytes += member.identity.SizeBytes
		identities = append(identities, member.identity)
	}
	if uint64(len(identities)) > maximumSearchArtifactFiles {
		return DirectoryIdentity{}, nil, errors.New("search-artifact directory exceeds its file-count limit")
	}
	hash := sha256.New()
	for _, identity := range identities {
		_, _ = hash.Write([]byte(identity.Name))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(strconv.FormatUint(identity.SizeBytes, 10)))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(identity.SHA256))
		_, _ = hash.Write([]byte{'\n'})
	}
	return DirectoryIdentity{
		Name:      searchArtifactsFilename,
		FileCount: uint64(len(identities)),
		SizeBytes: sizeBytes,
		SHA256:    hex.EncodeToString(hash.Sum(nil)),
	}, identities, nil
}

func copySearchArtifactDirectory(
	ctx context.Context,
	source *privatefs.Directory,
	destinationParent *privatefs.Directory,
	destinationName string,
) (identity DirectoryIdentity, child *privatefs.Directory, returnedErr error) {
	want, members, err := inspectSearchArtifactDirectory(ctx, source, true)
	if err != nil {
		return DirectoryIdentity{}, nil, err
	}
	_, child, err = destinationParent.CreateTemporaryDirectory(fixedNameGenerator(destinationName))
	if err != nil {
		return DirectoryIdentity{}, nil, err
	}
	removeOnError := true
	defer func() {
		if returnedErr == nil || !removeOnError {
			return
		}
		returnedErr = errors.Join(
			returnedErr,
			removeSearchArtifactDirectory(destinationParent, destinationName, child),
			child.Close(),
		)
		child = nil
	}()
	for _, member := range members {
		if err := copyMemberWithHooks(
			ctx,
			source,
			member.Name,
			child,
			member.Name,
			member,
			copyMemberHooks{},
		); err != nil {
			return DirectoryIdentity{}, child, err
		}
	}
	if err := child.Sync(); err != nil {
		return DirectoryIdentity{}, child, err
	}
	got, _, err := inspectSearchArtifactDirectory(ctx, child, false)
	if err != nil {
		return DirectoryIdentity{}, child, err
	}
	if got != want {
		return DirectoryIdentity{}, child, errors.New("search-artifact directory changed while copying")
	}
	stableSource, _, err := inspectSearchArtifactDirectory(ctx, source, true)
	if err != nil {
		return DirectoryIdentity{}, child, err
	}
	if stableSource != want {
		return DirectoryIdentity{}, child, errors.New("search-artifact source changed while copying")
	}
	removeOnError = false
	return want, child, nil
}

func verifySearchArtifactDirectory(
	ctx context.Context,
	parent *privatefs.Directory,
	name string,
	want DirectoryIdentity,
) error {
	child, err := openPinnedChild(parent, name)
	if err != nil {
		return err
	}
	defer child.Close()
	got, _, err := inspectSearchArtifactDirectory(ctx, child, false)
	if err != nil {
		return err
	}
	if got != want {
		return errors.New("search-artifact directory does not match its manifest")
	}
	return parent.RequirePinnedChildDirectory(name, child)
}

func removeSearchArtifactDirectory(
	parent *privatefs.Directory,
	name string,
	child *privatefs.Directory,
) (returnedErr error) {
	if child == nil {
		return errors.New("remove search-artifact directory: pinned child is required")
	}
	if err := parent.RequirePinnedChildDirectory(name, child); err != nil {
		return err
	}
	_, identities, err := inspectSearchArtifactDirectory(context.Background(), child, false)
	if err != nil {
		return err
	}
	files := make([]pinnedCleanupFile, 0, len(identities))
	defer func() { returnedErr = errors.Join(returnedErr, closePinnedCleanupFiles(files)) }()
	for _, identity := range identities {
		file, openErr := child.OpenRegular(identity.Name, mustPolicy(identity))
		if openErr != nil {
			return openErr
		}
		files = append(files, pinnedCleanupFile{name: identity.Name, file: file})
	}
	for _, file := range files {
		if err := child.RequirePinnedRegular(file.name, file.file); err != nil {
			return err
		}
	}
	for index := range files {
		file := &files[index]
		if err := child.UnlinkPinnedRegular(file.name, file.file); err != nil {
			return err
		}
		if err := file.file.Close(); err != nil {
			file.file = nil
			return err
		}
		file.file = nil
	}
	if err := child.Sync(); err != nil {
		return err
	}
	return parent.RemovePinnedEmptyDirectory(name, child)
}

func preflightRestoreSearchArtifacts(
	ctx context.Context,
	plan *restorePlan,
	entries []string,
) error {
	if plan.manifest.SearchArtifacts == nil {
		return nil
	}
	destination := plan.searchArtifactDestination()
	stageExists := containsName(entries, plan.artifactStageName)
	targetExists := containsName(entries, plan.targets.searchArtifactName)
	if plan.artifactDestination != nil {
		var err error
		stageExists, err = pinnedChildExists(destination, plan.artifactStageName)
		if err != nil {
			return fmt.Errorf("preflight control-plane restore: inspect search-artifact stage: %w", err)
		}
		targetExists, err = pinnedChildExists(destination, plan.targets.searchArtifactName)
		if err != nil {
			return fmt.Errorf("preflight control-plane restore: inspect search-artifact target: %w", err)
		}
	}
	if stageExists {
		stage, err := openPinnedChild(destination, plan.artifactStageName)
		if err != nil {
			return fmt.Errorf("preflight control-plane restore: unsafe search-artifact stage: %w", err)
		}
		_, _, inspectErr := inspectSearchArtifactDirectory(ctx, stage, false)
		closeErr := stage.Close()
		if inspectErr != nil || closeErr != nil {
			return errors.Join(inspectErr, closeErr)
		}
	}
	if targetExists {
		if err := verifySearchArtifactDirectory(
			ctx,
			destination,
			plan.targets.searchArtifactName,
			*plan.manifest.SearchArtifacts,
		); err != nil {
			return fmt.Errorf("preflight control-plane restore: existing search artifacts differ from the bundle: %w", err)
		}
	}
	return destination.Revalidate()
}

func cleanupRestoreSearchArtifactStage(plan *restorePlan, databaseLock *os.File) error {
	if plan.manifest.SearchArtifacts == nil {
		return nil
	}
	if err := validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		databaseLock,
	); err != nil {
		return err
	}
	destination := plan.searchArtifactDestination()
	stageExists := false
	if plan.artifactDestination == nil {
		entries, err := plan.destination.List(len(plan.knownNames))
		if err != nil {
			return fmt.Errorf("restore control-plane backup: inspect search-artifact stage: %w", err)
		}
		for _, entry := range entries {
			if !containsName(plan.knownNames, entry) {
				return fmt.Errorf("restore control-plane backup: destination contains unrelated entry %q", entry)
			}
		}
		stageExists = containsName(entries, plan.artifactStageName)
	} else {
		var err error
		stageExists, err = pinnedChildExists(destination, plan.artifactStageName)
		if err != nil {
			return fmt.Errorf("restore control-plane backup: inspect search-artifact stage: %w", err)
		}
	}
	if !stageExists {
		return nil
	}
	stage, err := openPinnedChild(destination, plan.artifactStageName)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: unsafe stale search-artifact stage: %w", err)
	}
	defer stage.Close()
	if err := removeSearchArtifactDirectory(destination, plan.artifactStageName, stage); err != nil {
		return fmt.Errorf("restore control-plane backup: remove stale search-artifact stage: %w", err)
	}
	return destination.Sync()
}

func ensureRestoredSearchArtifacts(
	ctx context.Context,
	plan *restorePlan,
	databaseLock *os.File,
) (returnedErr error) {
	if plan.manifest.SearchArtifacts == nil {
		return nil
	}
	destination := plan.searchArtifactDestination()
	targetExists := false
	if plan.artifactDestination == nil {
		entries, err := plan.destination.List(len(plan.namespaceNames))
		if err != nil {
			return fmt.Errorf("restore control-plane backup: inspect search-artifact target: %w", err)
		}
		targetExists = containsName(entries, plan.targets.searchArtifactName)
	} else {
		var err error
		targetExists, err = pinnedChildExists(destination, plan.targets.searchArtifactName)
		if err != nil {
			return fmt.Errorf("restore control-plane backup: inspect search-artifact target: %w", err)
		}
	}
	if targetExists {
		if err := verifySearchArtifactDirectory(
			ctx,
			destination,
			plan.targets.searchArtifactName,
			*plan.manifest.SearchArtifacts,
		); err != nil {
			return fmt.Errorf("restore control-plane backup: existing search artifacts differ from the bundle: %w", err)
		}
		return nil
	}
	source, err := openPinnedChild(plan.source, searchArtifactsFilename)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: open bundled search artifacts: %w", err)
	}
	defer source.Close()
	identity, stage, err := copySearchArtifactDirectory(
		ctx,
		source,
		destination,
		plan.artifactStageName,
	)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: stage search artifacts: %w", err)
	}
	stageOpen := true
	stageExists := true
	defer func() {
		if returnedErr != nil && stageExists {
			returnedErr = errors.Join(
				returnedErr,
				removeSearchArtifactDirectory(destination, plan.artifactStageName, stage),
			)
		}
		if stageOpen {
			returnedErr = errors.Join(returnedErr, stage.Close())
		}
	}()
	if identity != *plan.manifest.SearchArtifacts {
		return errors.New("restore control-plane backup: staged search artifacts differ from the manifest")
	}
	if err := destination.Sync(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		databaseLock,
	); err != nil {
		return err
	}
	outcome, err := destination.RenameDirectoryNoReplaceWithStatus(
		plan.artifactStageName,
		stage,
		destination,
		plan.targets.searchArtifactName,
	)
	if outcome == privatefs.RenameNoReplaceCompleted ||
		outcome == privatefs.RenameNoReplaceAmbiguous {
		stageExists = false
	}
	if err != nil {
		return fmt.Errorf("restore control-plane backup: publish search artifacts: %w", err)
	}
	if outcome != privatefs.RenameNoReplaceCompleted {
		return errors.New("restore control-plane backup: search-artifact publication did not complete")
	}
	if err := destination.Sync(); err != nil {
		return err
	}
	if err := validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		databaseLock,
	); err != nil {
		return err
	}
	if err := stage.Close(); err != nil {
		stageOpen = false
		return err
	}
	stageOpen = false
	return verifySearchArtifactDirectory(
		ctx,
		destination,
		plan.targets.searchArtifactName,
		*plan.manifest.SearchArtifacts,
	)
}

func containsName(names []string, name string) bool {
	return slices.Contains(names, name)
}

func searchArtifactDirectoryPath(databasePath, configured string) string {
	if configured != "" {
		return configured
	}
	return databasePath + ".search-artifacts"
}
