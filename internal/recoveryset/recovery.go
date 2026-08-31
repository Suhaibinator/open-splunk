//go:build darwin || linux

package recoveryset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/controlbackup"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
)

var outerMemberNames = []string{
	controlPlaneDirectory,
	manifestFilename,
}

// NativeBackupRequest contains only fixed or recovery-ID-derived names. The
// callback must create ArchivePath without replacement and return only after
// the native archive has closed successfully.
type NativeBackupRequest struct {
	RecoverySetID   string
	Disk            string
	Database        string
	ArchiveDatabase string
	ArchiveName     string
	ArchivePath     string
}

// NativeBackupIdentity is the release-owned ClickHouse state observed by the
// callback around its successful native backup operation.
type NativeBackupIdentity struct {
	ServerVersion                   string
	MigrationLedgerSHA256           string
	DatabaseUUID                    string
	SchemaMigrationsTableUUID       string
	EventsTableUUID                 string
	RecoverySetsTableUUID           string
	RecoveryArchiveMarkersTableUUID string
	MaxVisibilitySeq                uint64
	BackupOperationUUID             string
}

// NativeBackup creates one native ClickHouse archive for an already-created
// control-plane child. Implementations must not place credentials in returned
// values, filenames, or diagnostics.
type NativeBackup func(context.Context, NativeBackupRequest) (NativeBackupIdentity, error)

// CreateOptions identifies one stopped deployment, the new owner-private
// outer directory, and the separately mounted ClickHouse recovery root.
type CreateOptions struct {
	DatabasePath            string
	MasterKeyPath           string
	AdministratorTokenPath  string
	SearchArtifactDirectory string
	Destination             string
	ArchiveRoot             string
	ArchiveOwnership        ArchiveOwnershipPolicy
	Release                 controlbackup.ReleaseIdentity
	NativeBackup            NativeBackup
}

// VerifyOptions identifies a published outer directory and its external
// ClickHouse archive root.
type VerifyOptions struct {
	Source           string
	ArchiveRoot      string
	ArchiveOwnership ArchiveOwnershipPolicy
	Release          controlbackup.ReleaseIdentity
}

// Verification returns both the parsed canonical manifest and the digest used
// by the durable ClickHouse recovery receipt.
type Verification struct {
	Manifest       Manifest
	ManifestSHA256 string
}

// PublicationStatusError reports a failure after the atomic outer-directory
// publication completed or may have completed. Callers must preserve both the
// possible destination/stage and its archive and independently reconcile them;
// this state is not an unpublished orphan eligible for archive-only deletion.
type PublicationStatusError struct {
	Destination string
	Outcome     privatefs.RenameNoReplaceOutcome
	Err         error
}

func (err *PublicationStatusError) Error() string {
	if err == nil {
		return "create deployment recovery set: publication status is unavailable"
	}
	return fmt.Sprintf(
		"create deployment recovery set: publication completed or may have occurred at %q; preserve all candidate directories and do not delete its ClickHouse archive; independently reconcile the set: %v",
		err.Destination,
		err.Err,
	)
}

func (err *PublicationStatusError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

type createHooks struct {
	afterPublication   func() error
	createControlPlane func(
		context.Context,
		controlbackup.CreateOptions,
	) (controlbackup.Manifest, *privatefs.Directory, error)
	publish func(
		*privatefs.Directory,
		string,
		*privatefs.Directory,
		*privatefs.Directory,
		string,
	) (privatefs.RenameNoReplaceOutcome, error)
}

// Create builds and verifies an outer recovery set before publishing its
// two-member directory atomically. Any archive created by this attempt remains
// external. If publication does not occur, Create attempts an exact pinned
// removal and joins any cleanup failure into the returned error so an
// operator can identify the exact failed-attempt archive before explicitly
// attesting to its deletion through the archive-owner one-shot.
func Create(
	ctx context.Context,
	options CreateOptions,
) (Verification, error) {
	return createWithHooks(ctx, options, createHooks{})
}

func createWithHooks(
	ctx context.Context,
	options CreateOptions,
	hooks createHooks,
) (verification Verification, returnedErr error) {
	publicationMayHaveOccurred := false
	publicationOutcome := privatefs.RenameNoReplaceNotAttempted
	defer func() {
		if !publicationMayHaveOccurred || returnedErr == nil {
			return
		}
		if _, ok := errors.AsType[*PublicationStatusError](returnedErr); ok {
			return
		}
		returnedErr = &PublicationStatusError{
			Destination: options.Destination,
			Outcome:     publicationOutcome,
			Err:         returnedErr,
		}
	}()
	if ctx == nil {
		return Verification{}, errors.New("create deployment recovery set: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Verification{}, err
	}
	if options.NativeBackup == nil {
		return Verification{}, errors.New(
			"create deployment recovery set: native ClickHouse backup callback is required",
		)
	}
	if err := validateReleaseIdentity(options.Release); err != nil {
		return Verification{}, err
	}
	if err := validateArchiveOwnershipPolicy(options.ArchiveOwnership); err != nil {
		return Verification{}, err
	}
	for _, path := range []struct {
		label string
		value string
	}{
		{label: "control database", value: options.DatabasePath},
		{label: "master key", value: options.MasterKeyPath},
		{label: "administrator token", value: options.AdministratorTokenPath},
		{label: "destination", value: options.Destination},
	} {
		if err := controlbackup.ValidateExactAbsolutePath(path.label, path.value); err != nil {
			return Verification{}, fmt.Errorf("create deployment recovery set: %w", err)
		}
	}
	destinationParentPath := filepath.Dir(options.Destination)
	destinationName := filepath.Base(options.Destination)
	if _, err := os.Lstat(options.Destination); err == nil {
		return Verification{}, errors.New(
			"create deployment recovery set: destination already exists",
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: inspect destination: %w",
			err,
		)
	}
	destinationParent, err := privatefs.OpenDirectory(destinationParentPath)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: open destination parent: %w",
			err,
		)
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, destinationParent.Close())
	}()
	// Publication renames the staged set into place without replacement; a
	// filesystem that refuses that primitive must be reported before any copy.
	if err := privatefs.RequireRenameNoReplace(
		destinationParent,
		"recovery-set destination directory",
	); err != nil {
		return Verification{}, fmt.Errorf("create deployment recovery set: %w", err)
	}
	archiveRoot, err := openArchiveDirectory(
		options.ArchiveRoot,
		options.ArchiveOwnership,
	)
	if err != nil {
		return Verification{}, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, archiveRoot.close()) }()

	stageName, stage, err := destinationParent.CreateTemporaryDirectory(
		privatefs.RandomName(".deployment-recovery-set-tmp-"),
	)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set staging directory: %w",
			err,
		)
	}
	stageExists := true
	stageOpen := true
	preserveStage := false
	var child *privatefs.Directory
	defer func() {
		if preserveStage || !stageExists {
			if child != nil {
				returnedErr = errors.Join(returnedErr, child.Close())
				child = nil
			}
			if stageOpen {
				returnedErr = errors.Join(returnedErr, stage.Close())
				stageOpen = false
			}
			return
		}
		cleanupErr := cleanupOuterStage(stage, destinationParent, stageName, child)
		child = nil
		stageOpen = false
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()

	childPath := filepath.Join(stage.Path(), controlPlaneDirectory)
	createControlPlane := hooks.createControlPlane
	if createControlPlane == nil {
		createControlPlane = controlbackup.CreatePinned
	}
	childManifest, child, err := createControlPlane(ctx, controlbackup.CreateOptions{
		DatabasePath:            options.DatabasePath,
		MasterKeyPath:           options.MasterKeyPath,
		AdministratorTokenPath:  options.AdministratorTokenPath,
		SearchArtifactDirectory: options.SearchArtifactDirectory,
		Destination:             childPath,
		Release:                 options.Release,
	})
	if err != nil {
		if _, ok := errors.AsType[*controlbackup.PublicationStatusError](err); ok {
			preserveStage = true
			return Verification{}, fmt.Errorf(
				"create deployment recovery set control-plane member: child publication completed or may have occurred inside outer staging directory %q; preserve the entire outer stage and independently reconcile every child candidate: %w",
				stage.Path(),
				err,
			)
		}
		return Verification{}, fmt.Errorf(
			"create deployment recovery set control-plane member: %w",
			err,
		)
	}

	archiveName := clickHouseArchiveName(childManifest.RecoverySetID)
	archiveExisted, err := archiveRoot.pathExists(archiveName)
	if err != nil {
		return Verification{}, err
	}
	if archiveExisted {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: external ClickHouse archive %q already exists",
			archiveName,
		)
	}
	var archive *pinnedArchive
	removeArchiveOnFailure := false
	defer func() {
		if archive != nil {
			var cleanupErr error
			if removeArchiveOnFailure {
				_, cleanupErr = archiveRoot.removePinnedArchive(archiveName, archive)
			}
			returnedErr = errors.Join(
				returnedErr,
				cleanupErr,
				archive.close(),
			)
			archive = nil
		}
	}()

	request := NativeBackupRequest{
		RecoverySetID:   childManifest.RecoverySetID,
		Disk:            clickHouseRecoveryDisk,
		Database:        clickHouseDatabase,
		ArchiveDatabase: clickHouseArchiveDatabase(childManifest.RecoverySetID),
		ArchiveName:     archiveName,
		ArchivePath:     filepath.Join(options.ArchiveRoot, archiveName),
	}
	nativeIdentity, backupErr := options.NativeBackup(ctx, request)
	archivePresent, err := archiveRoot.pathExists(archiveName)
	if err != nil {
		return Verification{}, errors.Join(backupErr, err)
	}
	if backupErr != nil {
		if archivePresent {
			return Verification{}, fmt.Errorf(
				"create deployment recovery set ClickHouse member: native backup failed and archive name %q now exists with ambiguous ownership; it was preserved for the explicit confirmation-bound archive cleanup path: %w",
				archiveName,
				backupErr,
			)
		}
		return Verification{}, fmt.Errorf(
			"create deployment recovery set ClickHouse member: %w",
			backupErr,
		)
	}
	if !archivePresent {
		return Verification{}, errors.New(
			"create deployment recovery set ClickHouse member: callback created no archive",
		)
	}
	archiveIdentity, pinnedArchive, err := archiveRoot.inspectArchivePinned(
		ctx,
		archiveName,
		1,
		maximumClickHouseArchiveBytes,
	)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: inspect successful native archive without acquiring cleanup ownership; preserve archive %q for explicit confirmation-bound cleanup: %w",
			archiveName,
			err,
		)
	}
	archive = pinnedArchive
	removeArchiveOnFailure = true
	if err := ctx.Err(); err != nil {
		return Verification{}, err
	}
	if err := archiveRoot.sync(); err != nil {
		return Verification{}, err
	}
	controlManifestIdentity, err := inspectControlPlaneManifest(ctx, childPath)
	if err != nil {
		return Verification{}, err
	}

	manifest := Manifest{
		FormatVersion:        manifestFormatVersion,
		CreatedAtUnixMicro:   childManifest.CreatedAtUnixMicro,
		RecoverySetID:        childManifest.RecoverySetID,
		Scope:                deploymentRecoverySetScope,
		ClickHouseIncluded:   true,
		SourceRevision:       childManifest.SourceRevision,
		SQLiteMigrations:     childManifest.SQLiteMigrations,
		ClickHouseMigrations: childManifest.ClickHouseMigrations,
		ControlPlane: ControlPlaneIdentity{
			Directory: controlPlaneDirectory,
			Manifest:  controlManifestIdentity,
		},
		ClickHouse: ClickHouseIdentity{
			ServerVersion:                   nativeIdentity.ServerVersion,
			Disk:                            clickHouseRecoveryDisk,
			Database:                        clickHouseDatabase,
			ArchiveDatabase:                 request.ArchiveDatabase,
			ArchiveFormat:                   clickHouseArchiveFormat,
			Archive:                         archiveIdentity,
			MigrationLedgerSHA256:           nativeIdentity.MigrationLedgerSHA256,
			DatabaseUUID:                    nativeIdentity.DatabaseUUID,
			SchemaMigrationsTableUUID:       nativeIdentity.SchemaMigrationsTableUUID,
			EventsTableUUID:                 nativeIdentity.EventsTableUUID,
			RecoverySetsTableUUID:           nativeIdentity.RecoverySetsTableUUID,
			RecoveryArchiveMarkersTableUUID: nativeIdentity.RecoveryArchiveMarkersTableUUID,
			MaxVisibilitySeq:                nativeIdentity.MaxVisibilitySeq,
			BackupOperationUUID:             nativeIdentity.BackupOperationUUID,
		},
	}
	encodedManifest, err := marshalManifest(manifest)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set manifest: %w",
			err,
		)
	}
	if err := writePrivateFile(ctx, stage, manifestFilename, encodedManifest); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set manifest: %w",
			err,
		)
	}
	if err := stage.RequireEntries(outerMemberNames, len(outerMemberNames)); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set staged entries: %w",
			err,
		)
	}
	if err := stage.Sync(); err != nil {
		return Verification{}, err
	}
	verification, err = Verify(ctx, VerifyOptions{
		Source:           stage.Path(),
		ArchiveRoot:      options.ArchiveRoot,
		ArchiveOwnership: options.ArchiveOwnership,
		Release:          options.Release,
	})
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: verify staged set: %w",
			err,
		)
	}
	stageInfo, err := stage.PinnedInfo()
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: inspect pinned verified stage: %w",
			err,
		)
	}
	if err := ctx.Err(); err != nil {
		return Verification{}, err
	}
	if err := stage.RequirePinnedChildDirectory(controlPlaneDirectory, child); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: control-plane child identity changed before publication: %w",
			err,
		)
	}
	if _, err := archiveRoot.requirePinnedArchive(archiveName, archive); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: ClickHouse archive identity changed before publication: %w",
			err,
		)
	}
	var outcome privatefs.RenameNoReplaceOutcome
	if hooks.publish == nil {
		outcome, err = destinationParent.RenameDirectoryNoReplaceWithStatus(
			stageName,
			stage,
			destinationParent,
			destinationName,
		)
	} else {
		outcome, err = hooks.publish(
			destinationParent,
			stageName,
			stage,
			destinationParent,
			destinationName,
		)
	}
	if outcome == privatefs.RenameNoReplaceCompleted ||
		outcome == privatefs.RenameNoReplaceAmbiguous {
		publicationMayHaveOccurred = true
		publicationOutcome = outcome
		stageExists = false
		removeArchiveOnFailure = false
	}
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: publish outer directory: %w",
			err,
		)
	}
	if outcome != privatefs.RenameNoReplaceCompleted {
		return Verification{}, errors.New(
			"create deployment recovery set: publish outer directory returned without a completed rename",
		)
	}
	if hooks.afterPublication != nil {
		if err := hooks.afterPublication(); err != nil {
			return Verification{}, fmt.Errorf(
				"create deployment recovery set: complete published boundary: %w",
				err,
			)
		}
	}
	if err := stage.Close(); err != nil {
		stageOpen = false
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: close published stage: %w",
			err,
		)
	}
	stageOpen = false
	if err := destinationParent.Sync(); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: sync publication: %w",
			err,
		)
	}
	publishedInfo, err := os.Lstat(options.Destination)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: inspect publication: %w",
			err,
		)
	}
	if !os.SameFile(stageInfo, publishedInfo) {
		return Verification{}, errors.New(
			"create deployment recovery set: published directory identity changed",
		)
	}
	published, err := Verify(ctx, VerifyOptions{
		Source:           options.Destination,
		ArchiveRoot:      options.ArchiveRoot,
		ArchiveOwnership: options.ArchiveOwnership,
		Release:          options.Release,
	})
	if err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: verify publication: %w",
			err,
		)
	}
	if _, err := archiveRoot.requirePinnedArchive(archiveName, archive); err != nil {
		return Verification{}, fmt.Errorf(
			"create deployment recovery set: ClickHouse archive identity changed after publication: %w",
			err,
		)
	}
	return published, nil
}

// Verify authenticates the canonical outer manifest, unchanged controlbackup
// v1 child, and external native archive without mutating either root.
func Verify(
	ctx context.Context,
	options VerifyOptions,
) (verification Verification, returnedErr error) {
	if ctx == nil {
		return Verification{}, errors.New("verify deployment recovery set: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Verification{}, err
	}
	if err := validateReleaseIdentity(options.Release); err != nil {
		return Verification{}, err
	}
	if err := validateArchiveOwnershipPolicy(options.ArchiveOwnership); err != nil {
		return Verification{}, err
	}
	if err := controlbackup.ValidateExactAbsolutePath(
		"deployment recovery-set source",
		options.Source,
	); err != nil {
		return Verification{}, err
	}
	outer, err := privatefs.OpenDirectory(options.Source)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"verify deployment recovery set: open outer directory: %w",
			err,
		)
	}
	defer func() { returnedErr = errors.Join(returnedErr, outer.Close()) }()
	archiveRoot, err := openArchiveDirectory(
		options.ArchiveRoot,
		options.ArchiveOwnership,
	)
	if err != nil {
		return Verification{}, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, archiveRoot.close()) }()
	if err := outer.RequireEntries(outerMemberNames, len(outerMemberNames)); err != nil {
		return Verification{}, fmt.Errorf(
			"verify deployment recovery set: %w",
			err,
		)
	}
	manifestFile, err := inspectPrivateFile(
		ctx,
		outer,
		manifestFilename,
		1,
		maximumManifestBytes,
		true,
	)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"verify deployment recovery set manifest: %w",
			err,
		)
	}
	manifest, err := unmarshalManifest(manifestFile.contents)
	if err != nil {
		return Verification{}, err
	}
	if err := validateManifestRelease(manifest, options.Release); err != nil {
		return Verification{}, err
	}
	childPath := filepath.Join(options.Source, controlPlaneDirectory)
	childManifest, err := controlbackup.Verify(ctx, childPath, options.Release)
	if err != nil {
		return Verification{}, fmt.Errorf(
			"verify deployment recovery set control-plane member: %w",
			err,
		)
	}
	if childManifest.RecoverySetID != manifest.RecoverySetID ||
		childManifest.CreatedAtUnixMicro != manifest.CreatedAtUnixMicro ||
		childManifest.SourceRevision != manifest.SourceRevision ||
		childManifest.SQLiteMigrations != manifest.SQLiteMigrations ||
		childManifest.ClickHouseMigrations != manifest.ClickHouseMigrations {
		return Verification{}, errors.New(
			"verify deployment recovery set: control-plane identity differs from outer manifest",
		)
	}
	controlManifestIdentity, err := inspectControlPlaneManifest(ctx, childPath)
	if err != nil {
		return Verification{}, err
	}
	if controlManifestIdentity != manifest.ControlPlane.Manifest {
		return Verification{}, errors.New(
			"verify deployment recovery set: control-plane manifest differs from outer manifest",
		)
	}
	archiveIdentity, err := archiveRoot.inspectArchive(
		ctx,
		manifest.ClickHouse.Archive.Name,
		manifest.ClickHouse.Archive.SizeBytes,
		manifest.ClickHouse.Archive.SizeBytes,
	)
	if err != nil {
		return Verification{}, err
	}
	if archiveIdentity != manifest.ClickHouse.Archive {
		return Verification{}, errors.New(
			"verify deployment recovery set: ClickHouse archive differs from outer manifest",
		)
	}
	if err := outer.RequireEntries(outerMemberNames, len(outerMemberNames)); err != nil {
		return Verification{}, fmt.Errorf(
			"verify deployment recovery set after member inspection: %w",
			err,
		)
	}
	if err := outer.Revalidate(); err != nil {
		return Verification{}, err
	}
	if err := archiveRoot.revalidate(); err != nil {
		return Verification{}, err
	}
	digest := sha256.Sum256(manifestFile.contents)
	return Verification{
		Manifest:       manifest,
		ManifestSHA256: hex.EncodeToString(digest[:]),
	}, nil
}

func inspectControlPlaneManifest(
	ctx context.Context,
	childPath string,
) (identity FileIdentity, returnedErr error) {
	child, err := privatefs.OpenDirectory(childPath)
	if err != nil {
		return FileIdentity{}, fmt.Errorf(
			"inspect deployment recovery-set control-plane directory: %w",
			err,
		)
	}
	defer func() { returnedErr = errors.Join(returnedErr, child.Close()) }()
	member, err := inspectPrivateFile(
		ctx,
		child,
		"manifest.json",
		1,
		int64(maximumControlManifestBytes),
		false,
	)
	if err != nil {
		return FileIdentity{}, fmt.Errorf(
			"inspect deployment recovery-set control-plane manifest: %w",
			err,
		)
	}
	member.identity.Name = controlPlaneManifestName
	return member.identity, nil
}

func cleanupOuterStage(
	stage *privatefs.Directory,
	parent *privatefs.Directory,
	stageName string,
	child *privatefs.Directory,
) (returnedErr error) {
	if stage == nil {
		return nil
	}
	stageOpen := true
	childOpen := child != nil
	var manifestFile *os.File
	defer func() {
		if manifestFile != nil {
			returnedErr = errors.Join(returnedErr, manifestFile.Close())
		}
		if childOpen {
			returnedErr = errors.Join(returnedErr, child.Close())
		}
		if stageOpen {
			returnedErr = errors.Join(returnedErr, stage.Close())
		}
	}()

	entries, err := stage.List(len(outerMemberNames))
	if err != nil {
		return fmt.Errorf("clean deployment recovery-set outer stage: inventory: %w", err)
	}
	for _, entry := range entries {
		if !slices.Contains(outerMemberNames, entry) {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: unexpected entry %q",
				entry,
			)
		}
	}
	hasChild := slices.Contains(entries, controlPlaneDirectory)
	hasManifest := slices.Contains(entries, manifestFilename)
	if hasChild != (child != nil) {
		return errors.New(
			"clean deployment recovery-set outer stage: pinned control-plane child does not match the inventoried namespace",
		)
	}
	if hasChild {
		if err := controlbackup.ValidateCreatedBundleForRemoval(
			stage,
			controlPlaneDirectory,
			child,
		); err != nil {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: preflight control-plane child: %w",
				err,
			)
		}
	}
	if hasManifest {
		manifestFile, err = stage.OpenRegular(manifestFilename, privatefs.FilePolicy{
			AllowedModes: privateFileModes,
			MinimumSize:  0,
			MaximumSize:  maximumManifestBytes,
		})
		if err != nil {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: preflight manifest: %w",
				err,
			)
		}
		if err := stage.RequirePinnedRegular(manifestFilename, manifestFile); err != nil {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: bind manifest: %w",
				err,
			)
		}
	}

	if hasChild {
		if err := controlbackup.RemoveCreatedBundle(
			stage,
			controlPlaneDirectory,
			child,
		); err != nil {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: remove control-plane child: %w",
				err,
			)
		}
		if err := child.Close(); err != nil {
			childOpen = false
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: close removed control-plane child: %w",
				err,
			)
		}
		childOpen = false
	}
	if hasManifest {
		if err := stage.UnlinkPinnedRegular(manifestFilename, manifestFile); err != nil {
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: remove manifest: %w",
				err,
			)
		}
		if err := manifestFile.Close(); err != nil {
			manifestFile = nil
			return fmt.Errorf(
				"clean deployment recovery-set outer stage: close removed manifest: %w",
				err,
			)
		}
		manifestFile = nil
	}
	if err := stage.Sync(); err != nil {
		return fmt.Errorf("clean deployment recovery-set outer stage: sync: %w", err)
	}
	if err := parent.RemovePinnedEmptyDirectory(stageName, stage); err != nil {
		return fmt.Errorf("clean deployment recovery-set outer stage: remove directory: %w", err)
	}
	if err := stage.Close(); err != nil {
		stageOpen = false
		return fmt.Errorf("clean deployment recovery-set outer stage: close removed directory: %w", err)
	}
	stageOpen = false
	return nil
}
