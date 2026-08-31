//go:build darwin || linux

package controlbackup

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	"golang.org/x/sys/unix"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/migrations"
)

var bundleMemberNames = []string{
	administratorTokenFilename,
	databaseFilename,
	manifestFilename,
	masterKeyFilename,
}

var bundleStageCleanupNames = []string{
	administratorTokenFilename,
	databaseFilename,
	databaseFilename + "-journal",
	databaseFilename + "-shm",
	databaseFilename + "-wal",
	manifestFilename,
	masterKeyFilename,
	searchArtifactsFilename,
}

func bundleNames(manifest Manifest) []string {
	names := append([]string(nil), bundleMemberNames...)
	if manifest.SearchArtifacts != nil {
		names = append(names, searchArtifactsFilename)
	}
	return names
}

// ErrDeploymentBindingMismatch identifies a verified control-plane child that
// is valid on its own but does not match the outer deployment recovery set.
var ErrDeploymentBindingMismatch = errors.New(
	"controlbackup: deployment recovery binding mismatch",
)

// PublicationStatusError reports a control-plane bundle publication that
// completed or may have completed before Create failed. Cleanup must preserve
// every candidate stage/destination until an operator independently reconciles
// the result.
type PublicationStatusError struct {
	Destination string
	Outcome     privatefs.RenameNoReplaceOutcome
	Err         error
}

func (err *PublicationStatusError) Error() string {
	if err == nil {
		return "create control-plane backup: publication status is unavailable"
	}
	return fmt.Sprintf(
		"create control-plane backup: publication completed or may have occurred at %q; preserve and independently reconcile all candidate bundles: %v",
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

// CreateOptions identifies one stopped server's control-plane state and a new
// bundle directory. The caller must hold the supported server lock for the
// complete operation.
type CreateOptions struct {
	DatabasePath            string
	MasterKeyPath           string
	AdministratorTokenPath  string
	SearchArtifactDirectory string
	Destination             string
	Release                 ReleaseIdentity
}

// RestoreOptions identifies one verified bundle and three runtime targets in
// a single owner-private directory. Restore never overwrites mismatched state.
// DatabaseLock is optional. When set, it must be the caller-owned open file
// descriptor for the supported lock at exactly DatabasePath + ".server.lock".
// The caller retains ownership of the descriptor and must keep it open for the
// complete operation. Restore retains its exclusive flock and repeatedly
// proves that the exact pathname still names the same secure inode.
// ExpectedRecoverySetID and ExpectedManifestSHA256 are an optional pair used
// by deployment recovery to bind this child to its already-verified outer
// manifest. Control-plane-only restores leave both empty.
type RestoreOptions struct {
	Source                  string
	DatabasePath            string
	DatabaseLock            *os.File
	MasterKeyPath           string
	AdministratorTokenPath  string
	SearchArtifactDirectory string
	Release                 ReleaseIdentity
	ExpectedRecoverySetID   string
	ExpectedManifestSHA256  string
}

type createHooks struct {
	now             func() time.Time
	random          io.Reader
	stageName       privatefs.NameGenerator
	afterStageSync  func()
	retainPublished func(*privatefs.Directory)
	beforePublish   func()
	afterRename     func(*privatefs.Directory) error
	publish         func(
		*privatefs.Directory,
		string,
		*privatefs.Directory,
		*privatefs.Directory,
		string,
	) (privatefs.RenameNoReplaceOutcome, error)
}

type restoreHooks struct {
	beforePublication              func(index int)
	afterDatabasePublish           func() error
	afterMasterKeyPublish          func() error
	afterAdministratorTokenPublish func() error
}

type restoreMember struct {
	identity     FileIdentity
	stageName    string
	finalName    string
	afterPublish func() error
}

type restoreTargets struct {
	parentPath             string
	databaseName           string
	databaseLockName       string
	masterKeyName          string
	administratorTokenName string
	searchArtifactParent   string
	searchArtifactName     string
}

type restorePlan struct {
	source              *privatefs.Directory
	destination         *privatefs.Directory
	artifactDestination *privatefs.Directory
	manifest            Manifest
	targets             restoreTargets
	stageNames          []string
	finalNames          []string
	namespaceNames      []string
	knownNames          []string
	artifactStageName   string
}

func (plan *restorePlan) close() error {
	if plan == nil {
		return nil
	}
	var sourceErr error
	if plan.source != nil {
		sourceErr = plan.source.Close()
		plan.source = nil
	}
	var destinationErr error
	if plan.destination != nil {
		destinationErr = plan.destination.Close()
		plan.destination = nil
	}
	var artifactDestinationErr error
	if plan.artifactDestination != nil {
		artifactDestinationErr = plan.artifactDestination.Close()
		plan.artifactDestination = nil
	}
	return errors.Join(sourceErr, destinationErr, artifactDestinationErr)
}

func (plan *restorePlan) searchArtifactDestination() *privatefs.Directory {
	if plan.artifactDestination != nil {
		return plan.artifactDestination
	}
	return plan.destination
}

// Create produces and verifies one atomic directory bundle. It never copies a
// live SQLite file: the native backup API absorbs committed WAL state into the
// self-contained database member.
func Create(ctx context.Context, options CreateOptions) (Manifest, error) {
	return createWithHooks(ctx, options, createHooks{
		now:       time.Now,
		random:    rand.Reader,
		stageName: privatefs.RandomName(".control-plane-backup-tmp-"),
	})
}

// CreatePinned creates a verified bundle and returns a live descriptor for the
// exact published directory. The caller must close the descriptor. Recovery-set
// orchestration keeps it open so rollback can never confuse a replacement at
// the fixed child name with the bundle created by this attempt.
func CreatePinned(
	ctx context.Context,
	options CreateOptions,
) (manifest Manifest, published *privatefs.Directory, returnedErr error) {
	manifest, returnedErr = createWithHooks(ctx, options, createHooks{
		now:       time.Now,
		random:    rand.Reader,
		stageName: privatefs.RandomName(".control-plane-backup-tmp-"),
		retainPublished: func(directory *privatefs.Directory) {
			published = directory
		},
	})
	if returnedErr != nil && published != nil {
		returnedErr = errors.Join(returnedErr, published.Close())
		published = nil
	}
	return manifest, published, returnedErr
}

func createWithHooks(
	ctx context.Context,
	options CreateOptions,
	hooks createHooks,
) (manifest Manifest, returnedErr error) {
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
		return Manifest{}, errors.New("create control-plane backup: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := validateReleaseIdentity(options.Release); err != nil {
		return Manifest{}, err
	}
	if hooks.now == nil || hooks.random == nil || hooks.stageName == nil {
		return Manifest{}, errors.New("create control-plane backup: production dependencies are required")
	}
	destinationParentPath, destinationName, err := validateExactAbsolutePath(
		"control-plane backup destination",
		options.Destination,
	)
	if err != nil {
		return Manifest{}, err
	}
	if _, err := os.Lstat(options.Destination); err == nil {
		return Manifest{}, errors.New("create control-plane backup: destination already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, fmt.Errorf("create control-plane backup: inspect destination: %w", err)
	}
	destinationParent, err := privatefs.OpenDirectory(destinationParentPath)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: open destination parent: %w", err)
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, destinationParent.Close())
	}()
	// Publication renames the staged bundle into place without replacement; a
	// filesystem that refuses that primitive must be reported before any copy.
	// The probe uses the staging name space, so a probe file orphaned by a
	// crash is recognizable junk in the parent and never collides with a stage.
	if err := privatefs.RequireRenameNoReplace(
		destinationParent,
		"backup destination directory",
		privatefs.RandomName(".control-plane-backup-tmp-"),
	); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: %w", err)
	}

	masterKey, err := readSourceMasterKey(ctx, options.MasterKeyPath)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(masterKey)
	administratorToken, err := readSourceAdministratorToken(ctx, options.AdministratorTokenPath)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(administratorToken)

	databaseDirectory, databaseName, err := openPrivatePath(
		"control-plane database",
		options.DatabasePath,
	)
	if err != nil {
		return Manifest{}, err
	}
	defer databaseDirectory.Close()
	databasePolicy := privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  1,
		MaximumSize:  int64(maximumDatabaseBytes),
	}
	pinnedDatabase, err := databaseDirectory.OpenRegular(databaseName, databasePolicy)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: %w", err)
	}
	defer pinnedDatabase.Close()
	pinnedDatabaseInfo, err := pinnedDatabase.Stat()
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: inspect database: %w", err)
	}

	sourceDatabase, sourceMigrationIdentity, err := openVerifiedDatabase(
		ctx,
		options.DatabasePath,
		masterKey,
		options.Release,
	)
	if err != nil {
		return Manifest{}, err
	}
	databaseOpen := true
	defer func() {
		if databaseOpen {
			returnedErr = errors.Join(returnedErr, sourceDatabase.Close())
		}
	}()

	stageName, stage, err := destinationParent.CreateTemporaryDirectory(hooks.stageName)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup staging directory: %w", err)
	}
	stageExists := true
	stageOpen := true
	defer func() {
		if !stageExists {
			if stageOpen {
				returnedErr = errors.Join(returnedErr, stage.Close())
				stageOpen = false
			}
			return
		}
		cleanupErr := RemoveCreatedBundle(destinationParent, stageName, stage)
		cleanupErr = errors.Join(cleanupErr, stage.Close())
		stageOpen = false
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()

	backupPath := filepath.Join(stage.Path(), databaseFilename)
	if err := sourceDatabase.BackupTo(ctx, backupPath); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup SQLite snapshot: %w", err)
	}
	if err := sourceDatabase.Close(); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: close source database: %w", err)
	}
	databaseOpen = false
	reopenedDatabase, err := databaseDirectory.OpenRegular(databaseName, databasePolicy)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: reinspect source database: %w", err)
	}
	reopenedDatabaseInfo, statErr := reopenedDatabase.Stat()
	closeErr := reopenedDatabase.Close()
	if statErr != nil || closeErr != nil {
		return Manifest{}, errors.Join(statErr, closeErr)
	}
	if !privatefs.SameFileState(pinnedDatabaseInfo, reopenedDatabaseInfo) {
		return Manifest{}, errors.New("create control-plane backup: source database changed during snapshot")
	}

	databaseMember, err := inspectMember(ctx, stage, databaseFilename, databasePolicy, false)
	if err != nil {
		return Manifest{}, err
	}
	masterKeyMember, err := writeMember(ctx, stage, masterKeyFilename, masterKey)
	if err != nil {
		return Manifest{}, err
	}
	administratorTokenMember, err := writeMember(
		ctx,
		stage,
		administratorTokenFilename,
		administratorToken,
	)
	if err != nil {
		return Manifest{}, err
	}
	searchArtifactDirectory := options.SearchArtifactDirectory
	if searchArtifactDirectory == "" {
		candidate := options.DatabasePath + ".search-artifacts"
		if _, err := os.Lstat(candidate); err == nil {
			searchArtifactDirectory = candidate
		} else if !errors.Is(err, os.ErrNotExist) {
			return Manifest{}, fmt.Errorf("create control-plane backup: discover search artifacts: %w", err)
		}
	}
	var searchArtifacts *DirectoryIdentity
	if searchArtifactDirectory != "" {
		if _, _, err := validateExactAbsolutePath(
			"search-artifact directory",
			searchArtifactDirectory,
		); err != nil {
			return Manifest{}, err
		}
		sourceArtifacts, err := privatefs.OpenDirectory(searchArtifactDirectory)
		if err != nil {
			return Manifest{}, fmt.Errorf("create control-plane backup: open search artifacts: %w", err)
		}
		artifactIdentity, artifactStage, copyErr := copySearchArtifactDirectory(
			ctx,
			sourceArtifacts,
			stage,
			searchArtifactsFilename,
		)
		closeErr := sourceArtifacts.Close()
		if artifactStage != nil {
			closeErr = errors.Join(closeErr, artifactStage.Close())
		}
		if copyErr != nil || closeErr != nil {
			return Manifest{}, errors.Join(copyErr, closeErr)
		}
		searchArtifacts = &artifactIdentity
	}
	fingerprint, err := auth.FingerprintServerMasterKey(masterKey)
	if err != nil {
		return Manifest{}, err
	}
	recoverySetID, err := newRecoverySetID(hooks.random)
	if err != nil {
		return Manifest{}, err
	}
	createdAt := hooks.now().UTC().UnixMicro()
	formatVersion := manifestFormatVersion
	if searchArtifacts != nil {
		formatVersion = artifactManifestFormatVersion
	}
	manifest = Manifest{
		FormatVersion:               formatVersion,
		CreatedAtUnixMicro:          createdAt,
		RecoverySetID:               recoverySetID,
		Scope:                       controlPlaneOnlyScope,
		ClickHouseIncluded:          false,
		SourceRevision:              options.Release.SourceRevision,
		SQLiteMigrations:            options.Release.SQLiteMigrations,
		SQLiteMigrationLedgerSHA256: hex.EncodeToString(sourceMigrationIdentity.SHA256[:]),
		ClickHouseMigrations:        options.Release.ClickHouseMigrations,
		Database:                    databaseMember.identity,
		MasterKey:                   masterKeyMember.identity,
		AdministratorToken:          administratorTokenMember.identity,
		MasterKeyFingerprintSHA256:  hex.EncodeToString(fingerprint[:]),
		SearchArtifacts:             searchArtifacts,
	}
	encodedManifest, err := marshalManifest(manifest)
	if err != nil {
		return Manifest{}, err
	}
	if _, err := writeMember(ctx, stage, manifestFilename, encodedManifest); err != nil {
		return Manifest{}, err
	}
	memberNames := bundleNames(manifest)
	if err := stage.RequireEntries(memberNames, len(memberNames)); err != nil {
		return Manifest{}, err
	}
	if err := stage.Sync(); err != nil {
		return Manifest{}, err
	}
	if hooks.afterStageSync != nil {
		hooks.afterStageSync()
	}
	verified, err := Verify(ctx, stage.Path(), options.Release)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: verify staged bundle: %w", err)
	}
	stageInfo, err := stage.PinnedInfo()
	if err != nil {
		return Manifest{}, fmt.Errorf(
			"create control-plane backup: inspect pinned verified staging directory: %w",
			err,
		)
	}
	if hooks.beforePublish != nil {
		hooks.beforePublish()
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
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
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: publish bundle: %w", err)
	}
	if outcome != privatefs.RenameNoReplaceCompleted {
		return Manifest{}, errors.New(
			"create control-plane backup: publish bundle returned without a completed rename",
		)
	}
	if hooks.afterRename != nil {
		if err := hooks.afterRename(stage); err != nil {
			return Manifest{}, fmt.Errorf(
				"create control-plane backup: inspect live renamed stage: %w",
				err,
			)
		}
	}
	if err := destinationParent.Sync(); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: sync published bundle: %w", err)
	}
	published, err := privatefs.OpenDirectory(options.Destination)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: open published bundle: %w", err)
	}
	stableStageInfo, stageStatErr := stage.PinnedInfo()
	publishedInfo, statErr := published.PinnedInfo()
	entriesErr := published.RequireEntries(memberNames, len(memberNames))
	if stageStatErr != nil || statErr != nil || entriesErr != nil {
		publishedCloseErr := published.Close()
		return Manifest{}, fmt.Errorf(
			"create control-plane backup: verify published bundle identity: %w",
			errors.Join(stageStatErr, statErr, entriesErr, publishedCloseErr),
		)
	}
	if !os.SameFile(stageInfo, stableStageInfo) || !os.SameFile(stableStageInfo, publishedInfo) {
		return Manifest{}, errors.Join(
			errors.New("create control-plane backup: published bundle identity changed"),
			published.Close(),
		)
	}
	if err := stage.Close(); err != nil {
		stageOpen = false
		return Manifest{}, errors.Join(
			fmt.Errorf(
				"create control-plane backup: close published staging descriptor: %w",
				err,
			),
			published.Close(),
		)
	}
	stageOpen = false
	if hooks.retainPublished != nil {
		hooks.retainPublished(published)
		return verified, nil
	}
	if err := published.Close(); err != nil {
		return Manifest{}, fmt.Errorf(
			"create control-plane backup: close verified published bundle: %w",
			err,
		)
	}
	return verified, nil
}

// Verify validates the exact bounded bundle, current release compatibility,
// SQLite integrity and migration ledger, and database/master-key binding.
func Verify(
	ctx context.Context,
	source string,
	expected ReleaseIdentity,
) (manifest Manifest, returnedErr error) {
	if ctx == nil {
		return Manifest{}, errors.New("verify control-plane backup: context is required")
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := validateReleaseIdentity(expected); err != nil {
		return Manifest{}, err
	}
	if _, _, err := validateExactAbsolutePath("control-plane backup source", source); err != nil {
		return Manifest{}, err
	}
	bundle, err := privatefs.OpenDirectory(source)
	if err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup: open bundle: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, bundle.Close()) }()
	entries, err := bundle.List(len(bundleMemberNames) + 1)
	if err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup: invalid bundle entries: %w", err)
	}
	if !slices.Contains(entries, manifestFilename) {
		return Manifest{}, errors.New("verify control-plane backup: manifest is missing")
	}
	manifestMember, err := inspectMember(ctx, bundle, manifestFilename, privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  1,
		MaximumSize:  maximumManifestBytes,
	}, true)
	if err != nil {
		return Manifest{}, err
	}
	manifest, err = unmarshalManifest(manifestMember.contents)
	clear(manifestMember.contents)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifestRelease(manifest, expected); err != nil {
		return Manifest{}, err
	}
	memberNames := bundleNames(manifest)
	if err := bundle.RequireEntries(memberNames, len(memberNames)); err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup: %w", err)
	}
	if manifest.SearchArtifacts != nil {
		if err := verifySearchArtifactDirectory(
			ctx,
			bundle,
			searchArtifactsFilename,
			*manifest.SearchArtifacts,
		); err != nil {
			return Manifest{}, fmt.Errorf("verify control-plane backup search artifacts: %w", err)
		}
	}
	databaseMember, err := inspectMember(
		ctx,
		bundle,
		databaseFilename,
		mustPolicy(manifest.Database),
		false,
	)
	if err != nil {
		return Manifest{}, err
	}
	if err := requireFileIdentity(databaseMember, manifest.Database); err != nil {
		return Manifest{}, err
	}
	masterKeyMember, err := inspectMember(
		ctx,
		bundle,
		masterKeyFilename,
		mustPolicy(manifest.MasterKey),
		true,
	)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(masterKeyMember.contents)
	if err := requireFileIdentity(masterKeyMember, manifest.MasterKey); err != nil {
		return Manifest{}, err
	}
	administratorTokenMember, err := inspectMember(
		ctx,
		bundle,
		administratorTokenFilename,
		mustPolicy(manifest.AdministratorToken),
		true,
	)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(administratorTokenMember.contents)
	if err := requireFileIdentity(administratorTokenMember, manifest.AdministratorToken); err != nil {
		return Manifest{}, err
	}
	if err := auth.ValidateBrowserBearerToken(administratorTokenMember.contents); err != nil {
		return Manifest{}, errors.New("control-plane backup administrator token is invalid")
	}
	fingerprint, err := auth.FingerprintServerMasterKey(masterKeyMember.contents)
	if err != nil {
		return Manifest{}, errors.New("control-plane backup master key is invalid")
	}
	encodedFingerprint, err := hex.DecodeString(manifest.MasterKeyFingerprintSHA256)
	if err != nil || !hmac.Equal(encodedFingerprint, fingerprint[:]) {
		return Manifest{}, errors.New("control-plane backup master-key identity does not match its manifest")
	}
	databaseMigrationIdentity, err := verifyDatabase(
		ctx,
		filepath.Join(source, databaseFilename),
		masterKeyMember.contents,
		expected,
	)
	if err != nil {
		return Manifest{}, err
	}
	if hex.EncodeToString(databaseMigrationIdentity.SHA256[:]) !=
		manifest.SQLiteMigrationLedgerSHA256 {
		return Manifest{}, errors.New("control-plane backup SQLite migration ledger does not match its manifest")
	}
	if err := bundle.RequireEntries(memberNames, len(memberNames)); err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup after SQLite inspection: %w", err)
	}
	if err := bundle.Revalidate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

// PreflightRestore verifies the source, destination publication prefix,
// interrupted staging files, and optional held database lock without removing,
// creating, or publishing any file. Restore independently repeats these checks
// so a successful deployment preflight is never treated as an authorization
// for a later filesystem mutation.
func PreflightRestore(ctx context.Context, options RestoreOptions) (returnedErr error) {
	if ctx == nil {
		return errors.New("preflight control-plane restore: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := openRestorePlan(
		ctx,
		options,
		"preflight control-plane restore",
		"open source",
	)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, plan.close()) }()

	entries, err := plan.destination.List(len(plan.knownNames))
	if err != nil {
		return fmt.Errorf("preflight control-plane restore: inspect destination entries: %w", err)
	}
	for _, entry := range entries {
		if !slices.Contains(plan.knownNames, entry) {
			return fmt.Errorf("preflight control-plane restore: destination contains unrelated entry %q", entry)
		}
	}
	if err := validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		options.DatabaseLock,
	); err != nil {
		return err
	}
	if err := validateRestoreStageFiles(plan.destination, entries, plan.stageNames); err != nil {
		return err
	}
	if err := preflightRestoreSearchArtifacts(ctx, &plan, entries); err != nil {
		return err
	}
	if _, err := inspectRestorePrefix(
		ctx,
		plan.destination,
		plan.finalNames,
		plan.knownNames,
		plan.manifest,
		options.Release,
	); err != nil {
		return err
	}
	if err := plan.source.Revalidate(); err != nil {
		return fmt.Errorf("preflight control-plane restore: revalidate source: %w", err)
	}
	return validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		options.DatabaseLock,
	)
}

// Restore copies and verifies bundle members before publishing a resumable,
// fail-closed database -> master key -> administrator token prefix.
func Restore(ctx context.Context, options RestoreOptions) error {
	return restoreWithHooks(ctx, options, restoreHooks{})
}

func restoreWithHooks(
	ctx context.Context,
	options RestoreOptions,
	hooks restoreHooks,
) (returnedErr error) {
	if ctx == nil {
		return errors.New("restore control-plane backup: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	plan, err := openRestorePlan(
		ctx,
		options,
		"restore control-plane backup",
		"reopen source",
	)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, plan.close()) }()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := cleanupRestoreSearchArtifactStage(&plan, options.DatabaseLock); err != nil {
		return err
	}
	if err := cleanupRestoreStagesWithHooks(
		plan.destination,
		plan.stageNames,
		plan.namespaceNames,
		plan.targets.databaseLockName,
		options.DatabaseLock,
		cleanupRestoreStagesHooks{},
	); err != nil {
		return err
	}
	if err := probeRestoreDestinations(&plan); err != nil {
		return err
	}
	if err := ensureRestoredSearchArtifacts(ctx, &plan, options.DatabaseLock); err != nil {
		return err
	}
	prefix, err := inspectRestorePrefix(
		ctx,
		plan.destination,
		plan.finalNames,
		plan.namespaceNames,
		plan.manifest,
		options.Release,
	)
	if err != nil {
		return err
	}
	if prefix == len(plan.finalNames) {
		return requireFinalRestoreNamespace(
			plan.destination,
			plan.namespaceNames,
			plan.targets.databaseLockName,
			options.DatabaseLock,
		)
	}
	members := []restoreMember{
		{
			identity: plan.manifest.Database, stageName: plan.stageNames[0], finalName: plan.finalNames[0],
			afterPublish: hooks.afterDatabasePublish,
		},
		{
			identity: plan.manifest.MasterKey, stageName: plan.stageNames[1], finalName: plan.finalNames[1],
			afterPublish: hooks.afterMasterKeyPublish,
		},
		{
			identity: plan.manifest.AdministratorToken, stageName: plan.stageNames[2], finalName: plan.finalNames[2],
			afterPublish: hooks.afterAdministratorTokenPublish,
		},
	}

	remainingStages := make([]string, 0, len(plan.stageNames)-prefix)
	defer func() {
		cleanupErr := cleanupKnownFiles(plan.destination, remainingStages)
		if cleanupErr == nil && len(remainingStages) != 0 {
			cleanupErr = plan.destination.Sync()
		}
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()
	for index := prefix; index < len(members); index++ {
		member := members[index]
		if err := validateRestoreDatabaseLock(
			plan.destination,
			plan.targets.databaseLockName,
			options.DatabaseLock,
		); err != nil {
			return err
		}
		remainingStages = append(remainingStages, member.stageName)
		if err := copyMemberWithHooks(
			ctx,
			plan.source,
			member.identity.Name,
			plan.destination,
			member.stageName,
			member.identity,
			copyMemberHooks{},
		); err != nil {
			return fmt.Errorf("restore control-plane backup: stage %q: %w", member.identity.Name, err)
		}
	}
	resolvedNames := make([]string, len(members))
	for index, member := range members {
		if index < prefix {
			resolvedNames[index] = member.finalName
		} else {
			resolvedNames[index] = member.stageName
		}
	}
	if err := verifyRestoreMembers(
		ctx,
		plan.destination,
		resolvedNames,
		plan.manifest,
		options.Release,
	); err != nil {
		return err
	}
	if err := plan.destination.Sync(); err != nil {
		return fmt.Errorf("restore control-plane backup: sync staging files: %w", err)
	}
	if err := validateRestoreDatabaseLock(
		plan.destination,
		plan.targets.databaseLockName,
		options.DatabaseLock,
	); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	for index := prefix; index < len(members); index++ {
		member := members[index]
		if hooks.beforePublication != nil {
			hooks.beforePublication(index)
		}
		if err := validateRestoreDatabaseLock(
			plan.destination,
			plan.targets.databaseLockName,
			options.DatabaseLock,
		); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := plan.destination.RenameNoReplace(
			member.stageName,
			plan.destination,
			member.finalName,
		); err != nil {
			return fmt.Errorf("restore control-plane backup: publish %q: %w", member.identity.Name, err)
		}
		remainingStages = slices.DeleteFunc(remainingStages, func(name string) bool {
			return name == member.stageName
		})
		if err := plan.destination.Sync(); err != nil {
			return fmt.Errorf("restore control-plane backup: sync %q publication: %w", member.identity.Name, err)
		}
		if err := validateRestoreDatabaseLock(
			plan.destination,
			plan.targets.databaseLockName,
			options.DatabaseLock,
		); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if member.afterPublish != nil {
			if err := member.afterPublish(); err != nil {
				return err
			}
		}
	}
	return requireFinalRestoreNamespace(
		plan.destination,
		plan.namespaceNames,
		plan.targets.databaseLockName,
		options.DatabaseLock,
	)
}

func readSourceMasterKey(ctx context.Context, path string) ([]byte, error) {
	directory, name, err := openPrivatePath("server master key", path)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	member, err := inspectMember(ctx, directory, name, privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  auth.ServerMasterKeyBytes,
		MaximumSize:  auth.ServerMasterKeyBytes,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("read server master key for backup: %w", err)
	}
	if _, err := auth.FingerprintServerMasterKey(member.contents); err != nil {
		clear(member.contents)
		return nil, errors.New("read server master key for backup: invalid key")
	}
	return member.contents, nil
}

func readSourceAdministratorToken(ctx context.Context, path string) ([]byte, error) {
	directory, name, err := openPrivatePath("administrator token", path)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	member, err := inspectMember(ctx, directory, name, privatefs.FilePolicy{
		AllowedModes: []os.FileMode{0o400, 0o600},
		MinimumSize:  auth.MinimumBrowserBearerTokenBytes,
		MaximumSize:  auth.MaximumBrowserBearerTokenBytes + 2,
	}, true)
	if err != nil {
		return nil, fmt.Errorf("read administrator token for backup: %w", err)
	}
	contents := member.contents
	if len(contents) != 0 && contents[len(contents)-1] == '\n' {
		contents = contents[:len(contents)-1]
		if len(contents) != 0 && contents[len(contents)-1] == '\r' {
			contents = contents[:len(contents)-1]
		}
	}
	if err := auth.ValidateBrowserBearerToken(contents); err != nil {
		clear(member.contents)
		return nil, errors.New("read administrator token for backup: invalid bearer token")
	}
	result := append([]byte(nil), contents...)
	clear(member.contents)
	return result, nil
}

func verifyDatabase(
	ctx context.Context,
	path string,
	masterKey []byte,
	release ReleaseIdentity,
) (identity control.MigrationIdentity, returnedErr error) {
	database, identity, err := openVerifiedDatabase(ctx, path, masterKey, release)
	if err != nil {
		return control.MigrationIdentity{}, err
	}
	return identity, database.Close()
}

func openVerifiedDatabase(
	ctx context.Context,
	path string,
	masterKey []byte,
	release ReleaseIdentity,
) (*control.DB, control.MigrationIdentity, error) {
	database, err := control.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, control.MigrationIdentity{}, fmt.Errorf("verify control-plane backup database: %w", err)
	}
	closeOnError := func(openErr error) (*control.DB, control.MigrationIdentity, error) {
		return nil, control.MigrationIdentity{}, errors.Join(openErr, database.Close())
	}
	identity, err := database.VerifyCurrentMigrations(ctx, migrations.SQLite())
	if err != nil {
		return closeOnError(fmt.Errorf("verify control-plane backup migrations: %w", err))
	}
	if identity.LatestVersion != release.SQLiteMigrations.LatestVersion {
		return closeOnError(errors.New("control-plane backup migration version does not match the release"))
	}
	if err := database.VerifyIntegrity(ctx); err != nil {
		return closeOnError(fmt.Errorf("verify control-plane backup SQLite integrity: %w", err))
	}
	stored, registered, err := auth.ReadServerMasterKeyIdentity(ctx, database)
	if err != nil {
		return closeOnError(fmt.Errorf("verify control-plane backup master-key identity: %w", err))
	}
	if !registered {
		return closeOnError(errors.New("control-plane backup database has no registered server master-key identity"))
	}
	fingerprint, err := auth.FingerprintServerMasterKey(masterKey)
	if err != nil {
		return closeOnError(errors.New("control-plane backup master key is invalid"))
	}
	if !hmac.Equal(stored, fingerprint[:]) {
		return closeOnError(errors.New("control-plane backup master key does not match its database"))
	}
	return database, identity, nil
}

func newRecoverySetID(random io.Reader) (string, error) {
	var value [recoverySetIDBytes]byte
	if _, err := io.ReadFull(random, value[:]); err != nil {
		return "", errors.New("create control-plane backup: secure randomness unavailable")
	}
	result := hex.EncodeToString(value[:])
	clear(value[:])
	return result, nil
}

func mustPolicy(identity FileIdentity) privatefs.FilePolicy {
	policy, err := policyForIdentity(identity)
	if err != nil {
		panic(err)
	}
	return policy
}

func openRestorePlan(
	ctx context.Context,
	options RestoreOptions,
	errorPrefix string,
	sourceOpenAction string,
) (plan restorePlan, returnedErr error) {
	if err := validateRestoreBinding(options); err != nil {
		return restorePlan{}, err
	}
	if err := validateReleaseIdentity(options.Release); err != nil {
		return restorePlan{}, err
	}
	if _, _, err := validateExactAbsolutePath(
		"control-plane backup source",
		options.Source,
	); err != nil {
		return restorePlan{}, err
	}
	targets, err := validateRestoreTargets(options)
	if err != nil {
		return restorePlan{}, err
	}
	destination, err := privatefs.OpenDirectory(targets.parentPath)
	if err != nil {
		return restorePlan{}, fmt.Errorf("%s: open destination: %w", errorPrefix, err)
	}
	defer func() {
		if returnedErr != nil {
			returnedErr = errors.Join(returnedErr, destination.Close())
		}
	}()
	source, err := privatefs.OpenDirectory(options.Source)
	if err != nil {
		return restorePlan{}, fmt.Errorf("%s: %s: %w", errorPrefix, sourceOpenAction, err)
	}
	defer func() {
		if returnedErr != nil {
			returnedErr = errors.Join(returnedErr, source.Close())
		}
	}()

	sourceInfo, err := os.Stat(options.Source)
	if err != nil {
		return restorePlan{}, fmt.Errorf("%s: inspect source: %w", errorPrefix, err)
	}
	destinationInfo, err := os.Stat(targets.parentPath)
	if err != nil {
		return restorePlan{}, fmt.Errorf("%s: inspect destination: %w", errorPrefix, err)
	}
	if os.SameFile(sourceInfo, destinationInfo) {
		return restorePlan{}, errors.New(errorPrefix + ": source and destination directories must differ")
	}
	manifest, err := Verify(ctx, options.Source, options.Release)
	if err != nil {
		return restorePlan{}, err
	}
	if err := requireRestoreBinding(manifest, options); err != nil {
		return restorePlan{}, err
	}
	plan, err = buildRestorePlan(manifest, targets)
	if err != nil {
		return restorePlan{}, err
	}
	plan.source = source
	plan.destination = destination
	artifactPath := filepath.Join(targets.searchArtifactParent, targets.searchArtifactName)
	if manifest.SearchArtifacts != nil && pathsOverlap(artifactPath, options.Source) {
		return restorePlan{}, errors.New(errorPrefix + ": search-artifact destination must not overlap the backup source")
	}
	if manifest.SearchArtifacts != nil && targets.searchArtifactParent != targets.parentPath {
		artifactDestination, openErr := privatefs.OpenDirectory(targets.searchArtifactParent)
		if openErr != nil {
			return restorePlan{}, fmt.Errorf("%s: open search-artifact destination: %w", errorPrefix, openErr)
		}
		artifactInfo, statErr := os.Stat(targets.searchArtifactParent)
		if statErr != nil {
			_ = artifactDestination.Close()
			return restorePlan{}, fmt.Errorf("%s: inspect search-artifact destination: %w", errorPrefix, statErr)
		}
		if os.SameFile(sourceInfo, artifactInfo) || os.SameFile(destinationInfo, artifactInfo) {
			_ = artifactDestination.Close()
			return restorePlan{}, errors.New(errorPrefix + ": search-artifact destination must be a distinct directory")
		}
		plan.artifactDestination = artifactDestination
	}
	return plan, nil
}

func validateRestoreBinding(options RestoreOptions) error {
	hasRecoverySetID := options.ExpectedRecoverySetID != ""
	hasManifestSHA256 := options.ExpectedManifestSHA256 != ""
	if hasRecoverySetID != hasManifestSHA256 {
		return errors.New(
			"control-plane restore binding requires both recovery-set ID and manifest SHA-256",
		)
	}
	if !hasRecoverySetID {
		return nil
	}
	if !validLowerHex(options.ExpectedRecoverySetID, recoverySetIDBytes) {
		return errors.New("control-plane restore binding recovery-set ID is invalid")
	}
	if !validLowerHex(options.ExpectedManifestSHA256, sha256.Size) {
		return errors.New("control-plane restore binding manifest SHA-256 is invalid")
	}
	return nil
}

func requireRestoreBinding(manifest Manifest, options RestoreOptions) error {
	if options.ExpectedRecoverySetID == "" {
		return nil
	}
	encoded, err := marshalManifest(manifest)
	if err != nil {
		return fmt.Errorf("control-plane restore binding: encode verified manifest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	wantDigest, err := hex.DecodeString(options.ExpectedManifestSHA256)
	if err != nil {
		return errors.New("control-plane restore binding manifest SHA-256 is invalid")
	}
	if manifest.RecoverySetID != options.ExpectedRecoverySetID ||
		!hmac.Equal(digest[:], wantDigest) {
		return fmt.Errorf(
			"%w: verified control-plane bundle does not match the expected recovery-set ID and manifest SHA-256",
			ErrDeploymentBindingMismatch,
		)
	}
	return nil
}

func buildRestorePlan(manifest Manifest, targets restoreTargets) (restorePlan, error) {
	stageNames := restoreStageNames(manifest.RecoverySetID)
	finalNames := []string{
		targets.databaseName,
		targets.masterKeyName,
		targets.administratorTokenName,
	}
	namespaceNames := append([]string(nil), finalNames...)
	if targets.databaseLockName != "" {
		namespaceNames = append(namespaceNames, targets.databaseLockName)
	}
	artifactStageName := ""
	if manifest.SearchArtifacts != nil {
		artifactStageName = ".restore-" + manifest.RecoverySetID + "-search-artifacts"
		if targets.searchArtifactParent == targets.parentPath {
			namespaceNames = append(namespaceNames, targets.searchArtifactName)
		}
	}
	allStageNames := append([]string(nil), stageNames...)
	if artifactStageName != "" && targets.searchArtifactParent == targets.parentPath {
		allStageNames = append(allStageNames, artifactStageName)
	}
	if err := validateRestoreNamespace(allStageNames, namespaceNames); err != nil {
		return restorePlan{}, err
	}
	if artifactStageName != "" && artifactStageName == targets.searchArtifactName {
		return restorePlan{}, fmt.Errorf(
			"restore control-plane backup: search-artifact target name %q conflicts with the recovery staging namespace",
			targets.searchArtifactName,
		)
	}
	knownNames := append(append([]string(nil), allStageNames...), namespaceNames...)
	return restorePlan{
		manifest:          manifest,
		targets:           targets,
		stageNames:        stageNames,
		finalNames:        finalNames,
		namespaceNames:    namespaceNames,
		knownNames:        knownNames,
		artifactStageName: artifactStageName,
	}, nil
}

func validateRestoreTargets(options RestoreOptions) (restoreTargets, error) {
	databaseParent, databaseName, err := validateExactAbsolutePath(
		"restored control database",
		options.DatabasePath,
	)
	if err != nil {
		return restoreTargets{}, err
	}
	keyParent, keyName, err := validateExactAbsolutePath(
		"restored server master key",
		options.MasterKeyPath,
	)
	if err != nil {
		return restoreTargets{}, err
	}
	tokenParent, tokenName, err := validateExactAbsolutePath(
		"restored administrator token",
		options.AdministratorTokenPath,
	)
	if err != nil {
		return restoreTargets{}, err
	}
	if databaseParent != keyParent || databaseParent != tokenParent {
		return restoreTargets{}, errors.New("restore control-plane backup targets must share one exact parent directory")
	}
	artifactPath := searchArtifactDirectoryPath(
		options.DatabasePath,
		options.SearchArtifactDirectory,
	)
	artifactParent, artifactName, err := validateExactAbsolutePath(
		"restored search-artifact directory",
		artifactPath,
	)
	if err != nil {
		return restoreTargets{}, err
	}
	if artifactParent != databaseParent && pathsOverlap(artifactPath, databaseParent) {
		return restoreTargets{}, errors.New("restored search-artifact directory must not overlap the control-plane destination")
	}
	names := []string{databaseName, keyName, tokenName}
	if artifactParent == databaseParent {
		names = append(names, artifactName)
	}
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, exists := unique[name]; exists {
			return restoreTargets{}, errors.New("restore control-plane backup target names must be distinct")
		}
		unique[name] = struct{}{}
	}
	for _, sidecar := range []string{
		databaseName + "-wal",
		databaseName + "-shm",
		databaseName + "-journal",
		databaseName + ".server.lock",
	} {
		if slices.Contains(names, sidecar) {
			return restoreTargets{}, errors.New("restore control-plane backup target conflicts with a SQLite sidecar")
		}
	}
	databaseLockName := ""
	if options.DatabaseLock != nil {
		lockPath := options.DatabaseLock.Name()
		lockParent, lockName, lockErr := validateExactAbsolutePath(
			"restored control database lock",
			lockPath,
		)
		if lockErr != nil {
			return restoreTargets{}, lockErr
		}
		expectedLockPath := options.DatabasePath + ".server.lock"
		if lockPath != expectedLockPath || lockParent != databaseParent ||
			lockName != databaseName+".server.lock" {
			return restoreTargets{}, errors.New(
				"restored control database lock descriptor must name exactly the control database path plus .server.lock",
			)
		}
		databaseLockName = lockName
	}
	return restoreTargets{
		parentPath:             databaseParent,
		databaseName:           databaseName,
		databaseLockName:       databaseLockName,
		masterKeyName:          keyName,
		administratorTokenName: tokenName,
		searchArtifactParent:   artifactParent,
		searchArtifactName:     artifactName,
	}, nil
}

func pathsOverlap(left, right string) bool {
	contains := func(parent, child string) bool {
		relative, err := filepath.Rel(parent, child)
		if err != nil {
			return true
		}
		return relative == "." ||
			(relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
	}
	return contains(left, right) || contains(right, left)
}

func restoreStageNames(recoverySetID string) []string {
	return []string{
		".restore-" + recoverySetID + "-database",
		".restore-" + recoverySetID + "-master-key",
		".restore-" + recoverySetID + "-administrator-token",
	}
}

// restoreProbeNames are the two names the rename probe cycles through in an
// external search-artifact parent, derived from the artifact stage name so a
// crash leaves recognizable, reclaimable files.
func restoreProbeNames(artifactStageName string) []string {
	return []string{
		artifactStageName + "-rename-probe",
		artifactStageName + "-rename-probe-moved",
	}
}

// probeRestoreDestinations proves that every directory the restore publishes
// into honors atomic no-replace rename before any bundle member is copied. It
// runs only after stale stage cleanup and only during Restore, never during
// the read-only preflight. The control-plane destination is an exact-set
// namespace, so the probe cycles through two of the plan's own stage names:
// a probe file orphaned by a crash is then a stale stage that the next
// attempt's cleanup reclaims rather than an unrelated entry that refuses it.
// An external search-artifact parent tolerates unrelated entries; its probe
// names are removed here before probing for the same reason.
func probeRestoreDestinations(plan *restorePlan) error {
	if err := privatefs.RequireRenameNoReplace(
		plan.destination,
		"restore destination directory",
		privatefs.FixedNames(plan.stageNames[0], plan.stageNames[1]),
	); err != nil {
		return fmt.Errorf("restore control-plane backup: %w", err)
	}
	if plan.artifactDestination == nil {
		return nil
	}
	probeNames := restoreProbeNames(plan.artifactStageName)
	if err := removeStaleRestoreFiles(plan.artifactDestination, probeNames); err != nil {
		return fmt.Errorf("restore control-plane backup: reclaim stale search-artifact probe: %w", err)
	}
	if err := privatefs.RequireRenameNoReplace(
		plan.artifactDestination,
		"search-artifact restore destination directory",
		privatefs.FixedNames(probeNames...),
	); err != nil {
		return fmt.Errorf("restore control-plane backup: %w", err)
	}
	return nil
}

// removeStaleRestoreFiles unlinks the named owner-private regular files when
// present. Anything else under one of those names is refused, matching the
// stale-stage policy for the control-plane destination.
func removeStaleRestoreFiles(directory *privatefs.Directory, names []string) error {
	for _, name := range names {
		present, err := pinnedChildExists(directory, name)
		if err != nil {
			return err
		}
		if !present {
			continue
		}
		file, err := directory.OpenRegular(name, anySizePrivateFilePolicy)
		if err != nil {
			return fmt.Errorf("unsafe stale file %q: %w", name, err)
		}
		unlinkErr := directory.UnlinkPinnedRegular(name, file)
		if err := errors.Join(unlinkErr, file.Close()); err != nil {
			return fmt.Errorf("remove stale file %q: %w", name, err)
		}
	}
	return directory.Sync()
}

func validateRestoreNamespace(stageNames, finalNames []string) error {
	for _, stageName := range stageNames {
		if slices.Contains(finalNames, stageName) {
			return fmt.Errorf(
				"restore control-plane backup: target name %q conflicts with the recovery staging namespace",
				stageName,
			)
		}
	}
	return nil
}

type cleanupRestoreStagesHooks struct {
	afterPreflight func()
}

func cleanupRestoreStagesWithHooks(
	destination *privatefs.Directory,
	stageNames []string,
	namespaceNames []string,
	databaseLockName string,
	databaseLock *os.File,
	hooks cleanupRestoreStagesHooks,
) (returnedErr error) {
	entries, err := destination.List(len(stageNames) + len(namespaceNames))
	if err != nil {
		return fmt.Errorf("restore control-plane backup: inspect destination entries: %w", err)
	}
	allowed := append(append([]string(nil), stageNames...), namespaceNames...)
	for _, entry := range entries {
		if !slices.Contains(allowed, entry) {
			return fmt.Errorf("restore control-plane backup: destination contains unrelated entry %q", entry)
		}
	}
	if databaseLockName != "" {
		if !slices.Contains(entries, databaseLockName) {
			return errors.New("restore control-plane backup: declared control database lock is missing")
		}
		if err := validateRestoreDatabaseLock(
			destination,
			databaseLockName,
			databaseLock,
		); err != nil {
			return err
		}
	}
	pinned := make([]pinnedCleanupFile, 0, len(stageNames))
	defer func() {
		returnedErr = errors.Join(returnedErr, closePinnedCleanupFiles(pinned))
	}()
	for _, stageName := range stageNames {
		if !slices.Contains(entries, stageName) {
			continue
		}
		file, openErr := destination.OpenRegular(stageName, anySizePrivateFilePolicy)
		if openErr != nil {
			return fmt.Errorf(
				"restore control-plane backup: unsafe stale staging file %q: %w",
				stageName,
				openErr,
			)
		}
		pinned = append(pinned, pinnedCleanupFile{name: stageName, file: file})
	}
	if hooks.afterPreflight != nil {
		hooks.afterPreflight()
	}
	// Prove every stale name before mutating any of them, so a replacement
	// discovered during preflight cannot cause partial cleanup.
	for _, stage := range pinned {
		if err := destination.RequirePinnedRegular(stage.name, stage.file); err != nil {
			return fmt.Errorf(
				"restore control-plane backup: stale staging file %q changed before cleanup: %w",
				stage.name,
				err,
			)
		}
	}
	removed := false
	for _, stage := range pinned {
		if err := destination.UnlinkPinnedRegular(stage.name, stage.file); err != nil {
			return fmt.Errorf(
				"restore control-plane backup: remove stale staging file %q: %w",
				stage.name,
				err,
			)
		}
		removed = true
	}
	if removed {
		if err := destination.Sync(); err != nil {
			return err
		}
	}
	return validateRestoreDatabaseLock(destination, databaseLockName, databaseLock)
}

func validateRestoreStageFiles(
	destination *privatefs.Directory,
	entries []string,
	stageNames []string,
) error {
	for _, stageName := range stageNames {
		if !slices.Contains(entries, stageName) {
			continue
		}
		file, err := destination.OpenRegular(stageName, anySizePrivateFilePolicy)
		if err != nil {
			return fmt.Errorf(
				"preflight control-plane restore: unsafe stale staging file %q: %w",
				stageName,
				err,
			)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf(
				"preflight control-plane restore: close stale staging file %q: %w",
				stageName,
				err,
			)
		}
	}
	return nil
}

func requireFinalRestoreNamespace(
	destination *privatefs.Directory,
	namespaceNames []string,
	databaseLockName string,
	databaseLock *os.File,
) error {
	if err := destination.RequireEntries(namespaceNames, len(namespaceNames)); err != nil {
		return fmt.Errorf("restore control-plane backup: verify published entry set: %w", err)
	}
	return validateRestoreDatabaseLock(destination, databaseLockName, databaseLock)
}

func validateRestoreDatabaseLock(
	destination *privatefs.Directory,
	databaseLockName string,
	databaseLock *os.File,
) error {
	if databaseLockName == "" {
		if databaseLock != nil {
			return errors.New("restore control-plane backup: database lock descriptor has no exact target")
		}
		return nil
	}
	if databaseLock == nil {
		return errors.New("restore control-plane backup: declared control database lock descriptor is missing")
	}
	if err := retainRestoreDatabaseLock(databaseLock); err != nil {
		return err
	}
	heldInfo, err := databaseLock.Stat()
	if err != nil {
		return fmt.Errorf("restore control-plane backup: inspect held control database lock: %w", err)
	}
	if err := privatefs.ValidateExactLockFileInfo(heldInfo); err != nil {
		return fmt.Errorf("restore control-plane backup: unsafe held control database lock: %w", err)
	}
	pathFile, err := destination.OpenRegular(databaseLockName, privatefs.FilePolicy{
		AllowedModes: privateMode,
		MinimumSize:  0,
		MaximumSize:  0,
	})
	if err != nil {
		return fmt.Errorf("restore control-plane backup: unsafe control database lock: %w", err)
	}
	pathInfo, statErr := pathFile.Stat()
	closeErr := pathFile.Close()
	if statErr != nil {
		return errors.Join(
			fmt.Errorf("restore control-plane backup: inspect control database lock path: %w", statErr),
			closeErr,
		)
	}
	if closeErr != nil {
		return fmt.Errorf("restore control-plane backup: close inspected control database lock: %w", closeErr)
	}
	if !os.SameFile(heldInfo, pathInfo) {
		return errors.New("restore control-plane backup: control database lock path no longer names the held lock descriptor")
	}
	stableHeldInfo, err := databaseLock.Stat()
	if err != nil {
		return fmt.Errorf("restore control-plane backup: reinspect held control database lock: %w", err)
	}
	if err := privatefs.ValidateExactLockFileInfo(stableHeldInfo); err != nil {
		return fmt.Errorf("restore control-plane backup: unsafe held control database lock: %w", err)
	}
	if !os.SameFile(heldInfo, stableHeldInfo) {
		return errors.New("restore control-plane backup: held control database lock descriptor identity changed")
	}
	if err := destination.Revalidate(); err != nil {
		return fmt.Errorf("restore control-plane backup: revalidate database lock directory: %w", err)
	}
	return nil
}

func retainRestoreDatabaseLock(databaseLock *os.File) error {
	if databaseLock == nil {
		return errors.New("restore control-plane backup: control database lock descriptor is missing")
	}

	descriptor, err := safecast.Conv[int](databaseLock.Fd())
	if err != nil {
		return fmt.Errorf("restore control-plane backup: convert control database lock descriptor: %w", err)
	}
	if err := unix.Flock(descriptor, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return fmt.Errorf("restore control-plane backup: retain exclusive control database lock: %w", err)
	}
	return nil
}

func inspectRestorePrefix(
	ctx context.Context,
	destination *privatefs.Directory,
	finalNames []string,
	namespaceNames []string,
	manifest Manifest,
	release ReleaseIdentity,
) (int, error) {
	entries, err := destination.List(len(namespaceNames))
	if err != nil {
		return 0, fmt.Errorf("restore control-plane backup: inspect destination: %w", err)
	}
	for _, entry := range entries {
		if !slices.Contains(namespaceNames, entry) {
			return 0, fmt.Errorf("restore control-plane backup: destination contains unrelated entry %q", entry)
		}
	}
	prefix := 0
	for prefix < len(finalNames) && slices.Contains(entries, finalNames[prefix]) {
		prefix++
	}
	for index := prefix; index < len(finalNames); index++ {
		if slices.Contains(entries, finalNames[index]) {
			return 0, errors.New("restore control-plane backup: destination is not a valid publication prefix")
		}
	}
	members := []FileIdentity{manifest.Database, manifest.MasterKey, manifest.AdministratorToken}
	for index := 0; index < prefix; index++ {
		policy := mustPolicy(members[index])
		member, err := inspectMember(ctx, destination, finalNames[index], policy, false)
		if err != nil {
			return 0, err
		}
		member.identity.Name = members[index].Name
		if err := requireFileIdentity(member, members[index]); err != nil {
			return 0, errors.New("restore control-plane backup: existing target differs from the bundle")
		}
	}
	if prefix == len(finalNames) {
		if err := verifyRestoreMembers(ctx, destination, finalNames, manifest, release); err != nil {
			return 0, err
		}
	}
	return prefix, nil
}

func verifyRestoreMembers(
	ctx context.Context,
	destination *privatefs.Directory,
	names []string,
	manifest Manifest,
	release ReleaseIdentity,
) error {
	if len(names) != 3 {
		return errors.New("restore control-plane backup: exactly three resolved member names are required")
	}
	masterKey, err := inspectMember(
		ctx,
		destination,
		names[1],
		mustPolicy(manifest.MasterKey),
		true,
	)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: read recovery master key: %w", err)
	}
	defer clear(masterKey.contents)
	if _, err := verifyDatabase(
		ctx,
		filepath.Join(destination.Path(), names[0]),
		masterKey.contents,
		release,
	); err != nil {
		return fmt.Errorf("restore control-plane backup: verify recovery database: %w", err)
	}
	administratorToken, err := inspectMember(
		ctx,
		destination,
		names[2],
		mustPolicy(manifest.AdministratorToken),
		true,
	)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: read recovery administrator token: %w", err)
	}
	defer clear(administratorToken.contents)
	if err := auth.ValidateBrowserBearerToken(administratorToken.contents); err != nil {
		return errors.New("restore control-plane backup: recovery administrator token is invalid")
	}
	return nil
}
