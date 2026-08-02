//go:build darwin || linux

package controlbackup

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

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
}

// CreateOptions identifies one stopped server's control-plane state and a new
// bundle directory. The caller must hold the supported server lock for the
// complete operation.
type CreateOptions struct {
	DatabasePath           string
	MasterKeyPath          string
	AdministratorTokenPath string
	Destination            string
	Release                ReleaseIdentity
}

// RestoreOptions identifies one verified bundle and three runtime targets in
// a single owner-private directory. Restore never overwrites mismatched state.
type RestoreOptions struct {
	Source                 string
	DatabasePath           string
	MasterKeyPath          string
	AdministratorTokenPath string
	Release                ReleaseIdentity
}

type createHooks struct {
	now            func() time.Time
	random         io.Reader
	stageName      privatefs.NameGenerator
	afterStageSync func()
	beforePublish  func()
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

func createWithHooks(
	ctx context.Context,
	options CreateOptions,
	hooks createHooks,
) (manifest Manifest, returnedErr error) {
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
	defer func() {
		if !stageExists {
			return
		}
		cleanupErr := cleanupKnownFiles(stage, bundleStageCleanupNames)
		cleanupErr = errors.Join(cleanupErr, stage.Close())
		cleanupErr = errors.Join(
			cleanupErr,
			destinationParent.RemoveOwnedEmptyDirectory(stageName),
		)
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
	if !sameFileState(pinnedDatabaseInfo, reopenedDatabaseInfo) {
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
	fingerprint, err := auth.FingerprintServerMasterKey(masterKey)
	if err != nil {
		return Manifest{}, err
	}
	recoverySetID, err := newRecoverySetID(hooks.random)
	if err != nil {
		return Manifest{}, err
	}
	createdAt := hooks.now().UTC().UnixMicro()
	manifest = Manifest{
		FormatVersion:               manifestFormatVersion,
		CreatedAtUnixMicro:          createdAt,
		RecoverySetID:               recoverySetID,
		Scope:                       controlPlaneOnlyScope,
		ClickHouseIncluded:          false,
		ApplicationVersion:          options.Release.ApplicationVersion,
		SourceRevision:              options.Release.SourceRevision,
		SQLiteMigrations:            options.Release.SQLiteMigrations,
		SQLiteMigrationLedgerSHA256: hex.EncodeToString(sourceMigrationIdentity.SHA256[:]),
		ClickHouseMigrations:        options.Release.ClickHouseMigrations,
		Database:                    databaseMember.identity,
		MasterKey:                   masterKeyMember.identity,
		AdministratorToken:          administratorTokenMember.identity,
		MasterKeyFingerprintSHA256:  hex.EncodeToString(fingerprint[:]),
	}
	encodedManifest, err := marshalManifest(manifest)
	if err != nil {
		return Manifest{}, err
	}
	if _, err := writeMember(ctx, stage, manifestFilename, encodedManifest); err != nil {
		return Manifest{}, err
	}
	if err := stage.RequireEntries(bundleMemberNames, len(bundleMemberNames)); err != nil {
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
	stageInfo, err := os.Lstat(stage.Path())
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: inspect verified staging directory: %w", err)
	}
	if hooks.beforePublish != nil {
		hooks.beforePublish()
	}
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := destinationParent.RenameNoReplace(
		stageName,
		destinationParent,
		destinationName,
	); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: publish bundle: %w", err)
	}
	stageExists = false
	if err := stage.Close(); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: close published staging descriptor: %w", err)
	}
	if err := destinationParent.Sync(); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: sync published bundle: %w", err)
	}
	published, err := privatefs.OpenDirectory(options.Destination)
	if err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: open published bundle: %w", err)
	}
	publishedInfo, statErr := os.Lstat(options.Destination)
	entriesErr := published.RequireEntries(bundleMemberNames, len(bundleMemberNames))
	publishedCloseErr := published.Close()
	if statErr != nil || entriesErr != nil || publishedCloseErr != nil {
		return Manifest{}, fmt.Errorf(
			"create control-plane backup: verify published bundle identity: %w",
			errors.Join(statErr, entriesErr, publishedCloseErr),
		)
	}
	if !os.SameFile(stageInfo, publishedInfo) {
		return Manifest{}, errors.New("create control-plane backup: published bundle identity changed")
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
	if err := bundle.RequireEntries(bundleMemberNames, len(bundleMemberNames)); err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup: %w", err)
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
	if err := bundle.RequireEntries(bundleMemberNames, len(bundleMemberNames)); err != nil {
		return Manifest{}, fmt.Errorf("verify control-plane backup after SQLite inspection: %w", err)
	}
	if err := bundle.Revalidate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
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
	if err := validateReleaseIdentity(options.Release); err != nil {
		return err
	}
	if _, _, err := validateExactAbsolutePath("control-plane backup source", options.Source); err != nil {
		return err
	}
	destinationParentPath, databaseName, masterKeyName, administratorTokenName, err :=
		validateRestoreTargets(options)
	if err != nil {
		return err
	}
	destination, err := privatefs.OpenDirectory(destinationParentPath)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: open destination: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, destination.Close()) }()
	source, err := privatefs.OpenDirectory(options.Source)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: reopen source: %w", err)
	}
	defer func() { returnedErr = errors.Join(returnedErr, source.Close()) }()

	sourceInfo, err := os.Stat(options.Source)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: inspect source: %w", err)
	}
	destinationInfo, err := os.Stat(destinationParentPath)
	if err != nil {
		return fmt.Errorf("restore control-plane backup: inspect destination: %w", err)
	}
	if os.SameFile(sourceInfo, destinationInfo) {
		return errors.New("restore control-plane backup: source and destination directories must differ")
	}
	manifest, err := Verify(ctx, options.Source, options.Release)
	if err != nil {
		return err
	}

	stageNames := restoreStageNames(manifest.RecoverySetID)
	finalNames := []string{databaseName, masterKeyName, administratorTokenName}
	if err := validateRestoreNamespace(stageNames, finalNames); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := cleanupRestoreStages(destination, stageNames, finalNames); err != nil {
		return err
	}
	prefix, err := inspectRestorePrefix(
		ctx,
		destination,
		finalNames,
		manifest,
		options.Release,
	)
	if err != nil {
		return err
	}
	if prefix == len(finalNames) {
		return nil
	}
	members := []restoreMember{
		{
			identity: manifest.Database, stageName: stageNames[0], finalName: finalNames[0],
			afterPublish: hooks.afterDatabasePublish,
		},
		{
			identity: manifest.MasterKey, stageName: stageNames[1], finalName: finalNames[1],
			afterPublish: hooks.afterMasterKeyPublish,
		},
		{
			identity: manifest.AdministratorToken, stageName: stageNames[2], finalName: finalNames[2],
			afterPublish: hooks.afterAdministratorTokenPublish,
		},
	}

	remainingStages := make([]string, 0, len(stageNames)-prefix)
	defer func() {
		cleanupErr := cleanupKnownFiles(destination, remainingStages)
		if cleanupErr == nil && len(remainingStages) != 0 {
			cleanupErr = destination.Sync()
		}
		returnedErr = errors.Join(returnedErr, cleanupErr)
	}()
	for index := prefix; index < len(members); index++ {
		member := members[index]
		remainingStages = append(remainingStages, member.stageName)
		if err := copyMember(
			ctx,
			source,
			member.identity.Name,
			destination,
			member.stageName,
			member.identity,
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
		destination,
		resolvedNames,
		manifest,
		options.Release,
	); err != nil {
		return err
	}
	if err := destination.Sync(); err != nil {
		return fmt.Errorf("restore control-plane backup: sync staging files: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	for index := prefix; index < len(members); index++ {
		member := members[index]
		if hooks.beforePublication != nil {
			hooks.beforePublication(index)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := destination.RenameNoReplace(
			member.stageName,
			destination,
			member.finalName,
		); err != nil {
			return fmt.Errorf("restore control-plane backup: publish %q: %w", member.identity.Name, err)
		}
		remainingStages = slices.DeleteFunc(remainingStages, func(name string) bool {
			return name == member.stageName
		})
		if err := destination.Sync(); err != nil {
			return fmt.Errorf("restore control-plane backup: sync %q publication: %w", member.identity.Name, err)
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
	if err := destination.RequireEntries(finalNames, len(finalNames)); err != nil {
		return fmt.Errorf("restore control-plane backup: verify published entry set: %w", err)
	}
	return nil
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

func validateRestoreTargets(
	options RestoreOptions,
) (parent, database, masterKey, administratorToken string, err error) {
	databaseParent, databaseName, err := validateExactAbsolutePath(
		"restored control database",
		options.DatabasePath,
	)
	if err != nil {
		return "", "", "", "", err
	}
	keyParent, keyName, err := validateExactAbsolutePath(
		"restored server master key",
		options.MasterKeyPath,
	)
	if err != nil {
		return "", "", "", "", err
	}
	tokenParent, tokenName, err := validateExactAbsolutePath(
		"restored administrator token",
		options.AdministratorTokenPath,
	)
	if err != nil {
		return "", "", "", "", err
	}
	if databaseParent != keyParent || databaseParent != tokenParent {
		return "", "", "", "", errors.New("restore control-plane backup targets must share one exact parent directory")
	}
	names := []string{databaseName, keyName, tokenName}
	unique := make(map[string]struct{}, len(names))
	for _, name := range names {
		if _, exists := unique[name]; exists {
			return "", "", "", "", errors.New("restore control-plane backup target names must be distinct")
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
			return "", "", "", "", errors.New("restore control-plane backup target conflicts with a SQLite sidecar")
		}
	}
	return databaseParent, databaseName, keyName, tokenName, nil
}

func restoreStageNames(recoverySetID string) []string {
	return []string{
		".restore-" + recoverySetID + "-database",
		".restore-" + recoverySetID + "-master-key",
		".restore-" + recoverySetID + "-administrator-token",
	}
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

func cleanupRestoreStages(
	destination *privatefs.Directory,
	stageNames []string,
	finalNames []string,
) error {
	entries, err := destination.List(len(stageNames) + len(finalNames))
	if err != nil {
		return fmt.Errorf("restore control-plane backup: inspect destination entries: %w", err)
	}
	allowed := append(append([]string(nil), stageNames...), finalNames...)
	for _, entry := range entries {
		if !slices.Contains(allowed, entry) {
			return fmt.Errorf("restore control-plane backup: destination contains unrelated entry %q", entry)
		}
	}
	removed := false
	for _, stageName := range stageNames {
		if !slices.Contains(entries, stageName) {
			continue
		}
		file, err := destination.OpenRegular(stageName, privatefs.FilePolicy{
			AllowedModes: privateMode,
			MinimumSize:  0,
			MaximumSize:  int64(maximumDatabaseBytes),
		})
		if err != nil {
			return fmt.Errorf("restore control-plane backup: unsafe stale staging file %q: %w", stageName, err)
		}
		if err := file.Close(); err != nil {
			return err
		}
		if err := destination.Unlink(stageName); err != nil {
			return err
		}
		removed = true
	}
	if removed {
		return destination.Sync()
	}
	return nil
}

func inspectRestorePrefix(
	ctx context.Context,
	destination *privatefs.Directory,
	finalNames []string,
	manifest Manifest,
	release ReleaseIdentity,
) (int, error) {
	entries, err := destination.List(len(finalNames))
	if err != nil {
		return 0, fmt.Errorf("restore control-plane backup: inspect destination: %w", err)
	}
	for _, entry := range entries {
		if !slices.Contains(finalNames, entry) {
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
