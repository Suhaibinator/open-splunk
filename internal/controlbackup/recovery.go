//go:build darwin || linux

package controlbackup

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/buildassets"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/exportjournal"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/privatefs"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
	"github.com/Suhaibinator/open-splunk/migrations"
)

var bundleMemberNames = []string{databaseFilename, manifestFilename, masterKeyFilename, blobCatalogFilename, searchArtifactsFilename}
var bundleStageCleanupNames = bundleMemberNames

func bundleNames(_ Manifest) []string { return append([]string(nil), bundleMemberNames...) }

var ErrDeploymentBindingMismatch = errors.New("controlbackup: deployment recovery binding mismatch")

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

// CreateOptions binds PostgreSQL metadata and its referenced private objects.
// A maintenance lease excludes runtime writers throughout the generation.
type CreateOptions struct {
	PostgresDSN   string
	MasterKeyPath string
	Destination   string
	Release       ReleaseIdentity
	Blobs         searchartifacts.BlobStore
	Lease         *control.MaintenanceLease
}
type RestoreOptions struct {
	Source                 string
	PostgresDSN            string
	MasterKeyPath          string
	Release                ReleaseIdentity
	Blobs                  searchartifacts.BlobStore
	Lease                  *control.MaintenanceLease
	ExpectedRecoverySetID  string
	ExpectedManifestSHA256 string
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
	beforePublication     func(index int)
	afterDatabasePublish  func() error
	afterObjectsPublish   func() error
	afterMasterKeyPublish func() error
}

func acquireLease(ctx context.Context, dsn string, existing *control.MaintenanceLease) (*control.MaintenanceLease, func() error, error) {
	if existing != nil {
		if err := existing.Validate(dsn); err != nil {
			return nil, nil, err
		}
		return existing, func() error { return nil }, nil
	}
	lease, err := control.AcquireMaintenanceLease(ctx, dsn)
	if err != nil {
		return nil, nil, err
	}
	return lease, lease.Close, nil
}

// leaseContext cancels subprocesses and object operations if the server exclusion
// session loses ownership; Validate is also checked at publication boundaries.
func leaseContext(ctx context.Context, lease *control.MaintenanceLease) (context.Context, func()) {
	guarded, cancel := context.WithCancel(ctx)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-lease.OwnershipLost():
			cancel()
		case <-guarded.Done():
		}
	}()
	return guarded, func() { cancel(); <-stopped }
}
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

	masterKey, err := readSourceMasterKey(ctx, options.MasterKeyPath)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(masterKey)
	lease, releaseLease, err := acquireLease(ctx, options.PostgresDSN, options.Lease)
	if err != nil {
		return Manifest{}, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, releaseLease()) }()
	ctx, stopGuard := leaseContext(ctx, lease)
	defer stopGuard()

	sourceDatabase, sourceMigrationIdentity, err := openVerifiedDatabase(
		ctx,
		options.PostgresDSN,
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

	if err := visibility.ValidateBackupDrain(ctx, sourceDatabase); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: %w", err)
	}
	references, err := sourceObjectReferences(ctx, sourceDatabase)
	if err != nil {
		return Manifest{}, err
	}
	var allocated, committed uint64
	if err := sourceDatabase.SQLDB().QueryRowContext(ctx, `SELECT last_assigned,committed_through FROM ingest_visibility_state WHERE singleton=1`).Scan(&allocated, &committed); err != nil {
		return Manifest{}, err
	}
	databasePolicy := privatefs.FilePolicy{AllowedModes: privateMode, MinimumSize: 1, MaximumSize: int64(maximumDatabaseBytes)}

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
		return Manifest{}, fmt.Errorf("create control-plane backup PostgreSQL snapshot: %w", err)
	}
	if err := sourceDatabase.Close(); err != nil {
		return Manifest{}, fmt.Errorf("create control-plane backup: close source database: %w", err)
	}
	databaseOpen = false
	databaseMember, err := inspectMember(ctx, stage, databaseFilename, databasePolicy, false)
	if err != nil {
		return Manifest{}, err
	}
	masterKeyMember, err := writeMember(ctx, stage, masterKeyFilename, masterKey)
	if err != nil {
		return Manifest{}, err
	}
	objects, err := archiveObjects(ctx, stage, options.Blobs, references)
	if err != nil {
		return Manifest{}, err
	}
	catalog, err := MarshalCanonicalJSON(references, "object reference catalog", int(maximumObjectCatalogBytes), validateObjectCatalog)
	if err != nil {
		return Manifest{}, err
	}
	catalogMember, err := writeMember(ctx, stage, blobCatalogFilename, catalog)
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
		FormatVersion: manifestFormatVersion, CreatedAtUnixMicro: createdAt, RecoverySetID: recoverySetID,
		Scope: controlPlaneOnlyScope, SourceRevision: options.Release.SourceRevision,
		PostgresMigrations: options.Release.PostgresMigrations, PostgresMigrationLedgerSHA256: hex.EncodeToString(sourceMigrationIdentity.SHA256[:]),
		ClickHouseMigrations: options.Release.ClickHouseMigrations, Database: databaseMember.identity, MasterKey: masterKeyMember.identity,
		MasterKeyFingerprintSHA256: hex.EncodeToString(fingerprint[:]), Objects: &objects, BlobCatalog: catalogMember.identity,
		MaxAllocatedVisibility: allocated, CommittedCutoff: committed,
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
	if err := lease.Validate(options.PostgresDSN); err != nil {
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

func openVerifiedDatabase(
	ctx context.Context,
	path string,
	masterKey []byte,
	release ReleaseIdentity,
) (*control.DB, control.MigrationIdentity, error) {
	if err := verifyMigrationCorpus(release.PostgresMigrations); err != nil {
		return nil, control.MigrationIdentity{}, err
	}
	database, err := control.OpenReadOnly(ctx, path)
	if err != nil {
		return nil, control.MigrationIdentity{}, fmt.Errorf("verify control-plane backup database: %w", err)
	}
	closeOnError := func(openErr error) (*control.DB, control.MigrationIdentity, error) {
		return nil, control.MigrationIdentity{}, errors.Join(openErr, database.Close())
	}
	identity, err := database.VerifyCurrentMigrations(ctx, migrations.PostgreSQL())
	if err != nil {
		return closeOnError(fmt.Errorf("verify control-plane backup migrations: %w", err))
	}
	if identity.LatestVersion != release.PostgresMigrations.LatestVersion {
		return closeOnError(errors.New("control-plane backup migration version does not match the release"))
	}
	if err := database.VerifyIntegrity(ctx); err != nil {
		return closeOnError(fmt.Errorf("verify control-plane backup PostgreSQL integrity: %w", err))
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

// Verify checks canonical bounded files and native archive metadata without
// opening or mutating the configured PostgreSQL or object store.
func Verify(ctx context.Context, source string, expected ReleaseIdentity) (manifest Manifest, returnedErr error) {
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
		return Manifest{}, err
	}
	defer func() { returnedErr = errors.Join(returnedErr, bundle.Close()) }()
	return verifyPinnedBundle(ctx, bundle, expected)
}
func verifyPinnedBundle(ctx context.Context, bundle *privatefs.Directory, expected ReleaseIdentity) (Manifest, error) {
	member, err := inspectMember(ctx, bundle, manifestFilename, privatefs.FilePolicy{AllowedModes: privateMode, MinimumSize: 1, MaximumSize: maximumManifestBytes}, true)
	if err != nil {
		return Manifest{}, err
	}
	manifest, err := unmarshalManifest(member.contents)
	if err != nil {
		return Manifest{}, err
	}
	if err := validateManifestRelease(manifest, expected); err != nil {
		return Manifest{}, err
	}
	if err := bundle.RequireEntries(bundleNames(manifest), len(bundleMemberNames)); err != nil {
		return Manifest{}, err
	}
	for _, identity := range []FileIdentity{manifest.Database, manifest.MasterKey} {
		inspected, err := inspectMember(ctx, bundle, identity.Name, mustPolicy(identity), false)
		if err != nil {
			return Manifest{}, err
		}
		if err := requireFileIdentity(inspected, identity); err != nil {
			return Manifest{}, err
		}
	}
	key, err := inspectMember(ctx, bundle, masterKeyFilename, mustPolicy(manifest.MasterKey), true)
	if err != nil {
		return Manifest{}, err
	}
	defer clear(key.contents)
	fingerprint, err := auth.FingerprintServerMasterKey(key.contents)
	if err != nil {
		return Manifest{}, err
	}
	if hex.EncodeToString(fingerprint[:]) != manifest.MasterKeyFingerprintSHA256 {
		return Manifest{}, errors.New("backup master-key fingerprint differs from manifest")
	}
	references, err := readObjectCatalog(ctx, bundle, manifest)
	if err != nil {
		return Manifest{}, err
	}
	objects, err := openPinnedChild(bundle, searchArtifactsFilename)
	if err != nil {
		return Manifest{}, err
	}
	identity, files, inspectErr := inspectObjectDirectory(ctx, objects, false)
	err = errors.Join(inspectErr, objects.Close())
	if err != nil {
		return Manifest{}, err
	}
	if identity != *manifest.Objects {
		return Manifest{}, errors.New("backup object archive differs from manifest")
	}
	expectedFiles := map[string]int64{}
	for _, ref := range references {
		expectedFiles[ref.FileName] = ref.Identity.Bytes
	}
	if len(files) != len(expectedFiles) {
		return Manifest{}, errors.New("backup object archive contains missing or unreferenced content")
	}
	for _, file := range files {
		size, ok := expectedFiles[file.Name]
		if !ok || size < 0 || uint64(size) != file.SizeBytes {
			return Manifest{}, errors.New("backup object reference size differs from archive")
		}
	}
	archive, err := bundle.OpenRegular(databaseFilename, mustPolicy(manifest.Database))
	if err != nil {
		return Manifest{}, err
	}
	defer archive.Close()
	if err := verifyArchive(ctx, archive, manifest); err != nil {
		return Manifest{}, err
	}
	if err := bundle.RequirePinnedRegular(databaseFilename, archive); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

type boundedToolOutput struct {
	bytes.Buffer
	remaining int
}

func (w *boundedToolOutput) Write(p []byte) (int, error) {
	if len(p) > w.remaining {
		return 0, errors.New("PostgreSQL archive inspection exceeds bound")
	}
	w.remaining -= len(p)
	return w.Buffer.Write(p)
}
func archiveTool(ctx context.Context, archive *os.File, list bool) ([]byte, error) {
	if _, err := archive.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	createCommand := control.PostgresRestoreListCommand
	if !list {
		createCommand = control.PostgresRestoreAuthorityCommand
	}
	command, err := createCommand(ctx)
	if err != nil {
		return nil, err
	}
	command.ExtraFiles = []*os.File{archive}
	output := &boundedToolOutput{remaining: 4 << 20}
	command.Stdout = output
	diagnostic := &boundedToolOutput{remaining: 16 << 10}
	command.Stderr = diagnostic
	if err := command.Run(); err != nil {
		return nil, fmt.Errorf("inspect native PostgreSQL archive: %w: %s", err, strings.TrimSpace(diagnostic.String()))
	}
	return output.Bytes(), nil
}
func archiveRows(contents []byte, table string) ([][]string, error) {
	var rows [][]string
	reading := false
	found := false
	for line := range strings.SplitSeq(string(contents), "\n") {
		if strings.HasPrefix(line, "COPY ") && strings.Contains(line, "."+table+" (") {
			if found {
				return nil, errors.New("PostgreSQL archive has duplicate authority table")
			}
			found = true
			reading = true
			continue
		}
		if reading {
			if line == `\.` {
				reading = false
				continue
			}
			rows = append(rows, strings.Split(line, "\t"))
		}
	}
	if !found || reading {
		return nil, fmt.Errorf("PostgreSQL archive lacks complete authority table %s", table)
	}
	return rows, nil
}
func verifyArchive(ctx context.Context, archive *os.File, manifest Manifest) error {
	if _, err := archiveTool(ctx, archive, true); err != nil {
		return err
	}
	contents, err := archiveTool(ctx, archive, false)
	if err != nil {
		return err
	}
	keyRows, err := archiveRows(contents, "server_key_state")
	if err != nil {
		return err
	}
	if len(keyRows) != 1 || len(keyRows[0]) != 3 || keyRows[0][0] != "server-master-v1" || strings.TrimPrefix(strings.TrimLeft(keyRows[0][1], `\`), "x") != manifest.MasterKeyFingerprintSHA256 {
		return errors.New("PostgreSQL archive master-key identity differs from bundle")
	}
	state, err := archiveRows(contents, "ingest_visibility_state")
	if err != nil {
		return err
	}
	if len(state) != 1 || len(state[0]) != 3 || state[0][0] != "1" || state[0][1] != strconv.FormatUint(manifest.MaxAllocatedVisibility, 10) || state[0][2] != strconv.FormatUint(manifest.CommittedCutoff, 10) {
		return errors.New("PostgreSQL archive visibility cutoff differs from bundle")
	}
	ledger, err := archiveRows(contents, "open_splunk_schema_migrations")
	if err != nil {
		return err
	}
	if err := verifyArchiveLedger(ledger, manifest); err != nil {
		return err
	}
	return nil
}
func verifyArchiveLedger(rows [][]string, manifest Manifest) error {
	if err := verifyMigrationCorpus(manifest.PostgresMigrations); err != nil {
		return err
	}
	entries, err := fs.ReadDir(migrations.PostgreSQL(), ".")
	if err != nil {
		return err
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	byVersion := map[uint64][]string{}
	for _, row := range rows {
		if len(row) != 4 {
			return errors.New("PostgreSQL archive migration ledger is malformed")
		}
		version, err := strconv.ParseUint(row[0], 10, 32)
		if err != nil || version == 0 {
			return errors.New("PostgreSQL archive migration version is invalid")
		}
		if _, ok := byVersion[version]; ok {
			return errors.New("PostgreSQL archive migration version is duplicated")
		}
		byVersion[version] = row
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("open-splunk/postgres-migrations/format-1\x00"))
	var version uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		version++
		contents, err := fs.ReadFile(migrations.PostgreSQL(), entry.Name())
		if err != nil {
			return err
		}
		checksum := sha256.Sum256(contents)
		row, ok := byVersion[version]
		if !ok || row[1] != entry.Name() || strings.TrimPrefix(strings.TrimLeft(row[2], `\`), "x") != hex.EncodeToString(checksum[:]) {
			return errors.New("PostgreSQL archive migration ledger differs from current release")
		}
		applied, err := strconv.ParseInt(row[3], 10, 64)
		if err != nil || applied <= 0 {
			return errors.New("PostgreSQL archive migration timestamp is invalid")
		}
		_, _ = hash.Write([]byte(strconv.FormatUint(version, 10)))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(strconv.Itoa(len(entry.Name()))))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(entry.Name()))
		_, _ = hash.Write(checksum[:])
	}
	if len(rows) != int(version) || version != uint64(manifest.PostgresMigrations.LatestVersion) || hex.EncodeToString(hash.Sum(nil)) != manifest.PostgresMigrationLedgerSHA256 {
		return errors.New("PostgreSQL archive migration identity differs from bundle")
	}
	return nil
}

func manifestDigest(manifest Manifest) (string, error) {
	encoded, err := marshalManifest(manifest)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
func targetReceipt(ctx context.Context, pool *sql.DB, manifest Manifest) (empty bool, phase string, err error) {
	var populated bool
	if err := pool.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','S','f'))`).Scan(&populated); err != nil {
		return false, "", err
	}
	if !populated {
		return true, "", nil
	}
	var exists bool
	if err := pool.QueryRowContext(ctx, `SELECT to_regclass(current_schema() || '.control_recovery_publication') IS NOT NULL`).Scan(&exists); err != nil {
		return false, "", err
	}
	if !exists {
		return false, "", errors.New("PostgreSQL restore target is populated without a matching recovery receipt")
	}
	var generation, digest string
	if err := pool.QueryRowContext(ctx, `SELECT recovery_set_id,manifest_sha256,phase FROM control_recovery_publication WHERE singleton=1`).Scan(&generation, &digest, &phase); err != nil {
		return false, "", errors.New("PostgreSQL restore target lacks a complete recovery receipt")
	}
	want, err := manifestDigest(manifest)
	if err != nil {
		return false, "", err
	}
	if generation != manifest.RecoverySetID || digest != want {
		return false, "", errors.New("PostgreSQL restore target belongs to a different recovery generation")
	}
	return false, phase, nil
}
func existingMasterKey(ctx context.Context, parent *privatefs.Directory, name string, manifest Manifest) (bool, error) {
	member, err := inspectMember(ctx, parent, name, mustPolicy(manifest.MasterKey), false)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	member.identity.Name = masterKeyFilename
	if err := requireFileIdentity(member, manifest.MasterKey); err != nil {
		return false, errors.New("restore master-key target contains mismatched state")
	}
	return true, nil
}
func preflightBlobTargets(ctx context.Context, blobs searchartifacts.BlobStore, references []objectReference) error {
	if len(references) == 0 {
		return nil
	}
	current, ok := blobs.(currentBlobStore)
	if !ok {
		return errors.New("private object store requires current-version inspection for safe recovery")
	}
	for _, ref := range references {
		identity, err := current.Current(ctx, ref.Identity.Key)
		if errors.Is(err, searchartifacts.ErrNotFound) || errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		if identity.Key != ref.Identity.Key || identity.Bytes != ref.Identity.Bytes || identity.ETag == "" || identity.Version == "" {
			return errors.New("recovery object target has conflicting identity")
		}
		reader, err := blobs.Open(ctx, identity)
		if err != nil {
			return err
		}
		hash := sha256.New()
		n, readErr := io.Copy(hash, io.LimitReader(reader, identity.Bytes+1))
		readErr = errors.Join(readErr, reader.Close())
		if readErr != nil {
			return readErr
		}
		if n != identity.Bytes || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
			return errors.New("recovery object target has conflicting content")
		}
	}
	return nil
}
func PreflightRestore(ctx context.Context, options RestoreOptions) error {
	return restoreWithHooks(ctx, options, restoreHooks{}, true)
}
func Restore(ctx context.Context, options RestoreOptions) error {
	return restoreWithHooks(ctx, options, restoreHooks{}, false)
}

// restoreWithHooks retains the crash boundaries in tests. PostgreSQL and private
// objects are prepared before the master key makes the generation runnable.
func restoreWithHooks(ctx context.Context, options RestoreOptions, hooks restoreHooks, preflight ...bool) (returnedErr error) {
	if ctx == nil {
		return errors.New("restore control-plane backup: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateRestoreBinding(options); err != nil {
		return err
	}
	if _, _, err := validateExactAbsolutePath("control-plane backup source", options.Source); err != nil {
		return err
	}
	parentPath, keyName, err := validateExactAbsolutePath("restored server master key", options.MasterKeyPath)
	if err != nil {
		return err
	}
	if pathsOverlap(options.Source, options.MasterKeyPath) {
		return errors.New("restore master-key target must not overlap backup source")
	}
	parent, err := privatefs.OpenDirectory(parentPath)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, parent.Close()) }()
	bundle, err := privatefs.OpenDirectory(options.Source)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, bundle.Close()) }()
	manifest, err := verifyPinnedBundle(ctx, bundle, options.Release)
	if err != nil {
		return err
	}
	if err := requireRestoreBinding(manifest, options); err != nil {
		return err
	}
	if keyName == ".restore-"+manifest.RecoverySetID+"-master.key" {
		return errors.New("restore master-key target conflicts with recovery staging namespace")
	}
	keyExists, err := existingMasterKey(ctx, parent, keyName, manifest)
	if err != nil {
		return err
	}
	references, err := readObjectCatalog(ctx, bundle, manifest)
	if err != nil {
		return err
	}
	if err := preflightBlobTargets(ctx, options.Blobs, references); err != nil {
		return err
	}
	lease, releaseLease, err := acquireLease(ctx, options.PostgresDSN, options.Lease)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, releaseLease()) }()
	ctx, stopGuard := leaseContext(ctx, lease)
	defer stopGuard()
	pool, err := sql.Open("pgx", options.PostgresDSN)
	if err != nil {
		return errors.New("open native PostgreSQL restore target")
	}
	defer func() { returnedErr = errors.Join(returnedErr, pool.Close()) }()
	empty, phase, err := targetReceipt(ctx, pool, manifest)
	if err != nil {
		return err
	}
	if empty && keyExists {
		return errors.New("empty PostgreSQL restore target must not already contain a master key")
	}
	if len(preflight) > 0 && preflight[0] {
		return nil
	}
	if empty {
		archive, err := bundle.OpenRegular(databaseFilename, mustPolicy(manifest.Database))
		if err != nil {
			return err
		}
		if hooks.beforePublication != nil {
			hooks.beforePublication(0)
		}
		if err := ctx.Err(); err != nil {
			return errors.Join(err, archive.Close())
		}
		if err := lease.Validate(options.PostgresDSN); err != nil {
			return errors.Join(err, archive.Close())
		}
		err = control.RestorePinnedFromWithLease(ctx, options.PostgresDSN, archive, lease)
		err = errors.Join(err, archive.Close())
		if err != nil {
			return err
		}
		digest, err := manifestDigest(manifest)
		if err != nil {
			return err
		}
		if _, err := pool.ExecContext(ctx, `INSERT INTO control_recovery_publication(singleton,recovery_set_id,manifest_sha256,phase,created_at_unix_micro) VALUES(1,$1,$2,'database',$3)`, manifest.RecoverySetID, digest, time.Now().UTC().UnixMicro()); err != nil {
			return err
		}
		phase = "database"
		if hooks.afterDatabasePublish != nil {
			if err := hooks.afterDatabasePublish(); err != nil {
				return err
			}
		}
	}
	key, err := inspectMember(ctx, bundle, masterKeyFilename, mustPolicy(manifest.MasterKey), true)
	if err != nil {
		return err
	}
	defer clear(key.contents)
	database, identity, err := openVerifiedDatabase(ctx, options.PostgresDSN, key.contents, options.Release)
	if err != nil {
		return err
	}
	defer func() { returnedErr = errors.Join(returnedErr, database.Close()) }()
	if hex.EncodeToString(identity.SHA256[:]) != manifest.PostgresMigrationLedgerSHA256 {
		return errors.New("restored migration ledger differs from recovery generation")
	}
	if err := visibility.ValidateBackupDrain(ctx, database); err != nil {
		return err
	}
	var allocated, committed uint64
	if err := database.SQLDB().QueryRowContext(ctx, `SELECT last_assigned,committed_through FROM ingest_visibility_state WHERE singleton=1`).Scan(&allocated, &committed); err != nil {
		return err
	}
	if allocated != manifest.MaxAllocatedVisibility || committed != manifest.CommittedCutoff {
		return errors.New("restored visibility boundary differs from recovery generation")
	}
	actual, err := sourceObjectReferences(ctx, database)
	if err != nil {
		return err
	}
	if len(actual) != len(references) {
		return errors.New("restored SQL object catalog differs from archive")
	}
	for index, ref := range actual {
		want := references[index]
		if ref.Identity.Key != want.Identity.Key || ref.Identity.Bytes != want.Identity.Bytes || ref.SHA256 != want.SHA256 {
			return errors.New("restored SQL object catalog differs from archive")
		}
	}
	if err := lease.Validate(options.PostgresDSN); err != nil {
		return err
	}
	mapping, err := restoreObjects(ctx, bundle, options.Blobs, references)
	if err != nil {
		return err
	}
	tx, err := pgmeta.BeginSQL(ctx, pool, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
			returnedErr = errors.Join(returnedErr, err)
		}
	}()
	if err := searchartifacts.RebindBlobsInTransaction(ctx, tx, mapping); err != nil {
		return err
	}
	if err := exportjournal.RebindBlobsInTransaction(ctx, tx, mapping); err != nil {
		return err
	}
	if phase != "complete" {
		if _, err := tx.ExecContext(ctx, `UPDATE control_recovery_publication SET phase='objects' WHERE singleton=1`); err != nil {
			return err
		}
	}
	if err := lease.Validate(options.PostgresDSN); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if hooks.afterObjectsPublish != nil {
		if err := hooks.afterObjectsPublish(); err != nil {
			return err
		}
	}
	if !keyExists {
		if hooks.beforePublication != nil {
			hooks.beforePublication(1)
		}
		if err := lease.Validate(options.PostgresDSN); err != nil {
			return err
		}
		stageName := ".restore-" + manifest.RecoverySetID + "-master.key"
		staged, err := inspectMember(ctx, parent, stageName, mustPolicy(manifest.MasterKey), false)
		if errors.Is(err, os.ErrNotExist) {
			err = copyMemberWithHooks(ctx, bundle, masterKeyFilename, parent, stageName, manifest.MasterKey, copyMemberHooks{})
		} else if err == nil {
			staged.identity.Name = masterKeyFilename
			err = requireFileIdentity(staged, manifest.MasterKey)
		}
		if err != nil {
			return err
		}
		stagedFile, err := parent.OpenRegular(stageName, mustPolicy(manifest.MasterKey))
		if err != nil {
			return err
		}
		defer stagedFile.Close()
		if err := parent.RequirePinnedRegular(stageName, stagedFile); err != nil {
			return err
		}
		if err := lease.Validate(options.PostgresDSN); err != nil {
			return err
		}
		if err := parent.RenameNoReplace(stageName, parent, keyName); err != nil {
			return err
		}
		if err := parent.Sync(); err != nil {
			return err
		}
		if hooks.afterMasterKeyPublish != nil {
			if err := hooks.afterMasterKeyPublish(); err != nil {
				return err
			}
		}
	}
	if err := lease.Validate(options.PostgresDSN); err != nil {
		return err
	}
	if _, err := pool.ExecContext(ctx, `UPDATE control_recovery_publication SET phase='complete' WHERE singleton=1`); err != nil {
		return err
	}
	_, err = existingMasterKey(ctx, parent, keyName, manifest)
	return err
}
func pathsOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+string(filepath.Separator)) || strings.HasPrefix(right, left+string(filepath.Separator))
}

func verifyMigrationCorpus(expected MigrationIdentity) error {
	actual, err := buildassets.DigestMigrations(migrations.PostgreSQL(), ".")
	if err != nil {
		return err
	}
	if actual.SHA256 != expected.SHA256 || actual.LatestVersion != expected.LatestVersion {
		return errors.New("PostgreSQL migration source corpus differs from current release")
	}
	return nil
}
