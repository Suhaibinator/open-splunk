package lookupasset

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	defaultStageTTL       = 24 * time.Hour
	maximumStageTTL       = 7 * 24 * time.Hour
	maximumIDAttempts     = 8
	maximumPruneBatchSize = 1_000
	maximumTenantIDBytes  = 255
	maximumOwnerIDBytes   = 255
	maximumAssetIDBytes   = 128
	maximumStageIDBytes   = 128
	maximumUnixMicro      = int64(253_402_300_799_999_999)
)

type StoreOptions struct {
	Clock            func() time.Time
	StageIDGenerator func() (string, error)
	AssetIDGenerator func() (string, error)
	StageTTL         time.Duration
}

// Store persists verified staging uploads and immutable published versions in
// migration-owned SQLite tables. It does not expose a server route or perform
// logical lookup authorization.
type Store struct {
	database         *sql.DB
	clock            func() time.Time
	stageIDGenerator func() (string, error)
	assetIDGenerator func() (string, error)
	stageTTL         time.Duration
}

func NewStore(database *control.DB, options StoreOptions) (*Store, error) {
	if database == nil || database.SQLDB() == nil {
		return nil, fmt.Errorf("%w: lookup asset control database is required", ErrInvalidArgument)
	}
	clock := options.Clock
	if clock == nil {
		clock = time.Now
	}
	stageIDs := options.StageIDGenerator
	if stageIDs == nil {
		stageIDs = func() (string, error) { return randomIdentifier("lst_") }
	}
	assetIDs := options.AssetIDGenerator
	if assetIDs == nil {
		assetIDs = func() (string, error) { return randomIdentifier("lua_") }
	}
	ttl := options.StageTTL
	if ttl == 0 {
		ttl = defaultStageTTL
	}
	if ttl < time.Microsecond || ttl > maximumStageTTL {
		return nil, fmt.Errorf("%w: lookup staging TTL must be between one microsecond and seven days", ErrInvalidArgument)
	}
	ttl = (ttl / time.Microsecond) * time.Microsecond
	return &Store{
		database:         database.SQLDB(),
		clock:            clock,
		stageIDGenerator: stageIDs,
		assetIDGenerator: assetIDs,
		stageTTL:         ttl,
	}, nil
}

func randomIdentifier(prefix string) (string, error) {
	random := make([]byte, 24)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate lookup asset identity: %w", err)
	}
	return prefix + base64.RawURLEncoding.EncodeToString(random), nil
}

func (store *Store) StageCSV(ctx context.Context, request StageRequest) (StagedAsset, error) {
	if err := validateStoreContext(ctx, store); err != nil {
		return StagedAsset{}, err
	}
	if !validIdentity(request.TenantID, maximumTenantIDBytes) ||
		!validIdentity(request.OwnerID, maximumOwnerIDBytes) || request.Asset == nil {
		return StagedAsset{}, fmt.Errorf("%w: lookup stage scope and asset are required", ErrInvalidArgument)
	}
	verified, err := verifyDetachedAsset(ctx, request.Asset)
	if err != nil {
		return StagedAsset{}, err
	}
	conn, err := store.database.Conn(ctx)
	if err != nil {
		return StagedAsset{}, fmt.Errorf("acquire lookup staging connection: %w", err)
	}
	defer conn.Close()

	var lastCollision error
	for range maximumIDAttempts {
		stageID, generateErr := store.stageIDGenerator()
		if generateErr != nil {
			return StagedAsset{}, generateErr
		}
		if !validIdentity(stageID, maximumStageIDBytes) {
			return StagedAsset{}, fmt.Errorf("%w: generated lookup stage identity is invalid", ErrInvalidArgument)
		}
		if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
			return StagedAsset{}, fmt.Errorf("begin lookup staging: %w", err)
		}
		rollback := func() {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
		// The connection may have waited behind another SQLite writer. Define
		// the TTL only after acquiring that write boundary, then atomically prune
		// and insert so a successful response never describes a stage that
		// expired while the request was blocked.
		now, nowErr := store.now()
		if nowErr != nil {
			rollback()
			return StagedAsset{}, nowErr
		}
		expires := now.Add(store.stageTTL)
		if expires.UnixMicro() > maximumUnixMicro || !expires.After(now) {
			rollback()
			return StagedAsset{}, fmt.Errorf("%w: lookup stage expiry is out of range", ErrInvalidArgument)
		}
		// A crashed request may leave its private stage behind. Reclaim this
		// tenant's expired stages before applying the fixed concurrency and byte
		// ledgers. At most 64 rows can exist for one tenant.
		if _, err := conn.ExecContext(ctx, `
			DELETE FROM knowledge_lookup_asset_stages
			WHERE tenant_id = ? AND expires_at_unix_micro <= ?`,
			request.TenantID,
			now.UnixMicro(),
		); err != nil {
			rollback()
			return StagedAsset{}, mapStorageError("reclaim expired lookup stages", err)
		}

		_, execErr := conn.ExecContext(ctx, `
			INSERT INTO knowledge_lookup_asset_stages (
				tenant_id, stage_id, owner_id, source_sha256, content_sha256,
				source_bytes, canonical_csv, canonical_bytes, row_count,
				column_count, created_at_unix_micro, expires_at_unix_micro
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			request.TenantID,
			stageID,
			request.OwnerID,
			verified.sourceSHA256[:],
			verified.contentSHA256[:],
			verified.sourceBytes,
			verified.canonicalCSV,
			len(verified.canonicalCSV),
			len(verified.rows),
			len(verified.headers),
			now.UnixMicro(),
			expires.UnixMicro(),
		)
		if execErr == nil {
			if _, commitErr := conn.ExecContext(ctx, `COMMIT`); commitErr != nil {
				rollback()
				return StagedAsset{}, mapStorageError("commit lookup staging", commitErr)
			}
			return stagedProjection(request.TenantID, request.OwnerID, stageID, verified, now, expires), nil
		}
		rollback()
		if isIdentityCollision(execErr) {
			lastCollision = execErr
			continue
		}
		return StagedAsset{}, mapStorageError("stage lookup CSV", execErr)
	}
	return StagedAsset{}, fmt.Errorf("stage lookup CSV: generated identities repeatedly collided: %w", lastCollision)
}

func stagedProjection(tenantID, ownerID, stageID string, asset *Asset, created, expires time.Time) StagedAsset {
	return StagedAsset{
		TenantID:      tenantID,
		StageID:       stageID,
		OwnerID:       ownerID,
		ContentSHA256: asset.contentSHA256,
		SourceSHA256:  asset.sourceSHA256,
		SourceBytes:   asset.sourceBytes,
		SizeBytes:     uint64(len(asset.canonicalCSV)),
		RowCount:      uint64(len(asset.rows)),
		// Parsed assets are capped at MaximumColumns.
		// #nosec G115 -- the validated cap fits in uint32.
		ColumnCount: uint32(len(asset.headers)),
		CreatedAt:   created,
		ExpiresAt:   expires,
	}
}

func (store *Store) Publish(ctx context.Context, request PublishRequest) (Version, error) {
	return store.PublishAtomic(ctx, request, nil)
}

// PublishAtomic publishes one immutable physical version and invokes finalize
// on the same SQLite connection before COMMIT. Any callback error rolls back
// the new identity/version and restores the consumed stage as one transaction.
func (store *Store) PublishAtomic(
	ctx context.Context,
	request PublishRequest,
	finalize PublicationFinalizer,
) (Version, error) {
	if err := validateStoreContext(ctx, store); err != nil {
		return Version{}, err
	}
	if !validIdentity(request.TenantID, maximumTenantIDBytes) ||
		!validIdentity(request.OwnerID, maximumOwnerIDBytes) ||
		!validIdentity(request.StageID, maximumStageIDBytes) {
		return Version{}, fmt.Errorf("%w: lookup publication scope and stage are required", ErrInvalidArgument)
	}
	creating := request.ExpectedCurrentVersion == 0
	if creating != (request.LookupAssetID == "") {
		return Version{}, fmt.Errorf("%w: new publication requires no asset identity; replacement requires identity and expected version", ErrInvalidArgument)
	}
	lookupAssetID := request.LookupAssetID
	if !creating && !validIdentity(lookupAssetID, maximumAssetIDBytes) {
		return Version{}, fmt.Errorf("%w: lookup asset identity is invalid", ErrInvalidArgument)
	}
	if !creating && request.ExpectedCurrentVersion >= math.MaxInt64 {
		return Version{}, fmt.Errorf("%w: expected lookup asset version is out of range", ErrInvalidArgument)
	}
	if creating {
		var err error
		lookupAssetID, err = store.assetIDGenerator()
		if err != nil {
			return Version{}, err
		}
		if !validIdentity(lookupAssetID, maximumAssetIDBytes) {
			return Version{}, fmt.Errorf("%w: generated lookup asset identity is invalid", ErrInvalidArgument)
		}
	}
	conn, err := store.database.Conn(ctx)
	if err != nil {
		return Version{}, fmt.Errorf("acquire lookup publication connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return Version{}, fmt.Errorf("begin lookup publication: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.WithoutCancel(ctx), `ROLLBACK`)
		}
	}()
	// The connection may have waited behind another SQLite writer. Sample the
	// publication time only after BEGIN IMMEDIATE owns the write boundary so a
	// stage that expired while waiting cannot be promoted with a stale clock.
	now, err := store.now()
	if err != nil {
		return Version{}, err
	}

	staged, err := readStagedAsset(ctx, conn, request.TenantID, request.OwnerID, request.StageID)
	if err != nil {
		return Version{}, err
	}
	if !staged.expiresAt.After(now) {
		return Version{}, ErrNotFound
	}
	asset, err := staged.asset(ctx)
	if err != nil {
		return Version{}, err
	}

	var version uint64
	if creating {
		version = 1
		if _, err := conn.ExecContext(ctx, `
			INSERT INTO knowledge_lookup_assets (
				tenant_id, lookup_asset_id, current_version,
				created_at_unix_micro, updated_at_unix_micro
			) VALUES (?, ?, 1, ?, ?)`,
			request.TenantID, lookupAssetID, now.UnixMicro(), now.UnixMicro(),
		); err != nil {
			if isIdentityCollision(err) {
				return Version{}, fmt.Errorf("%w: generated lookup asset identity collided", ErrConflict)
			}
			return Version{}, mapStorageError("create lookup asset", err)
		}
	} else {
		var current int64
		if err := conn.QueryRowContext(ctx, `
			SELECT current_version FROM knowledge_lookup_assets
			WHERE tenant_id = ? AND lookup_asset_id = ?`,
			request.TenantID, lookupAssetID,
		).Scan(&current); errors.Is(err, sql.ErrNoRows) {
			return Version{}, ErrNotFound
		} else if err != nil {
			return Version{}, fmt.Errorf("read current lookup asset version: %w", err)
		}
		if current < 1 {
			return Version{}, fmt.Errorf("%w: current lookup asset version is invalid", ErrCorrupt)
		}
		// SQLite INTEGER values are signed; the positivity check makes this
		// conversion lossless.
		// #nosec G115 -- current is positive.
		currentVersion := uint64(current)
		if currentVersion != request.ExpectedCurrentVersion {
			return Version{}, ErrConflict
		}
		version = currentVersion + 1
	}

	if !creating {
		result, err := conn.ExecContext(ctx, `
			UPDATE knowledge_lookup_assets
			SET current_version = ?, updated_at_unix_micro = ?
			WHERE tenant_id = ? AND lookup_asset_id = ? AND current_version = ?`,
			version, now.UnixMicro(), request.TenantID, lookupAssetID, request.ExpectedCurrentVersion,
		)
		if err != nil {
			return Version{}, mapStorageError("advance current lookup asset version", err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return Version{}, ErrConflict
		}
	}
	// Reclaim the staging copy before the immutable-version capacity trigger
	// runs. The surrounding transaction restores it if insertion or finalization
	// fails, while successful publication charges exactly one retained copy.
	result, err := conn.ExecContext(ctx, `
		DELETE FROM knowledge_lookup_asset_stages
		WHERE tenant_id = ? AND stage_id = ? AND owner_id = ?`,
		request.TenantID, request.StageID, request.OwnerID,
	)
	if err != nil {
		return Version{}, mapStorageError("consume lookup asset stage", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return Version{}, fmt.Errorf("%w: lookup stage changed during publication", ErrConflict)
	}
	if _, err := conn.ExecContext(ctx, `
		INSERT INTO knowledge_lookup_asset_versions (
			tenant_id, lookup_asset_id, asset_version, source_sha256,
			content_sha256, source_bytes, canonical_csv, canonical_bytes,
			row_count, column_count, created_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		request.TenantID,
		lookupAssetID,
		version,
		staged.sourceSHA256[:],
		staged.contentSHA256[:],
		staged.sourceBytes,
		staged.canonicalCSV,
		len(staged.canonicalCSV),
		staged.rowCount,
		staged.columnCount,
		now.UnixMicro(),
	); err != nil {
		return Version{}, mapStorageError("insert immutable lookup asset version", err)
	}
	published := versionProjection(
		request.TenantID,
		lookupAssetID,
		version,
		staged,
		now,
		asset,
	)
	if finalize != nil {
		if err := finalize(ctx, conn, published); err != nil {
			return Version{}, err
		}
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		return Version{}, fmt.Errorf("commit lookup publication: %w", err)
	}
	committed = true
	return published, nil
}

func versionProjection(tenantID, assetID string, version uint64, stored stagedRecord, created time.Time, asset *Asset) Version {
	return Version{
		Ref: VersionRef{
			TenantID:      tenantID,
			LookupAssetID: assetID,
			Version:       version,
			SizeBytes:     uint64(len(stored.canonicalCSV)),
			ContentSHA256: stored.contentSHA256,
		},
		SourceSHA256: stored.sourceSHA256,
		SourceBytes:  stored.sourceBytes,
		CreatedAt:    created,
		Asset:        asset,
	}
}

func (store *Store) GetVersion(ctx context.Context, ref VersionRef) (Version, error) {
	if err := validateStoreContext(ctx, store); err != nil {
		return Version{}, err
	}
	if !validIdentity(ref.TenantID, maximumTenantIDBytes) ||
		!validIdentity(ref.LookupAssetID, maximumAssetIDBytes) || ref.Version == 0 || ref.Version > math.MaxInt64 ||
		ref.SizeBytes == 0 || ref.SizeBytes > MaximumSourceBytes ||
		ref.ContentSHA256 == ([sha256.Size]byte{}) {
		return Version{}, fmt.Errorf("%w: exact lookup asset version reference is required", ErrInvalidArgument)
	}
	version, err := readVersion(ctx, store.database, ref.TenantID, ref.LookupAssetID, ref.Version)
	if err != nil {
		return Version{}, err
	}
	if version.Ref.SizeBytes != ref.SizeBytes || version.Ref.ContentSHA256 != ref.ContentSHA256 {
		return Version{}, fmt.Errorf("%w: lookup asset reference metadata disagrees with storage", ErrCorrupt)
	}
	return version, nil
}

func (store *Store) GetCurrent(ctx context.Context, tenantID, lookupAssetID string) (Version, error) {
	if err := validateStoreContext(ctx, store); err != nil {
		return Version{}, err
	}
	if !validIdentity(tenantID, maximumTenantIDBytes) || !validIdentity(lookupAssetID, maximumAssetIDBytes) {
		return Version{}, fmt.Errorf("%w: lookup asset identity is invalid", ErrInvalidArgument)
	}
	var version uint64
	if err := store.database.QueryRowContext(ctx, `
		SELECT current_version FROM knowledge_lookup_assets
		WHERE tenant_id = ? AND lookup_asset_id = ?`, tenantID, lookupAssetID,
	).Scan(&version); errors.Is(err, sql.ErrNoRows) {
		return Version{}, ErrNotFound
	} else if err != nil {
		return Version{}, fmt.Errorf("read current lookup asset identity: %w", err)
	}
	return readVersion(ctx, store.database, tenantID, lookupAssetID, version)
}

func (store *Store) DiscardStage(ctx context.Context, tenantID, ownerID, stageID string) error {
	if err := validateStoreContext(ctx, store); err != nil {
		return err
	}
	if !validIdentity(tenantID, maximumTenantIDBytes) ||
		!validIdentity(ownerID, maximumOwnerIDBytes) || !validIdentity(stageID, maximumStageIDBytes) {
		return fmt.Errorf("%w: lookup stage scope is invalid", ErrInvalidArgument)
	}
	result, err := store.database.ExecContext(ctx, `
		DELETE FROM knowledge_lookup_asset_stages
		WHERE tenant_id = ? AND owner_id = ? AND stage_id = ?`, tenantID, ownerID, stageID)
	if err != nil {
		return mapStorageError("discard lookup asset stage", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect discarded lookup stage: %w", err)
	}
	if changed != 1 {
		return ErrNotFound
	}
	return nil
}

func (store *Store) PruneExpiredStages(ctx context.Context, before time.Time, limit uint32) (uint32, error) {
	if err := validateStoreContext(ctx, store); err != nil {
		return 0, err
	}
	before = before.UTC().Truncate(time.Microsecond)
	if before.UnixMicro() < 1 || before.UnixMicro() > maximumUnixMicro || limit == 0 || limit > maximumPruneBatchSize {
		return 0, fmt.Errorf("%w: lookup stage prune boundary or limit is invalid", ErrInvalidArgument)
	}
	result, err := store.database.ExecContext(ctx, `
		DELETE FROM knowledge_lookup_asset_stages
		WHERE (tenant_id, stage_id) IN (
			SELECT tenant_id, stage_id
			FROM knowledge_lookup_asset_stages
			WHERE expires_at_unix_micro <= ?
			ORDER BY expires_at_unix_micro, tenant_id, stage_id
			LIMIT ?
		)`, before.UnixMicro(), limit)
	if err != nil {
		return 0, mapStorageError("prune expired lookup stages", err)
	}
	changed, err := result.RowsAffected()
	if err != nil || changed < 0 || changed > int64(limit) {
		return 0, fmt.Errorf("%w: lookup stage prune count is invalid", ErrCorrupt)
	}
	// RowsAffected is checked against the uint32 limit above.
	// #nosec G115 -- changed is in [0, limit].
	return uint32(changed), nil
}

type stagedRecord struct {
	sourceSHA256  [sha256.Size]byte
	contentSHA256 [sha256.Size]byte
	sourceBytes   uint64
	canonicalCSV  []byte
	rowCount      uint64
	columnCount   uint32
	createdAt     time.Time
	expiresAt     time.Time
}

func readStagedAsset(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenantID, ownerID, stageID string) (stagedRecord, error) {
	var sourceDigest, contentDigest []byte
	var sourceBytes, canonicalBytes, rowCount, columnCount int64
	var canonical []byte
	var created, expires int64
	err := query.QueryRowContext(ctx, `
		SELECT source_sha256, content_sha256, source_bytes, canonical_csv,
		       canonical_bytes, row_count, column_count,
		       created_at_unix_micro, expires_at_unix_micro
		FROM knowledge_lookup_asset_stages
		WHERE tenant_id = ? AND owner_id = ? AND stage_id = ?`,
		tenantID, ownerID, stageID,
	).Scan(
		&sourceDigest, &contentDigest, &sourceBytes, &canonical,
		&canonicalBytes, &rowCount, &columnCount, &created, &expires,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return stagedRecord{}, ErrNotFound
	}
	if err != nil {
		return stagedRecord{}, fmt.Errorf("read staged lookup asset: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return stagedRecord{}, err
	}
	if len(sourceDigest) != sha256.Size || len(contentDigest) != sha256.Size ||
		sourceBytes < 1 || sourceBytes > MaximumSourceBytes ||
		canonicalBytes < 1 || canonicalBytes != int64(len(canonical)) || canonicalBytes > MaximumSourceBytes ||
		rowCount < 0 || rowCount > MaximumRows || columnCount < 1 || columnCount > MaximumColumns ||
		created < 1 || created > maximumUnixMicro || expires <= created || expires > maximumUnixMicro {
		return stagedRecord{}, fmt.Errorf("%w: staged lookup asset has invalid metadata", ErrCorrupt)
	}
	result := stagedRecord{
		// All persisted counts are validated against the package maxima above.
		// #nosec G115 -- validated bounded nonnegative SQLite INTEGER values.
		sourceBytes: uint64(sourceBytes), canonicalCSV: canonical,
		rowCount: uint64(rowCount), columnCount: uint32(columnCount),
		createdAt: time.UnixMicro(created).UTC(), expiresAt: time.UnixMicro(expires).UTC(),
	}
	copy(result.sourceSHA256[:], sourceDigest)
	copy(result.contentSHA256[:], contentDigest)
	if err := ctx.Err(); err != nil {
		return stagedRecord{}, err
	}
	return result, nil
}

func (record stagedRecord) asset(ctx context.Context) (*Asset, error) {
	asset, err := ParseCSVContext(ctx, bytes.NewReader(record.canonicalCSV), DefaultLimits())
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("%w: persisted canonical CSV cannot be decoded: %w", ErrCorrupt, err)
	}
	canonicalMatches, err := equalBytesContext(ctx, asset.canonicalCSV, record.canonicalCSV)
	if err != nil {
		return nil, err
	}
	if asset.contentSHA256 != record.contentSHA256 ||
		uint64(len(asset.canonicalCSV)) != uint64(len(record.canonicalCSV)) ||
		uint64(len(asset.rows)) != record.rowCount ||
		// Parsed assets are capped at MaximumColumns.
		// #nosec G115 -- the validated cap fits in uint32.
		uint32(len(asset.headers)) != record.columnCount ||
		!canonicalMatches {
		return nil, fmt.Errorf("%w: persisted canonical CSV metadata disagrees", ErrCorrupt)
	}
	// The canonical parse's source identity describes the canonical bytes, while
	// record.sourceSHA256 intentionally describes the original upload.
	asset.sourceSHA256 = record.sourceSHA256
	asset.sourceBytes = record.sourceBytes
	return asset, nil
}

func readVersion(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenantID, assetID string, version uint64) (Version, error) {
	var sourceDigest, contentDigest []byte
	var sourceBytes, canonicalBytes, rowCount, columnCount, created int64
	var canonical []byte
	err := query.QueryRowContext(ctx, `
		SELECT source_sha256, content_sha256, source_bytes, canonical_csv,
		       canonical_bytes, row_count, column_count, created_at_unix_micro
		FROM knowledge_lookup_asset_versions
		WHERE tenant_id = ? AND lookup_asset_id = ? AND asset_version = ?`,
		tenantID, assetID, version,
	).Scan(
		&sourceDigest, &contentDigest, &sourceBytes, &canonical,
		&canonicalBytes, &rowCount, &columnCount, &created,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	if err != nil {
		return Version{}, fmt.Errorf("read immutable lookup asset version: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Version{}, err
	}
	record := stagedRecord{canonicalCSV: canonical}
	if len(sourceDigest) == sha256.Size {
		copy(record.sourceSHA256[:], sourceDigest)
	}
	if len(contentDigest) == sha256.Size {
		copy(record.contentSHA256[:], contentDigest)
	}
	if sourceBytes >= 0 {
		record.sourceBytes = uint64(sourceBytes)
	}
	if rowCount >= 0 {
		record.rowCount = uint64(rowCount)
	}
	if columnCount >= 0 {
		record.columnCount = uint32(columnCount)
	}
	if len(sourceDigest) != sha256.Size || len(contentDigest) != sha256.Size ||
		sourceBytes < 1 || sourceBytes > MaximumSourceBytes || canonicalBytes != int64(len(canonical)) ||
		canonicalBytes < 1 || canonicalBytes > MaximumSourceBytes || rowCount < 0 || rowCount > MaximumRows ||
		columnCount < 1 || columnCount > MaximumColumns || created < 1 || created > maximumUnixMicro {
		return Version{}, fmt.Errorf("%w: immutable lookup asset version has invalid metadata", ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return Version{}, err
	}
	asset, err := record.asset(ctx)
	if err != nil {
		return Version{}, err
	}
	return versionProjection(tenantID, assetID, version, record, time.UnixMicro(created).UTC(), asset), nil
}

func verifyDetachedAsset(ctx context.Context, asset *Asset) (*Asset, error) {
	if asset == nil || len(asset.canonicalCSV) == 0 {
		return nil, fmt.Errorf("%w: parsed CSV asset is required", ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reparsed, err := ParseCSVContext(ctx, bytes.NewReader(asset.canonicalCSV), DefaultLimits())
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return nil, fmt.Errorf("%w: detached CSV asset is invalid: %w", ErrInvalidArgument, err)
	}
	canonicalMatches, err := equalBytesContext(ctx, reparsed.canonicalCSV, asset.canonicalCSV)
	if err != nil {
		return nil, err
	}
	rowsMatch, err := equalRowsContext(ctx, reparsed.rows, asset.rows)
	if err != nil {
		return nil, err
	}
	if reparsed.contentSHA256 != asset.contentSHA256 || !canonicalMatches ||
		!slices.Equal(reparsed.headers, asset.headers) || !rowsMatch ||
		asset.sourceBytes < 1 || asset.sourceBytes > MaximumSourceBytes || asset.sourceSHA256 == ([sha256.Size]byte{}) {
		return nil, fmt.Errorf("%w: detached CSV asset metadata disagrees", ErrInvalidArgument)
	}
	// Preserve the original source identity from the caller's successfully
	// parsed asset while returning a fresh immutable detached representation.
	reparsed.sourceSHA256 = asset.sourceSHA256
	reparsed.sourceBytes = asset.sourceBytes
	return reparsed, nil
}

func equalRowsContext(ctx context.Context, left, right [][]string) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	for index := range left {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if !slices.Equal(left[index], right[index]) {
			return false, nil
		}
	}
	return true, ctx.Err()
}

func equalBytesContext(ctx context.Context, left, right []byte) (bool, error) {
	if len(left) != len(right) {
		return false, nil
	}
	for offset := 0; offset < len(left); offset += csvContextCheckpointBytes {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		end := min(offset+csvContextCheckpointBytes, len(left))
		if !bytes.Equal(left[offset:end], right[offset:end]) {
			return false, nil
		}
	}
	return true, ctx.Err()
}

func validateStoreContext(ctx context.Context, store *Store) error {
	if ctx == nil || store == nil || store.database == nil || store.clock == nil ||
		store.stageIDGenerator == nil || store.assetIDGenerator == nil {
		return fmt.Errorf("%w: lookup asset store and context are required", ErrInvalidArgument)
	}
	return ctx.Err()
}

func (store *Store) now() (time.Time, error) {
	now := store.clock().UTC().Truncate(time.Microsecond)
	if now.UnixMicro() < 1 || now.UnixMicro() > maximumUnixMicro {
		return time.Time{}, fmt.Errorf("%w: lookup asset clock is out of range", ErrInvalidArgument)
	}
	return now, nil
}

func validIdentity(value string, maximumBytes int) bool {
	return value != "" && len(value) <= maximumBytes && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && !strings.ContainsFunc(value, unicode.IsControl)
}

func isIdentityCollision(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "unique constraint failed") || strings.Contains(message, "already exists")
}

func mapStorageError(operation string, err error) error {
	if err == nil {
		return nil
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "capacity") || strings.Contains(message, "too many") {
		return fmt.Errorf("%s: %w", operation, ErrResourceLimit)
	}
	if strings.Contains(message, "foreign key constraint failed") || strings.Contains(message, "ledger is missing") {
		return fmt.Errorf("%s: %w", operation, ErrNotFound)
	}
	if isIdentityCollision(err) {
		return fmt.Errorf("%s: %w", operation, ErrConflict)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
