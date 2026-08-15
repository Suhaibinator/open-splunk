// Package lookupasset implements the bounded, immutable CSV asset layer used
// by exact search-time lookups. It deliberately contains no SPL, visibility,
// or logical knowledge-object policy; callers bind an exact VersionRef from
// this package into their separately authorized lookup definition.
package lookupasset

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"
)

const (
	MaximumSourceBytes = 8 << 20
	MaximumRows        = 100_000
	MaximumColumns     = 64
	MaximumCellBytes   = 64 << 10
	MaximumRowBytes    = 1 << 20
	MaximumHeaderBytes = 255

	MaximumKeyColumns = 4

	MaximumStagedAssetsPerTenant    = 64
	MaximumAssetIdentitiesPerTenant = 2_048
	MaximumAssetVersionsPerTenant   = 8_192
	MaximumStoredBytesPerTenant     = 2 << 30
)

var (
	ErrInvalidArgument = errors.New("lookupasset: invalid argument")
	ErrResourceLimit   = errors.New("lookupasset: resource limit exceeded")
	ErrMalformedCSV    = errors.New("lookupasset: malformed CSV")
	ErrDuplicateKey    = errors.New("lookupasset: duplicate exact key")
	ErrNotFound        = errors.New("lookupasset: not found")
	ErrConflict        = errors.New("lookupasset: version conflict")
	ErrCorrupt         = errors.New("lookupasset: persisted state is corrupt")
)

// Limits is a caller-selectable restriction beneath the fixed package hard
// limits. A zero field selects the corresponding hard limit. This permits
// tests and callers with smaller budgets without allowing a larger dialect.
type Limits struct {
	SourceBytes int
	Rows        int
	Columns     int
	CellBytes   int
	RowBytes    int
	HeaderBytes int
}

// DefaultLimits returns the fixed v0.4 CSV resource contract.
func DefaultLimits() Limits {
	return Limits{
		SourceBytes: MaximumSourceBytes,
		Rows:        MaximumRows,
		Columns:     MaximumColumns,
		CellBytes:   MaximumCellBytes,
		RowBytes:    MaximumRowBytes,
		HeaderBytes: MaximumHeaderBytes,
	}
}

func normalizeLimits(input Limits) (Limits, error) {
	defaults := DefaultLimits()
	values := []*int{
		&input.SourceBytes,
		&input.Rows,
		&input.Columns,
		&input.CellBytes,
		&input.RowBytes,
		&input.HeaderBytes,
	}
	defaultValues := []int{
		defaults.SourceBytes,
		defaults.Rows,
		defaults.Columns,
		defaults.CellBytes,
		defaults.RowBytes,
		defaults.HeaderBytes,
	}
	for index := range values {
		if *values[index] == 0 {
			*values[index] = defaultValues[index]
		}
		if *values[index] < 1 || *values[index] > defaultValues[index] {
			return Limits{}, fmt.Errorf("%w: CSV limits must be positive and no larger than the v0.4 hard limits", ErrInvalidArgument)
		}
	}
	if input.HeaderBytes > input.CellBytes || input.CellBytes > input.RowBytes {
		return Limits{}, fmt.Errorf("%w: CSV header, cell, and row byte limits are inconsistent", ErrInvalidArgument)
	}
	return input, nil
}

// Asset is a detached immutable normalized CSV document. Slice and byte
// accessors return copies; scalar string accessors are safe because Go strings
// are immutable. Callers cannot alter the bytes that ContentSHA256
// authenticates.
type Asset struct {
	headers        []string
	rows           [][]string
	canonicalCSV   []byte
	contentSHA256  [sha256.Size]byte
	sourceSHA256   [sha256.Size]byte
	sourceBytes    uint64
	headerOrdinals map[string]int
}

func (asset *Asset) Headers() []string {
	if asset == nil {
		return nil
	}
	return slices.Clone(asset.headers)
}

func (asset *Asset) Rows() [][]string {
	if asset == nil {
		return nil
	}
	return cloneRows(asset.rows)
}

func (asset *Asset) Row(index int) ([]string, bool) {
	if asset == nil || index < 0 || index >= len(asset.rows) {
		return nil, false
	}
	return slices.Clone(asset.rows[index]), true
}

// Header returns one immutable header value without cloning the complete
// schema. It is intended for bounded consumers that already retain the exact
// Asset authority.
func (asset *Asset) Header(index int) (string, bool) {
	if asset == nil || index < 0 || index >= len(asset.headers) {
		return "", false
	}
	return asset.headers[index], true
}

// Cell returns one immutable cell value without cloning its row or the full
// table. No returned value can mutate the retained asset.
func (asset *Asset) Cell(row, column int) (string, bool) {
	if asset == nil || row < 0 || row >= len(asset.rows) ||
		column < 0 || column >= len(asset.rows[row]) {
		return "", false
	}
	return asset.rows[row][column], true
}

func (asset *Asset) RowCount() uint64 {
	if asset == nil {
		return 0
	}
	return uint64(len(asset.rows))
}

func (asset *Asset) ColumnCount() uint32 {
	if asset == nil {
		return 0
	}
	// Parsed assets are capped at MaximumColumns.
	// #nosec G115 -- the validated cap fits in uint32.
	return uint32(len(asset.headers))
}

func (asset *Asset) CanonicalCSV() []byte {
	if asset == nil {
		return nil
	}
	return bytes.Clone(asset.canonicalCSV)
}

// CanonicalSizeBytes returns the authenticated canonical CSV size without
// cloning the retained byte slice. It is safe for budget and authority checks.
func (asset *Asset) CanonicalSizeBytes() uint64 {
	if asset == nil {
		return 0
	}
	return uint64(len(asset.canonicalCSV))
}

// ContentSHA256 authenticates CanonicalCSV. Equivalent CSV quoting, line
// endings, and an optional leading UTF-8 BOM normalize to the same digest.
func (asset *Asset) ContentSHA256() [sha256.Size]byte {
	if asset == nil {
		return [sha256.Size]byte{}
	}
	return asset.contentSHA256
}

// SourceSHA256 authenticates the exact bounded byte stream supplied to
// ParseCSV or ParseCSVContext before normalization.
func (asset *Asset) SourceSHA256() [sha256.Size]byte {
	if asset == nil {
		return [sha256.Size]byte{}
	}
	return asset.sourceSHA256
}

func (asset *Asset) SourceBytes() uint64 {
	if asset == nil {
		return 0
	}
	return asset.sourceBytes
}

func (asset *Asset) columnOrdinal(name string) (int, bool) {
	if asset == nil {
		return 0, false
	}
	ordinal, ok := asset.headerOrdinals[name]
	return ordinal, ok
}

func cloneRows(rows [][]string) [][]string {
	cloned := make([][]string, len(rows))
	for index := range rows {
		cloned[index] = slices.Clone(rows[index])
	}
	return cloned
}

// VersionRef is the stable physical authority embedded into snapshots and
// logical lookup definitions. Published versions are never updated or deleted.
type VersionRef struct {
	TenantID      string
	LookupAssetID string
	Version       uint64
	SizeBytes     uint64
	ContentSHA256 [sha256.Size]byte
}

// Version is a detached immutable published asset.
type Version struct {
	Ref          VersionRef
	SourceSHA256 [sha256.Size]byte
	SourceBytes  uint64
	CreatedAt    time.Time
	Asset        *Asset
}

// StagedAsset is a verified but unpublished upload. A stage is temporary and
// may be consumed once, explicitly discarded, or pruned after ExpiresAt.
type StagedAsset struct {
	TenantID      string
	StageID       string
	OwnerID       string
	ContentSHA256 [sha256.Size]byte
	SourceSHA256  [sha256.Size]byte
	SourceBytes   uint64
	SizeBytes     uint64
	RowCount      uint64
	ColumnCount   uint32
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type StageRequest struct {
	TenantID string
	OwnerID  string
	Asset    *Asset
}

type PublishRequest struct {
	TenantID               string
	OwnerID                string
	StageID                string
	LookupAssetID          string
	ExpectedCurrentVersion uint64
}

// Repository is the unexposed physical-asset control-plane contract. Logical
// lookup publication stores its metadata separately and refers to VersionRef.
type Repository interface {
	StageCSV(ctx context.Context, request StageRequest) (StagedAsset, error)
	Publish(ctx context.Context, request PublishRequest) (Version, error)
	GetVersion(ctx context.Context, ref VersionRef) (Version, error)
	GetCurrent(ctx context.Context, tenantID, lookupAssetID string) (Version, error)
	DiscardStage(ctx context.Context, tenantID, ownerID, stageID string) error
	PruneExpiredStages(ctx context.Context, before time.Time, limit uint32) (uint32, error)
}

// PublicationTransaction is the narrow same-connection authority exposed to
// an internal atomic publication finalizer. It lets a sibling logical catalog
// bind the newly inserted immutable version before the physical transaction
// commits without exposing database/sql transaction methods.
type PublicationTransaction interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// PublicationFinalizer runs after the immutable physical version and stage
// consumption have been written, but before COMMIT. Returning an error rolls
// the complete physical publication back.
type PublicationFinalizer func(context.Context, PublicationTransaction, Version) error

// AtomicRepository extends the physical store with cross-layer publication.
// Management uses this surface so a logical definition can never lose a race
// after leaving behind an unreachable, quota-consuming immutable asset.
type AtomicRepository interface {
	Repository
	PublishAtomic(context.Context, PublishRequest, PublicationFinalizer) (Version, error)
}
