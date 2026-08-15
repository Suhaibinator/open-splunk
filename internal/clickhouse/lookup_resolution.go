package clickhouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// lookupContextCheckRows bounds cancellation latency while walking the
// maximum admitted asset without paying a context lookup for every cell.
const lookupContextCheckRows = 1024

const (
	// MaximumLookupStagesPerQuery bounds both retained asset payload and the
	// number of hash joins added to one physical query.
	MaximumLookupStagesPerQuery = 16
	// MaximumLookupKeys is the v0.4 exact-composite-key compatibility bound.
	MaximumLookupKeys = 4
	// MaximumLookupMatchKeyComponentsPerEvent is the independent runtime match
	// work ceiling across all explicit and automatic stages. One event never
	// evaluates more exact key components even when every selector matches.
	MaximumLookupMatchKeyComponentsPerEvent = MaximumLookupStagesPerQuery * MaximumLookupKeys
	// MaximumLookupAssetRows matches the immutable CSV publication boundary.
	MaximumLookupAssetRows = lookupasset.MaximumRows
	// MaximumLookupAssetColumns matches the immutable CSV publication boundary.
	MaximumLookupAssetColumns = lookupasset.MaximumColumns
	// MaximumLookupAssetBytes charges decoded headers and cells. Decoding CSV
	// cannot increase their aggregate payload beyond the admitted source bytes.
	MaximumLookupAssetBytes uint64 = lookupasset.MaximumSourceBytes
	// MaximumLookupCellBytes and MaximumLookupRowBytes independently prevent a
	// small row count from concentrating the complete asset budget in one cell
	// or one ClickHouse block row.
	MaximumLookupCellBytes = lookupasset.MaximumCellBytes
	MaximumLookupRowBytes  = lookupasset.MaximumRowBytes
	// MaximumLookupResolvedCellsPerQuery independently bounds Go slice/string
	// header amplification even when every decoded CSV cell is empty. It admits
	// one maximum-width, maximum-row asset or sixteen 100k-row two-column
	// assets, while preventing stage multiplication of the full 64-column
	// storage envelope.
	MaximumLookupResolvedCellsPerQuery uint64 = uint64(MaximumLookupAssetRows) * uint64(MaximumLookupAssetColumns)
	// MaximumLookupSelectedCellsPerQuery bounds native external-table cell work:
	// every selected String cell plus one internal UInt8 match-marker cell per
	// asset row. The marker is charged even though it is not an authored column.
	MaximumLookupSelectedCellsPerQuery = MaximumLookupResolvedCellsPerQuery
)

// LookupResolution is an immutable, detached lookup version supplied by a
// trusted control-plane resolver. DefinitionName is the visible knowledge
// object selected by authored SPL; ObjectID, Version, and ContentSHA256 pin the
// physical asset version. Compiler values clone resolutions before retaining
// them, and the final compiled-query seal covers both identity and selected
// column payloads.
type LookupResolution struct {
	tenantID       string
	definitionName string
	logicalID      string
	logicalVersion uint64
	objectID       string
	version        uint64
	sizeBytes      uint64
	contentSHA256  [sha256.Size]byte
	contract       plan.Lookup
	contractSet    bool
	headers        []string
	asset          *lookupasset.Asset
	rows           [][]string
	backing        *lookupResolutionBacking
}

// lookupResolutionBacking records facts established while the detached asset
// is validated. The backing is created only by this package and thereafter
// shared by value clones; no exported method exposes any of its slices. That
// lets admission and seal revalidation charge a maximum-envelope asset from
// scalar facts instead of rescanning every cell.
type lookupResolutionBacking struct {
	headers      []string
	asset        *lookupasset.Asset
	rows         [][]string
	rowCount     int
	payloadBytes uint64
	cellCount    uint64
}

// NewLookupResolution validates and detaches one immutable CSV asset version.
// Semantic key uniqueness is deliberately validated later against the exact
// ordered key columns selected by a logical Lookup operator.
func NewLookupResolution(
	tenantID string,
	definitionName string,
	objectID string,
	version uint64,
	sizeBytes uint64,
	contentSHA256 [sha256.Size]byte,
	headers []string,
	rows [][]string,
) (LookupResolution, error) {
	// Detach caller-owned slices exactly once. Every later clone remains inside
	// this package and may safely share the immutable backing; exported accessors
	// still return detached copies.
	return newOwnedLookupResolution(
		tenantID,
		definitionName,
		objectID,
		version,
		sizeBytes,
		contentSHA256,
		cloneStrings(headers),
		cloneLookupRows(rows),
	)
}

// newOwnedLookupResolution takes ownership of headers and rows. Callers must
// supply freshly detached slices which are never mutated after this call.
// Keeping that ownership boundary separate prevents immutable compiler-value
// clones from multiplying the potentially 6.4-million-cell backing table.
func newOwnedLookupResolution(
	tenantID string,
	definitionName string,
	objectID string,
	version uint64,
	sizeBytes uint64,
	contentSHA256 [sha256.Size]byte,
	headers []string,
	rows [][]string,
) (LookupResolution, error) {
	resolution := LookupResolution{
		tenantID:       strings.Clone(tenantID),
		definitionName: strings.Clone(definitionName),
		objectID:       strings.Clone(objectID),
		version:        version,
		sizeBytes:      sizeBytes,
		contentSHA256:  contentSHA256,
		headers:        headers,
		rows:           rows,
	}
	payloadBytes, err := validateLookupResolutionAndMeasureContext(
		context.Background(),
		resolution,
	)
	if err != nil {
		return LookupResolution{}, err
	}
	resolution.backing = newLookupResolutionBacking(resolution, payloadBytes)
	return resolution, nil
}

// NewLookupResolutionFromVersion bridges the immutable asset repository to
// physical compilation without reparsing CSV or weakening the exact VersionRef
// captured by the catalog. Definition visibility and mapping authorization are
// still resolved separately; this function retains only the package-immutable
// backing of the already-pinned physical version.
func NewLookupResolutionFromVersion(
	definitionName string,
	version lookupasset.Version,
) (LookupResolution, error) {
	if version.Asset == nil ||
		version.Ref.TenantID == "" ||
		version.Ref.LookupAssetID == "" ||
		version.Ref.Version == 0 ||
		version.Ref.SizeBytes == 0 ||
		version.Ref.SizeBytes != version.Asset.CanonicalSizeBytes() ||
		version.Ref.ContentSHA256 != version.Asset.ContentSHA256() {
		return LookupResolution{}, errors.New(
			"create ClickHouse lookup resolution: immutable asset version is inconsistent",
		)
	}
	resolution := LookupResolution{
		tenantID:       strings.Clone(version.Ref.TenantID),
		definitionName: strings.Clone(definitionName),
		objectID:       strings.Clone(version.Ref.LookupAssetID),
		version:        version.Ref.Version,
		sizeBytes:      version.Ref.SizeBytes,
		contentSHA256:  version.Ref.ContentSHA256,
		headers:        version.Asset.Headers(),
		asset:          version.Asset,
	}
	// lookupasset.Asset is already a validated immutable value, and its
	// canonical source size conservatively bounds decoded header/cell bytes.
	// Authenticate that backing before generic validation so bridging a maximum
	// published version does not walk all cells again.
	resolution.backing = newLookupResolutionBacking(resolution, version.Ref.SizeBytes)
	if err := validateLookupResolution(resolution); err != nil {
		return LookupResolution{}, err
	}
	return resolution, nil
}

// WithLogicalContract binds the immutable asset version to the exact logical
// mappings and overwrite behavior stored by the resolved definition. Asset
// identity alone is insufficient authority: authored SPL must not be able to
// select arbitrary columns from a visible CSV or change its collision policy.
func (resolution LookupResolution) WithLogicalContract(
	contract plan.Lookup,
	logicalLookupID string,
	logicalLookupVersion uint64,
) (LookupResolution, error) {
	if err := validateLookupResolution(resolution); err != nil {
		return LookupResolution{}, err
	}
	normalized, err := normalizeLookupResolutionContract(contract)
	if err != nil {
		return LookupResolution{}, err
	}
	if normalized.DefinitionName != resolution.definitionName {
		return LookupResolution{}, errors.New(
			"bind ClickHouse lookup contract: definition name disagrees with asset authority",
		)
	}
	if logicalLookupID == "" || logicalLookupVersion == 0 ||
		!utf8.ValidString(logicalLookupID) ||
		len(logicalLookupID) > MaximumLookupCellBytes ||
		strings.IndexByte(logicalLookupID, 0) >= 0 {
		return LookupResolution{}, errors.New(
			"bind ClickHouse lookup contract: logical identity is invalid",
		)
	}
	result := resolution.clone()
	result.logicalID = strings.Clone(logicalLookupID)
	result.logicalVersion = logicalLookupVersion
	result.contract = normalized
	result.contractSet = true
	if err := validateLookupResolution(result); err != nil {
		return LookupResolution{}, err
	}
	return result, nil
}

// NewLookupResolutionWithContract creates and binds one resolution in a
// single fail-closed operation.
func NewLookupResolutionWithContract(
	contract plan.Lookup,
	logicalLookupID string,
	logicalLookupVersion uint64,
	tenantID string,
	objectID string,
	version uint64,
	sizeBytes uint64,
	contentSHA256 [sha256.Size]byte,
	headers []string,
	rows [][]string,
) (LookupResolution, error) {
	resolution, err := NewLookupResolution(
		tenantID,
		contract.DefinitionName,
		objectID,
		version,
		sizeBytes,
		contentSHA256,
		headers,
		rows,
	)
	if err != nil {
		return LookupResolution{}, err
	}
	return resolution.WithLogicalContract(
		contract,
		logicalLookupID,
		logicalLookupVersion,
	)
}

// NewLookupResolutionFromVersionWithContract bridges and binds one repository
// version without exposing a period in which it can be compiled unbound.
func NewLookupResolutionFromVersionWithContract(
	contract plan.Lookup,
	logicalLookupID string,
	logicalLookupVersion uint64,
	version lookupasset.Version,
) (LookupResolution, error) {
	resolution, err := NewLookupResolutionFromVersion(
		contract.DefinitionName,
		version,
	)
	if err != nil {
		return LookupResolution{}, err
	}
	return resolution.WithLogicalContract(
		contract,
		logicalLookupID,
		logicalLookupVersion,
	)
}

// WithLookupResolutions returns a detached compiler value configured with one
// resolution per logical Lookup occurrence, in pipeline order. Compilation
// revalidates tenant, name, schema, key uniqueness, and aggregate budgets
// against the exact logical query before any SQL is emitted.
func (compiler Compiler) WithLookupResolutions(
	resolutions []LookupResolution,
) (Compiler, error) {
	return compiler.WithLookupResolutionsContext(context.Background(), resolutions)
}

// WithLookupResolutionsContext is the cancellable form of
// WithLookupResolutions for admission-time maximum-envelope validation.
func (compiler Compiler) WithLookupResolutionsContext(
	ctx context.Context,
	resolutions []LookupResolution,
) (Compiler, error) {
	if ctx == nil {
		return Compiler{}, errors.New(
			"configure ClickHouse lookup resolutions: context is nil",
		)
	}
	if err := ctx.Err(); err != nil {
		return Compiler{}, err
	}
	if len(resolutions) > MaximumLookupStagesPerQuery {
		return Compiler{}, fmt.Errorf(
			"configure ClickHouse lookup resolutions: more than %d stages",
			MaximumLookupStagesPerQuery,
		)
	}
	var retainedBytes uint64
	var resolvedCells uint64
	for index, resolution := range resolutions {
		if err := validateLookupResolutionContext(ctx, resolution); err != nil {
			return Compiler{}, fmt.Errorf(
				"configure ClickHouse lookup resolution %d: %w",
				index,
				err,
			)
		}
		if !resolution.contractSet {
			return Compiler{}, fmt.Errorf(
				"configure ClickHouse lookup resolution %d: stored logical contract is required",
				index,
			)
		}
		contract, err := normalizeLookupResolutionContract(resolution.contract)
		if err != nil || !lookupResolutionContractsEqual(contract, resolution.contract) {
			return Compiler{}, fmt.Errorf(
				"configure ClickHouse lookup resolution %d: stored logical contract is invalid",
				index,
			)
		}
		cells, ok := lookupResolutionCellCount(resolution)
		if !ok || cells > MaximumLookupResolvedCellsPerQuery-resolvedCells {
			return Compiler{}, errors.New(
				"configure ClickHouse lookup resolutions: aggregate cell-work budget exceeded",
			)
		}
		resolvedCells += cells
		bytes, ok := lookupResolutionPayloadBytes(resolution)
		if !ok {
			return Compiler{}, errors.New(
				"configure ClickHouse lookup resolutions: retained byte count overflows",
			)
		}
		retainedBytes, ok = checkedLookupBytesAdd(retainedBytes, bytes)
		if !ok || retainedBytes >
			uint64(MaximumLookupStagesPerQuery)*MaximumLookupAssetBytes {
			return Compiler{}, errors.New(
				"configure ClickHouse lookup resolutions: aggregate asset budget exceeded",
			)
		}
	}
	compiler.lookupResolutions = make([]LookupResolution, len(resolutions))
	for index, resolution := range resolutions {
		compiler.lookupResolutions[index] = resolution.clone()
	}
	return compiler, nil
}

// CompileWithLookupResolutions is a convenience wrapper for a one-shot
// resolved compilation. Derived compiler surfaces can instead use the value
// returned by WithLookupResolutions.
func (compiler Compiler) CompileWithLookupResolutions(
	query *plan.Query,
	resolutions []LookupResolution,
) (CompiledQuery, error) {
	return compiler.CompileWithLookupResolutionsContext(
		context.Background(),
		query,
		resolutions,
	)
}

func (compiler Compiler) CompileWithLookupResolutionsContext(
	ctx context.Context,
	query *plan.Query,
	resolutions []LookupResolution,
) (CompiledQuery, error) {
	configured, err := compiler.WithLookupResolutionsContext(ctx, resolutions)
	if err != nil {
		return CompiledQuery{}, err
	}
	return configured.CompileContext(ctx, query)
}

// TenantID returns the exact tenant authority of the pinned version.
func (resolution LookupResolution) TenantID() string {
	return strings.Clone(resolution.tenantID)
}

// DefinitionName returns the visible lookup object selected by SPL.
func (resolution LookupResolution) DefinitionName() string {
	return strings.Clone(resolution.definitionName)
}

// LogicalID and LogicalVersion identify the exact persisted lookup-definition
// revision whose mappings, selector, and overwrite behavior were admitted.
func (resolution LookupResolution) LogicalID() string {
	return strings.Clone(resolution.logicalID)
}

func (resolution LookupResolution) LogicalVersion() uint64 {
	return resolution.logicalVersion
}

// ObjectID returns the immutable lookup asset identity.
func (resolution LookupResolution) ObjectID() string {
	return strings.Clone(resolution.objectID)
}

// Version returns the exact positive asset version.
func (resolution LookupResolution) Version() uint64 { return resolution.version }

// SizeBytes returns the exact canonical source byte length pinned by the
// immutable asset VersionRef.
func (resolution LookupResolution) SizeBytes() uint64 { return resolution.sizeBytes }

// ContentSHA256 returns the immutable source-asset digest.
func (resolution LookupResolution) ContentSHA256() [sha256.Size]byte {
	return resolution.contentSHA256
}

// LogicalContract returns a detached stored-definition contract. The boolean
// is false for an asset-only resolution, which physical compilation rejects.
func (resolution LookupResolution) LogicalContract() (plan.Lookup, bool) {
	if !resolution.contractSet {
		return plan.Lookup{}, false
	}
	return cloneLookupResolutionContract(resolution.contract), true
}

// Headers returns detached CSV headers in source order.
func (resolution LookupResolution) Headers() []string {
	return cloneStrings(resolution.headers)
}

// Rows returns detached decoded CSV rows in source order.
func (resolution LookupResolution) Rows() [][]string {
	if resolution.asset != nil {
		return resolution.asset.Rows()
	}
	return cloneLookupRows(resolution.rows)
}

// RowCount and ColumnCount expose only scalar shape metadata, allowing
// admission to charge a resolution without cloning its retained cells.
func (resolution LookupResolution) RowCount() uint64 {
	if resolution.asset != nil {
		return resolution.asset.RowCount()
	}
	return uint64(len(resolution.rows))
}

func (resolution LookupResolution) ColumnCount() uint32 {
	return uint32(len(resolution.headers)) // #nosec G115 -- validated at 64 columns.
}

func (resolution LookupResolution) clone() LookupResolution {
	// headers and rows originate at newOwnedLookupResolution and are never
	// mutated inside this package. LookupResolution exposes them only through
	// detached accessors, so compiler-value clones can share this immutable
	// backing without weakening caller isolation.
	return LookupResolution{
		tenantID:       strings.Clone(resolution.tenantID),
		definitionName: strings.Clone(resolution.definitionName),
		logicalID:      strings.Clone(resolution.logicalID),
		logicalVersion: resolution.logicalVersion,
		objectID:       strings.Clone(resolution.objectID),
		version:        resolution.version,
		sizeBytes:      resolution.sizeBytes,
		contentSHA256:  resolution.contentSHA256,
		contract:       cloneLookupResolutionContract(resolution.contract),
		contractSet:    resolution.contractSet,
		headers:        resolution.headers,
		asset:          resolution.asset,
		rows:           resolution.rows,
		backing:        resolution.backing,
	}
}

func newLookupResolutionBacking(
	resolution LookupResolution,
	payloadBytes uint64,
) *lookupResolutionBacking {
	cells, _ := lookupResolutionCellCountUncached(resolution)
	return &lookupResolutionBacking{
		headers:      resolution.headers,
		asset:        resolution.asset,
		rows:         resolution.rows,
		rowCount:     lookupResolutionRowCount(resolution),
		payloadBytes: payloadBytes,
		cellCount:    cells,
	}
}

func lookupResolutionHasImmutableBacking(resolution LookupResolution) bool {
	backing := resolution.backing
	if backing == nil || backing.asset != resolution.asset ||
		backing.rowCount != lookupResolutionRowCount(resolution) ||
		!sameStringSliceStorage(backing.headers, resolution.headers) {
		return false
	}
	if resolution.asset != nil {
		return resolution.rows == nil && backing.rows == nil
	}
	return sameLookupRowStorage(backing.rows, resolution.rows)
}

func sameStringSliceStorage(left, right []string) bool {
	if len(left) != len(right) || (left == nil) != (right == nil) {
		return false
	}
	return len(left) == 0 || &left[0] == &right[0]
}

func sameLookupRowStorage(left, right [][]string) bool {
	if len(left) != len(right) || (left == nil) != (right == nil) {
		return false
	}
	return len(left) == 0 || &left[0] == &right[0]
}

func normalizeLookupResolutionContract(contract plan.Lookup) (plan.Lookup, error) {
	if err := validateCompiledLookupOperator(&contract); err != nil {
		return plan.Lookup{}, fmt.Errorf("bind ClickHouse lookup contract: %w", err)
	}
	result := cloneLookupResolutionContract(contract)
	result.DefinitionRange = planEmptyRange
	result.Range = planEmptyRange
	for index := range result.Keys {
		result.Keys[index].LookupFieldRange = planEmptyRange
		result.Keys[index].Range = planEmptyRange
		result.Keys[index].EventField.Range = planEmptyRange
	}
	for index := range result.Outputs {
		result.Outputs[index].LookupFieldRange = planEmptyRange
		result.Outputs[index].Range = planEmptyRange
		result.Outputs[index].EventField.Range = planEmptyRange
	}
	return result, nil
}

// planEmptyRange avoids importing parser authority into a stored-definition
// contract while keeping the normalization assignment readable.
var planEmptyRange = spl.Range{}

func cloneLookupResolutionContract(contract plan.Lookup) plan.Lookup {
	result := contract
	result.DefinitionName = strings.Clone(contract.DefinitionName)
	result.Keys = slices.Clone(contract.Keys)
	result.Outputs = slices.Clone(contract.Outputs)
	for index := range result.Keys {
		result.Keys[index].LookupField = strings.Clone(result.Keys[index].LookupField)
		result.Keys[index].EventField.Name = strings.Clone(result.Keys[index].EventField.Name)
		result.Keys[index].EventField.Path = cloneStrings(result.Keys[index].EventField.Path)
	}
	for index := range result.Outputs {
		result.Outputs[index].LookupField = strings.Clone(result.Outputs[index].LookupField)
		result.Outputs[index].EventField.Name = strings.Clone(result.Outputs[index].EventField.Name)
		result.Outputs[index].EventField.Path = cloneStrings(result.Outputs[index].EventField.Path)
	}
	return result
}

func lookupResolutionContractsEqual(left, right plan.Lookup) bool {
	if left.DefinitionName != right.DefinitionName || left.WriteMode != right.WriteMode ||
		len(left.Keys) != len(right.Keys) || len(left.Outputs) != len(right.Outputs) {
		return false
	}
	for index := range left.Keys {
		if left.Keys[index].LookupField != right.Keys[index].LookupField ||
			left.Keys[index].EventField.Name != right.Keys[index].EventField.Name {
			return false
		}
	}
	for index := range left.Outputs {
		if left.Outputs[index].LookupField != right.Outputs[index].LookupField ||
			left.Outputs[index].EventField.Name != right.Outputs[index].EventField.Name {
			return false
		}
	}
	return true
}

func cloneLookupRows(rows [][]string) [][]string {
	if rows == nil {
		return nil
	}
	cloned := make([][]string, len(rows))
	for index, row := range rows {
		cloned[index] = cloneStrings(row)
	}
	return cloned
}

func validateLookupResolution(resolution LookupResolution) error {
	return validateLookupResolutionContext(context.Background(), resolution)
}

func validateLookupResolutionContext(
	ctx context.Context,
	resolution LookupResolution,
) error {
	_, err := validateLookupResolutionAndMeasureContext(ctx, resolution)
	return err
}

func validateLookupResolutionAndMeasureContext(
	ctx context.Context,
	resolution LookupResolution,
) (uint64, error) {
	if ctx == nil {
		return 0, errors.New("validate ClickHouse lookup resolution: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if resolution.tenantID == "" || resolution.definitionName == "" ||
		resolution.objectID == "" || resolution.version == 0 ||
		resolution.sizeBytes == 0 || resolution.sizeBytes > MaximumLookupAssetBytes ||
		resolution.contentSHA256 == ([sha256.Size]byte{}) {
		return 0, errors.New("create ClickHouse lookup resolution: pinned identity is incomplete")
	}
	logicalIdentityPresent := resolution.logicalID != "" || resolution.logicalVersion != 0
	if logicalIdentityPresent != resolution.contractSet ||
		resolution.contractSet && (resolution.logicalID == "" || resolution.logicalVersion == 0) {
		return 0, errors.New(
			"create ClickHouse lookup resolution: logical definition authority is incomplete",
		)
	}
	for _, identity := range []string{
		resolution.tenantID,
		resolution.definitionName,
		resolution.objectID,
	} {
		if !utf8.ValidString(identity) || len(identity) > MaximumLookupCellBytes ||
			strings.IndexByte(identity, 0) >= 0 {
			return 0, errors.New("create ClickHouse lookup resolution: pinned identity is invalid")
		}
	}
	if resolution.logicalID != "" && (!utf8.ValidString(resolution.logicalID) ||
		len(resolution.logicalID) > MaximumLookupCellBytes ||
		strings.IndexByte(resolution.logicalID, 0) >= 0) {
		return 0, errors.New(
			"create ClickHouse lookup resolution: logical definition identity is invalid",
		)
	}
	if len(resolution.headers) == 0 ||
		len(resolution.headers) > MaximumLookupAssetColumns {
		return 0, fmt.Errorf(
			"create ClickHouse lookup resolution: asset must contain 1 through %d columns",
			MaximumLookupAssetColumns,
		)
	}
	rowCount := lookupResolutionRowCount(resolution)
	if rowCount > MaximumLookupAssetRows {
		return 0, fmt.Errorf(
			"create ClickHouse lookup resolution: asset contains more than %d rows",
			MaximumLookupAssetRows,
		)
	}
	seenHeaders := make(map[string]struct{}, len(resolution.headers))
	var decodedBytes uint64
	for _, header := range resolution.headers {
		if header == "" || !utf8.ValidString(header) ||
			len(header) > lookupasset.MaximumHeaderBytes ||
			strings.IndexByte(header, 0) >= 0 {
			return 0, errors.New("create ClickHouse lookup resolution: asset header is invalid")
		}
		if _, duplicate := seenHeaders[header]; duplicate {
			return 0, errors.New("create ClickHouse lookup resolution: asset header is duplicated")
		}
		seenHeaders[header] = struct{}{}
		var ok bool
		decodedBytes, ok = checkedLookupBytesAdd(decodedBytes, uint64(len(header)))
		if !ok {
			return 0, errors.New("create ClickHouse lookup resolution: asset byte count overflows")
		}
	}
	if resolution.asset != nil &&
		resolution.asset.ColumnCount() != resolution.ColumnCount() {
		return 0, errors.New(
			"create ClickHouse lookup resolution: asset schema changed",
		)
	}
	if lookupResolutionHasImmutableBacking(resolution) {
		expectedCells, ok := lookupResolutionCellCountUncached(resolution)
		if !ok || resolution.backing.cellCount != expectedCells ||
			resolution.backing.payloadBytes < decodedBytes ||
			resolution.backing.payloadBytes > MaximumLookupAssetBytes {
			return 0, errors.New(
				"create ClickHouse lookup resolution: immutable backing is invalid",
			)
		}
		return resolution.backing.payloadBytes, nil
	}
	for rowIndex := range rowCount {
		if rowIndex%lookupContextCheckRows == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		if resolution.asset == nil && len(resolution.rows[rowIndex]) != len(resolution.headers) {
			return 0, fmt.Errorf(
				"create ClickHouse lookup resolution: row %d has %d cells, want %d",
				rowIndex+1,
				len(resolution.rows[rowIndex]),
				len(resolution.headers),
			)
		}
		var rowBytes uint64
		for columnIndex := range resolution.headers {
			cell, ok := lookupResolutionCell(resolution, rowIndex, columnIndex)
			if !ok {
				return 0, fmt.Errorf(
					"create ClickHouse lookup resolution: row %d has an invalid width",
					rowIndex+1,
				)
			}
			if !utf8.ValidString(cell) || len(cell) > MaximumLookupCellBytes ||
				strings.IndexByte(cell, 0) >= 0 {
				return 0, fmt.Errorf(
					"create ClickHouse lookup resolution: row %d contains an invalid cell",
					rowIndex+1,
				)
			}
			var added bool
			rowBytes, added = checkedLookupBytesAdd(rowBytes, uint64(len(cell)))
			if !added || rowBytes > MaximumLookupRowBytes {
				return 0, fmt.Errorf(
					"create ClickHouse lookup resolution: row %d exceeds %d decoded bytes",
					rowIndex+1,
					MaximumLookupRowBytes,
				)
			}
		}
		var ok bool
		decodedBytes, ok = checkedLookupBytesAdd(decodedBytes, rowBytes)
		if !ok || decodedBytes > MaximumLookupAssetBytes {
			return 0, fmt.Errorf(
				"create ClickHouse lookup resolution: asset exceeds %d decoded bytes",
				MaximumLookupAssetBytes,
			)
		}
	}
	return decodedBytes, nil
}

func checkedLookupBytesAdd(left, right uint64) (uint64, bool) {
	if right > ^uint64(0)-left {
		return 0, false
	}
	return left + right, true
}

func lookupResolutionPayloadBytes(resolution LookupResolution) (uint64, bool) {
	if lookupResolutionHasImmutableBacking(resolution) {
		return resolution.backing.payloadBytes, true
	}
	return lookupResolutionPayloadBytesUncached(resolution)
}

func lookupResolutionPayloadBytesUncached(resolution LookupResolution) (uint64, bool) {
	var total uint64
	for _, header := range resolution.headers {
		var ok bool
		total, ok = checkedLookupBytesAdd(total, uint64(len(header)))
		if !ok {
			return 0, false
		}
	}
	for rowIndex := 0; rowIndex < lookupResolutionRowCount(resolution); rowIndex++ {
		for columnIndex := range resolution.headers {
			cell, ok := lookupResolutionCell(resolution, rowIndex, columnIndex)
			if !ok {
				return 0, false
			}
			var added bool
			total, added = checkedLookupBytesAdd(total, uint64(len(cell)))
			if !added {
				return 0, false
			}
		}
	}
	return total, true
}

func lookupResolutionCellCount(resolution LookupResolution) (uint64, bool) {
	if lookupResolutionHasImmutableBacking(resolution) {
		return resolution.backing.cellCount, true
	}
	return lookupResolutionCellCountUncached(resolution)
}

func lookupResolutionCellCountUncached(resolution LookupResolution) (uint64, bool) {
	rowCount := resolution.RowCount()
	columnCount := uint64(resolution.ColumnCount())
	if rowCount != 0 && columnCount > ^uint64(0)/rowCount {
		return 0, false
	}
	return rowCount * columnCount, true
}

func lookupResolutionEqual(left, right LookupResolution) bool {
	if left.tenantID != right.tenantID ||
		left.definitionName != right.definitionName ||
		left.logicalID != right.logicalID ||
		left.logicalVersion != right.logicalVersion ||
		left.objectID != right.objectID ||
		left.version != right.version ||
		left.sizeBytes != right.sizeBytes ||
		left.contentSHA256 != right.contentSHA256 ||
		left.contractSet != right.contractSet ||
		!lookupResolutionContractsEqual(left.contract, right.contract) ||
		!slices.Equal(left.headers, right.headers) ||
		lookupResolutionRowCount(left) != lookupResolutionRowCount(right) {
		return false
	}
	if lookupResolutionHasImmutableBacking(left) &&
		lookupResolutionHasImmutableBacking(right) &&
		left.backing == right.backing {
		return true
	}
	for rowIndex := 0; rowIndex < lookupResolutionRowCount(left); rowIndex++ {
		for columnIndex := range left.headers {
			leftCell, leftOK := lookupResolutionCell(left, rowIndex, columnIndex)
			rightCell, rightOK := lookupResolutionCell(right, rowIndex, columnIndex)
			if !leftOK || !rightOK || leftCell != rightCell {
				return false
			}
		}
	}
	return true
}

func lookupResolutionRowCount(resolution LookupResolution) int {
	if resolution.asset != nil {
		// Immutable lookup assets are capped at lookupasset.MaximumRows.
		// #nosec G115 -- the validated cap fits in int on every supported target.
		return int(resolution.asset.RowCount())
	}
	return len(resolution.rows)
}

func lookupResolutionCell(
	resolution LookupResolution,
	row int,
	column int,
) (string, bool) {
	if resolution.asset != nil {
		return resolution.asset.Cell(row, column)
	}
	if row < 0 || row >= len(resolution.rows) ||
		column < 0 || column >= len(resolution.rows[row]) {
		return "", false
	}
	return resolution.rows[row][column], true
}
