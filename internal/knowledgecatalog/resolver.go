package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/indexread"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"modernc.org/sqlite"
)

const (
	// MaximumConcurrentResolutions is the fixed fail-fast admission bound for
	// catalog resolution. A saturated resolver never queues another database or
	// definition-decoding operation behind the admitted work.
	MaximumConcurrentResolutions = 32
	// MaximumResolutionDuration is the complete per-call budget, including
	// normalization, SQLite retries, catalog hydration, and precedence work.
	MaximumResolutionDuration = 250 * time.Millisecond
	// MaximumResolutionAttempts includes the initial SQLite attempt. Only
	// SQLITE_BUSY and SQLITE_LOCKED may consume a later attempt.
	MaximumResolutionAttempts = 3
	// MaximumResolutionCandidates is the migration-owned tenant active-object
	// ceiling. The resolver reads one extra row only to prove the bound.
	MaximumResolutionCandidates = 4096
	// MaximumResolutionExtractionOutputs is shared by authored rex/spath and
	// knowledge regex/JSON extraction outputs in one query.
	MaximumResolutionExtractionOutputs = 64
	// MaximumResolutionScalarPredicates is the query-wide authored eval/where
	// predicate ceiling shared by all winning calculated definitions.
	MaximumResolutionScalarPredicates = 32

	resolutionRetryInitialDelay = 2 * time.Millisecond
)

// ResolutionScope is trusted search-admission authority. Every field must be
// derived by the server after authentication and index authorization; no
// protobuf request body may directly populate it.
type ResolutionScope struct {
	TenantID                   string
	PrincipalID                string
	AppID                      string
	EffectiveAuthorizedIndexes []string
}

type normalizedResolutionScope struct {
	tenantID    string
	principalID string
	appID       string
	indexes     []string
}

// ResolverOptions is reserved for bounded cache or instrumentation additions.
// KO-0G deliberately exposes no knobs that can weaken the fixed concurrency,
// deadline, retry, or hydration limits.
type ResolverOptions struct{}

// Resolver performs authorization-leading immutable catalog resolution. It is
// safe for concurrent use.
type Resolver struct {
	store   *Store
	gate    *control.AdmissionGate
	attempt resolutionAttemptFunc
	backoff resolutionBackoffFunc
}

type resolutionAttemptFunc func(
	context.Context,
	normalizedResolutionScope,
) (Resolution, error)

type resolutionBackoffFunc func(context.Context, time.Duration) error

// NewResolver constructs the search-admission resolver over an existing Store.
func NewResolver(store *Store, _ ResolverOptions) (*Resolver, error) {
	if store == nil || store.orm == nil {
		return nil, fmt.Errorf("%w: knowledge catalog store is required", control.ErrInvalidArgument)
	}
	resolver := &Resolver{
		store: store,
		gate:  store.resolutionGate,
	}
	resolver.attempt = resolver.resolveOnce
	resolver.backoff = waitResolutionRetry
	return resolver, nil
}

// NewResolver constructs the search-admission resolver over store.
func (store *Store) NewResolver(options ResolverOptions) (*Resolver, error) {
	return NewResolver(store, options)
}

// Resolution is the detached, pre-compiler catalog authority for one search.
// It retains only the opaque prepared authority; every accessor returns
// detached data. KO-0G deliberately has no public snapshot finalization seam:
// KO-0H must supply opaque, trusted compiler evidence.
type Resolution struct {
	authority knowledgesnapshot.Authority
}

// ResolutionStaticCharges are the exact compiler-independent knowledge costs
// derived from the winning catalog definitions. KO-0H combines and
// cross-checks these exact contributions with authored-search charges.
type ResolutionStaticCharges = knowledgesnapshot.StaticCharges

// ResolutionSummary is definition-free inventory suitable for diagnostics.
type ResolutionSummary = knowledgesnapshot.AuthoritySummary

// ResolvedObjectSummary is the safe scalar inventory of one winning object.
type ResolvedObjectSummary = knowledgesnapshot.ObjectSummary

// Summary returns a fully detached definition-free resolution inventory.
func (resolution Resolution) Summary() ResolutionSummary {
	return resolution.authority.Summary()
}

// ObjectSummaries returns detached safe scalar inventory in canonical stage
// order assigned by knowledgesnapshot.Prepare.
func (resolution Resolution) ObjectSummaries() []ResolvedObjectSummary {
	return resolution.authority.ObjectSummaries()
}

// Objects returns detached executable definitions for a bounded compiler.
func (resolution Resolution) Objects() []knowledgesnapshot.Object {
	return resolution.authority.Objects()
}

// Dependencies returns detached exact winning dependency edges.
func (resolution Resolution) Dependencies() []knowledgesnapshot.Dependency {
	return resolution.authority.Dependencies()
}

// Shadows returns detached visible lower-precedence candidates.
func (resolution Resolution) Shadows() []knowledgesnapshot.Shadow {
	return resolution.authority.Shadows()
}

// StaticCharges returns the immutable semantic costs of winning definitions.
func (resolution Resolution) StaticCharges() ResolutionStaticCharges {
	return resolution.authority.StaticCharges()
}

// IsZero reports whether the resolution contains no prepared authority.
func (resolution Resolution) IsZero() bool { return resolution.authority.IsZero() }

// Finalize binds this exact opaque resolution to one exact sealed compiler
// output. Catalog authority remains private to Resolution throughout.
func (resolution Resolution) Finalize(compiled clickhouse.CompiledQuery) (knowledgesnapshot.Snapshot, error) {
	return resolution.authority.Finalize(compiled)
}

// Resolve returns the complete old-or-new catalog authority observed by one
// SQLite read snapshot. It never mixes revisions and never reads object bodies
// before app and object authorization have selected a bounded candidate set.
func (resolver *Resolver) Resolve(
	ctx context.Context,
	scope ResolutionScope,
) (Resolution, error) {
	if resolver == nil || resolver.store == nil || resolver.store.orm == nil {
		return Resolution{}, fmt.Errorf("%w: knowledge resolver is unavailable", control.ErrInvalidArgument)
	}
	if ctx == nil {
		return Resolution{}, fmt.Errorf("%w: knowledge resolver context is nil", control.ErrInvalidArgument)
	}
	limited, cancel := context.WithTimeout(ctx, MaximumResolutionDuration)
	defer cancel()

	normalized, err := normalizeResolutionScope(scope)
	if err != nil {
		return Resolution{}, err
	}
	if err := limited.Err(); err != nil {
		return Resolution{}, err
	}
	if resolver.gate == nil || !resolver.gate.TryAcquire() {
		if err := limited.Err(); err != nil {
			return Resolution{}, err
		}
		return Resolution{}, fmt.Errorf("%w: knowledge resolver concurrency is saturated", control.ErrCapacityExceeded)
	}
	defer resolver.gate.Release()

	for attempt := 1; attempt <= MaximumResolutionAttempts; attempt++ {
		resolved, resolveErr := resolver.attempt(limited, normalized)
		if resolveErr == nil {
			return resolved, nil
		}
		if err := limited.Err(); err != nil {
			return Resolution{}, copyCatalogErrorMetadata(err, resolveErr)
		}
		if attempt == MaximumResolutionAttempts || !resolverSQLiteBusyOrLocked(resolveErr) {
			return Resolution{}, resolveErr
		}
		if err := resolver.backoff(limited, resolutionRetryDelay(attempt)); err != nil {
			return Resolution{}, copyCatalogErrorMetadata(err, resolveErr)
		}
	}
	panic("unreachable knowledge resolver retry loop")
}

func resolutionRetryDelay(completedAttempt int) time.Duration {
	return resolutionRetryInitialDelay << (completedAttempt - 1)
}

func waitResolutionRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	select {
	case <-ctx.Done():
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (resolver *Resolver) resolveOnce(
	ctx context.Context,
	scope normalizedResolutionScope,
) (resolved Resolution, returnedErr error) {
	tx := resolver.store.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return Resolution{}, mapError(ctx.Err(), "begin resolution", tx.Error)
	}
	defer finishReadTransaction(tx, &returnedErr)

	// This is deliberately the first SQLite read: it establishes the WAL
	// snapshot whose exact raw state commitment is re-read at the end.
	catalog, err := readCatalogState(tx, scope.tenantID)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "read resolution catalog authority", err)
	}
	app, found, err := readActiveResolutionApp(tx, scope.tenantID, scope.appID)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "authorize resolution app", err)
	}
	if !found {
		return Resolution{}, control.ErrNotFound
	}
	if !catalog.found {
		return Resolution{}, fmt.Errorf("%w: active app has no knowledge catalog authority", ErrCorrupt)
	}
	token, err := base64.RawURLEncoding.DecodeString(catalog.token)
	if err != nil || len(token) != sha256.Size {
		return Resolution{}, fmt.Errorf("%w: knowledge catalog state token is invalid", ErrCorrupt)
	}

	readScope := normalizedScope{
		tenantID:       scope.tenantID,
		ownerID:        scope.principalID,
		readableAppIDs: []string{scope.appID},
	}
	activeCount, err := readBoundedActiveResolutionCount(tx, readScope)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "count active resolution candidates", err)
	}
	if activeCount > MaximumResolutionCandidates {
		return Resolution{}, fmt.Errorf("%w: active knowledge catalog exceeds its resolution bound", ErrCorrupt)
	}

	query := applyActiveResolutionProjectionAuthorization(baseProjectionQuery(tx), readScope).
		Order("projection.knowledge_object_id ASC")
	projections, err := readProjectionRecordsBounded(
		query,
		MaximumResolutionCandidates+1,
		knowledgesnapshot.MaximumCandidateDefinitionBytes,
	)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "read active resolution candidates", err)
	}
	if len(projections) > MaximumResolutionCandidates || int64(len(projections)) != activeCount {
		return Resolution{}, fmt.Errorf("%w: active resolution projection coverage disagrees", ErrCorrupt)
	}

	objects, hydrated, err := resolver.store.objectsFromProjectionsWithAuthorities(
		tx,
		projections,
		resolutionHydrationBudget,
	)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "validate active resolution candidates", err)
	}
	var staticCharges ResolutionStaticCharges
	input, err := resolveCandidatePrecedence(
		ctx,
		scope,
		catalog,
		token,
		objects,
		hydrated.versions,
		hydrated.dependencies,
		&staticCharges,
	)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "resolve active knowledge candidates", err)
	}
	authority, err := knowledgesnapshot.Prepare(input)
	if err != nil {
		if errors.Is(err, knowledgesnapshot.ErrResourceLimit) {
			return Resolution{}, fmt.Errorf("%w: prepare resolved knowledge authority: %v", control.ErrCapacityExceeded, err)
		}
		return Resolution{}, fmt.Errorf("%w: prepared knowledge authority rejected persisted candidates: %v", ErrCorrupt, err)
	}
	if authority.StaticCharges() != staticCharges {
		return Resolution{}, fmt.Errorf("%w: independently derived static charges disagree", ErrCorrupt)
	}

	finalApp, found, err := readActiveResolutionApp(tx, scope.tenantID, scope.appID)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "re-read resolution app authority", err)
	}
	if !found || finalApp != app {
		return Resolution{}, fmt.Errorf("%w: app authority changed inside one read snapshot", ErrCorrupt)
	}
	finalCatalog, err := readCatalogState(tx, scope.tenantID)
	if err != nil {
		return Resolution{}, mapError(ctx.Err(), "re-read resolution catalog authority", err)
	}
	if !finalCatalog.equal(catalog) {
		return Resolution{}, fmt.Errorf("%w: catalog authority changed inside one read snapshot", ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return Resolution{}, err
	}
	if err := tx.Commit().Error; err != nil {
		return Resolution{}, mapError(ctx.Err(), "commit resolution", err)
	}
	return Resolution{authority: authority}, nil
}

var resolutionHydrationBudget = listHydrationBudget{
	definitionBytes:    knowledgesnapshot.MaximumCandidateDefinitionBytes,
	projectionBytes:    MaximumListFilterIntegrityProjectionBytes,
	selectorPatterns:   MaximumListFilterIntegritySelectorPatterns,
	selectorValueBytes: MaximumListFilterIntegritySelectorBytes,
	dependencies:       MaximumListFilterIntegrityDependencies,
}

type resolutionAppAuthority struct {
	revision int64
}

func readActiveResolutionApp(
	database *gorm.DB,
	tenantID, appID string,
) (resolutionAppAuthority, bool, error) {
	var records []struct {
		AppID         string        `gorm:"column:app_id"`
		TenantID      string        `gorm:"column:tenant_id"`
		State         string        `gorm:"column:state"`
		AppIDBytes    int64         `gorm:"column:app_id_bytes"`
		TenantIDBytes int64         `gorm:"column:tenant_id_bytes"`
		StateBytes    int64         `gorm:"column:state_bytes"`
		Revision      sql.NullInt64 `gorm:"column:revision"`
	}
	err := database.Table("app_workspaces AS app").
		Joins(`LEFT JOIN app_catalog_revisions AS authority
			ON authority.tenant_id = app.tenant_id`).
		Where("app.app_id = ? AND app.tenant_id = ?", appID, tenantID).
		Select(`
			CASE WHEN length(CAST(app.app_id AS BLOB)) BETWEEN 1 AND ? THEN app.app_id ELSE '' END AS app_id,
			CASE WHEN length(CAST(app.tenant_id AS BLOB)) BETWEEN 1 AND ? THEN app.tenant_id ELSE '' END AS tenant_id,
			CASE WHEN length(CAST(app.state AS BLOB)) BETWEEN 1 AND ? THEN app.state ELSE '' END AS state,
			length(CAST(app.app_id AS BLOB)) AS app_id_bytes,
			length(CAST(app.tenant_id AS BLOB)) AS tenant_id_bytes,
			length(CAST(app.state AS BLOB)) AS state_bytes,
			authority.revision AS revision
		`, maximumAppIDBytes, maximumTenantIDBytes, len("archived")).
		Limit(2).
		Find(&records).Error
	if err != nil {
		return resolutionAppAuthority{}, false, err
	}
	if len(records) == 0 {
		return resolutionAppAuthority{}, false, nil
	}
	record := records[0]
	if len(records) != 1 || record.AppID != appID || record.TenantID != tenantID ||
		record.AppIDBytes < 1 || record.AppIDBytes > maximumAppIDBytes ||
		record.TenantIDBytes < 1 || record.TenantIDBytes > maximumTenantIDBytes ||
		record.StateBytes != int64(len(record.State)) ||
		(record.State != string(control.AppStateActive) && record.State != string(control.AppStateArchived)) ||
		!record.Revision.Valid || record.Revision.Int64 < 1 || record.Revision.Int64 > math.MaxInt64 {
		return resolutionAppAuthority{}, false, fmt.Errorf("%w: selected app authority is invalid", ErrCorrupt)
	}
	if record.State != string(control.AppStateActive) {
		return resolutionAppAuthority{}, false, nil
	}
	return resolutionAppAuthority{revision: record.Revision.Int64}, true, nil
}

func readBoundedActiveResolutionCount(database *gorm.DB, scope normalizedScope) (int64, error) {
	driver := fmt.Sprintf(`(
		SELECT 1
		FROM knowledge_objects INDEXED BY knowledge_objects_authorized_global_idx
		WHERE tenant_id = ? AND sharing_scope = 'global' AND state = 'active'
		UNION ALL
		SELECT 1
		FROM knowledge_objects INDEXED BY knowledge_objects_authorized_app_idx
		WHERE tenant_id = ? AND sharing_scope = 'app' AND app_id IN ? AND state = 'active'
		UNION ALL
		SELECT 1
		FROM knowledge_objects INDEXED BY knowledge_objects_authorized_private_idx
		WHERE tenant_id = ? AND sharing_scope = 'private'
		  AND owner_id = ? AND app_id IN ? AND state = 'active'
		LIMIT %d
	) AS bounded_active_resolution_objects`, MaximumResolutionCandidates+1)
	var count int64
	if err := database.Table(
		driver,
		scope.tenantID,
		scope.tenantID, scope.readableAppIDs,
		scope.tenantID, scope.ownerID, scope.readableAppIDs,
	).Count(&count).Error; err != nil {
		return 0, err
	}
	if count < 0 || count > MaximumResolutionCandidates+1 {
		return 0, fmt.Errorf("%w: bounded active resolution count is invalid", ErrCorrupt)
	}
	return count, nil
}

func applyActiveResolutionProjectionAuthorization(
	query *gorm.DB,
	scope normalizedScope,
) *gorm.DB {
	// Every branch starts from an ACTIVE current-registry authorization index,
	// joins only its exact immutable projection, and shares one 4,097-row cap.
	// Inactive and hidden identities cannot consume the candidate set or turn
	// definition hydration into an existence oracle.
	exactProjectionJoin := `candidate.tenant_id = authorized_registry.tenant_id
		AND candidate.knowledge_object_id = authorized_registry.knowledge_object_id
		AND candidate.object_version = authorized_registry.current_version
		AND candidate.app_id = authorized_registry.app_id
		AND candidate.owner_id = authorized_registry.owner_id
		AND candidate.object_type = authorized_registry.object_type
		AND candidate.name = authorized_registry.name
		AND candidate.sharing_scope = authorized_registry.sharing_scope
		AND candidate.state = authorized_registry.state`
	driver := fmt.Sprintf(`(
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_global_idx
		CROSS JOIN app_workspaces AS defining_app
		  ON defining_app.tenant_id = authorized_registry.tenant_id
		 AND defining_app.app_id = authorized_registry.app_id
		 AND defining_app.state = 'active'
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ?
		  AND authorized_registry.sharing_scope = 'global' AND authorized_registry.state = 'active'
		UNION ALL
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_app_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ?
		  AND authorized_registry.sharing_scope = 'app' AND authorized_registry.app_id IN ?
		  AND authorized_registry.state = 'active'
		UNION ALL
		SELECT candidate.tenant_id, candidate.knowledge_object_id, candidate.object_version
		FROM knowledge_objects AS authorized_registry INDEXED BY knowledge_objects_authorized_private_idx
		CROSS JOIN knowledge_object_list_projections AS candidate ON %s
		WHERE authorized_registry.tenant_id = ?
		  AND authorized_registry.sharing_scope = 'private'
		  AND authorized_registry.owner_id = ? AND authorized_registry.app_id IN ?
		  AND authorized_registry.state = 'active'
		LIMIT %d
	) AS authorized_active_resolution_projection
	CROSS JOIN knowledge_object_list_projections AS projection`,
		exactProjectionJoin,
		exactProjectionJoin,
		exactProjectionJoin,
		MaximumResolutionCandidates+1,
	)
	return query.Table(
		driver,
		scope.tenantID,
		scope.tenantID, scope.readableAppIDs,
		scope.tenantID, scope.ownerID, scope.readableAppIDs,
	).Where(`authorized_active_resolution_projection.tenant_id = projection.tenant_id
		AND authorized_active_resolution_projection.knowledge_object_id = projection.knowledge_object_id
		AND authorized_active_resolution_projection.object_version = projection.object_version`).
		Where("projection.tenant_id = ? AND projection.state = ?", scope.tenantID, StateActive)
}

type resolutionNameKey struct {
	objectType ObjectType
	name       string
}

type resolutionCandidate struct {
	object    Object
	rank      uint8
	semantics resolutionDefinitionSemantics
}

type resolutionDefinitionSemantics struct {
	dependency dependencyDefinitionSemantics
	selector   *knowledge.Selector
	charges    ResolutionStaticCharges
}

type resolutionShadowCandidate struct {
	winner resolutionCandidate
	loser  resolutionCandidate
}

func resolveCandidatePrecedence(
	ctx context.Context,
	scope normalizedResolutionScope,
	catalog catalogState,
	catalogToken []byte,
	objects []Object,
	versions map[string]versionRecord,
	dependenciesByID map[string][]dependencyRecord,
	staticCharges *ResolutionStaticCharges,
) (knowledgesnapshot.Input, error) {
	if staticCharges == nil {
		return knowledgesnapshot.Input{}, fmt.Errorf("%w: nil resolution charge destination", ErrCorrupt)
	}
	groups := make(map[resolutionNameKey][]resolutionCandidate, len(objects))
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return knowledgesnapshot.Input{}, err
		}
		if object.State != StateActive || object.Definition == nil || object.Version == 0 ||
			object.Version > math.MaxInt64 {
			return knowledgesnapshot.Input{}, fmt.Errorf("%w: active resolution object is invalid", ErrCorrupt)
		}
		rank, err := resolutionPrecedenceRank(scope, object)
		if err != nil {
			return knowledgesnapshot.Input{}, err
		}
		normalized, err := knowledgedefinition.Normalize(object.Definition)
		if err != nil || !proto.Equal(normalized.Definition, object.Definition) ||
			!bytes.Equal(normalized.Digest[:], object.DefinitionSHA256) {
			return knowledgesnapshot.Input{}, fmt.Errorf("%w: active candidate definition authority disagrees", ErrCorrupt)
		}
		semantics, err := compileResolutionDefinition(normalized)
		if err != nil {
			return knowledgesnapshot.Input{}, fmt.Errorf("%w: active candidate definition is not executable: %v", ErrCorrupt, err)
		}
		intersects, err := resolutionSelectorIntersects(ctx, normalized.Selector, scope.indexes)
		if err != nil {
			return knowledgesnapshot.Input{}, err
		}
		if !intersects {
			continue
		}
		key := resolutionNameKey{objectType: object.ObjectType, name: object.Name}
		groups[key] = append(groups[key], resolutionCandidate{
			object: object, rank: rank, semantics: semantics,
		})
	}

	winners := make([]resolutionCandidate, 0, len(groups))
	losers := make([]resolutionShadowCandidate, 0)
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		winner := group[0]
		for index := 1; index < len(group); index++ {
			candidate := group[index]
			if candidate.rank == winner.rank {
				return knowledgesnapshot.Input{}, fmt.Errorf("%w: active precedence slot is ambiguous", ErrCorrupt)
			}
			if candidate.rank > winner.rank {
				winner = candidate
			}
		}
		winners = append(winners, winner)
		for _, candidate := range group {
			if candidate.object.KnowledgeObjectID == winner.object.KnowledgeObjectID {
				continue
			}
			losers = append(losers, resolutionShadowCandidate{winner: winner, loser: candidate})
		}
	}
	if len(winners) > knowledgesnapshot.MaximumExecutableObjects ||
		len(losers) > knowledgesnapshot.MaximumShadows {
		return knowledgesnapshot.Input{}, fmt.Errorf("%w: resolved knowledge set exceeds snapshot bounds", control.ErrCapacityExceeded)
	}
	sort.Slice(winners, func(left, right int) bool {
		leftStage := resolutionObjectStageRank(winners[left].object.ObjectType)
		rightStage := resolutionObjectStageRank(winners[right].object.ObjectType)
		if leftStage != rightStage {
			return leftStage < rightStage
		}
		if winners[left].object.Name != winners[right].object.Name {
			return winners[left].object.Name < winners[right].object.Name
		}
		return winners[left].object.KnowledgeObjectID < winners[right].object.KnowledgeObjectID
	})
	if err := validateParallelResolutionSemantics(ctx, winners); err != nil {
		return knowledgesnapshot.Input{}, err
	}
	winnerOrdinal := make(map[string]int, len(winners))
	for index, winner := range winners {
		winnerOrdinal[winner.object.KnowledgeObjectID] = index
	}
	sort.Slice(losers, func(left, right int) bool {
		leftWinner := winnerOrdinal[losers[left].winner.object.KnowledgeObjectID]
		rightWinner := winnerOrdinal[losers[right].winner.object.KnowledgeObjectID]
		if leftWinner != rightWinner {
			return leftWinner < rightWinner
		}
		if losers[left].loser.rank != losers[right].loser.rank {
			return losers[left].loser.rank > losers[right].loser.rank
		}
		return losers[left].loser.object.KnowledgeObjectID < losers[right].loser.object.KnowledgeObjectID
	})
	winningCharges, err := aggregateResolutionStaticCharges(winners)
	if err != nil {
		return knowledgesnapshot.Input{}, err
	}

	input := knowledgesnapshot.Input{
		TenantID:                   strings.Clone(scope.tenantID),
		PrincipalID:                strings.Clone(scope.principalID),
		AppID:                      strings.Clone(scope.appID),
		TenantCatalogRevision:      uint64(catalog.revision),
		EffectiveAuthorizedIndexes: slices.Clone(scope.indexes),
		Objects:                    make([]knowledgesnapshot.Object, 0, len(winners)),
		Dependencies:               make([]knowledgesnapshot.Dependency, 0),
		Shadows:                    make([]knowledgesnapshot.Shadow, 0, len(losers)),
	}
	if len(catalogToken) != sha256.Size {
		return knowledgesnapshot.Input{}, fmt.Errorf("%w: catalog state token is invalid", ErrCorrupt)
	}
	input.TenantCatalogStateToken = bytes.Clone(catalogToken)
	// app_catalog_revisions is a tenant-wide app-catalog invalidation counter,
	// not a revision of the selected app. Its exact value is still read and
	// re-read as part of app authorization, but the snapshot's optional
	// selected-app revision remains absent until such an authority exists.
	input.AppCatalogRevision = nil

	winnerKeys := make(map[dependencyVersionKey]resolutionCandidate, len(winners))
	for _, winner := range winners {
		version, found := versions[winner.object.KnowledgeObjectID]
		if !found || version.TenantID != scope.tenantID ||
			version.KnowledgeObjectID != winner.object.KnowledgeObjectID ||
			version.ObjectVersion != int64(winner.object.Version) {
			return knowledgesnapshot.Input{}, fmt.Errorf(
				"%w: hydrated current-version authority is incomplete",
				ErrCorrupt,
			)
		}
		if _, found := dependenciesByID[winner.object.KnowledgeObjectID]; !found {
			return knowledgesnapshot.Input{}, fmt.Errorf(
				"%w: hydrated dependency authority is incomplete",
				ErrCorrupt,
			)
		}
		object, err := resolutionSnapshotObject(winner.object)
		if err != nil {
			return knowledgesnapshot.Input{}, err
		}
		input.Objects = append(input.Objects, object)
		key := dependencyVersionKey{
			objectID: winner.object.KnowledgeObjectID,
			version:  int64(winner.object.Version),
		}
		winnerKeys[key] = winner
	}
	for _, winner := range winners {
		for _, dependency := range dependenciesByID[winner.object.KnowledgeObjectID] {
			targetKey := dependencyVersionKey{
				objectID: dependency.TargetObjectID,
				version:  dependency.TargetObjectVersion,
			}
			target, found := winnerKeys[targetKey]
			if !found {
				return knowledgesnapshot.Input{}, fmt.Errorf(
					"%w: resolved dependency target is outside the exact winner closure",
					control.ErrDependencyConflict,
				)
			}
			if err := validateWinningResolutionDependency(
				winner,
				target,
				versions[winner.object.KnowledgeObjectID],
				versions[target.object.KnowledgeObjectID],
			); err != nil {
				return knowledgesnapshot.Input{}, err
			}
			input.Dependencies = append(input.Dependencies, knowledgesnapshot.Dependency{
				SourceObjectID: winner.object.KnowledgeObjectID,
				SourceVersion:  winner.object.Version,
				TargetObjectID: dependency.TargetObjectID,
				TargetVersion:  uint64(dependency.TargetObjectVersion),
				Role:           opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			})
		}
	}
	if len(input.Dependencies) > knowledgesnapshot.MaximumDependencyEdges {
		return knowledgesnapshot.Input{}, fmt.Errorf("%w: resolved dependency set exceeds snapshot bounds", control.ErrCapacityExceeded)
	}
	for _, shadow := range losers {
		loserObject, err := resolutionSnapshotObject(shadow.loser.object)
		if err != nil {
			return knowledgesnapshot.Input{}, err
		}
		input.Shadows = append(input.Shadows, knowledgesnapshot.Shadow{
			WinnerObjectID:    shadow.winner.object.KnowledgeObjectID,
			WinnerVersion:     shadow.winner.object.Version,
			KnowledgeObjectID: loserObject.KnowledgeObjectID,
			Version:           loserObject.Version,
			ObjectType:        loserObject.ObjectType,
			Name:              loserObject.Name,
			AppID:             loserObject.AppID,
			OwnerID:           loserObject.OwnerID,
			SharingScope:      loserObject.SharingScope,
			Definition:        loserObject.Definition,
			DefinitionSHA256:  loserObject.DefinitionSHA256,
		})
	}
	*staticCharges = winningCharges
	return input, nil
}

func validateParallelResolutionSemantics(ctx context.Context, winners []resolutionCandidate) error {
	for leftIndex, left := range winners {
		if err := ctx.Err(); err != nil {
			return err
		}
		for rightIndex := leftIndex + 1; rightIndex < len(winners); rightIndex++ {
			if rightIndex&15 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			right := winners[rightIndex]
			if left.semantics.dependency.stage != right.semantics.dependency.stage {
				break
			}
			if knowledge.SelectorsProvablyDisjoint(left.semantics.selector, right.semantics.selector) {
				continue
			}
			if dependencyFieldsIntersect(
				left.semantics.dependency.outputFields,
				right.semantics.dependency.outputFields,
			) {
				return fmt.Errorf("%w: active same-stage definitions may write the same destination", ErrCorrupt)
			}
			if dependencyFieldsIntersect(
				left.semantics.dependency.outputFields,
				right.semantics.dependency.inputFields,
			) || dependencyFieldsIntersect(
				right.semantics.dependency.outputFields,
				left.semantics.dependency.inputFields,
			) {
				return fmt.Errorf("%w: active same-stage definitions form a possible data dependency", ErrCorrupt)
			}
		}
	}
	return nil
}

func resolutionObjectStageRank(objectType ObjectType) uint8 {
	switch objectType {
	case ObjectTypeFieldExtraction:
		return 1
	case ObjectTypeFieldAlias:
		return 2
	case ObjectTypeCalculatedField:
		return 3
	default:
		return math.MaxUint8
	}
}

func compileResolutionDefinition(
	normalized knowledgedefinition.Normalized,
) (resolutionDefinitionSemantics, error) {
	if normalized.Definition == nil || normalized.Selector == nil {
		return resolutionDefinitionSemantics{}, errors.New("normalized definition authority is incomplete")
	}
	dependency, err := dependencySemanticsFromDefinition(normalized.Definition)
	if err != nil {
		return resolutionDefinitionSemantics{}, err
	}
	semantics := resolutionDefinitionSemantics{
		dependency: dependency,
		selector:   normalized.Selector,
	}
	switch body := normalized.Definition.GetBody().(type) {
	case *opensplunkv1.KnowledgeObjectDefinition_FieldExtraction:
		if body == nil || body.FieldExtraction == nil {
			return resolutionDefinitionSemantics{}, errors.New("field extraction is nil")
		}
		switch extraction := body.FieldExtraction.GetExtraction().(type) {
		case *opensplunkv1.FieldExtractionDefinition_Regex:
			if extraction == nil || extraction.Regex == nil {
				return resolutionDefinitionSemantics{}, errors.New("regex extraction is nil")
			}
			compiled, compileErr := splregex.CompileExtractionPattern(extraction.Regex.GetPattern())
			if compileErr != nil {
				return resolutionDefinitionSemantics{}, fmt.Errorf("regex extraction: %w", compileErr)
			}
			outputs := extraction.Regex.GetOutputFields()
			if compiled.GroupCount != len(compiled.Captures) || len(outputs) != len(compiled.Captures) {
				return resolutionDefinitionSemantics{}, errors.New("regex captures do not exactly match declared named outputs")
			}
			for index, capture := range compiled.Captures {
				if outputs[index] != capture.Name {
					return resolutionDefinitionSemantics{}, errors.New("regex capture order disagrees with declared outputs")
				}
			}
			semantics.charges.GeneratedFields = uint32(len(outputs))
			semantics.charges.ExtractionRegexPrograms = 1
			semantics.charges.ExtractionRegexWorkUnits = uint64(compiled.ProgramWorkUnits)
			semantics.charges.ExtractionOutputs = uint32(len(outputs))
		case *opensplunkv1.FieldExtractionDefinition_Json:
			if extraction == nil || extraction.Json == nil {
				return resolutionDefinitionSemantics{}, errors.New("JSON extraction is nil")
			}
			steps, pathErr := splpath.ParseJSON(extraction.Json.GetPath())
			if pathErr != nil {
				return resolutionDefinitionSemantics{}, fmt.Errorf("JSON extraction: %w", pathErr)
			}
			work := splpath.EvaluationWorkUnits(steps)
			if work < 1 || work > splpath.MaximumEvaluationWorkUnits {
				return resolutionDefinitionSemantics{}, errors.New("JSON extraction work is invalid")
			}
			semantics.charges.GeneratedFields = 1
			semantics.charges.ExtractionOutputs = 1
			semantics.charges.JSONEvaluationWorkUnits = uint32(work)
		default:
			return resolutionDefinitionSemantics{}, errors.New("field extraction kind is unsupported")
		}
	case *opensplunkv1.KnowledgeObjectDefinition_FieldAlias:
		if body == nil || body.FieldAlias == nil {
			return resolutionDefinitionSemantics{}, errors.New("field alias is nil")
		}
		semantics.charges.GeneratedFields = 1
	case *opensplunkv1.KnowledgeObjectDefinition_CalculatedField:
		if body == nil || body.CalculatedField == nil {
			return resolutionDefinitionSemantics{}, errors.New("calculated field is nil")
		}
		if dependency.scalarExpressionNodes < 1 ||
			dependency.scalarExpressionNodes > knowledgesnapshot.MaximumScalarExpressionNodes {
			return resolutionDefinitionSemantics{}, errors.New("calculated expression node charge is invalid")
		}
		semantics.charges.GeneratedFields = 1
		semantics.charges.ScalarExpressions = 1
		semantics.charges.ScalarExpressionNodes = dependency.scalarExpressionNodes
		semantics.charges.ScalarPredicates = dependency.scalarPredicates
	default:
		return resolutionDefinitionSemantics{}, errors.New("definition body is unsupported")
	}
	return semantics, nil
}

func aggregateResolutionStaticCharges(
	winners []resolutionCandidate,
) (ResolutionStaticCharges, error) {
	var aggregate ResolutionStaticCharges
	for _, winner := range winners {
		charges := winner.semantics.charges
		aggregate.GeneratedFields += charges.GeneratedFields
		aggregate.ExtractionRegexPrograms += charges.ExtractionRegexPrograms
		aggregate.ExtractionRegexWorkUnits += charges.ExtractionRegexWorkUnits
		aggregate.ExtractionOutputs += charges.ExtractionOutputs
		aggregate.JSONEvaluationWorkUnits += charges.JSONEvaluationWorkUnits
		aggregate.ScalarExpressions += charges.ScalarExpressions
		aggregate.ScalarExpressionNodes += charges.ScalarExpressionNodes
		aggregate.ScalarPredicates += charges.ScalarPredicates
		if aggregate.GeneratedFields > knowledgesnapshot.MaximumGeneratedFields ||
			aggregate.ExtractionRegexPrograms > knowledgesnapshot.MaximumRegexPrograms ||
			aggregate.ExtractionRegexWorkUnits > knowledgesnapshot.MaximumRegexWorkUnits ||
			aggregate.ExtractionOutputs > MaximumResolutionExtractionOutputs ||
			aggregate.JSONEvaluationWorkUnits > splpath.MaximumEvaluationWorkUnits ||
			aggregate.ScalarExpressions > knowledgesnapshot.MaximumScalarExpressions ||
			aggregate.ScalarExpressionNodes > knowledgesnapshot.MaximumScalarExpressionNodes ||
			aggregate.ScalarPredicates > MaximumResolutionScalarPredicates {
			return ResolutionStaticCharges{}, fmt.Errorf(
				"%w: resolved definition semantics exceed query-wide limits",
				control.ErrCapacityExceeded,
			)
		}
	}
	return aggregate, nil
}

func validateWinningResolutionDependency(
	source, target resolutionCandidate,
	sourceVersion, targetVersion versionRecord,
) error {
	if !resolutionVersionMatchesCandidate(sourceVersion, source) ||
		!resolutionVersionMatchesCandidate(targetVersion, target) {
		return fmt.Errorf("%w: winning dependency version authority disagrees", ErrCorrupt)
	}
	if !dependencyScopeAllows(sourceVersion, targetVersion) {
		return fmt.Errorf("%w: winning dependency violates sharing authority", ErrCorrupt)
	}
	if source.semantics.dependency.stage <= target.semantics.dependency.stage {
		return fmt.Errorf("%w: winning dependency does not point to an earlier stage", ErrCorrupt)
	}
	if !dependencyFieldsIntersect(
		source.semantics.dependency.inputFields,
		target.semantics.dependency.outputFields,
	) {
		return fmt.Errorf("%w: winning dependency field identity disagrees", ErrCorrupt)
	}
	if !knowledge.SelectorImplies(source.semantics.selector, target.semantics.selector) {
		return fmt.Errorf("%w: winning dependency selector implication is unproven", ErrCorrupt)
	}
	return nil
}

func resolutionVersionMatchesCandidate(version versionRecord, candidate resolutionCandidate) bool {
	object := candidate.object
	return version.TenantID == object.TenantID &&
		version.KnowledgeObjectID == object.KnowledgeObjectID &&
		version.ObjectVersion == int64(object.Version) &&
		version.AppID == object.AppID && version.OwnerID == object.OwnerID &&
		version.ObjectType == object.ObjectType && version.Name == object.Name &&
		version.SharingScope == object.SharingScope && version.State == StateActive &&
		bytes.Equal(version.DefinitionDigest, object.DefinitionSHA256)
}

func resolutionPrecedenceRank(scope normalizedResolutionScope, object Object) (uint8, error) {
	switch object.SharingScope {
	case SharingScopePrivate:
		if object.AppID != scope.appID || object.OwnerID != scope.principalID {
			return 0, fmt.Errorf("%w: private resolution candidate escaped authorization", ErrCorrupt)
		}
		return 3, nil
	case SharingScopeApp:
		if object.AppID != scope.appID {
			return 0, fmt.Errorf("%w: app resolution candidate escaped authorization", ErrCorrupt)
		}
		return 2, nil
	case SharingScopeGlobal:
		return 1, nil
	default:
		return 0, fmt.Errorf("%w: resolution candidate sharing scope is invalid", ErrCorrupt)
	}
}

func resolutionSelectorIntersects(
	ctx context.Context,
	selector *knowledge.Selector,
	indexes []string,
) (bool, error) {
	if selector == nil || len(indexes) == 0 {
		return false, fmt.Errorf("%w: resolution selector authority is invalid", ErrCorrupt)
	}
	patterns := selector.Patterns(knowledge.DimensionIndex)
	if len(patterns) == 0 {
		return true, nil
	}
	indexOnly, err := knowledge.CompileSelector(knowledge.SelectorSpec{Dimensions: []knowledge.DimensionSpec{{
		Dimension: knowledge.DimensionIndex,
		Patterns:  patterns,
	}}})
	if err != nil {
		return false, fmt.Errorf("%w: active index selector is invalid", ErrCorrupt)
	}
	for _, indexName := range indexes {
		// Resolver pruning is a bounded semantic probe over at most 256 authorized
		// index names, not event execution. Give each name an independent hard
		// matcher budget so a conservative cross-engine execution charge cannot
		// accumulate into a false catalog-corruption classification.
		matched, _, matchErr := indexOnly.Match(ctx, knowledge.EventMetadata{
			Index: knowledge.StringMetadata(indexName),
		}, knowledge.DefaultRuntimeBudget())
		if matchErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return false, ctxErr
			}
			return false, fmt.Errorf("%w: active index selector matching failed", ErrCorrupt)
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func resolutionSnapshotObject(object Object) (knowledgesnapshot.Object, error) {
	objectType, typeOK := objectTypeToProto(object.ObjectType)
	sharingScope, scopeOK := knowledgeSharingScopeToProto(object.SharingScope)
	if !typeOK || !scopeOK || object.Definition == nil {
		return knowledgesnapshot.Object{}, fmt.Errorf("%w: active resolution object type is invalid", ErrCorrupt)
	}
	definition, ok := proto.Clone(object.Definition).(*opensplunkv1.KnowledgeObjectDefinition)
	if !ok || definition == nil {
		return knowledgesnapshot.Object{}, fmt.Errorf("%w: active resolution definition cannot be detached", ErrCorrupt)
	}
	return knowledgesnapshot.Object{
		KnowledgeObjectID: strings.Clone(object.KnowledgeObjectID),
		Version:           object.Version,
		ObjectType:        objectType,
		Name:              strings.Clone(object.Name),
		AppID:             strings.Clone(object.AppID),
		OwnerID:           strings.Clone(object.OwnerID),
		SharingScope:      sharingScope,
		Definition:        definition,
		DefinitionSHA256:  bytes.Clone(object.DefinitionSHA256),
	}, nil
}

func normalizeResolutionScope(scope ResolutionScope) (normalizedResolutionScope, error) {
	if !validIdentity(scope.TenantID, maximumTenantIDBytes) ||
		!validIdentity(scope.PrincipalID, maximumOwnerIDBytes) ||
		!validIdentity(scope.AppID, maximumAppIDBytes) {
		return normalizedResolutionScope{}, fmt.Errorf("%w: knowledge resolution scope is invalid", control.ErrInvalidArgument)
	}
	indexScope, err := indexread.NormalizeScope(scope.TenantID, scope.EffectiveAuthorizedIndexes)
	if err != nil {
		return normalizedResolutionScope{}, fmt.Errorf("%w: knowledge resolution index scope is invalid", control.ErrInvalidArgument)
	}
	return normalizedResolutionScope{
		tenantID:    strings.Clone(indexScope.TenantID),
		principalID: strings.Clone(scope.PrincipalID),
		appID:       strings.Clone(scope.AppID),
		indexes:     slices.Clone(indexScope.IndexNames),
	}, nil
}

func resolverSQLiteBusyOrLocked(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	return resolverSQLiteResultCodeBusyOrLocked(sqliteErr.Code())
}

func resolverSQLiteResultCodeBusyOrLocked(code int) bool {
	switch code & 0xff {
	case 5, 6:
		return true
	default:
		return false
	}
}
