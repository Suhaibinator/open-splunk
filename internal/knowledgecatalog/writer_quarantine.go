package knowledgecatalog

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"slices"
	"strings"
	"time"

	"fortio.org/safecast"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	quarantineRecoveryTokenLifetime = 10 * time.Minute
	quarantineRecoveryTokenVersion  = byte(1)
	quarantineRecoveryTokenMACBytes = sha256.Size
	quarantineRecoveryTokenDomain   = "open-splunk/knowledge-quarantine-token/v1\x00"
	quarantineAuthorityDigestDomain = "open-splunk/knowledge-quarantine-authority/v1\x00"
)

type quarantineAuthorityNode struct {
	registry     registryRecord
	version      versionRecord
	dependencies []dependencyRecord
}

type quarantineAuthority struct {
	catalog catalogState
	rootID  string
	ordered []quarantineAuthorityNode
	digest  [sha256.Size]byte
}

type quarantineRecoveryToken struct {
	tenantID        string
	actorKind       audit.ActorKind
	actorID         string
	rootID          string
	expiresAtMicros int64
	catalogRevision int64
	catalogToken    [catalogStateTokenBytes]byte
	authorityDigest [sha256.Size]byte
}

// PrepareQuarantine performs one definition-free integrity scan and mints a
// short-lived token bound to its exact raw registry, current-version, sealed
// dependency, ordered cascade, and catalog-branch authorities.
func (writer *Writer) PrepareQuarantine(
	ctx context.Context,
	scope WriteScope,
	request *opensplunk.PrepareKnowledgeObjectQuarantineRequest,
) (result *opensplunk.PrepareKnowledgeObjectQuarantineResponse, returnedErr error) {
	defer func() {
		returnedErr = withDefaultErrorDisposition(
			returnedErr,
			ErrorDispositionDefinitiveRejection,
		)
	}()
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if !writer.ReadyForQuarantine() {
		return nil, fmt.Errorf("%w: knowledge quarantine is unavailable", control.ErrInvalidArgument)
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	submitted, err := detachPrepareQuarantineRequest(request)
	if err != nil {
		return nil, err
	}

	tx := writer.orm.WithContext(ctx).Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		return nil, writerError(ctx, "begin quarantine preparation", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	authority, err := writer.scanQuarantineAuthority(
		ctx,
		tx,
		normalizedScope,
		submitted.GetKnowledgeObjectId(),
	)
	if err != nil {
		return nil, writerError(ctx, "scan quarantine authority", err)
	}
	now, err := normalizeWriterClock(writer.clock())
	if err != nil {
		return nil, err
	}
	if now.UnixMicro() > 253402300799999999-int64(quarantineRecoveryTokenLifetime/time.Microsecond) {
		return nil, control.ErrCapacityExceeded
	}
	expiresAt := now.Add(quarantineRecoveryTokenLifetime)
	catalogToken, err := catalogStateTokenValue(authority.catalog)
	if err != nil {
		return nil, err
	}
	tokenAuthority := quarantineRecoveryToken{
		tenantID:        normalizedScope.tenantID,
		actorKind:       actor.Kind,
		actorID:         actor.ID,
		rootID:          authority.rootID,
		expiresAtMicros: expiresAt.UnixMicro(),
		catalogRevision: authority.catalog.revision,
		authorityDigest: authority.digest,
	}
	copy(tokenAuthority.catalogToken[:], catalogToken)
	token, err := writer.encodeQuarantineRecoveryToken(tokenAuthority)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit().Error; err != nil {
		return nil, writerError(ctx, "commit quarantine preparation", err)
	}
	return &opensplunk.PrepareKnowledgeObjectQuarantineResponse{
		RootKnowledgeObjectId: strings.Clone(authority.rootID),
		RecoveryToken:         token,
		ExpiresAt:             timestamppb.New(expiresAt),
		DependentCount:        uint32(len(authority.ordered) - 1),
		TenantCatalogRevision: safecast.MustConv[uint64](authority.catalog.revision),
	}, nil
}

// Quarantine consumes one signed recovery plan, re-proves its authority in the
// mutation transaction, and terminally quarantines the active-dependent
// closure before its root. Exact committed retries remain permanently
// redacted by the shared idempotency reader.
func (writer *Writer) Quarantine(
	ctx context.Context,
	scope WriteScope,
	request *opensplunk.QuarantineKnowledgeObjectRequest,
) (result *opensplunk.QuarantineKnowledgeObjectResponse, returnedErr error) {
	var authorized *AuthorizedContext
	replayOutcomeUnknown := false
	receiptAbsenceProven := false
	defer func() {
		if returnedErr != nil && replayOutcomeUnknown && !receiptAbsenceProven {
			returnedErr = withDefaultErrorDisposition(returnedErr, ErrorDispositionIndeterminate)
		}
		returnedErr = withDefaultErrorDisposition(
			returnedErr,
			ErrorDispositionDefinitiveRejection,
		)
		if returnedErr != nil && authorized != nil {
			returnedErr = withAuthorizedContext(returnedErr, *authorized)
		}
	}()
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	if !writer.ReadyForQuarantine() {
		return nil, fmt.Errorf("%w: knowledge quarantine is unavailable", control.ErrInvalidArgument)
	}
	normalizedScope, err := normalizeWriteScope(scope)
	if err != nil {
		return nil, err
	}
	actor, err := requireMutationActor(ctx)
	if err != nil {
		return nil, err
	}
	prepared, err := prepareQuarantineMutation(normalizedScope, actor, request)
	if err != nil {
		return nil, err
	}
	replayOutcomeUnknown = true
	request = prepared.quarantineRequest

	tx := writer.orm.WithContext(ctx).Begin()
	if tx.Error != nil {
		return nil, writerError(ctx, "begin quarantine", tx.Error)
	}
	defer finishWriterTransaction(tx, &returnedErr)
	_, found, replayAuthorized, err := writer.readAuthorizedIdempotencyRecord(
		ctx,
		tx,
		&prepared,
		mutationRouteQuarantine,
	)
	if err != nil {
		if errors.Is(err, ErrIdempotentOutcomeRedacted) || errors.Is(err, control.ErrNotFound) {
			return nil, withErrorDisposition(err, ErrorDispositionDefinitiveRejection)
		}
		return nil, writerError(ctx, "read quarantine idempotency", err)
	}
	authorized = replayAuthorized
	receiptAbsenceProven = !found
	if found {
		return nil, withErrorDisposition(ErrIdempotentOutcomeRedacted, ErrorDispositionKnownCommitted)
	}

	token, err := writer.decodeQuarantineRecoveryToken(request.GetRecoveryToken())
	if err != nil || token.tenantID != prepared.scope.tenantID ||
		token.actorKind != prepared.actor.Kind || token.actorID != prepared.actor.ID {
		return nil, control.ErrNotFound
	}
	now, err := normalizeWriterClock(writer.clock())
	if err != nil {
		return nil, err
	}
	if now.UnixMicro() > token.expiresAtMicros {
		return nil, control.ErrNotFound
	}
	health, oldCatalog, err := writer.prepareMutationTenant(tx, prepared.scope.tenantID)
	if err != nil {
		return nil, err
	}
	authority, err := writer.scanQuarantineAuthority(
		ctx,
		tx,
		prepared.scope,
		token.rootID,
	)
	if err != nil {
		return nil, writerError(ctx, "re-read quarantine authority", err)
	}
	authorizedValue := authorizedObjectContext(
		authority.ordered[len(authority.ordered)-1].registry,
	)
	authorized = &authorizedValue
	oldToken, err := catalogStateTokenValue(oldCatalog)
	if err != nil {
		return nil, err
	}
	if authority.catalog.revision != oldCatalog.revision ||
		token.catalogRevision != oldCatalog.revision ||
		subtle.ConstantTimeCompare(token.catalogToken[:], oldToken) != 1 ||
		subtle.ConstantTimeCompare(token.authorityDigest[:], authority.digest[:]) != 1 {
		return nil, control.ErrVersionConflict
	}
	transitionCount := int64(len(authority.ordered))
	if transitionCount < 1 ||
		health.VersionCount > maximumVersionsPerTenant-transitionCount ||
		health.RecoveryAuditCount > maximumRecoveryAuditRecords-transitionCount ||
		health.IdempotencyCount >= absoluteIdempotencyCapacity {
		return nil, control.ErrCapacityExceeded
	}
	latestUpdate := int64(0)
	for _, node := range authority.ordered {
		latestUpdate = max(latestUpdate, node.registry.UpdatedAtUnixMicro)
	}
	mutationTime, err := nextWriterTime(now, latestUpdate)
	if err != nil {
		return nil, control.ErrCapacityExceeded
	}
	transitions, rootRecoverySequence, rootVersion, err := writer.publishQuarantineClosure(
		ctx,
		tx,
		prepared,
		authority,
		health.RecoveryAuditCount,
		mutationTime,
	)
	if err != nil {
		return nil, err
	}
	advanced, err := advancePublicationCatalogState(tx, prepared.scope.tenantID, oldCatalog)
	if err != nil {
		return nil, err
	}
	advancedToken, err := catalogStateTokenValue(advanced)
	if err != nil {
		return nil, err
	}
	if err := writer.recordQuarantineOutcome(
		ctx,
		tx,
		prepared,
		authority.rootID,
		rootVersion,
		rootRecoverySequence,
		mutationTime,
		advanced,
		advancedToken,
	); err != nil {
		return nil, err
	}
	result = &opensplunk.QuarantineKnowledgeObjectResponse{
		RootKnowledgeObjectId: strings.Clone(authority.rootID),
		Transitions:           transitions,
		TenantCatalogRevision: safecast.MustConv[uint64](advanced.revision),
	}
	if err := tx.Commit().Error; err != nil {
		return nil, withErrorDisposition(
			writerError(ctx, "commit quarantine", err),
			ErrorDispositionIndeterminate,
		)
	}
	return result, nil
}

func detachPrepareQuarantineRequest(
	request *opensplunk.PrepareKnowledgeObjectQuarantineRequest,
) (*opensplunk.PrepareKnowledgeObjectQuarantineRequest, error) {
	if request == nil ||
		!validIdentity(request.GetKnowledgeObjectId(), maximumObjectIDBytes) ||
		proto.Size(request) < 1 || proto.Size(request) > 1<<10 ||
		len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, invalidMutation("quarantine preparation request is invalid")
	}
	cloned, ok := proto.Clone(request).(*opensplunk.PrepareKnowledgeObjectQuarantineRequest)
	if !ok || cloned == nil {
		return nil, invalidMutation("quarantine preparation request cannot be detached")
	}
	return cloned, nil
}

type quarantineClosureIdentity struct {
	KnowledgeObjectID string `gorm:"column:knowledge_object_id"`
	CurrentVersion    int64  `gorm:"column:current_version"`
}

func (writer *Writer) scanQuarantineAuthority(
	ctx context.Context,
	tx *gorm.DB,
	scope normalizedWriteScope,
	rootID string,
) (quarantineAuthority, error) {
	if writer == nil || writer.reader == nil || tx == nil ||
		!validIdentity(rootID, maximumObjectIDBytes) {
		return quarantineAuthority{}, control.ErrInvalidArgument
	}
	if err := validateContext(ctx); err != nil {
		return quarantineAuthority{}, err
	}
	catalog, err := readCatalogState(tx, scope.tenantID)
	if err != nil {
		return quarantineAuthority{}, err
	}
	if !catalog.found || catalog.revision < 1 {
		return quarantineAuthority{}, control.ErrNotFound
	}
	var closure []quarantineClosureIdentity
	if err := tx.Raw(`WITH RECURSIVE closure(knowledge_object_id, current_version) AS (
		SELECT knowledge_object_id, current_version
		FROM knowledge_objects
		WHERE tenant_id = ? AND knowledge_object_id = ?
		UNION
		SELECT source.knowledge_object_id, source.current_version
		FROM closure AS target
		JOIN knowledge_object_dependencies AS dependency
		  ON dependency.tenant_id = ?
		 AND dependency.target_kind = 'object'
		 AND dependency.target_object_id = target.knowledge_object_id
		JOIN knowledge_objects AS source
		  ON source.tenant_id = dependency.tenant_id
		 AND source.knowledge_object_id = dependency.source_object_id
		 AND source.current_version = dependency.source_object_version
		 AND source.state = 'active'
	)
	SELECT knowledge_object_id, current_version
	FROM closure
	ORDER BY knowledge_object_id ASC
	LIMIT ?`, scope.tenantID, rootID, scope.tenantID, maximumObjectsPerTenant+1).
		Scan(&closure).Error; err != nil {
		return quarantineAuthority{}, err
	}
	if len(closure) < 1 || len(closure) > maximumObjectsPerTenant {
		if len(closure) == 0 {
			return quarantineAuthority{}, control.ErrNotFound
		}
		return quarantineAuthority{}, control.ErrCapacityExceeded
	}
	objectIDs := make([]string, len(closure))
	for index, identity := range closure {
		if !validIdentity(identity.KnowledgeObjectID, maximumObjectIDBytes) ||
			identity.CurrentVersion < 1 || identity.CurrentVersion > maximumVersionsPerTenant ||
			(index > 0 && closure[index-1].KnowledgeObjectID >= identity.KnowledgeObjectID) {
			return quarantineAuthority{}, fmt.Errorf("%w: quarantine closure identity is invalid", ErrCorrupt)
		}
		objectIDs[index] = identity.KnowledgeObjectID
	}
	if _, found := slices.BinarySearch(objectIDs, rootID); !found {
		return quarantineAuthority{}, control.ErrNotFound
	}
	readScope, err := normalizeScope(ReadScope{
		TenantID:       scope.tenantID,
		OwnerID:        scope.ownerID,
		ReadableAppIDs: slices.Clone(scope.writableAppIDs),
	})
	if err != nil {
		return quarantineAuthority{}, err
	}
	registries, err := readAuthorizedGraphRegistries(tx, readScope, objectIDs)
	if err != nil {
		return quarantineAuthority{}, err
	}
	if len(registries) != len(objectIDs) {
		return quarantineAuthority{}, control.ErrNotFound
	}
	root := registries[rootID]
	if root.KnowledgeObjectID != rootID ||
		root.State == StateQuarantined || root.State == StateDeleted {
		return quarantineAuthority{}, control.ErrNotFound
	}
	versions, err := readCurrentVersionRecordsBatch(tx, scope.tenantID, objectIDs)
	if err != nil {
		return quarantineAuthority{}, err
	}
	nodes := make(map[string]quarantineAuthorityNode, len(objectIDs))
	for _, objectID := range objectIDs {
		registry := registries[objectID]
		version := versions[objectID]
		if validateCurrentVersion(registry, version) != nil ||
			registry.State == StateQuarantined || registry.State == StateDeleted ||
			objectID != rootID && registry.State != StateActive {
			return quarantineAuthority{}, fmt.Errorf("%w: quarantine closure current authority is invalid", ErrCorrupt)
		}
		dependencies, err := readValidatedVersionDependencies(tx, version)
		if err != nil {
			return quarantineAuthority{}, err
		}
		nodes[objectID] = quarantineAuthorityNode{
			registry:     registry,
			version:      version,
			dependencies: dependencies,
		}
	}
	ordered, err := orderQuarantineClosure(rootID, nodes)
	if err != nil {
		return quarantineAuthority{}, err
	}
	authority := quarantineAuthority{
		catalog: catalog,
		rootID:  strings.Clone(rootID),
		ordered: ordered,
	}
	authority.digest = digestQuarantineAuthority(authority)
	return authority, nil
}

func orderQuarantineClosure(
	rootID string,
	nodes map[string]quarantineAuthorityNode,
) ([]quarantineAuthorityNode, error) {
	if len(nodes) < 1 || len(nodes) > maximumObjectsPerTenant {
		return nil, fmt.Errorf("%w: quarantine closure cardinality is invalid", ErrCorrupt)
	}
	dependentCounts := make(map[string]int, len(nodes))
	targets := make(map[string][]string, len(nodes))
	for objectID := range nodes {
		dependentCounts[objectID] = 0
	}
	for objectID, node := range nodes {
		for _, dependency := range node.dependencies {
			if _, included := nodes[dependency.TargetObjectID]; !included {
				continue
			}
			dependentCounts[dependency.TargetObjectID]++
			targets[objectID] = append(targets[objectID], dependency.TargetObjectID)
		}
	}
	ready := make([]string, 0, len(nodes))
	for objectID, count := range dependentCounts {
		if count == 0 {
			ready = append(ready, objectID)
		}
	}
	slices.Sort(ready)
	ordered := make([]quarantineAuthorityNode, 0, len(nodes))
	for len(ready) != 0 {
		objectID := ready[0]
		ready = ready[1:]
		ordered = append(ordered, nodes[objectID])
		for _, targetID := range targets[objectID] {
			dependentCounts[targetID]--
			if dependentCounts[targetID] == 0 {
				ready = append(ready, targetID)
				slices.Sort(ready)
			}
		}
	}
	if len(ordered) != len(nodes) ||
		ordered[len(ordered)-1].registry.KnowledgeObjectID != rootID {
		return nil, fmt.Errorf("%w: quarantine closure is cyclic or does not terminate at its root", ErrCorrupt)
	}
	return slices.Clip(ordered), nil
}

func digestQuarantineAuthority(authority quarantineAuthority) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(quarantineAuthorityDigestDomain))
	quarantineDigestString(hasher, authority.rootID)
	quarantineDigestInt64(hasher, authority.catalog.revision)
	quarantineDigestString(hasher, authority.catalog.token)
	quarantineDigestInt64(hasher, int64(len(authority.ordered)))
	for ordinal, node := range authority.ordered {
		quarantineDigestInt64(hasher, int64(ordinal))
		digestQuarantineRegistry(hasher, node.registry)
		digestQuarantineVersion(hasher, node.version)
		quarantineDigestInt64(hasher, int64(len(node.dependencies)))
		for _, dependency := range node.dependencies {
			quarantineDigestString(hasher, dependency.TenantID)
			quarantineDigestString(hasher, dependency.SourceObjectID)
			quarantineDigestInt64(hasher, dependency.SourceObjectVersion)
			quarantineDigestInt64(hasher, dependency.Ordinal)
			quarantineDigestString(hasher, dependency.TargetKind)
			quarantineDigestString(hasher, dependency.TargetObjectID)
			quarantineDigestInt64(hasher, dependency.TargetObjectVersion)
			quarantineDigestString(hasher, dependency.DependencyRole)
		}
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func digestQuarantineRegistry(hasher hash.Hash, record registryRecord) {
	quarantineDigestString(hasher, record.TenantID)
	quarantineDigestString(hasher, record.KnowledgeObjectID)
	quarantineDigestInt64(hasher, record.CurrentVersion)
	quarantineDigestString(hasher, record.AppID)
	quarantineDigestString(hasher, record.OwnerID)
	quarantineDigestString(hasher, string(record.ObjectType))
	quarantineDigestString(hasher, record.Name)
	quarantineDigestString(hasher, string(record.SharingScope))
	quarantineDigestString(hasher, string(record.State))
	writeDigestFrame(hasher, record.DefinitionDigest)
	quarantineDigestInt64(hasher, record.CreatedAtUnixMicro)
	quarantineDigestInt64(hasher, record.UpdatedAtUnixMicro)
	quarantineDigestOptionalInt64(hasher, record.DisabledAtUnixMicro)
	quarantineDigestOptionalInt64(hasher, record.QuarantinedAtUnixMicro)
	quarantineDigestOptionalInt64(hasher, record.DeletedAtUnixMicro)
	quarantineDigestOptionalString(hasher, record.QuarantineReason)
}

func digestQuarantineVersion(hasher hash.Hash, record versionRecord) {
	quarantineDigestString(hasher, record.TenantID)
	quarantineDigestString(hasher, record.KnowledgeObjectID)
	quarantineDigestInt64(hasher, record.ObjectVersion)
	quarantineDigestString(hasher, record.AppID)
	quarantineDigestString(hasher, record.OwnerID)
	quarantineDigestString(hasher, string(record.ObjectType))
	quarantineDigestString(hasher, record.Name)
	quarantineDigestString(hasher, string(record.SharingScope))
	quarantineDigestString(hasher, string(record.State))
	writeDigestFrame(hasher, record.DefinitionDigest)
	quarantineDigestInt64(hasher, record.DependencyCount)
	quarantineDigestString(hasher, record.MutationKind)
	quarantineDigestOptionalString(hasher, record.QuarantineReason)
	quarantineDigestInt64(hasher, record.CreatedAtUnixMicro)
}

func quarantineDigestString(hasher hash.Hash, value string) {
	writeDigestFrame(hasher, []byte(value))
}

func quarantineDigestInt64(hasher hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	writeDigestFrame(hasher, encoded[:])
}

func quarantineDigestOptionalInt64(hasher hash.Hash, value *int64) {
	if value == nil {
		writeDigestFrame(hasher, nil)
		return
	}
	var encoded [9]byte
	encoded[0] = 1
	binary.BigEndian.PutUint64(encoded[1:], uint64(*value))
	writeDigestFrame(hasher, encoded[:])
}

func quarantineDigestOptionalString(hasher hash.Hash, value *string) {
	if value == nil {
		writeDigestFrame(hasher, nil)
		return
	}
	writeDigestFrame(hasher, append([]byte{1}, []byte(*value)...))
}

func (writer *Writer) encodeQuarantineRecoveryToken(
	authority quarantineRecoveryToken,
) (string, error) {
	if !writer.ReadyForQuarantine() ||
		!validIdentity(authority.tenantID, maximumTenantIDBytes) ||
		!validQuarantineActorKind(authority.actorKind) || !validIdentity(authority.actorID, maximumOwnerIDBytes) ||
		!validIdentity(authority.rootID, maximumObjectIDBytes) ||
		authority.expiresAtMicros < 1 || authority.catalogRevision < 1 {
		return "", control.ErrInvalidArgument
	}
	body := make([]byte, 0, 512)
	body = append(body, quarantineRecoveryTokenVersion)
	body = appendTokenInt64(body, authority.expiresAtMicros)
	body = appendTokenInt64(body, authority.catalogRevision)
	body = appendTokenString(body, authority.tenantID)
	body = appendTokenString(body, string(authority.actorKind))
	body = appendTokenString(body, authority.actorID)
	body = appendTokenString(body, authority.rootID)
	body = append(body, authority.catalogToken[:]...)
	body = append(body, authority.authorityDigest[:]...)
	mac := hmac.New(sha256.New, writer.recoveryTokenKey)
	_, _ = mac.Write([]byte(quarantineRecoveryTokenDomain))
	_, _ = mac.Write(body)
	encoded := append(body, mac.Sum(nil)...)
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func (writer *Writer) decodeQuarantineRecoveryToken(
	encoded string,
) (quarantineRecoveryToken, error) {
	if !writer.ReadyForQuarantine() || !validOpaqueRecoveryToken(encoded) {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(raw) < 1+16+8+quarantineRecoveryTokenMACBytes ||
		base64.RawURLEncoding.EncodeToString(raw) != encoded {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	body := raw[:len(raw)-quarantineRecoveryTokenMACBytes]
	providedMAC := raw[len(raw)-quarantineRecoveryTokenMACBytes:]
	mac := hmac.New(sha256.New, writer.recoveryTokenKey)
	_, _ = mac.Write([]byte(quarantineRecoveryTokenDomain))
	_, _ = mac.Write(body)
	if subtle.ConstantTimeCompare(providedMAC, mac.Sum(nil)) != 1 {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	reader := tokenReader{remaining: body}
	version, ok := reader.byte()
	if !ok || version != quarantineRecoveryTokenVersion {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	result := quarantineRecoveryToken{}
	if result.expiresAtMicros, ok = reader.int64(); !ok {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	if result.catalogRevision, ok = reader.int64(); !ok {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	tenantID, ok := reader.string(maximumTenantIDBytes)
	if !ok {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	actorKind, ok := reader.string(len(audit.ActorKindBrowser))
	if !ok {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	actorID, ok := reader.string(maximumOwnerIDBytes)
	if !ok {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	rootID, ok := reader.string(maximumObjectIDBytes)
	if !ok || len(reader.remaining) != catalogStateTokenBytes+sha256.Size {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	result.tenantID = tenantID
	result.actorKind = audit.ActorKind(actorKind)
	result.actorID = actorID
	result.rootID = rootID
	copy(result.catalogToken[:], reader.remaining[:catalogStateTokenBytes])
	copy(result.authorityDigest[:], reader.remaining[catalogStateTokenBytes:])
	if !validIdentity(result.tenantID, maximumTenantIDBytes) ||
		!validQuarantineActorKind(result.actorKind) || !validIdentity(result.actorID, maximumOwnerIDBytes) ||
		!validIdentity(result.rootID, maximumObjectIDBytes) ||
		result.expiresAtMicros < 1 || result.catalogRevision < 1 {
		return quarantineRecoveryToken{}, control.ErrNotFound
	}
	return result, nil
}

func validQuarantineActorKind(kind audit.ActorKind) bool {
	return kind == audit.ActorKindSystem || kind == audit.ActorKindBrowser
}

func appendTokenInt64(output []byte, value int64) []byte {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	return append(output, encoded[:]...)
}

func appendTokenString(output []byte, value string) []byte {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], uint16(len(value)))
	output = append(output, encoded[:]...)
	return append(output, value...)
}

type tokenReader struct {
	remaining []byte
}

func (reader *tokenReader) byte() (byte, bool) {
	if len(reader.remaining) < 1 {
		return 0, false
	}
	value := reader.remaining[0]
	reader.remaining = reader.remaining[1:]
	return value, true
}

func (reader *tokenReader) int64() (int64, bool) {
	if len(reader.remaining) < 8 {
		return 0, false
	}
	value := int64(binary.BigEndian.Uint64(reader.remaining[:8]))
	reader.remaining = reader.remaining[8:]
	return value, true
}

func (reader *tokenReader) string(maximumBytes int) (string, bool) {
	if len(reader.remaining) < 2 {
		return "", false
	}
	length := int(binary.BigEndian.Uint16(reader.remaining[:2]))
	reader.remaining = reader.remaining[2:]
	if length < 1 || length > maximumBytes || len(reader.remaining) < length {
		return "", false
	}
	value := string(reader.remaining[:length])
	reader.remaining = reader.remaining[length:]
	return value, true
}

func (writer *Writer) publishQuarantineClosure(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	authority quarantineAuthority,
	recoveryAuditCount int64,
	occurredAt time.Time,
) ([]*opensplunk.KnowledgeQuarantineTransition, int64, int64, error) {
	if writer == nil || tx == nil || len(authority.ordered) < 1 ||
		authority.ordered[len(authority.ordered)-1].registry.KnowledgeObjectID != authority.rootID {
		return nil, 0, 0, control.ErrInvalidArgument
	}
	transitions := make([]*opensplunk.KnowledgeQuarantineTransition, len(authority.ordered))
	rootRecoverySequence := int64(0)
	rootVersion := int64(0)
	for ordinal, node := range authority.ordered {
		reason := "dependency_recovery"
		reasonProto := opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_DEPENDENCY_RECOVERY
		if node.registry.KnowledgeObjectID == authority.rootID {
			reason = "root_corruption"
			reasonProto = opensplunk.KnowledgeQuarantineReason_KNOWLEDGE_QUARANTINE_REASON_ROOT_CORRUPTION
		}
		version := node.registry.CurrentVersion + 1
		reasonCopy := reason
		immutable := versionRecord{
			TenantID:           prepared.scope.tenantID,
			KnowledgeObjectID:  node.registry.KnowledgeObjectID,
			ObjectVersion:      version,
			AppID:              node.registry.AppID,
			OwnerID:            node.registry.OwnerID,
			ObjectType:         node.registry.ObjectType,
			Name:               node.registry.Name,
			SharingScope:       node.registry.SharingScope,
			State:              StateQuarantined,
			DefinitionDigest:   nil,
			DependencyCount:    0,
			MutationKind:       "quarantine",
			QuarantineReason:   &reasonCopy,
			CreatedAtUnixMicro: occurredAt.UnixMicro(),
		}
		if err := tx.Create(&immutable).Error; err != nil {
			return nil, 0, 0, writerError(ctx, "insert quarantine version", err)
		}
		if result := tx.Exec(`INSERT INTO knowledge_object_dependency_seals (
			tenant_id, knowledge_object_id, object_version, dependency_count
		) VALUES (?, ?, ?, 0)`, prepared.scope.tenantID, node.registry.KnowledgeObjectID, version); result.Error != nil {
			return nil, 0, 0, writerError(ctx, "seal quarantine dependencies", result.Error)
		}
		if result := tx.Exec(`INSERT INTO knowledge_object_list_projections (
			tenant_id, knowledge_object_id, object_version,
			app_id, owner_id, object_type, name, sharing_scope, state,
			description_present, description,
			index_selector_count, host_selector_count,
			source_selector_count, sourcetype_selector_count,
			selector_value_bytes, canonical_selector_bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'quarantined', 0, '', 0, 0, 0, 0, 0, 0)`,
			prepared.scope.tenantID,
			node.registry.KnowledgeObjectID,
			version,
			node.registry.AppID,
			node.registry.OwnerID,
			node.registry.ObjectType,
			node.registry.Name,
			node.registry.SharingScope,
		); result.Error != nil {
			return nil, 0, 0, writerError(ctx, "insert quarantine projection", result.Error)
		}
		if result := tx.Exec(`INSERT INTO knowledge_object_list_projection_seals (
			tenant_id, knowledge_object_id, object_version,
			projection_bytes, canonical_selector_bytes
		) VALUES (?, ?, ?, 0, 0)`, prepared.scope.tenantID, node.registry.KnowledgeObjectID, version); result.Error != nil {
			return nil, 0, 0, writerError(ctx, "seal quarantine projection", result.Error)
		}
		result := tx.Model(&registryRecord{}).Where(
			`tenant_id = ? AND knowledge_object_id = ? AND current_version = ?
			 AND app_id = ? AND owner_id = ? AND state = ?`,
			node.registry.TenantID,
			node.registry.KnowledgeObjectID,
			node.registry.CurrentVersion,
			node.registry.AppID,
			node.registry.OwnerID,
			node.registry.State,
		).Updates(map[string]any{
			"current_version":           version,
			"state":                     StateQuarantined,
			"definition_digest":         nil,
			"updated_at_unix_micro":     occurredAt.UnixMicro(),
			"disabled_at_unix_micro":    nil,
			"quarantined_at_unix_micro": occurredAt.UnixMicro(),
			"deleted_at_unix_micro":     nil,
			"quarantine_reason":         reason,
		})
		if result.Error != nil {
			return nil, 0, 0, writerError(ctx, "publish quarantine registry", result.Error)
		}
		if result.RowsAffected != 1 {
			return nil, 0, 0, control.ErrVersionConflict
		}
		published := node.registry
		published.CurrentVersion = version
		published.State = StateQuarantined
		published.DefinitionDigest = nil
		published.UpdatedAtUnixMicro = occurredAt.UnixMicro()
		published.DisabledAtUnixMicro = nil
		published.QuarantinedAtUnixMicro = new(occurredAt.UnixMicro())
		published.DeletedAtUnixMicro = nil
		published.QuarantineReason = &reasonCopy
		if _, err := writer.reader.objectFromCurrentRegistry(tx, published); err != nil {
			return nil, 0, 0, err
		}
		sequence := recoveryAuditCount + int64(ordinal) + 1
		if result := tx.Exec(`INSERT INTO knowledge_recovery_audit (
			tenant_id, sequence, knowledge_object_id, object_version,
			actor_kind, actor_id, actor_role,
			app_id, object_type, sharing_scope, recovery_reason,
			occurred_at_unix_micro
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			prepared.scope.tenantID,
			sequence,
			node.registry.KnowledgeObjectID,
			version,
			prepared.actor.Kind,
			prepared.actor.ID,
			prepared.actor.Role,
			node.registry.AppID,
			node.registry.ObjectType,
			node.registry.SharingScope,
			reason,
			occurredAt.UnixMicro(),
		); result.Error != nil {
			return nil, 0, 0, writerError(ctx, "append knowledge recovery audit", result.Error)
		}
		transitions[ordinal] = &opensplunk.KnowledgeQuarantineTransition{
			CascadeOrdinal:     uint32(ordinal),
			KnowledgeObjectId:  strings.Clone(node.registry.KnowledgeObjectID),
			PreviousVersion:    safecast.MustConv[uint64](node.registry.CurrentVersion),
			QuarantinedVersion: safecast.MustConv[uint64](version),
			Reason:             reasonProto,
		}
		if node.registry.KnowledgeObjectID == authority.rootID {
			rootRecoverySequence = sequence
			rootVersion = version
		}
	}
	if rootRecoverySequence < 1 || rootVersion < 2 {
		return nil, 0, 0, fmt.Errorf("%w: quarantine root outcome is absent", ErrCorrupt)
	}
	return transitions, rootRecoverySequence, rootVersion, nil
}

func (writer *Writer) recordQuarantineOutcome(
	ctx context.Context,
	tx *gorm.DB,
	prepared preparedMutation,
	rootID string,
	rootVersion int64,
	recoverySequence int64,
	occurredAt time.Time,
	advanced catalogState,
	advancedToken []byte,
) error {
	retentionAnchor, err := writerRetentionAnchor(tx, occurredAt.UnixMicro())
	if err != nil {
		return err
	}
	retentionMicros := int64(writer.idempotencyRetention / time.Microsecond)
	if retentionAnchor > 253402300799999999-retentionMicros {
		return control.ErrCapacityExceeded
	}
	retainUntil := retentionAnchor + retentionMicros
	commitAuthority := mutationCommitAuthorityRecord{
		TenantID:                 prepared.scope.tenantID,
		ActorKind:                prepared.actor.Kind,
		ActorID:                  prepared.actor.ID,
		Route:                    mutationRouteQuarantine,
		ClientRequestID:          prepared.clientRequestID,
		RequestDigest:            bytes.Clone(prepared.requestDigest[:]),
		CatalogRevision:          advanced.revision,
		CatalogStateToken:        bytes.Clone(advancedToken),
		MutationKind:             "quarantine",
		KnowledgeObjectID:        rootID,
		ObjectVersion:            rootVersion,
		OccurredAtUnixMicro:      occurredAt.UnixMicro(),
		RetentionAnchorUnixMicro: retentionAnchor,
		RetainUntilUnixMicro:     retainUntil,
		RecoveryAuditSequence:    &recoverySequence,
	}
	if err := tx.Create(&commitAuthority).Error; err != nil {
		return writerError(ctx, "record quarantine commit authority", err)
	}
	outcome, err := encodeOutcomeReference(mutationOutcomeAuthority{
		route:                    mutationRouteQuarantine,
		mutationKind:             "quarantine",
		objectID:                 rootID,
		version:                  safecast.MustConv[uint64](rootVersion),
		catalogRevision:          safecast.MustConv[uint64](advanced.revision),
		catalogStateToken:        advancedToken,
		recoveryAuditSequence:    safecast.MustConv[uint64](recoverySequence),
		occurredAtUnixMicro:      occurredAt.UnixMicro(),
		retentionAnchorUnixMicro: retentionAnchor,
		retainUntilUnixMicro:     retainUntil,
	})
	if err != nil {
		return err
	}
	receipt := idempotencyRecord{
		TenantID:                   prepared.scope.tenantID,
		ActorKind:                  prepared.actor.Kind,
		ActorID:                    prepared.actor.ID,
		Route:                      mutationRouteQuarantine,
		ClientRequestID:            prepared.clientRequestID,
		MutationKind:               "quarantine",
		RequestDigestFormatVersion: mutationRequestDigestFormatVersion,
		RequestDigest:              bytes.Clone(prepared.requestDigest[:]),
		OutcomeFormatVersion:       mutationOutcomeFormatVersion,
		OutcomeProto:               outcome,
		CommittedCatalogRevision:   advanced.revision,
		CommittedCatalogStateToken: bytes.Clone(advancedToken),
		KnowledgeObjectID:          rootID,
		ObjectVersion:              rootVersion,
		RecoveryAuditSequence:      &recoverySequence,
		CreatedAtUnixMicro:         occurredAt.UnixMicro(),
		RetentionAnchorUnixMicro:   retentionAnchor,
		RetainUntilUnixMicro:       retainUntil,
	}
	if err := tx.Create(&receipt).Error; err != nil {
		return writerError(ctx, "record quarantine idempotency outcome", err)
	}
	return nil
}
