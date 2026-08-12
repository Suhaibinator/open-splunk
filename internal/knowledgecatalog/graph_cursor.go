package knowledgecatalog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	graphCursorVersion = 1
	graphCursorPurpose = "knowledge-catalog-graph-list"
)

type graphCursor struct {
	Version            int    `json:"v"`
	Fingerprint        string `json:"f"`
	CatalogRevision    int64  `json:"r"`
	CatalogState       string `json:"g"`
	ResolvedObjectID   string `json:"i"`
	ResolvedVersion    int64  `json:"n"`
	LastSourceObjectID string `json:"s"`
	LastSourceVersion  int64  `json:"x"`
	LastTargetObjectID string `json:"t"`
	LastTargetVersion  int64  `json:"y"`
	LastDependencyRole int32  `json:"o"`
}

type graphFingerprint struct {
	Version                 int            `json:"v"`
	Direction               graphDirection `json:"d"`
	TenantID                string         `json:"t"`
	OwnerID                 string         `json:"o"`
	ReadableAppIDs          []string       `json:"a"`
	KnowledgeObjectID       string         `json:"i"`
	RequestedVersionPresent bool           `json:"h"`
	RequestedVersion        int64          `json:"n"`
	PageSize                uint32         `json:"p"`
	IncludeTotal            bool           `json:"z"`
}

func graphRequestFingerprint(request normalizedDependencyListRequest) (string, error) {
	payload, err := json.Marshal(graphFingerprint{
		Version:                 graphCursorVersion,
		Direction:               request.direction,
		TenantID:                request.scope.tenantID,
		OwnerID:                 request.scope.ownerID,
		ReadableAppIDs:          request.scope.readableAppIDs,
		KnowledgeObjectID:       request.knowledgeObjectID,
		RequestedVersionPresent: request.requestedVersionPresent,
		RequestedVersion:        request.requestedVersion,
		PageSize:                request.pageSize,
		IncludeTotal:            request.includeTotal,
	})
	if err != nil {
		return "", fmt.Errorf("encode knowledge graph cursor fingerprint: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeGraphCursor(key []byte, cursor graphCursor) (string, error) {
	cursor.Version = graphCursorVersion
	if !validGraphCursor(cursor) {
		return "", ErrInvalidCursor
	}
	token, err := cursorcodec.Encode(
		key,
		graphCursorPurpose,
		graphCursorVersion,
		maximumCursorBytes,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("encode knowledge graph cursor: %w", err)
	}
	return token, nil
}

func decodeGraphCursor(key []byte, token, fingerprint string) (graphCursor, error) {
	var cursor graphCursor
	if err := cursorcodec.Decode(
		key,
		graphCursorPurpose,
		graphCursorVersion,
		maximumCursorBytes,
		token,
		&cursor,
	); err != nil || !validGraphCursor(cursor) || cursor.Fingerprint != fingerprint {
		return graphCursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

func validGraphCursor(cursor graphCursor) bool {
	fingerprint, fingerprintErr := base64.RawURLEncoding.DecodeString(cursor.Fingerprint)
	state, stateErr := base64.RawURLEncoding.DecodeString(cursor.CatalogState)
	return cursor.Version == graphCursorVersion && cursor.CatalogRevision >= 1 &&
		fingerprintErr == nil && len(fingerprint) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(fingerprint) == cursor.Fingerprint &&
		stateErr == nil && len(state) == catalogStateTokenBytes &&
		base64.RawURLEncoding.EncodeToString(state) == cursor.CatalogState &&
		validIdentity(cursor.ResolvedObjectID, maximumObjectIDBytes) &&
		cursor.ResolvedVersion >= 1 && cursor.ResolvedVersion <= maximumVersionsPerTenant &&
		validIdentity(cursor.LastSourceObjectID, maximumObjectIDBytes) &&
		cursor.LastSourceVersion >= 1 && cursor.LastSourceVersion <= maximumVersionsPerTenant &&
		validIdentity(cursor.LastTargetObjectID, maximumObjectIDBytes) &&
		cursor.LastTargetVersion >= 1 && cursor.LastTargetVersion <= maximumVersionsPerTenant &&
		cursor.LastDependencyRole == int32(DependencyRoleFieldInput)
}
