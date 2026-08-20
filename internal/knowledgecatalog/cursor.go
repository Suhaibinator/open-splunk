package knowledgecatalog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	cursorVersion          = 1
	cursorPurpose          = "knowledge-catalog-list"
	catalogStateTokenBytes = sha256.Size
)

type listCursor struct {
	Version         int    `json:"v"`
	Fingerprint     string `json:"f"`
	CatalogRevision int64  `json:"r"`
	CatalogState    string `json:"g"`
	PrimaryString   string `json:"s,omitempty"`
	PrimaryInteger  *int64 `json:"n,omitempty"`
	SecondaryString string `json:"x,omitempty"`
	ObjectID        string `json:"i"`
}

type listFingerprint struct {
	Version        int            `json:"v"`
	TenantID       string         `json:"t"`
	OwnerID        string         `json:"o"`
	ReadableAppIDs []string       `json:"a"`
	PageSize       uint32         `json:"p"`
	IncludeTotal   bool           `json:"z"`
	AppID          *string        `json:"j"`
	OwnerFilter    *string        `json:"w"`
	Text           *string        `json:"q"`
	ObjectTypes    []ObjectType   `json:"y"`
	States         []State        `json:"e"`
	SharingScopes  []SharingScope `json:"h"`
	SelectorText   *string        `json:"l"`
	SortBy         SortBy         `json:"b"`
	SortDirection  SortDirection  `json:"d"`
}

func requestFingerprint(request normalizedListRequest) (string, error) {
	payload, err := json.Marshal(listFingerprint{
		Version:        cursorVersion,
		TenantID:       request.scope.tenantID,
		OwnerID:        request.scope.ownerID,
		ReadableAppIDs: request.scope.readableAppIDs,
		PageSize:       request.pageSize,
		IncludeTotal:   request.includeTotal,
		AppID:          request.appIDFilter,
		OwnerFilter:    request.ownerIDFilter,
		Text:           request.textFilter,
		ObjectTypes:    request.objectTypeFilters,
		States:         request.stateFilters,
		SharingScopes:  request.sharingScopeFilters,
		SelectorText:   request.selectorTextFilter,
		SortBy:         request.sortBy,
		SortDirection:  request.direction,
	})
	if err != nil {
		return "", fmt.Errorf("encode knowledge catalog cursor fingerprint: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = cursorVersion
	if !validCursor(cursor) {
		return "", ErrInvalidCursor
	}
	token, err := cursorcodec.Encode(key, cursorPurpose, cursorVersion, maximumCursorBytes, cursor)
	if err != nil {
		return "", fmt.Errorf("encode knowledge catalog cursor: %w", err)
	}
	return token, nil
}

func decodeCursor(key []byte, token, fingerprint string, sortBy SortBy) (listCursor, error) {
	invalid := func() (listCursor, error) { return listCursor{}, ErrInvalidCursor }
	var cursor listCursor
	if err := cursorcodec.Decode(key, cursorPurpose, cursorVersion, maximumCursorBytes, token, &cursor); err != nil ||
		!validCursor(cursor) || cursor.Fingerprint != fingerprint {
		return invalid()
	}
	switch sortBy {
	case SortByCreatedAt, SortByUpdatedAt:
		if cursor.PrimaryInteger == nil || cursor.PrimaryString != "" || cursor.SecondaryString != "" {
			return invalid()
		}
	case SortByName:
		if cursor.PrimaryInteger != nil || cursor.PrimaryString == "" || cursor.SecondaryString != "" {
			return invalid()
		}
	case SortByObjectType:
		if cursor.PrimaryInteger != nil || cursor.PrimaryString == "" || cursor.SecondaryString == "" {
			return invalid()
		}
	default:
		return invalid()
	}
	return cursor, nil
}

func validCursor(cursor listCursor) bool {
	fingerprint, fingerprintErr := base64.RawURLEncoding.DecodeString(cursor.Fingerprint)
	state, stateErr := base64.RawURLEncoding.DecodeString(cursor.CatalogState)
	return cursor.Version == cursorVersion && cursor.CatalogRevision >= 1 &&
		validIdentity(cursor.ObjectID, maximumObjectIDBytes) && fingerprintErr == nil &&
		len(fingerprint) == sha256.Size && base64.RawURLEncoding.EncodeToString(fingerprint) == cursor.Fingerprint &&
		stateErr == nil && len(state) == catalogStateTokenBytes &&
		base64.RawURLEncoding.EncodeToString(state) == cursor.CatalogState
}
