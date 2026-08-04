package searchaudit

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

type listCursor struct {
	Version         int     `json:"v"`
	FilterHash      string  `json:"f"`
	Sequence        int64   `json:"s"`
	FirstSequence   int64   `json:"b"`
	HighWater       int64   `json:"h"`
	HighWaterDigest string  `json:"d"`
	TotalSize       *uint64 `json:"t,omitempty"`
}

type listFingerprint struct {
	Version      int     `json:"v"`
	TenantID     string  `json:"n"`
	PageSize     uint32  `json:"p"`
	IncludeTotal bool    `json:"i"`
	ActorID      *string `json:"a"`
	OwnerID      *string `json:"o"`
}

func listFilterHash(request normalizedListRequest) (string, error) {
	payload, err := json.Marshal(listFingerprint{
		Version:      searchAuditCursorVersion,
		TenantID:     request.tenantID,
		PageSize:     request.pageSize,
		IncludeTotal: request.includeTotal,
		ActorID:      request.actorID,
		OwnerID:      request.ownerID,
	})
	if err != nil {
		return "", fmt.Errorf("encode search-attempt audit list filters: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeListCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = searchAuditCursorVersion
	if !validListCursor(cursor) {
		return "", fmt.Errorf("%w: search-attempt audit cursor is invalid", control.ErrInvalidArgument)
	}
	token, err := cursorcodec.Encode(
		key,
		searchAuditCursorPurpose,
		searchAuditCursorVersion,
		maximumListCursorBytes,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("encode search-attempt audit cursor: %w", err)
	}
	return token, nil
}

func decodeListCursor(
	key []byte,
	token string,
	filterHash string,
	includeTotal bool,
) (listCursor, error) {
	invalid := func() (listCursor, error) {
		return listCursor{}, fmt.Errorf(
			"%w: token is invalid or does not match the request",
			ErrInvalidCursor,
		)
	}
	var cursor listCursor
	if err := cursorcodec.Decode(
		key,
		searchAuditCursorPurpose,
		searchAuditCursorVersion,
		maximumListCursorBytes,
		token,
		&cursor,
	); err != nil {
		return invalid()
	}
	if !validListCursor(cursor) || cursor.FilterHash != filterHash ||
		(cursor.TotalSize != nil) != includeTotal {
		return invalid()
	}
	return cursor, nil
}

func validListCursor(cursor listCursor) bool {
	if cursor.Version != searchAuditCursorVersion ||
		cursor.FirstSequence < 1 ||
		cursor.Sequence <= cursor.FirstSequence ||
		cursor.Sequence > maximumPersistedSequence ||
		cursor.HighWater < cursor.Sequence ||
		cursor.HighWater > maximumPersistedSequence ||
		!validSHA256Encoding(cursor.FilterHash) ||
		!validSHA256Encoding(cursor.HighWaterDigest) {
		return false
	}
	return cursor.TotalSize == nil || *cursor.TotalSize <= MaximumRetainedAttempts
}

func validSHA256Encoding(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == value
}
