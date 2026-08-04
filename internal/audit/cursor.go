package audit

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
	HighWater       int64   `json:"h"`
	HighWaterDigest string  `json:"d"`
	TotalSize       *uint64 `json:"t,omitempty"`
}

type listFingerprint struct {
	Version      int         `json:"v"`
	TenantID     string      `json:"n"`
	PageSize     uint32      `json:"p"`
	IncludeTotal bool        `json:"i"`
	Actions      []Action    `json:"a"`
	ActorID      *string     `json:"r"`
	TargetKind   *TargetKind `json:"k"`
}

func listFilterHash(request normalizedListRequest) (string, error) {
	payload, err := json.Marshal(listFingerprint{
		Version:      auditListCursorVersion,
		TenantID:     request.tenantID,
		PageSize:     request.pageSize,
		IncludeTotal: request.includeTotal,
		Actions:      request.actionFilters,
		ActorID:      request.actorID,
		TargetKind:   request.targetKind,
	})
	if err != nil {
		return "", fmt.Errorf("encode audit list filters: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeListCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = auditListCursorVersion
	if !validListCursor(cursor) {
		return "", fmt.Errorf("%w: audit list cursor is invalid", control.ErrInvalidArgument)
	}
	token, err := cursorcodec.Encode(
		key,
		auditListCursorPurpose,
		auditListCursorVersion,
		maximumListCursorBytes,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("encode audit list cursor: %w", err)
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
		auditListCursorPurpose,
		auditListCursorVersion,
		maximumListCursorBytes,
		token,
		&cursor,
	); err != nil {
		return invalid()
	}
	if !validListCursor(cursor) ||
		cursor.FilterHash != filterHash ||
		(cursor.TotalSize != nil) != includeTotal {
		return invalid()
	}
	return cursor, nil
}

func validListCursor(cursor listCursor) bool {
	if cursor.Version != auditListCursorVersion ||
		cursor.Sequence < 1 ||
		cursor.Sequence > MaximumEventsPerTenant ||
		cursor.HighWater < cursor.Sequence ||
		cursor.HighWater > MaximumEventsPerTenant {
		return false
	}
	if !validSHA256Encoding(cursor.FilterHash) ||
		!validSHA256Encoding(cursor.HighWaterDigest) {
		return false
	}
	return cursor.TotalSize == nil || *cursor.TotalSize <= MaximumEventsPerTenant
}

func validSHA256Encoding(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == value
}
