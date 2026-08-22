package export

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"fortio.org/safecast"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	minimumExportListCursorKeyBytes = 32
	exportListCursorVersion         = 1
	exportListCursorDomain          = "export-job-list-cursor"
	exportListFixedSort             = "created_at_desc,id_desc"
)

type exportListCursor struct {
	Version          int    `json:"v"`
	Epoch            string `json:"e"`
	FilterHash       string `json:"f"`
	HighWater        uint64 `json:"h"`
	LastCreatedUnix  int64  `json:"s"`
	LastCreatedNanos int32  `json:"n"`
	LastID           string `json:"i"`
}

type exportListFilterFingerprint struct {
	Version     int     `json:"v"`
	TenantID    string  `json:"t"`
	OwnerID     string  `json:"o"`
	States      []State `json:"s"`
	SearchJobID *string `json:"j"`
	Sort        string  `json:"k"`
}

func exportListFilterHash(
	access searchjobs.AccessScope,
	request normalizedExportListRequest,
) (string, error) {
	payload, err := json.Marshal(exportListFilterFingerprint{
		Version:     exportListCursorVersion,
		TenantID:    access.TenantID,
		OwnerID:     access.OwnerID,
		States:      request.states,
		SearchJobID: request.searchJobID,
		Sort:        exportListFixedSort,
	})
	if err != nil {
		return "", fmt.Errorf("encode export job list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeExportListCursor(
	key []byte,
	epoch string,
	filterHash string,
	highWater uint64,
	last Job,
) (string, error) {
	token, err := cursorcodec.Encode(
		key,
		exportListCursorDomain,
		exportListCursorVersion,
		maximumExportListTokenSize,
		exportListCursor{
			Version:         exportListCursorVersion,
			Epoch:           epoch,
			FilterHash:      filterHash,
			HighWater:       highWater,
			LastCreatedUnix: last.CreatedAt.Unix(),

			LastCreatedNanos: safecast.MustConv[int32](last.CreatedAt.Nanosecond()),
			LastID:           last.ID,
		},
	)
	if err != nil {
		return "", fmt.Errorf("encode export job list cursor: %w", err)
	}
	return token, nil
}

func decodeExportListCursor(
	key []byte,
	token string,
	epoch string,
	filterHash string,
) (exportListCursor, error) {
	invalid := func() (exportListCursor, error) {
		return exportListCursor{}, ErrInvalidCursor
	}
	var cursor exportListCursor
	if err := cursorcodec.Decode(
		key,
		exportListCursorDomain,
		exportListCursorVersion,
		maximumExportListTokenSize,
		token,
		&cursor,
	); err != nil {
		return invalid()
	}
	decodedHash, err := base64.RawURLEncoding.DecodeString(cursor.FilterHash)
	if err != nil ||
		len(decodedHash) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(decodedHash) != cursor.FilterHash ||
		cursor.Version != exportListCursorVersion ||
		cursor.Epoch != epoch ||
		cursor.FilterHash != filterHash ||
		cursor.HighWater == 0 ||
		cursor.LastCreatedNanos < 0 ||
		cursor.LastCreatedNanos >= int32(time.Second) ||
		!validID(cursor.LastID) {
		return invalid()
	}
	return cursor, nil
}

func (cursor exportListCursor) lastCreatedAt() time.Time {
	return time.Unix(cursor.LastCreatedUnix, int64(cursor.LastCreatedNanos)).UTC()
}
