package searchjobs

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	jobListCursorVersion = 1
	jobListCursorDomain  = "search-job-list-cursor"
	jobListFixedSort     = "created_at_desc,id_desc"
)

type jobListCursor struct {
	Version          int    `json:"v"`
	Epoch            string `json:"e"`
	FilterHash       string `json:"f"`
	HighWater        uint64 `json:"h"`
	LastCreatedUnix  int64  `json:"s"`
	LastCreatedNanos int32  `json:"n"`
	LastID           string `json:"i"`
}

type jobListFilterFingerprint struct {
	Version  int     `json:"v"`
	TenantID string  `json:"t"`
	OwnerID  string  `json:"o"`
	AppID    *string `json:"a"`
	States   []State `json:"s"`
	Text     *string `json:"q"`
	Sort     string  `json:"k"`
}

func jobListFilterHash(access AccessScope, request normalizedJobListRequest) (string, error) {
	payload, err := json.Marshal(jobListFilterFingerprint{
		Version:  1,
		TenantID: access.TenantID,
		OwnerID:  access.OwnerID,
		AppID:    request.appID,
		States:   request.states,
		Text:     request.text,
		Sort:     jobListFixedSort,
	})
	if err != nil {
		return "", fmt.Errorf("encode search job list filter: %w", err)
	}
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:]), nil
}

func encodeJobListCursor(key []byte, epoch, filterHash string, highWater uint64, last Job) (string, error) {
	token, err := cursorcodec.Encode(key, jobListCursorDomain, jobListCursorVersion, maximumJobListTokenSize, jobListCursor{
		Version:          jobListCursorVersion,
		Epoch:            epoch,
		FilterHash:       filterHash,
		HighWater:        highWater,
		LastCreatedUnix:  last.CreatedAt.Unix(),
		LastCreatedNanos: int32(last.CreatedAt.Nanosecond()),
		LastID:           last.ID,
	})
	if err != nil {
		return "", fmt.Errorf("encode search job list cursor: %w", err)
	}
	return token, nil
}

func decodeJobListCursor(key []byte, token, epoch, filterHash string) (jobListCursor, error) {
	invalid := func() (jobListCursor, error) {
		return jobListCursor{}, ErrInvalidCursor
	}
	var cursor jobListCursor
	if err := cursorcodec.Decode(key, jobListCursorDomain, jobListCursorVersion, maximumJobListTokenSize, token, &cursor); err != nil {
		return invalid()
	}
	decodedHash, err := base64.RawURLEncoding.DecodeString(cursor.FilterHash)
	if err != nil || len(decodedHash) != sha256.Size ||
		base64.RawURLEncoding.EncodeToString(decodedHash) != cursor.FilterHash ||
		cursor.Version != jobListCursorVersion ||
		cursor.Epoch != epoch ||
		cursor.FilterHash != filterHash ||
		cursor.HighWater == 0 ||
		cursor.LastCreatedNanos < 0 || cursor.LastCreatedNanos >= int32(time.Second) ||
		cursor.LastID == "" || len(cursor.LastID) > 256 || !utf8.ValidString(cursor.LastID) {
		return invalid()
	}
	return cursor, nil
}

func (cursor jobListCursor) lastCreatedAt() time.Time {
	return time.Unix(cursor.LastCreatedUnix, int64(cursor.LastCreatedNanos)).UTC()
}
