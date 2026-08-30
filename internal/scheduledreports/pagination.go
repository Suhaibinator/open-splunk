package scheduledreports

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	runCursorDomain  = "scheduled-report-runs"
	runCursorVersion = 1
)

type runCursor struct {
	Version            int    `json:"v"`
	Scope              string `json:"s"`
	ClaimedAtUnixMicro int64  `json:"c"`
	RunID              string `json:"i"`
}

func encodeRunCursor(key []byte, ownerID, savedSearchID string, claimedAtUnixMicro int64, runID string) (string, error) {
	cursor := runCursor{
		Version: runCursorVersion, Scope: runCursorScope(ownerID, savedSearchID),
		ClaimedAtUnixMicro: claimedAtUnixMicro, RunID: runID,
	}
	token, err := cursorcodec.Encode(key, runCursorDomain, runCursorVersion, MaximumRunHistoryCursorBytes, cursor)
	if err != nil {
		return "", fmt.Errorf("encode scheduled-report run cursor: %w", err)
	}
	return token, nil
}

func decodeRunCursor(key []byte, ownerID, savedSearchID, token string) (*runCursor, error) {
	if token == "" {
		return nil, nil
	}
	var cursor runCursor
	if err := cursorcodec.Decode(key, runCursorDomain, runCursorVersion, MaximumRunHistoryCursorBytes, token, &cursor); err != nil ||
		cursor.Version != runCursorVersion || cursor.Scope != runCursorScope(ownerID, savedSearchID) ||
		cursor.ClaimedAtUnixMicro <= 0 || strings.TrimSpace(cursor.RunID) == "" || len(cursor.RunID) > 128 {
		return nil, fmt.Errorf("%w: scheduled-report run page token is invalid", ErrInvalidArgument)
	}
	return &cursor, nil
}

func runCursorScope(ownerID, savedSearchID string) string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + savedSearchID))
	return base64.RawURLEncoding.EncodeToString(digest[:])
}
