package searchjobs

import (
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	cursorVersion   = 1
	maxCursorLength = 2048
)

type cursorPayload struct {
	Version    int    `json:"v"`
	Scope      string `json:"s"`
	JobID      string `json:"j"`
	Generation uint64 `json:"g"`
	Offset     uint64 `json:"o"`
}

func encodeCursor(key []byte, scope, jobID string, generation, offset uint64) (string, error) {
	return cursorcodec.Encode(key, "search-result-cursor", cursorVersion, maxCursorLength, cursorPayload{
		Version:    cursorVersion,
		Scope:      scope,
		JobID:      jobID,
		Generation: generation,
		Offset:     offset,
	})
}

func decodeCursor(key []byte, token string) (cursorPayload, error) {
	var decoded cursorPayload
	if err := cursorcodec.Decode(key, "search-result-cursor", cursorVersion, maxCursorLength, token, &decoded); err != nil {
		return cursorPayload{}, ErrInvalidCursor
	}
	if decoded.Version != cursorVersion || decoded.Scope == "" || decoded.JobID == "" || decoded.Generation == 0 || decoded.Offset == 0 {
		return cursorPayload{}, ErrInvalidCursor
	}
	return decoded, nil
}
