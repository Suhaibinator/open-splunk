package searchhistory

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const historyCursorVersion = 1

type listCursor struct {
	Version    int    `json:"v"`
	FilterHash string `json:"f"`
	SortKey    *int64 `json:"k"`
	JobID      string `json:"i"`
}

func encodeCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = historyCursorVersion
	token, err := cursorcodec.Encode(key, "search-history-list-cursor", historyCursorVersion, maximumCursorBytes, cursor)
	if err != nil {
		return "", fmt.Errorf("encode search-history cursor: %w", err)
	}
	return token, nil
}

func decodeCursor(key []byte, token, filterHash string) (listCursor, error) {
	invalidToken := func() (listCursor, error) {
		return listCursor{}, fmt.Errorf("%w: page token is invalid or does not match the request", control.ErrInvalidArgument)
	}
	var cursor listCursor
	if err := cursorcodec.Decode(key, "search-history-list-cursor", historyCursorVersion, maximumCursorBytes, token, &cursor); err != nil {
		return invalidToken()
	}
	if cursor.Version != historyCursorVersion || cursor.FilterHash != filterHash || cursor.SortKey == nil || cursor.JobID == "" {
		return invalidToken()
	}
	if err := validateText("cursor search job ID", cursor.JobID, maximumSearchJobIDBytes, false); err != nil {
		return invalidToken()
	}
	return cursor, nil
}
