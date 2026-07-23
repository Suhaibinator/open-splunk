package savedobjects

import (
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	savedSearchCursorVersion = 1
	maximumCursorBytes       = 4096
)

type listCursor struct {
	Version     int    `json:"v"`
	FilterHash  string `json:"f"`
	StringKey   string `json:"s,omitempty"`
	IntegerKey  *int64 `json:"n,omitempty"`
	SavedSearch string `json:"i"`
}

func encodeListCursor(key []byte, cursor listCursor) (string, error) {
	cursor.Version = savedSearchCursorVersion
	token, err := cursorcodec.Encode(key, "saved-search-list-cursor", savedSearchCursorVersion, maximumCursorBytes, cursor)
	if err != nil {
		return "", fmt.Errorf("encode saved-search cursor: %w", err)
	}
	return token, nil
}

func decodeListCursor(key []byte, token, filterHash string, stringSort bool) (listCursor, error) {
	invalid := func() (listCursor, error) {
		return listCursor{}, fmt.Errorf("%w: page token is invalid or does not match the request", control.ErrInvalidArgument)
	}
	var cursor listCursor
	if err := cursorcodec.Decode(key, "saved-search-list-cursor", savedSearchCursorVersion, maximumCursorBytes, token, &cursor); err != nil {
		return invalid()
	}
	if cursor.Version != savedSearchCursorVersion || cursor.FilterHash != filterHash || cursor.SavedSearch == "" {
		return invalid()
	}
	if stringSort {
		if cursor.IntegerKey != nil || cursor.StringKey == "" {
			return invalid()
		}
	} else if cursor.IntegerKey == nil || cursor.StringKey != "" {
		return invalid()
	}
	return cursor, nil
}
