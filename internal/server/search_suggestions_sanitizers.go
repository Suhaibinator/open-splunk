package server

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/router"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// sanitizeGetSearchSuggestionsRequest bounds the editor buffer and the cursor
// into it. The source keeps its bytes exactly — it is the text being completed,
// so trimming it would move every offset the response reports. The time range
// and the index scope stay in the handler: resolving them needs the clock and
// the index catalog.
func (handler *apiHandler) sanitizeGetSearchSuggestionsRequest(
	_ context.Context,
	request *opensplunk.GetSearchSuggestionsRequest,
) (*opensplunk.GetSearchSuggestionsRequest, error) {
	source := request.GetSpl()
	if len(source) > spl.MaximumSuggestionSourceBytes {
		return request, router.NewHTTPError(
			http.StatusRequestEntityTooLarge,
			"search suggestion source is too large",
		)
	}
	if !utf8.ValidString(source) || strings.IndexByte(source, 0) >= 0 {
		return request, badRequestError("search suggestion source is invalid")
	}
	cursor := request.GetCursorByteOffset()
	if cursor > uint64(len(source)) {
		return request, badRequestError("cursor byte offset is outside the search source")
	}
	if offset := safecast.MustConv[int](cursor); offset < len(source) && !utf8.RuneStart(source[offset]) {
		return request, badRequestError("cursor byte offset must be on a UTF-8 boundary")
	}
	if request.MaxSuggestions != nil {
		value := request.GetMaxSuggestions()
		if value == 0 || value > handler.maximumSuggestions {
			return request, badRequestError("maximum suggestions is outside the supported range")
		}
	}
	if request.AppId != nil {
		appID, err := normalizeSearchAppID(request.GetAppId())
		if err != nil {
			return request, badRequestError("search app ID is invalid")
		}
		request.AppId = &appID
	}
	return request, nil
}
