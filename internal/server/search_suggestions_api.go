package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsuggestions"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

const (
	searchSuggestionsRoute = "/search/suggestions"
	searchSuggestionsPath  = apiPathPrefix + searchSuggestionsRoute

	// These bounds independently validate an implementation of
	// SearchSuggestions before any dependency-owned data reaches protobuf
	// serialization. They deliberately mirror, rather than trust, the
	// service-side diagnostic contract.
	maximumSearchSuggestionTextBytes              = spl.MaximumSuggestionSourceBytes
	maximumSearchSuggestionDetailBytes            = 4 << 10
	maximumSearchSuggestionDiagnostics            = 64
	maximumSearchSuggestionDiagnosticCodeBytes    = 128
	maximumSearchSuggestionDiagnosticMessageBytes = 4 << 10
	maximumSearchSuggestionDiagnosticHints        = 64
	maximumSearchSuggestionDiagnosticHintBytes    = 1 << 10
	// A maximum-size source plus 128 maximum-size authorized index names,
	// time-range metadata, and protobuf framing fits below this endpoint cap.
	maximumSearchSuggestionRequestBytes  = int64(64 << 10)
	maximumSearchSuggestionResponseBytes = 10 << 20
)

func (handler *apiHandler) searchSuggestionRoutes(
	noAuth router.AuthLevel,
	smallRequestBytes int64,
) []protobufRouteDefinition {
	return []protobufRouteDefinition{
		newForwardCompatibleProtoRoute(router.RouteConfig[
			*opensplunk.GetSearchSuggestionsRequest,
			*serializedSearchSuggestionsResponse,
		]{
			Path: searchSuggestionsRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newSerializedSearchSuggestionsCodec(), Handler: handler.getSearchSuggestions,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
		}),
	}
}

func (handler *apiHandler) getSearchSuggestions(
	request *http.Request,
	input *opensplunk.GetSearchSuggestionsRequest,
) (*serializedSearchSuggestionsResponse, error) {
	if input == nil {
		return nil, badRequestError("search suggestion request is required")
	}
	source := input.GetSpl()
	if len(source) > spl.MaximumSuggestionSourceBytes {
		return nil, router.NewHTTPError(
			http.StatusRequestEntityTooLarge,
			"search suggestion source is too large",
		)
	}
	if !utf8.ValidString(source) || strings.IndexByte(source, 0) >= 0 {
		return nil, badRequestError("search suggestion source is invalid")
	}
	cursor := input.GetCursorByteOffset()
	if cursor > uint64(len(source)) {
		return nil, badRequestError("cursor byte offset is outside the search source")
	}

	cursorOffset := safecast.MustConv[int](cursor)
	if cursorOffset < len(source) && !utf8.RuneStart(source[cursorOffset]) {
		return nil, badRequestError("cursor byte offset must be on a UTF-8 boundary")
	}

	maximumResponseSuggestions := min(
		handler.maximumSuggestions,
		uint32(spl.DefaultSuggestionLimit),
	)
	var maximum *uint32
	if input.MaxSuggestions != nil {
		value := input.GetMaxSuggestions()
		if value == 0 || value > handler.maximumSuggestions {
			return nil, badRequestError("maximum suggestions is outside the supported range")
		}
		maximum = &value
		maximumResponseSuggestions = value
	}
	if _, err := normalizeSearchAppID(input.GetAppId()); err != nil {
		return nil, badRequestError("search app ID is invalid")
	}
	resolvedRange, err := resolveSearchTimeRange(input.GetTimeRange(), handler.now())
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	authorizedIndexes, err := handler.resolveAuthorizedSearchIndexes(
		request.Context(),
		input.GetIndexScope(),
	)
	if err != nil {
		return nil, err
	}

	result, err := handler.searchSuggestions.Suggest(
		request.Context(),
		searchsuggestions.Request{
			SPL:                       strings.Clone(source),
			CursorByteOffset:          cursorOffset,
			TenantID:                  strings.Clone(handler.tenantID),
			AuthorizedIndexes:         slicesCloneStrings(authorizedIndexes),
			RequestedIndexes:          slicesCloneStrings(authorizedIndexes),
			TimeRange:                 resolvedRange,
			AuthorizedIndexCandidates: slicesCloneStrings(authorizedIndexes),
			MaxSuggestions:            maximum,
		},
	)
	if err := mapSearchSuggestionCallError(request.Context(), err); err != nil {
		return nil, err
	}
	if err := searchSuggestionRequestContextError(request.Context()); err != nil {
		return nil, err
	}

	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("search suggestion response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	response, err := searchSuggestionsResultToProto(
		request.Context(),
		source,
		cursorOffset,
		result,
		maximumResponseSuggestions,
	)
	if err != nil {
		if contextErr := searchSuggestionRequestContextError(request.Context()); contextErr != nil {
			return nil, contextErr
		}
		return nil, internalError()
	}
	if err := searchSuggestionRequestContextError(request.Context()); err != nil {
		return nil, err
	}
	transferred = true
	return &serializedSearchSuggestionsResponse{
		message: response,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func searchSuggestionsResultToProto(
	ctx context.Context,
	source string,
	cursor int,
	result searchsuggestions.Result,
	maximum uint32,
) (*opensplunk.GetSearchSuggestionsResponse, error) {
	if ctx == nil {
		return nil, errors.New("search suggestion conversion context is required")
	}
	if len(source) > spl.MaximumSuggestionSourceBytes ||
		!utf8.ValidString(source) ||
		strings.IndexByte(source, 0) >= 0 ||
		cursor < 0 ||
		cursor > len(source) ||
		!sourceOffsetOnRuneBoundary(source, cursor) ||
		maximum == 0 ||
		maximum > uint32(spl.MaximumSuggestionLimit) ||
		uint64(len(result.Suggestions)) > uint64(maximum) ||
		len(result.Diagnostics) > maximumSearchSuggestionDiagnostics {
		return nil, errors.New("invalid search suggestion result bounds")
	}

	suggestions := make([]*opensplunk.SearchSuggestion, len(result.Suggestions))
	seen := make(map[string]struct{}, len(result.Suggestions))
	var replacement spl.Range
	var previousRelevance float64
	for index, suggestion := range result.Suggestions {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		kind, ok := searchSuggestionKindToProto(suggestion.Kind)
		if !ok ||
			!validSearchSuggestionString(
				suggestion.Label,
				maximumSearchSuggestionTextBytes,
				false,
			) ||
			!validSearchSuggestionString(
				suggestion.Insertion,
				maximumSearchSuggestionTextBytes,
				false,
			) ||
			!validSearchSuggestionString(
				suggestion.Detail,
				maximumSearchSuggestionDetailBytes,
				true,
			) ||
			math.IsNaN(suggestion.Relevance) ||
			math.IsInf(suggestion.Relevance, 0) ||
			suggestion.Relevance < 0 ||
			suggestion.Relevance > 1 ||
			index > 0 && suggestion.Relevance > previousRelevance ||
			!validSearchSuggestionRange(source, cursor, suggestion.Replacement) {
			return nil, errors.New("invalid search suggestion")
		}
		if index == 0 {
			replacement = suggestion.Replacement
		} else if suggestion.Replacement != replacement {
			return nil, errors.New("inconsistent search suggestion replacement range")
		}
		deduplicationKey := searchSuggestionDeduplicationKey(
			suggestion.Kind,
			suggestion.Label,
		)
		if _, duplicate := seen[deduplicationKey]; duplicate {
			return nil, errors.New("duplicate search suggestion")
		}
		seen[deduplicationKey] = struct{}{}

		converted := &opensplunk.SearchSuggestion{
			Kind:             kind,
			Label:            suggestion.Label,
			InsertionText:    suggestion.Insertion,
			ReplacementRange: searchSuggestionRangeToProto(suggestion.Replacement),
			Relevance:        suggestion.Relevance,
		}
		if suggestion.Detail != "" {
			converted.Detail = new(suggestion.Detail)
		}
		suggestions[index] = converted
		previousRelevance = suggestion.Relevance
	}

	for _, diagnostic := range result.Diagnostics {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !validSearchSuggestionDiagnostic(source, diagnostic) {
			return nil, errors.New("invalid search suggestion diagnostic")
		}
	}
	return &opensplunk.GetSearchSuggestionsResponse{
		Suggestions: suggestions,
		Diagnostics: searchjobproto.Diagnostics(result.Diagnostics),
	}, nil
}

func searchSuggestionKindToProto(
	kind spl.SuggestionKind,
) (opensplunk.SearchSuggestionKind, bool) {
	switch kind {
	case spl.SuggestionKindCommand:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_COMMAND, true
	case spl.SuggestionKindFunction:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_FUNCTION, true
	case spl.SuggestionKindField:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_FIELD, true
	case spl.SuggestionKindKeyword:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_KEYWORD, true
	case spl.SuggestionKindIndex:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_INDEX, true
	default:
		return opensplunk.SearchSuggestionKind_SEARCH_SUGGESTION_KIND_UNSPECIFIED, false
	}
}

func validSearchSuggestionString(value string, maximumBytes int, allowEmpty bool) bool {
	if (!allowEmpty && value == "") ||
		len(value) > maximumBytes ||
		!utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}

func validSearchSuggestionRange(source string, cursor int, sourceRange spl.Range) bool {
	if sourceRange.Start.Offset < 0 ||
		sourceRange.End.Offset < sourceRange.Start.Offset ||
		sourceRange.End.Offset > len(source) ||
		cursor < sourceRange.Start.Offset ||
		cursor > sourceRange.End.Offset ||
		!sourceOffsetOnRuneBoundary(source, sourceRange.Start.Offset) ||
		!sourceOffsetOnRuneBoundary(source, sourceRange.End.Offset) {
		return false
	}
	return searchSuggestionPositionAt(source, sourceRange.Start.Offset) == sourceRange.Start &&
		searchSuggestionPositionAt(source, sourceRange.End.Offset) == sourceRange.End
}

func validSearchSuggestionDiagnostic(
	source string,
	diagnostic searchjobs.Diagnostic,
) bool {
	if !validBoundedUTF8String(
		diagnostic.Code,
		maximumSearchSuggestionDiagnosticCodeBytes,
		false,
	) ||
		!validBoundedUTF8String(
			diagnostic.Message,
			maximumSearchSuggestionDiagnosticMessageBytes,
			false,
		) ||
		len(diagnostic.Suggestions) > maximumSearchSuggestionDiagnosticHints {
		return false
	}
	for _, hint := range diagnostic.Suggestions {
		if !validBoundedUTF8String(
			hint,
			maximumSearchSuggestionDiagnosticHintBytes,
			false,
		) {
			return false
		}
	}

	if positionlessSearchSuggestionDiagnostic(diagnostic) {
		return true
	}
	if !diagnostic.ValidSourceRange() ||
		diagnostic.EndByteOffset > len(source) ||
		!sourceOffsetOnRuneBoundary(source, diagnostic.ByteOffset) ||
		!sourceOffsetOnRuneBoundary(source, diagnostic.EndByteOffset) {
		return false
	}
	start := searchSuggestionPositionAt(source, diagnostic.ByteOffset)
	end := searchSuggestionPositionAt(source, diagnostic.EndByteOffset)
	return diagnostic.Line == start.Line &&
		diagnostic.Column == start.Column &&
		diagnostic.EndLine == end.Line &&
		diagnostic.EndColumn == end.Column
}

func validBoundedUTF8String(value string, maximumBytes int, allowEmpty bool) bool {
	return (allowEmpty || value != "") &&
		len(value) <= maximumBytes &&
		utf8.ValidString(value) &&
		strings.IndexByte(value, 0) < 0
}

func positionlessSearchSuggestionDiagnostic(diagnostic searchjobs.Diagnostic) bool {
	return diagnostic.ByteOffset == 0 &&
		diagnostic.Line == 0 &&
		diagnostic.Column == 0 &&
		diagnostic.EndByteOffset == 0 &&
		diagnostic.EndLine == 0 &&
		diagnostic.EndColumn == 0
}

func sourceOffsetOnRuneBoundary(source string, offset int) bool {
	return offset >= 0 &&
		offset <= len(source) &&
		(offset == len(source) || utf8.RuneStart(source[offset]))
}

func searchSuggestionPositionAt(source string, offset int) spl.Position {
	position := spl.Position{Line: 1, Column: 1}
	for position.Offset < offset {
		character, width := utf8.DecodeRuneInString(source[position.Offset:])
		position.Offset += width
		if character == '\n' {
			position.Line++
			position.Column = 1
		} else {
			position.Column++
		}
	}
	return position
}

func searchSuggestionRangeToProto(sourceRange spl.Range) *opensplunk.SourceRange {

	return &opensplunk.SourceRange{
		Start: &opensplunk.SourcePosition{
			ByteOffset: safecast.MustConv[uint64](sourceRange.Start.Offset),
			Line:       safecast.MustConv[uint32](sourceRange.Start.Line),
			Column:     safecast.MustConv[uint32](sourceRange.Start.Column),
		},
		End: &opensplunk.SourcePosition{
			ByteOffset: safecast.MustConv[uint64](sourceRange.End.Offset),
			Line:       safecast.MustConv[uint32](sourceRange.End.Line),
			Column:     safecast.MustConv[uint32](sourceRange.End.Column),
		},
	}
}

func searchSuggestionDeduplicationKey(kind spl.SuggestionKind, label string) string {
	if kind != spl.SuggestionKindField {
		label = eventfields.FoldASCII(label)
	}
	return string(kind) + "\x00" + label
}

func mapSearchSuggestionCallError(ctx context.Context, operationErr error) error {
	if operationErr == nil {
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"search suggestion request was canceled",
		)
	}
	switch {
	case errors.Is(operationErr, searchsuggestions.ErrInvalidRequest):
		return badRequestError("search suggestion request is invalid")
	case errors.Is(operationErr, searchjobs.ErrRequestTooLarge):
		return router.NewHTTPError(
			http.StatusRequestEntityTooLarge,
			"search suggestion request is too large",
		)
	case errors.Is(operationErr, searchjobs.ErrCapacity):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"search suggestion capacity is exhausted",
		)
	case errors.Is(operationErr, searchjobs.ErrExecutionLimit):
		return router.NewHTTPError(
			http.StatusUnprocessableEntity,
			"search suggestion execution limit was exceeded",
		)
	case errors.Is(operationErr, searchjobs.ErrClosed),
		errors.Is(operationErr, searchjobs.ErrStorageUnavailable),
		errors.Is(operationErr, searchjobs.ErrJournalUnavailable):
		return unavailableError("search suggestion service is unavailable")
	default:
		return internalError()
	}
}

func searchSuggestionRequestContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "search suggestion request was canceled")
}

// The hard response field/count bounds above keep successful messages below
// this codec's independent ceiling. The serialization gate prevents many
// worst-case completion responses from retaining their encodings at once.
type serializedSearchSuggestionsResponse = boundedProtoResponse[*opensplunk.GetSearchSuggestionsResponse]

type serializedSearchSuggestionsCodec = boundedProtoCodec[
	*opensplunk.GetSearchSuggestionsRequest,
	*opensplunk.GetSearchSuggestionsResponse,
]

func newSerializedSearchSuggestionsCodec() *serializedSearchSuggestionsCodec {
	return newBoundedProtoCodec(
		codec.NewProtoCodec[
			*opensplunk.GetSearchSuggestionsRequest,
			*opensplunk.GetSearchSuggestionsResponse,
		](),
		boundedProtoCodecOptions{
			stateError:   "search suggestion serialization state is invalid",
			messageError: "search suggestion response is missing",
			contextError: searchSuggestionRequestContextError,
			maximumBytes: maximumSearchSuggestionResponseBytes,
			sizeError:    "search suggestion response exceeds its byte limit",
		},
	)
}

func slicesCloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, len(values))
	for index, value := range values {
		result[index] = strings.Clone(value)
	}
	return result
}
