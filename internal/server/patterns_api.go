package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/http"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobproto"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	searchPatternsListRoute     = "/search/jobs/patterns/list"
	searchPatternMembersRoute   = "/search/jobs/patterns/members"
	maximumPatternResponseBytes = 8 << 20
)

// SearchPatterns reads an immutable, owner-scoped retained relation. Results
// retain their accounting reservation until Close, including wire conversion
// and serialization; callers must transfer that ownership to the response.
type SearchPatterns interface {
	MaximumPageSize() int
	List(context.Context, searchjobs.AccessScope, patterns.ListRequest) (patterns.ListResult, error)
	Members(context.Context, searchjobs.AccessScope, patterns.MemberRequest) (patterns.MemberResult, error)
}

// patternAPI shares the result snapshot validator with ordinary retained result
// pages and Nearby. The callback validates both the job and its generation.
type patternAPI struct {
	handler       *apiHandler
	service       SearchPatterns
	parseSnapshot func(string, string) (uint64, error)
}

func (api *patternAPI) register(group *apiRouteGroup, smallRequestBytes int64) {
	group.Route(
		sizedPostRoute(searchPatternsListRoute, smallRequestBytes, newSerializedPatternsCodec(), api.list, sanitizeListPatterns),
		sizedPostRoute(searchPatternMembersRoute, smallRequestBytes, newSerializedPatternMembersCodec(), api.members, sanitizePatternMembers),
	)
}

type serializedPatternsResponse = boundedProtoResponse[*opensplunk.ListSearchPatternsResponse]
type serializedPatternMembersResponse = boundedProtoResponse[*opensplunk.ListSearchPatternMembersResponse]

func newSerializedPatternsCodec() *boundedProtoCodec[*opensplunk.ListSearchPatternsRequest, *opensplunk.ListSearchPatternsResponse] {
	return newBoundedProtoCodec(codec.NewProtoCodec[*opensplunk.ListSearchPatternsRequest, *opensplunk.ListSearchPatternsResponse](), patternCodecOptions())
}

func newSerializedPatternMembersCodec() *boundedProtoCodec[*opensplunk.ListSearchPatternMembersRequest, *opensplunk.ListSearchPatternMembersResponse] {
	return newBoundedProtoCodec(codec.NewProtoCodec[*opensplunk.ListSearchPatternMembersRequest, *opensplunk.ListSearchPatternMembersResponse](), patternCodecOptions())
}

func patternCodecOptions() boundedProtoCodecOptions {
	return boundedProtoCodecOptions{
		stateError:   "pattern serialization state is invalid",
		messageError: "pattern response is missing",
		maximumBytes: maximumPatternResponseBytes,
		sizeError:    "pattern response exceeds its byte limit",
	}
}

func (api *patternAPI) list(request *http.Request, input *opensplunk.ListSearchPatternsRequest) (*serializedPatternsResponse, error) {
	generation, err := api.parseSnapshot(input.GetSearchJobId(), input.GetSnapshotRef())
	if err != nil {
		return nil, badRequestError("snapshot reference is invalid")
	}
	pageSize, pageToken, includeTotal := api.pageRequest(input.GetPage())
	query := patterns.ListRequest{SearchJobID: input.GetSearchJobId(), Generation: generation,
		Sensitivity: patterns.Sensitivity(input.GetSensitivity()), PageSize: pageSize, PageToken: pageToken, IncludeTotal: includeTotal}
	result, err := api.service.List(request.Context(), api.handler.accessScope(), query)
	transferred := false
	defer func() {
		if !transferred {
			result.Close()
		}
	}()
	if err := mapPatternsCallError(request.Context(), err); err != nil {
		return nil, err
	}
	release, acquired := api.handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("pattern response capacity is exhausted")
	}
	defer func() {
		if !transferred {
			release()
		}
	}()
	response, err := patternsToProto(request.Context(), result, query, input.GetSnapshotRef())
	if err != nil {
		return nil, patternConversionError(request.Context(), err)
	}
	transferred = true
	return &serializedPatternsResponse{message: response, ctx: request.Context(), release: func() { result.Close(); release() }}, nil
}

func (api *patternAPI) members(request *http.Request, input *opensplunk.ListSearchPatternMembersRequest) (*serializedPatternMembersResponse, error) {
	generation, err := api.parseSnapshot(input.GetSearchJobId(), input.GetSnapshotRef())
	if err != nil {
		return nil, badRequestError("snapshot reference is invalid")
	}
	pageSize, pageToken, includeTotal := api.pageRequest(input.GetPage())
	query := patterns.MemberRequest{SearchJobID: input.GetSearchJobId(), Generation: generation,
		Sensitivity: patterns.Sensitivity(input.GetSensitivity()), PatternID: input.GetPatternId(), Columns: input.GetColumns(),
		PageSize: pageSize, PageToken: pageToken, IncludeTotal: includeTotal}
	result, err := api.service.Members(request.Context(), api.handler.accessScope(), query)
	transferred := false
	defer func() {
		if !transferred {
			result.Close()
		}
	}()
	if err := mapPatternsCallError(request.Context(), err); err != nil {
		return nil, err
	}
	release, acquired := api.handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("pattern response capacity is exhausted")
	}
	defer func() {
		if !transferred {
			release()
		}
	}()
	response, err := patternMembersToProto(request.Context(), result, query, input.GetSnapshotRef())
	if err != nil {
		return nil, patternConversionError(request.Context(), err)
	}
	transferred = true
	return &serializedPatternMembersResponse{message: response, ctx: request.Context(), release: func() { result.Close(); release() }}, nil
}

func patternsToProto(ctx context.Context, result patterns.ListResult, query patterns.ListRequest, snapshotRef string) (*opensplunk.ListSearchPatternsResponse, error) {
	if ctx == nil {
		return nil, errors.New("pattern conversion context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Generation != query.Generation || snapshotRef == "" ||
		result.RetainedEventCount > patterns.DefaultMaximumRows || result.EligibleEventCount > result.RetainedEventCount ||
		result.ExcludedEventCount != result.RetainedEventCount-result.EligibleEventCount ||
		result.SnapshotComplete == result.RetainedTruncated {
		return nil, errors.New("invalid pattern coverage")
	}
	page, err := patternPageToProto(len(result.Patterns), query.PageSize, query.PageToken,
		result.NextPageToken, result.TotalSize, result.TotalSizeExact, query.IncludeTotal, false)
	if err != nil {
		return nil, err
	}
	if result.TotalSize != nil && (*result.TotalSize > result.EligibleEventCount || (*result.TotalSize == 0) != (result.EligibleEventCount == 0)) {
		return nil, errors.New("invalid pattern total")
	}
	rows := make([]*opensplunk.SearchPattern, len(result.Patterns))
	seen := make(map[string]struct{}, len(rows))
	var sum uint64
	for index, item := range result.Patterns {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if item.ID == "" || len(item.ID) > patterns.MaximumCursorBytes || !utf8.ValidString(item.ID) ||
			len(item.Signature) > patterns.DefaultMaximumSignatureBytes || !utf8.ValidString(item.Signature) ||
			item.EventCount == 0 || item.EventCount > result.EligibleEventCount-sum {
			return nil, errors.New("invalid pattern group")
		}
		if index > 0 {
			previous := result.Patterns[index-1]
			if item.EventCount > previous.EventCount || (item.EventCount == previous.EventCount && item.Signature <= previous.Signature) {
				return nil, errors.New("unordered pattern groups")
			}
		}
		if _, duplicate := seen[item.ID]; duplicate {
			return nil, errors.New("duplicate pattern identity")
		}
		seen[item.ID] = struct{}{}
		sum += item.EventCount
		rows[index] = &opensplunk.SearchPattern{PatternId: item.ID, Signature: item.Signature, EventCount: item.EventCount}
	}
	if query.PageToken == "" && result.NextPageToken == "" && sum != result.EligibleEventCount {
		return nil, errors.New("incomplete pattern relation")
	}
	response := &opensplunk.ListSearchPatternsResponse{Patterns: rows, Page: page, SnapshotRef: snapshotRef,
		AlgorithmVersion: patterns.AlgorithmVersion, EligibleEventCount: result.EligibleEventCount,
		ExcludedEventCount: result.ExcludedEventCount, RetainedEventCount: result.RetainedEventCount,
		RetainedTruncated: result.RetainedTruncated, SnapshotComplete: result.SnapshotComplete}
	if proto.Size(response) > maximumPatternResponseBytes {
		return nil, patterns.ErrLimit
	}
	return response, nil
}

func patternMembersToProto(ctx context.Context, result patterns.MemberResult, query patterns.MemberRequest, snapshotRef string) (*opensplunk.ListSearchPatternMembersResponse, error) {
	if ctx == nil {
		return nil, errors.New("pattern conversion context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if result.Generation != query.Generation || result.PatternID != query.PatternID || result.PatternID == "" || snapshotRef == "" {
		return nil, errors.New("invalid pattern member identity")
	}
	page, err := patternPageToProto(len(result.Rows), query.PageSize, query.PageToken,
		result.NextPageToken, result.TotalSize, result.TotalSizeExact, query.IncludeTotal, true)
	if err != nil {
		return nil, err
	}
	for index, row := range result.Rows {
		if row.Ordinal >= patterns.DefaultMaximumRows || index > 0 && row.Ordinal <= result.Rows[index-1].Ordinal {
			return nil, errors.New("invalid pattern member order")
		}
	}
	if len(query.Columns) != 0 {
		if len(result.Schema.Columns) != len(query.Columns) {
			return nil, errors.New("invalid pattern member projection")
		}
		for index, column := range result.Schema.Columns {
			if column.Name != query.Columns[index] {
				return nil, errors.New("invalid pattern member projection")
			}
		}
	}
	schema, err := searchjobproto.Schema(patternMemberSchemaID(query.SearchJobID, query.Generation, result.Schema), result.Schema, searchjobproto.ResultShape{Kind: opensplunk.ResultSetKind_RESULT_SET_KIND_EVENTS})
	if err != nil {
		return nil, err
	}
	rows, err := searchjobproto.Rows(ctx, query.SearchJobID, result.Schema, result.Rows, patterns.MaximumPageSize)
	if err != nil {
		return nil, err
	}
	response := &opensplunk.ListSearchPatternMembersResponse{PatternId: result.PatternID,
		ResultPage: &opensplunk.ResultPage{Schema: schema, Rows: rows, Page: page, SnapshotComplete: result.SnapshotComplete, SnapshotRef: snapshotRef}}
	if proto.Size(response) > maximumPatternResponseBytes {
		return nil, patterns.ErrLimit
	}
	return response, nil
}

// One schema identity names one immutable generation and ordered projection.
// Length framing prevents column names containing separators from colliding.
func patternMemberSchemaID(jobID string, generation uint64, schema searchjobs.Schema) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("open-splunk/pattern-member-schema/v1"))
	var size [8]byte
	writeString := func(value string) {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(value))
	}
	writeString(jobID)
	binary.BigEndian.PutUint64(size[:], generation)
	_, _ = digest.Write(size[:])
	for _, column := range schema.Columns {
		writeString(column.Name)
	}
	return "pattern-schema-" + hex.EncodeToString(digest.Sum(nil))
}

func patternPageToProto(count, requestedSize int, token, next string, total *uint64, exact, includeTotal, shortPages bool) (*opensplunk.PageResponse, error) {
	if requestedSize == 0 {
		requestedSize = patterns.DefaultMaximumPageSize
	}
	if requestedSize < 1 || requestedSize > patterns.MaximumPageSize || count > requestedSize ||
		len(next) > patterns.MaximumCursorBytes || !utf8.ValidString(next) ||
		next != "" && (count == 0 || next == token || !shortPages && count != requestedSize) ||
		count == 0 && token != "" || includeTotal != (total != nil) || exact != includeTotal {
		return nil, errors.New("invalid pattern page")
	}
	if total != nil {
		if *total > patterns.DefaultMaximumRows || *total < uint64(count) || next != "" && *total <= uint64(count) ||
			token == "" && next == "" && *total != uint64(count) {
			return nil, errors.New("invalid pattern page total")
		}
	}
	page := &opensplunk.PageResponse{TotalSizeExact: exact}
	if total != nil {
		page.TotalSize = new(*total)
	}
	if next != "" {
		page.NextPageToken = new(next)
	}
	return page, nil
}

func (api *patternAPI) pageRequest(page *opensplunk.PageRequest) (int, string, bool) {
	size := patterns.DefaultMaximumPageSize
	if page != nil && page.PageSize != nil {
		size = int(page.GetPageSize())
	}
	return min(size, api.service.MaximumPageSize()), page.GetPageToken(), page.GetIncludeTotalSize()
}

func patternConversionError(ctx context.Context, err error) error {
	if requestContextFailure(ctx, err) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "pattern request was canceled")
	}
	if errors.Is(err, patterns.ErrLimit) {
		return mapPatternsCallError(ctx, err)
	}
	return internalError()
}

func mapPatternsCallError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if requestContextFailure(ctx, err) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "pattern request was canceled")
	}
	switch {
	case errors.Is(err, patterns.ErrInvalidRequest), errors.Is(err, patterns.ErrInvalidCursor), errors.Is(err, searchartifacts.ErrInvalid), errors.Is(err, searchartifacts.ErrInvalidCursor):
		return badRequestError("pattern request or page token is invalid")
	case errors.Is(err, patterns.ErrGenerationMismatch):
		return router.NewHTTPError(http.StatusConflict, "pattern snapshot changed")
	case errors.Is(err, patterns.ErrPatternNotFound), errors.Is(err, searchjobs.ErrNotFound), errors.Is(err, searchartifacts.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "pattern or search job not found")
	case errors.Is(err, searchjobs.ErrResultsNotReady), errors.Is(err, searchjobs.ErrResultsUnavailable), errors.Is(err, searchartifacts.ErrNotReady), errors.Is(err, searchartifacts.ErrConflict), errors.Is(err, searchartifacts.ErrCorrupt):
		return router.NewHTTPError(http.StatusConflict, "pattern results are unavailable")
	case errors.Is(err, searchjobs.ErrExpired), errors.Is(err, searchartifacts.ErrExpired):
		return router.NewHTTPError(http.StatusGone, "pattern results expired")
	case errors.Is(err, patterns.ErrUnsupported), errors.Is(err, patterns.ErrLimit), errors.Is(err, searchjobs.ErrExecutionLimit), errors.Is(err, searchjobs.ErrUnsupportedValue):
		return router.NewHTTPError(http.StatusUnprocessableEntity, "pattern analysis is unsupported or exceeded its execution limit")
	case errors.Is(err, patterns.ErrCapacity), errors.Is(err, searchjobs.ErrCapacity), errors.Is(err, searchartifacts.ErrCapacity):
		return router.NewHTTPError(http.StatusTooManyRequests, "pattern capacity is exhausted")
	case errors.Is(err, patterns.ErrClosed), errors.Is(err, searchartifacts.ErrClosed), errors.Is(err, searchjobs.ErrClosed), errors.Is(err, searchjobs.ErrStorageUnavailable), errors.Is(err, searchjobs.ErrJournalUnavailable):
		return unavailableError("pattern service is unavailable")
	default:
		return internalError()
	}
}
