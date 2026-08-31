package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

// sanitizeCreateIndexRequest rejects the idempotency envelope this API version
// does not honor. The definition itself is converted, and validated against the
// clock, by indexDefinitionFromProto.
func sanitizeCreateIndexRequest(
	_ context.Context,
	request *opensplunk.CreateIndexRequest,
) (*opensplunk.CreateIndexRequest, error) {
	if request.ClientRequestId != nil {
		return request, badRequestError(
			"client request idempotency is not supported",
		)
	}
	return request, nil
}

func sanitizeGetIndexRequest(
	_ context.Context,
	request *opensplunk.GetIndexRequest,
) (*opensplunk.GetIndexRequest, error) {
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	return request, nil
}

// sanitizeListIndexesRequest states the whole list contract: the page envelope
// is within the browser-wide bounds, at most three known state filters are
// requested, the text filter is printable and bounded, and the sort pair names a
// sort this API version serves. The handler then projects the accepted request
// with indexStateFilterOrder, indexSortByOrDefault and adminPageProjection,
// none of which can fail.
func (handler *apiHandler) sanitizeListIndexesRequest(
	_ context.Context,
	request *opensplunk.ListIndexesRequest,
) (*opensplunk.ListIndexesRequest, error) {
	if err := handler.sanitizeAdminListPage(
		request.GetPage(),
		maximumIndexRowsPerResponse,
	); err != nil {
		return request, err
	}
	if _, err := normalizeIndexStateFilters(
		request.GetStateFilters(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	textFilter, err := sanitizedAdminTextFilter(request.TextFilter)
	if err != nil {
		return request, err
	}
	request.TextFilter = textFilter
	if _, _, err := normalizeIndexSort(
		request.GetSortBy(),
		request.GetSortDirection(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeUpdateIndexRequest(
	_ context.Context,
	request *opensplunk.UpdateIndexRequest,
) (*opensplunk.UpdateIndexRequest, error) {
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	return request, nil
}

func sanitizeSetIndexStateRequest(
	_ context.Context,
	request *opensplunk.SetIndexStateRequest,
) (*opensplunk.SetIndexStateRequest, error) {
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	if _, err := indexStateFromProto(request.GetState()); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

// sanitizeDeleteIndexRequest guarantees a known deletion mode and a
// confirmation name already in canonical index-name form, so the handler
// compares it to the stored name instead of re-deriving it.
//
// Two DELETE_DATA checks deliberately stay in the handler. Whether physical
// deletion is configured at all reads handler dependencies. The
// expected_version == math.MaxInt64 rejection is pure, but it must keep
// reporting after that availability check, so a server without the deletion
// dependencies still answers "physical index data deletion is not available"
// rather than leaking a version complaint about a mode it cannot serve.
func sanitizeDeleteIndexRequest(
	_ context.Context,
	request *opensplunk.DeleteIndexRequest,
) (*opensplunk.DeleteIndexRequest, error) {
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	switch request.GetDataDeletionMode() {
	case opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
		opensplunk.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA:
	default:
		return request, badRequestError(
			"index data deletion mode is invalid",
		)
	}
	confirmation := request.GetConfirmationName()
	canonical, err := control.NormalizeIndexName(confirmation)
	if err != nil || canonical != confirmation {
		return request, badRequestError(
			"index delete confirmation is invalid",
		)
	}
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	return request, nil
}

func sanitizeGetIndexStatsRequest(
	_ context.Context,
	request *opensplunk.GetIndexStatsRequest,
) (*opensplunk.GetIndexStatsRequest, error) {
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	return request, nil
}

// sanitizeListIndexFieldsRequest bounds an explicit page size against the
// browser-wide limit and then lowers it to the field service's own maximum, so
// the handler forwards page_size unchanged. The requested time range still
// resolves against the handler clock.
func (handler *apiHandler) sanitizeListIndexFieldsRequest(
	_ context.Context,
	request *opensplunk.ListIndexFieldsRequest,
) (*opensplunk.ListIndexFieldsRequest, error) {
	selector, err := sanitizedIndexSelector(request.GetSelector())
	if err != nil {
		return request, err
	}
	request.Selector = selector
	page := request.GetPage()
	if page == nil || page.PageSize == nil {
		return request, nil
	}
	pageSize := page.GetPageSize()
	if pageSize == 0 || pageSize > handler.maximumPageSize {
		return request, badRequestError(
			"index field page size is outside the supported range",
		)
	}
	// page_size is a requested maximum. The field service may enforce a lower
	// endpoint-specific bound than the browser-wide limit.
	page.PageSize = new(min(pageSize, handler.maxIndexFieldPageSize))
	return request, nil
}

// sanitizeCreateIngestionTokenRequest mirrors sanitizeCreateIndexRequest: the
// idempotency envelope is unsupported and the definition is converted by
// tokenDefinitionFromProto.
func sanitizeCreateIngestionTokenRequest(
	_ context.Context,
	request *opensplunk.CreateIngestionTokenRequest,
) (*opensplunk.CreateIngestionTokenRequest, error) {
	if request.ClientRequestId != nil {
		return request, badRequestError(
			"client request idempotency is not supported",
		)
	}
	return request, nil
}

func sanitizeGetIngestionTokenRequest(
	_ context.Context,
	request *opensplunk.GetIngestionTokenRequest,
) (*opensplunk.GetIngestionTokenRequest, error) {
	id, err := sanitizedIngestionTokenID(request.GetIngestionTokenId())
	if err != nil {
		return request, err
	}
	request.IngestionTokenId = id
	return request, nil
}

// sanitizeListIngestionTokensRequest is the token twin of
// sanitizeListIndexesRequest and additionally canonicalises the index-name
// filter.
func (handler *apiHandler) sanitizeListIngestionTokensRequest(
	_ context.Context,
	request *opensplunk.ListIngestionTokensRequest,
) (*opensplunk.ListIngestionTokensRequest, error) {
	if err := handler.sanitizeAdminListPage(
		request.GetPage(),
		maximumTokenRowsPerResponse,
	); err != nil {
		return request, err
	}
	if _, err := normalizeTokenStateFilters(
		request.GetStateFilters(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	if request.IndexNameFilter != nil {
		name, err := control.NormalizeIndexName(request.GetIndexNameFilter())
		if err != nil {
			return request, badRequestError("index name filter is invalid")
		}
		request.IndexNameFilter = &name
	}
	textFilter, err := sanitizedAdminTextFilter(request.TextFilter)
	if err != nil {
		return request, err
	}
	request.TextFilter = textFilter
	if _, _, err := normalizeTokenSort(
		request.GetSortBy(),
		request.GetSortDirection(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeUpdateIngestionTokenRequest(
	_ context.Context,
	request *opensplunk.UpdateIngestionTokenRequest,
) (*opensplunk.UpdateIngestionTokenRequest, error) {
	id, err := sanitizedIngestionTokenID(request.GetIngestionTokenId())
	if err != nil {
		return request, err
	}
	request.IngestionTokenId = id
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

func sanitizeSetIngestionTokenEnabledRequest(
	_ context.Context,
	request *opensplunk.SetIngestionTokenEnabledRequest,
) (*opensplunk.SetIngestionTokenEnabledRequest, error) {
	id, err := sanitizedIngestionTokenID(request.GetIngestionTokenId())
	if err != nil {
		return request, err
	}
	request.IngestionTokenId = id
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	return request, nil
}

// sanitizeRevokeIngestionTokenRequest rejects the revocation reason, which this
// API version accepts on the wire but does not persist.
func sanitizeRevokeIngestionTokenRequest(
	_ context.Context,
	request *opensplunk.RevokeIngestionTokenRequest,
) (*opensplunk.RevokeIngestionTokenRequest, error) {
	id, err := sanitizedIngestionTokenID(request.GetIngestionTokenId())
	if err != nil {
		return request, err
	}
	request.IngestionTokenId = id
	if err := administrationExpectedVersion(
		request.GetExpectedVersion(),
	); err != nil {
		return request, badRequestError(err.Error())
	}
	if request.Reason != nil {
		return request, badRequestError(
			"revocation reasons are not persisted by this API version",
		)
	}
	return request, nil
}

// sanitizedIndexSelector requires exactly one populated selector arm and returns
// it in canonical form: an object ID is trimmed, an index name is lowercased and
// trimmed. Every rejection carries the message resolveIndex used to produce by
// wrapping control.ErrInvalidArgument.
func sanitizedIndexSelector(
	selector *opensplunk.IndexSelector,
) (*opensplunk.IndexSelector, error) {
	switch selected := selector.GetSelector().(type) {
	case *opensplunk.IndexSelector_IndexId:
		id, err := adminObjectID(selected.IndexId, "index ID")
		if err != nil {
			return nil, badRequestError("index request is invalid")
		}
		return &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexId{IndexId: id},
		}, nil
	case *opensplunk.IndexSelector_IndexName:
		name, err := control.NormalizeIndexName(selected.IndexName)
		if err != nil {
			return nil, badRequestError("index request is invalid")
		}
		return &opensplunk.IndexSelector{
			Selector: &opensplunk.IndexSelector_IndexName{IndexName: name},
		}, nil
	default:
		return nil, badRequestError("index request is invalid")
	}
}

// sanitizedIngestionTokenID trims the identifier every ingestion-token route
// addresses a token with and rejects one that is empty, oversized, or carries
// control characters.
func sanitizedIngestionTokenID(value string) (string, error) {
	id, err := adminObjectID(value, "ingestion token ID")
	if err != nil {
		return "", badRequestError(err.Error())
	}
	return id, nil
}

// sanitizedAdminTextFilter trims an optional administrative free-text filter and
// rejects one that is unprintable or exceeds the filter bound.
func sanitizedAdminTextFilter(filter *string) (*string, error) {
	if filter == nil {
		return nil, nil
	}
	value, err := normalizeAdminFilter(filter)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	return &value, nil
}

// sanitizeAdminListPage only rejects: it leaves the page envelope untouched
// rather than writing the resolved size and token back, because the index-list
// handler unit tests (index_list_control_page_test.go, index_list_stats_api_test.go)
// call listIndexes directly with a raw envelope and require the handler to keep
// resolving it through adminPageProjection. The same reason keeps the state
// filters and the sort pair validate-only in both administrative list
// sanitizers.
func (handler *apiHandler) sanitizeAdminListPage(
	page *opensplunk.PageRequest,
	endpointMaximum int,
) error {
	if _, _, _, err := handler.adminPageRequest(page, endpointMaximum); err != nil {
		return badRequestError(err.Error())
	}
	return nil
}
