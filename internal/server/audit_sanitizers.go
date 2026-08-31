package server

import (
	"context"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/searchaudit"
)

// sanitizeListAuditEventsRequest states the whole audit list contract: the page
// envelope is within the ledger's bounds; every action filter maps to a known
// action and appears once; the actor filter is unpadded and bounded; and the
// target-kind filter, when present, names a known kind.
func (handler *apiHandler) sanitizeListAuditEventsRequest(
	ctx context.Context,
	request *opensplunk.ListAuditEventsRequest,
) (*opensplunk.ListAuditEventsRequest, error) {
	if _, err := discardUnknownProtoFields(ctx, request); err != nil {
		return request, err
	}
	page, err := handler.sanitizedAuditFamilyPage(
		request.GetPage(),
		"audit event",
		defaultAuditListPageSize,
		audit.MaximumListPageSize,
	)
	if err != nil {
		return request, err
	}
	request.Page = page
	if len(request.GetActionFilters()) > audit.MaximumActionFilters {
		return request, badRequestError(
			"audit event action filters are invalid",
		)
	}
	seenActions := make(
		map[audit.Action]struct{},
		len(request.GetActionFilters()),
	)
	for _, value := range request.GetActionFilters() {
		action, ok := auditActionFromProto(value)
		if !ok {
			return request, badRequestError(
				"audit event action filter is invalid",
			)
		}
		if _, duplicate := seenActions[action]; duplicate {
			return request, badRequestError(
				"audit event action filter is duplicated",
			)
		}
		seenActions[action] = struct{}{}
	}

	if request.ActorIdFilter != nil {
		value, err := sanitizedAuditIdentityFilter(
			request.GetActorIdFilter(),
			maximumAuditActorIDBytes,
			"audit event actor filter is invalid",
		)
		if err != nil {
			return request, err
		}
		request.ActorIdFilter = &value
	}

	if request.TargetKindFilter != nil {
		if _, ok := auditTargetKindFromProto(
			request.GetTargetKindFilter(),
		); !ok {
			return request, badRequestError(
				"audit event target filter is invalid",
			)
		}
	}
	return request, nil
}

// sanitizeListSearchAttemptAuditEventsRequest mirrors
// sanitizeListAuditEventsRequest for the search-attempt ledger, which filters on
// identities rather than actions.
func (handler *apiHandler) sanitizeListSearchAttemptAuditEventsRequest(
	ctx context.Context,
	request *opensplunk.ListSearchAttemptAuditEventsRequest,
) (*opensplunk.ListSearchAttemptAuditEventsRequest, error) {
	if _, err := discardUnknownProtoFields(ctx, request); err != nil {
		return request, err
	}
	page, err := handler.sanitizedAuditFamilyPage(
		request.GetPage(),
		"search attempt audit",
		defaultSearchAttemptAuditListPageSize,
		searchaudit.MaximumListPageSize,
	)
	if err != nil {
		return request, err
	}
	request.Page = page
	if request.ActorIdFilter != nil {
		value, err := sanitizedAuditIdentityFilter(
			request.GetActorIdFilter(),
			maximumSearchAttemptAuditIdentityBytes,
			"search attempt audit actor filter is invalid",
		)
		if err != nil {
			return request, err
		}
		request.ActorIdFilter = &value
	}
	if request.OwnerIdFilter != nil {
		value, err := sanitizedAuditIdentityFilter(
			request.GetOwnerIdFilter(),
			maximumSearchAttemptAuditIdentityBytes,
			"search attempt audit owner filter is invalid",
		)
		if err != nil {
			return request, err
		}
		request.OwnerIdFilter = &value
	}
	return request, nil
}

// sanitizedAuditIdentityFilter rejects any identity filter that is padded or
// unbounded and returns a copy that no longer aliases the decoded request body.
func sanitizedAuditIdentityFilter(
	value string,
	maximumBytes int,
	message string,
) (string, error) {
	if strings.TrimSpace(value) != value ||
		validateBoundedIdentifier(value, maximumBytes, false) != nil {
		return "", badRequestError(message)
	}
	return strings.Clone(value), nil
}

// sanitizedAuditFamilyPage rewrites a bounded ledger page envelope as the
// effective values the endpoint will use, so the handler reads page_size and
// page_token straight off the request instead of re-deriving the default, the
// maximum and the token bound.
func (handler *apiHandler) sanitizedAuditFamilyPage(
	page *opensplunk.PageRequest,
	noun string,
	defaultPageSize uint32,
	serviceMaximum uint32,
) (*opensplunk.PageRequest, error) {
	pageSize, pageToken, includeTotal, err := handler.boundedListPageRequest(
		page,
		noun,
		defaultPageSize,
		serviceMaximum,
	)
	if err != nil {
		return nil, err
	}
	resolved := &opensplunk.PageRequest{
		PageSize:         &pageSize,
		IncludeTotalSize: includeTotal,
	}
	if pageToken != "" {
		resolved.PageToken = &pageToken
	}
	return resolved, nil
}
