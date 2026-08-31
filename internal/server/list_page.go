package server

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"fortio.org/safecast"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

const maximumBoundedListRequestTokenBytes = 2 << 10

type boundedListPageMetadata struct {
	itemCount     int
	nextPageToken string
	totalSize     *uint64
	totalExact    bool
}

func boundedListPageResponse(
	serviceName string,
	result boundedListPageMetadata,
	pageSize int,
	requestToken string,
	includeTotal bool,
	maximumTokenBytes int,
) (*opensplunk.PageResponse, error) {
	if result.itemCount < 0 {
		return nil, errors.New(
			serviceName + " service returned an invalid item count",
		)
	}

	itemCount := safecast.MustConv[uint64](result.itemCount)
	page := &opensplunk.PageResponse{}
	if result.nextPageToken != "" {
		if !validBoundedListPageToken(
			result.nextPageToken,
			maximumTokenBytes,
			false,
		) ||
			result.nextPageToken == requestToken ||
			result.itemCount != pageSize {
			return nil, errors.New(
				serviceName + " service returned an invalid page token",
			)
		}
		page.NextPageToken = new(
			strings.Clone(result.nextPageToken),
		)
	}
	if includeTotal {
		if result.totalSize == nil ||
			!result.totalExact ||
			*result.totalSize < itemCount {
			return nil, errors.New(
				serviceName + " service returned an invalid total",
			)
		}
		if result.nextPageToken != "" &&
			*result.totalSize <= itemCount {
			return nil, errors.New(
				serviceName +
					" service returned an invalid total for a continued page",
			)
		}
		if requestToken == "" &&
			result.nextPageToken == "" &&
			*result.totalSize != itemCount {
			return nil, errors.New(
				serviceName +
					" service returned an invalid first-page total",
			)
		}
		page.TotalSize = new(*result.totalSize)
		page.TotalSizeExact = true
	} else if result.totalSize != nil || result.totalExact {
		return nil, errors.New(
			serviceName + " service returned an unexpected total",
		)
	}
	return page, nil
}

// boundedListPageRequest applies the shared bounded paging predicate: the
// per-service maximum, the default page size, and bounded page-token
// validation. Unknown fields are tolerated, as everywhere else on a served
// route. The noun prefixes every error message so each endpoint keeps its own
// wording.
func (handler *apiHandler) boundedListPageRequest(
	page *opensplunk.PageRequest,
	noun string,
	defaultPageSize uint32,
	serviceMaximum uint32,
) (uint32, string, bool, error) {
	maximumPageSize := min(handler.maximumPageSize, serviceMaximum)
	pageSize := min(defaultPageSize, maximumPageSize)
	if page == nil {
		return pageSize, "", false, nil
	}
	includeTotal := page.GetIncludeTotalSize()
	if page.PageSize != nil {
		pageSize = page.GetPageSize()
		if pageSize == 0 || pageSize > maximumPageSize {
			return 0, "", false, badRequestError(noun + " page size is invalid")
		}
	}
	pageToken := ""
	if page.PageToken != nil {
		pageToken = page.GetPageToken()
		if !validBoundedListPageToken(pageToken, maximumBoundedListRequestTokenBytes, false) {
			return 0, "", false, badRequestError(
				noun + " page token is invalid",
			)
		}
	}
	return pageSize, pageToken, includeTotal, nil
}

// sanitizedListPage resolves a bounded list page envelope through
// boundedListPageRequest and writes the effective size, token and total flag
// back into it, so every paged route hands its handler a page it can read
// straight off the request instead of re-deriving the default, the maximum and
// the token bound. The returned pointer is the one the request now carries and
// the result is stable under a second sanitize.
func (handler *apiHandler) sanitizedListPage(
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
	return resolvedListPage(page, pageSize, pageToken, includeTotal), nil
}

// resolvedListPage writes an already-resolved envelope back into page in place,
// allocating a PageRequest only when the request omitted one. The routes whose
// page bounds are not boundedListPageRequest's - the search job, export, saved
// search and search history lists - resolve with their own helper and land the
// result here. A zero pageSize leaves page_size absent rather than writing a
// zero the services would reject: that is the getSearchResults contract, where
// an omitted size still means "the service default".
func resolvedListPage(
	page *opensplunk.PageRequest,
	pageSize uint32,
	pageToken string,
	includeTotal bool,
) *opensplunk.PageRequest {
	if page == nil {
		page = &opensplunk.PageRequest{}
	}
	page.PageSize = nil
	if pageSize != 0 {
		page.PageSize = new(pageSize)
	}
	page.PageToken = nil
	if pageToken != "" {
		page.PageToken = new(pageToken)
	}
	page.IncludeTotalSize = includeTotal
	return page
}

func validBoundedListPageToken(
	token string,
	maximumBytes int,
	allowEmpty bool,
) bool {
	if token == "" {
		return allowEmpty
	}
	if len(token) > maximumBytes ||
		!utf8.ValidString(token) ||
		strings.TrimSpace(token) != token {
		return false
	}
	for _, character := range token {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
