package server

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
)

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
) (*opensplunkv1.PageResponse, error) {
	if result.itemCount < 0 {
		return nil, errors.New(
			serviceName + " service returned an invalid item count",
		)
	}
	// #nosec G115 -- the non-negative item count always comes from len() on an
	// in-memory, transport-bounded response slice.
	itemCount := uint64(result.itemCount)
	page := &opensplunkv1.PageResponse{}
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
// per-service maximum, the default page size, nested unknown-field rejection,
// and bounded page-token validation. The noun prefixes every error message so
// each endpoint keeps its own wording.
func (handler *apiHandler) boundedListPageRequest(
	page *opensplunkv1.PageRequest,
	noun string,
	defaultPageSize uint32,
	serviceMaximum uint32,
	maximumTokenBytes int,
) (uint32, string, bool, error) {
	maximumPageSize := min(handler.maximumPageSize, serviceMaximum)
	pageSize := min(defaultPageSize, maximumPageSize)
	if page == nil {
		return pageSize, "", false, nil
	}
	if len(page.ProtoReflect().GetUnknown()) != 0 {
		return 0, "", false, badRequestError(noun + " page request is invalid")
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
		if !validBoundedListPageToken(pageToken, maximumTokenBytes, false) {
			return 0, "", false, badRequestError(
				noun + " page token is invalid",
			)
		}
	}
	return pageSize, pageToken, includeTotal, nil
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
