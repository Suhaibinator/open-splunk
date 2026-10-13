package common_util

import (
	"math"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// UnknownTotalCount marks an intentionally skipped exact count in internal return values.
// It must not be exposed through PaginationResponse.TotalItems.
const UnknownTotalCount int64 = -1

// CreatePaginationResponse builds count-included page metadata. limit is the
// page size the caller fetched (already defaulted and bounded from configuration
// by the request sanitizer) and is reported as-is; a zero limit reports no pages.
func CreatePaginationResponse(totalItems int64, limit uint32, page uint32) *proto_models.PaginationResponse {
	if totalItems < 0 {
		return CreatePaginationResponseWithoutTotalCount(limit, page, false)
	}

	if page <= 0 {
		page = 1 // Default page
	}

	var totalPages uint32
	if totalItems > 0 && limit > 0 {
		totalPages = uint32(math.Ceil(float64(totalItems) / float64(limit)))
	}

	return &proto_models.PaginationResponse{
		CurrentPage:        page,
		TotalPages:         totalPages,
		TotalItems:         uint64(totalItems),
		ItemsPerPage:       limit,
		HasNextPage:        page < totalPages,
		HasPrevPage:        page > 1,
		TotalCountIncluded: true,
	}
}

// ShouldIncludeTotalCount returns whether a list request opted into exact count metadata.
func ShouldIncludeTotalCount(pagination *proto_models.PaginationRequest) bool {
	return pagination != nil && pagination.GetIncludeTotalCount()
}

// CreatePaginationResponseWithoutTotalCount builds page metadata when exact totals were skipped.
// limit is reported as-is (see CreatePaginationResponse).
func CreatePaginationResponseWithoutTotalCount(limit uint32, page uint32, hasNextPage bool) *proto_models.PaginationResponse {
	if page <= 0 {
		page = 1
	}

	return &proto_models.PaginationResponse{
		CurrentPage:        page,
		ItemsPerPage:       limit,
		HasNextPage:        hasNextPage,
		HasPrevPage:        page > 1,
		TotalCountIncluded: false,
	}
}

// PaginationQueryLimit returns the row count to fetch for a page of limit rows:
// limit itself, plus one probe row when exact totals are skipped. The caller owns
// the bound; limit is used as given.
func PaginationQueryLimit(limit uint32, includeTotalCount bool) int {
	queryLimit := int(limit)
	if !includeTotalCount {
		queryLimit = QueryLimitWithProbe(queryLimit)
	}
	return queryLimit
}

// PaginationOffset calculates a page-based database offset without overflowing
// uint32 during multiplication or int during conversion. Values too large for
// the current platform are saturated at the largest representable int.
func PaginationOffset(page, limit uint32) int {
	if page <= 1 || limit == 0 {
		return 0
	}

	offset := uint64(page-1) * uint64(limit)
	maxInt := uint64(^uint(0) >> 1)
	if offset > maxInt {
		return int(maxInt)
	}
	return int(offset)
}

// QueryLimitWithProbe safely adds the extra row used to detect another page.
func QueryLimitWithProbe(limit int) int {
	if limit <= 0 {
		return 1
	}
	maxInt := int(^uint(0) >> 1)
	if limit >= maxInt {
		return maxInt
	}
	return limit + 1
}

// TrimPaginationProbe drops the probe row fetched by PaginationQueryLimit and
// reports whether it was present (another page exists).
func TrimPaginationProbe[T any](items []T, limit uint32, includeTotalCount bool) ([]T, bool) {
	if includeTotalCount || uint32(len(items)) <= limit {
		return items, false
	}
	return items[:limit], true
}
