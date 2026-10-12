package agentkit

import "github.com/Suhaibinator/go-common/pkg/proto_models"

// EffectiveBoundedLimit resolves a model-supplied limit against a tool's default
// and maximum: a zero value uses defaultValue (itself clamped to maxValue so a
// misconfigured default cannot exceed the max), and any explicit value above
// maxValue is clamped to maxValue. It guards tool fanout so a model cannot request
// an unbounded page while still honoring an explicit smaller request.
func EffectiveBoundedLimit(value uint32, defaultValue uint32, maxValue uint32) uint32 {
	if value == 0 {
		return min(defaultValue, maxValue)
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

// EffectivePage normalizes a model-supplied page number: page 0 (unset) becomes
// the first page.
func EffectivePage(page uint32) uint32 {
	if page == 0 {
		return 1
	}
	return page
}

// PaginationRequest builds a proto pagination request for a tool read. Page 0 is
// normalized to 1; the limit is passed through unchanged (bound it with
// EffectiveBoundedLimit first).
func PaginationRequest(page uint32, limit uint32, includeTotal bool) *proto_models.PaginationRequest {
	return &proto_models.PaginationRequest{
		Page:              EffectivePage(page),
		Limit:             limit,
		IncludeTotalCount: includeTotal,
	}
}

// PaginationPayload is a JSON-friendly projection of a proto PaginationResponse for
// embedding in a tool result. The JSON tags are a renderer/model-visible contract.
type PaginationPayload struct {
	CurrentPage        uint32 `json:"current_page"`
	TotalPages         uint32 `json:"total_pages"`
	TotalItems         uint64 `json:"total_items"`
	ItemsPerPage       uint32 `json:"items_per_page"`
	HasNextPage        bool   `json:"has_next_page"`
	HasPrevPage        bool   `json:"has_prev_page"`
	TotalCountIncluded bool   `json:"total_count_included"`
}

// NewPaginationPayload projects a proto PaginationResponse into a PaginationPayload.
// A nil response yields nil so a tool result can omit pagination entirely.
func NewPaginationPayload(pagination *proto_models.PaginationResponse) *PaginationPayload {
	if pagination == nil {
		return nil
	}
	return &PaginationPayload{
		CurrentPage:        pagination.GetCurrentPage(),
		TotalPages:         pagination.GetTotalPages(),
		TotalItems:         pagination.GetTotalItems(),
		ItemsPerPage:       pagination.GetItemsPerPage(),
		HasNextPage:        pagination.GetHasNextPage(),
		HasPrevPage:        pagination.GetHasPrevPage(),
		TotalCountIncluded: pagination.GetTotalCountIncluded(),
	}
}
