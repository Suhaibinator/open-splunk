package common_persistence

import (
	"context"
	"fmt"
	"strings"

	pm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/Suhaibinator/go-common/pkg/common_util"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// InferenceUsageDimension is one bounded dimension and its pagination metadata.
type InferenceUsageDimension struct {
	Rows []*InferenceUsageBreakdown
	Page *proto_models.PaginationResponse
}

// ListChatInferenceDimensionsForSessions computes the requested usage dimensions from one ledger
// scan. GROUPING SETS preserves numeric run ordering and durable session attribution; pagination
// happens after aggregation so the run/model dimensions never hydrate unbounded rows.
func (h *CommonPersistenceHandler[U]) ListChatInferenceDimensionsForSessions(ctx context.Context, ids []uint64, requested map[InferenceBreakdownDimension]*proto_models.PaginationRequest) (map[InferenceBreakdownDimension]*InferenceUsageDimension, error) {
	out := make(map[InferenceBreakdownDimension]*InferenceUsageDimension, len(requested))
	pages := make(map[InferenceBreakdownDimension]*proto_models.PaginationRequest, len(requested))
	var selected, filters []string
	args := []any{ids}
	for dim := InferenceBreakdownByRun; dim <= InferenceBreakdownBySession; dim++ {
		page, wanted := requested[dim]
		if !wanted {
			continue
		}
		selected = append(selected, fmt.Sprintf("%d", dim))
		out[dim] = &InferenceUsageDimension{Rows: []*InferenceUsageBreakdown{}}
		if page == nil {
			filters = append(filters, fmt.Sprintf("dimension = %d", dim))
			continue
		}
		// A paged dimension carries the sanitized page size; nil (above) is the
		// deliberate "unpaged" request for the small dimensions.
		if err := requirePageLimit("list_chat_inference_dimensions_for_sessions", page); err != nil {
			return nil, err
		}
		pages[dim] = page
		offset := common_util.PaginationOffset(page.Page, page.Limit)
		filters = append(filters, fmt.Sprintf("(dimension = %d AND row_index > ? AND row_index <= ?)", dim))
		args = append(args, offset, offset+common_util.PaginationQueryLimit(page.Limit, page.IncludeTotalCount))
		out[dim].Page = common_util.CreatePaginationResponseWithoutTotalCount(page.Limit, page.Page, false)
		if page.IncludeTotalCount {
			out[dim].Page = common_util.CreatePaginationResponse(0, page.Limit, page.Page)
		}
	}
	if len(selected) == 0 || len(ids) == 0 {
		return out, nil
	}
	// Each grouping set contains exactly one dimension. CASE uses GROUPING rather than nullable
	// values to distinguish a real NULL group from columns omitted by the grouping set.
	query := `WITH grouped AS MATERIALIZED (
 SELECT CASE WHEN GROUPING(agent_run_id)=0 THEN 1 WHEN GROUPING(purpose)=0 THEN 2
 WHEN GROUPING(provider_id)=0 THEN 3 WHEN GROUPING(provider_model_id)=0 THEN 4 ELSE 5 END AS dimension,
 agent_run_id, purpose, provider_id, provider_model_id, agent_session_id, ` + usageSelectColumns + `
 FROM ` + pm.TableNameChatInferenceCalls + ` WHERE agent_session_id IN ?
 GROUP BY GROUPING SETS ((agent_run_id),(purpose),(provider_id),(provider_model_id),(agent_session_id))
), ranked AS MATERIALIZED (
 SELECT *, CASE dimension WHEN 1 THEN agent_run_id::text WHEN 2 THEN purpose::text
 WHEN 3 THEN provider_id WHEN 4 THEN provider_model_id ELSE agent_session_id::text END AS group_key,
 ROW_NUMBER() OVER (PARTITION BY dimension ORDER BY agent_run_id, purpose, provider_id, provider_model_id, agent_session_id) AS row_index
 FROM grouped WHERE (dimension <> 1 OR agent_run_id IS NOT NULL) AND dimension IN (` + strings.Join(selected, ",") + `)
)
 SELECT dimension, group_key, call_count, input_tokens, output_tokens, total_tokens, thoughts_tokens,
 cached_content_tokens, row_index, 0::bigint AS total_count FROM ranked WHERE ` + strings.Join(filters, " OR ") + `
 UNION ALL SELECT dimension, NULL, 0,0,0,0,0,0,0,COUNT(*) FROM ranked GROUP BY dimension
 ORDER BY dimension, row_index`
	var rows []struct {
		Dimension                                                                              InferenceBreakdownDimension
		GroupKey                                                                               *string
		CallCount, InputTokens, OutputTokens, TotalTokens, ThoughtsTokens, CachedContentTokens int64
		RowIndex, TotalCount                                                                   int64
	}
	if err := h.getDBFromContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, chatPersistenceDatabaseError(err)
	}
	for _, row := range rows {
		result := out[row.Dimension]
		if row.RowIndex == 0 {
			if page := pages[row.Dimension]; page != nil && page.IncludeTotalCount {
				result.Page = common_util.CreatePaginationResponse(row.TotalCount, page.Limit, page.Page)
			}
			continue
		}
		b := &InferenceUsageBreakdown{CallCount: row.CallCount, InputTokens: row.InputTokens, OutputTokens: row.OutputTokens, TotalTokens: row.TotalTokens, ThoughtsTokens: row.ThoughtsTokens, CachedContentTokens: row.CachedContentTokens}
		row.Dimension.assign(b, row.GroupKey)
		result.Rows = append(result.Rows, b)
	}
	for dim, result := range out {
		if page := pages[dim]; page != nil && !page.IncludeTotalCount {
			var more bool
			result.Rows, more = common_util.TrimPaginationProbe(result.Rows, page.Limit, false)
			result.Page = common_util.CreatePaginationResponseWithoutTotalCount(page.Limit, page.Page, more)
		}
	}
	return out, nil
}
