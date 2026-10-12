package common_persistence

import (
	"context"
	json "encoding/json/v2"
	"fmt"
	"strings"

	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/lib/pq"
)

// GetInferenceRoutingPreviewSnapshots loads all affected policy/session orgs in
// one statement. A provider-policy preview includes descendants; a platform edit
// (org ID zero) includes every active org with saved policy or active sessions.
// The session limit is global. Counts are explicitly incomplete when truncated
// or when a legacy session has no operation/model pin; no organization is omitted
// merely because it was absent from the bounded session sample.
func (s *ControlPlaneStore) GetInferenceRoutingPreviewSnapshots(ctx context.Context, organizationID int64, operations []int32, sessionLimit int) ([]cpm.InferenceRoutingPreviewSnapshot, error) {
	if organizationID < 0 || sessionLimit <= 0 || sessionLimit == int(^uint(0)>>1) {
		return nil, ErrInferenceRoutingInvalidPolicy
	}
	const prefix = `WITH input AS (SELECT ?::bigint AS organization_id, ?::integer[] AS operations, ?::bigint AS session_limit, ?::smallint AS active_status),
 root_org AS (SELECT id FROM gocommon_organizations, input WHERE id = input.organization_id AND deleted_at IS NULL),
 preview_orgs AS (SELECT o.id FROM gocommon_organizations o, input WHERE o.deleted_at IS NULL
   AND (input.organization_id = 0 OR EXISTS (SELECT 1 FROM root_org) AND (o.id = input.organization_id OR o.lineage @> ARRAY[input.organization_id]::bigint[]))
   AND (o.id = input.organization_id
     OR EXISTS (SELECT 1 FROM gocommon_organization_inference_policies p WHERE p.organization_id = o.id)
     OR EXISTS (SELECT 1 FROM gocommon_organization_inference_operation_policies p WHERE p.organization_id = o.id AND p.operation = ANY(input.operations))
     OR EXISTS (SELECT 1 FROM gocommon_chat_agent_sessions s WHERE s.organization_id = o.id AND s.status = input.active_status))),
 session_probe AS (SELECT s.id, s.organization_id, s.inference_operation, s.execution_inference_model_id, s.selection_source, s.execution_inference_api, (s.site_override_user_id IS NOT NULL) AS site_override, s.member_choice_selection
   FROM gocommon_chat_agent_sessions s, input WHERE s.status = input.active_status AND s.organization_id IN (SELECT id FROM preview_orgs)
     AND (s.inference_operation = 0 OR s.inference_operation = ANY(input.operations))
   ORDER BY s.id LIMIT (SELECT session_limit + 1 FROM input)),
 sampled AS (SELECT * FROM session_probe ORDER BY id LIMIT (SELECT session_limit FROM input)),
 completeness AS (SELECT count(*) <= (SELECT session_limit FROM input)
   AND count(*) FILTER (WHERE inference_operation = 0 OR execution_inference_model_id IS NULL OR execution_inference_api IS NULL) = 0 AS complete FROM session_probe),
 counts AS (SELECT organization_id, inference_operation AS operation, execution_inference_model_id AS inference_model_id, selection_source, execution_inference_api AS inference_api, site_override, member_choice_selection, count(*) AS count
   FROM sampled WHERE inference_operation > 0 AND execution_inference_model_id IS NOT NULL GROUP BY organization_id, inference_operation, execution_inference_model_id, selection_source, execution_inference_api, site_override, member_choice_selection)
 SELECT EXISTS (SELECT 1 FROM root_org) AS root_found, COALESCE(jsonb_agg(jsonb_build_object(
   'organization_id', preview_org.id, 'snapshot', snapshot.data,
   'session_counts', (SELECT COALESCE(jsonb_agg(to_jsonb(grouped) ORDER BY operation, inference_model_id, selection_source, inference_api, site_override, member_choice_selection), '[]'::jsonb)
     FROM (SELECT operation, inference_model_id, selection_source, inference_api, site_override, member_choice_selection, count, (SELECT complete FROM completeness) AS complete FROM counts WHERE organization_id = preview_org.id
       UNION ALL SELECT unnest(input.operations) AS operation, 0::bigint AS inference_model_id, 0::smallint AS selection_source, 0::smallint AS inference_api, false AS site_override, false AS member_choice_selection, 0::bigint AS count, (SELECT complete FROM completeness) AS complete) grouped)
 ) ORDER BY preview_org.id), '[]'::jsonb) AS data FROM preview_orgs preview_org CROSS JOIN input
 CROSS JOIN LATERAL (`
	// Reuse the exact same snapshot query as dispatch/discovery. Only trusted
	// constant SQL expressions replace its seven parameter positions.
	fragments := strings.Split(inferenceRoutingSnapshotQuery, "?")
	bindings := []string{
		"preview_org.id", "input.operations", "preview_org.id", "input.operations", "preview_org.id", "input.operations",
		"(SELECT COALESCE(array_agg(DISTINCT execution_inference_model_id) FILTER (WHERE execution_inference_model_id IS NOT NULL), ARRAY[]::bigint[]) FROM sampled WHERE organization_id = preview_org.id)",
	}
	var query strings.Builder
	query.WriteString(prefix)
	for position, fragment := range fragments {
		query.WriteString(fragment)
		if position < len(bindings) {
			query.WriteString(bindings[position])
		}
	}
	query.WriteString(") AS snapshot(data)")
	var rootFound bool
	var encoded []byte
	if err := s.routingSession(ctx).Raw(query.String(), organizationID, pq.Array(operations), sessionLimit, cpm.ChatAgentSessionStatusActive).Row().Scan(&rootFound, &encoded); err != nil {
		return nil, fmt.Errorf("read inference routing preview snapshots: %w", err)
	}
	if organizationID > 0 && !rootFound {
		return nil, ErrInferenceRoutingOrganizationNotFound
	}
	var snapshots []cpm.InferenceRoutingPreviewSnapshot
	if err := json.Unmarshal(encoded, &snapshots, inferenceRoutingJSONOptions); err != nil {
		return nil, fmt.Errorf("decode inference routing preview snapshots: %w", err)
	}
	return snapshots, nil
}
