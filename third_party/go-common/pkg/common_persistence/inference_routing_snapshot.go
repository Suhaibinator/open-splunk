package common_persistence

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
	cpm "github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
	"github.com/lib/pq"
	"gorm.io/gorm"
)

var (
	ErrInferencePolicyVersionConflict       = errors.New("inference routing: policy version conflict")
	ErrInferenceRoutingOrganizationNotFound = errors.New("inference routing: organization not found")
	ErrInferenceRoutingInvalidPolicy        = errors.New("inference routing: invalid policy")
)

// Routing policy belongs to the product transaction, unlike attempt-ledger
// recording. A savepoint keeps a failed collection replacement atomic even when
// the caller chooses to continue using its ambient transaction.
func (s *ControlPlaneStore) routingSession(ctx context.Context) *gorm.DB {
	if tx, ok := scontext.GetTransaction(ctx); ok && tx != nil {
		return tx.GetDB().WithContext(ctx)
	}
	return s.session(ctx)
}

// GetInferenceRoutingSnapshot batches operations, their models and rate limits,
// and the work organization's policies in one PostgreSQL statement snapshot.
// Provider restrictions include every ancestor; model policy remains same-org.
// Extra model IDs cover pinned selections and site overrides outside the list.
// A zero org ID is reserved for platform-only administration and previews.
func (s *ControlPlaneStore) GetInferenceRoutingSnapshot(ctx context.Context, orgID int64, operations []int32, extraModelIDs []uint64) (*cpm.InferenceRoutingSnapshot, error) {
	if orgID < 0 {
		return nil, ErrInferenceRoutingInvalidPolicy
	}
	modelIDs := make([]int64, len(extraModelIDs))
	for position, id := range extraModelIDs {
		if !validBigintID(id) {
			return nil, ErrInferenceRoutingInvalidPolicy
		}
		modelIDs[position] = int64(id)
	}
	var encoded []byte
	err := s.routingSession(ctx).Raw(inferenceRoutingSnapshotQuery, orgID, pq.Array(operations), orgID, pq.Array(operations), orgID, pq.Array(operations), pq.Array(modelIDs)).Row().Scan(&encoded)
	if err != nil {
		return nil, fmt.Errorf("read inference routing snapshot: %w", err)
	}
	var snapshot cpm.InferenceRoutingSnapshot
	if err := json.Unmarshal(encoded, &snapshot, inferenceRoutingJSONOptions); err != nil {
		return nil, fmt.Errorf("decode inference routing snapshot: %w", err)
	}
	if orgID > 0 && len(snapshot.OrganizationIDs) == 0 {
		return nil, ErrInferenceRoutingOrganizationNotFound
	}
	return &snapshot, nil
}

// PostgreSQL stores durations as integer nanoseconds. JSON v2 deliberately has
// no implicit duration encoding, so decode that storage representation explicitly.
var inferenceRoutingJSONOptions = json.WithUnmarshalers(json.UnmarshalFunc(func(data []byte, duration *time.Duration) error {
	var nanos int64
	if err := json.Unmarshal(data, &nanos); err != nil {
		return err
	}
	*duration = time.Duration(nanos)
	return nil
}))

const inferenceRoutingSnapshotQuery = `WITH
 work_org AS (SELECT id, lineage FROM gocommon_organizations WHERE id = ? AND deleted_at IS NULL),
 org_ids AS (SELECT ancestor.id, ancestor.position FROM work_org,
   LATERAL unnest(lineage || ARRAY[work_org.id]::bigint[]) WITH ORDINALITY AS ancestor(id, position)),
 routes AS (SELECT * FROM gocommon_inference_operation_routes WHERE operation = ANY(?::integer[])),
 options AS (SELECT * FROM gocommon_inference_operation_model_options WHERE operation IN (SELECT operation FROM routes)),
 org_operations AS (SELECT * FROM gocommon_organization_inference_operation_policies
   WHERE organization_id = ? AND operation = ANY(?::integer[])),
 org_models AS (SELECT * FROM gocommon_organization_inference_model_options
   WHERE organization_id = ? AND operation = ANY(?::integer[])),
 model_ids AS (SELECT inference_model_id AS id FROM routes UNION SELECT inference_model_id FROM options
   UNION SELECT default_model_id FROM org_operations WHERE default_model_id IS NOT NULL
   UNION SELECT inference_model_id FROM org_models UNION SELECT unnest(?::bigint[]))
 SELECT jsonb_build_object(
   'routes', COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY operation) FROM routes r), '[]'::jsonb),
   'options', COALESCE((SELECT jsonb_agg(to_jsonb(o) ORDER BY operation, inference_model_id) FROM options o), '[]'::jsonb),
   'models', COALESCE((SELECT jsonb_agg(to_jsonb(m) ORDER BY id) FROM gocommon_inference_models m WHERE id IN (SELECT id FROM model_ids)), '[]'::jsonb),
   'rate_limits', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY inference_model_id) FROM gocommon_inference_model_rate_limit_policies p WHERE inference_model_id IN (SELECT id FROM model_ids)), '[]'::jsonb),
   'organization_policies', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY organization_id) FROM gocommon_organization_inference_policies p WHERE organization_id IN (SELECT id FROM org_ids)), '[]'::jsonb),
   'organization_operation_policies', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY operation) FROM org_operations p), '[]'::jsonb),
   'organization_model_options', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY operation, inference_model_id) FROM org_models p), '[]'::jsonb),
   'provider_allowlist', COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY organization_id, provider_id) FROM gocommon_organization_inference_providers p WHERE organization_id IN (SELECT id FROM org_ids)), '[]'::jsonb),
   'organization_ids', COALESCE((SELECT jsonb_agg(id ORDER BY position) FROM org_ids), '[]'::jsonb)
 )`
