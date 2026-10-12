package common_persistence

import (
	"context"
	"fmt"

	"github.com/Suhaibinator/go-common/pkg/common_persistence/common_persistence_models"
)

// InferenceAttemptDetail is a bounded snapshot of one invocation's recorded history.
// RelatedAttempts excludes Attempt and is ordered by attempt number, then ID.
type InferenceAttemptDetail struct {
	Attempt                  *common_persistence_models.InferenceAttempt
	RelatedAttempts          []common_persistence_models.InferenceAttempt
	RelatedAttemptsTruncated bool
}

const relatedInferenceAttemptLimit = 50

// attemptDetailRow embeds the ledger row while projecting nullable live identities.
type attemptDetailRow struct {
	common_persistence_models.InferenceAttempt `gorm:"embedded"`
	JoinedActorID                              *uint64
	JoinedActorUsername                        string
	JoinedActorFirstName                       string
	JoinedActorLastName                        string
	JoinedActorIsBot                           bool
	JoinedOrganizationID                       *int64
	JoinedOrganizationName                     string
	JoinedOrganizationSlug                     string
}

// attemptDetailSQL selects IDs before joining full rows: the requested attempt is
// always present, with one extra related row used only to detect truncation. The
// invocation/attempt-number index bounds the history lookup.
var attemptDetailSQL = fmt.Sprintf(`
WITH selected AS (
  SELECT id, invocation_id FROM %[1]s WHERE id = ?
), related AS (
  SELECT a.id, a.attempt_number FROM %[1]s a
  JOIN selected s ON s.invocation_id <> '' AND a.invocation_id = s.invocation_id
  WHERE a.id <> s.id
  ORDER BY a.attempt_number, a.id LIMIT ?
), chosen AS (
  SELECT id FROM selected UNION ALL SELECT id FROM related
)
SELECT a.*,
  u.id AS joined_actor_id, u.username AS joined_actor_username,
  u.first_name AS joined_actor_first_name, u.last_name AS joined_actor_last_name,
  u.is_bot AS joined_actor_is_bot,
  o.id AS joined_organization_id, o.name AS joined_organization_name,
  o.slug AS joined_organization_slug
FROM chosen c
JOIN %[1]s a ON a.id = c.id
LEFT JOIN %[2]s u ON u.id = a.actor_user_id AND u.deleted_at IS NULL
LEFT JOIN %[3]s o ON o.id = a.organization_id AND o.deleted_at IS NULL
ORDER BY a.attempt_number, a.id`,
	common_persistence_models.TableNameInferenceAttempts,
	common_persistence_models.TableNameUsers,
	common_persistence_models.TableNameOrganizations,
)

// GetAttemptDetail loads the attempt, related history, and live attribution in
// exactly one SQL statement. It does not change list or legacy GetAttempt reads.
func (s *ControlPlaneStore) GetAttemptDetail(ctx context.Context, id uint64) (*InferenceAttemptDetail, error) {
	var rows []attemptDetailRow
	if err := s.session(ctx).Raw(attemptDetailSQL, id, relatedInferenceAttemptLimit+1).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("get inference attempt detail %d: %w", id, err)
	}
	result := &InferenceAttemptDetail{}
	for i := range rows {
		r := &rows[i]
		a := &r.InferenceAttempt
		if r.JoinedActorID != nil {
			a.Actor = &common_persistence_models.InferenceAttemptActor{
				ID: *r.JoinedActorID, Username: r.JoinedActorUsername,
				FirstName: r.JoinedActorFirstName, LastName: r.JoinedActorLastName, IsBot: r.JoinedActorIsBot,
			}
		}
		if r.JoinedOrganizationID != nil {
			a.Organization = &common_persistence_models.InferenceAttemptOrganization{
				ID: *r.JoinedOrganizationID, Name: r.JoinedOrganizationName, Slug: r.JoinedOrganizationSlug,
			}
		}
		if a.ID == id {
			result.Attempt = a
		} else if len(result.RelatedAttempts) < relatedInferenceAttemptLimit {
			result.RelatedAttempts = append(result.RelatedAttempts, *a)
		} else {
			result.RelatedAttemptsTruncated = true
		}
	}
	if result.Attempt == nil {
		return nil, ErrControlPlaneAttemptNotFound
	}
	return result, nil
}
