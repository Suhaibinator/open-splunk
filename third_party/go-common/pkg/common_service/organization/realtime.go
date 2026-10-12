package organization

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	serviceerrors "github.com/Suhaibinator/go-common/pkg/common_service/errors"
	"github.com/Suhaibinator/go-common/pkg/common_ws"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// ErrNilOrganizationMembershipReader indicates that a membership topic
// authorizer was constructed without an organization membership authority.
var ErrNilOrganizationMembershipReader = errors.New("nil organization membership reader")

// OrganizationMembershipReader is the narrow organization-service dependency
// required to authorize an entity topic by active organization membership.
// OrganizationHandler implements this interface directly.
type OrganizationMembershipReader interface {
	GetUserRoleInOrganization(
		ctx context.Context,
		userID common_service_models.UserIdType,
		organizationID common_service_models.OrganizationIdType,
	) (proto_models.OrganizationRole, bool, error)
}

// NewMembershipTopicAuthorizer returns a realtime topic authorizer for topics
// in the form "{namespace}.{organizationID}". A subscription is allowed only
// when the authenticated user is a member of the active organization. The
// role itself is deliberately ignored; applications needing a role threshold
// should register their own policy-specific authorizer.
func NewMembershipTopicAuthorizer[U common_service_models.UserLike](
	namespace string,
	memberships OrganizationMembershipReader,
) (common_ws.TopicAuthorizer[U], error) {
	if memberships == nil {
		return nil, ErrNilOrganizationMembershipReader
	}
	if _, err := common_ws.BuildEntityTopic(namespace, 1); err != nil {
		return nil, err
	}

	return common_ws.TopicAuthorizerFunc[U](
		func(ctx context.Context, user U, topic string) error {
			if user.GetID() == 0 {
				return serviceerrors.ErrInsufficientPermissions
			}

			entityID, err := common_ws.ParseEntityTopic(namespace, topic)
			if err != nil || entityID > math.MaxInt64 {
				return common_ws.ErrInvalidTopic
			}

			_, member, err := memberships.GetUserRoleInOrganization(
				ctx,
				user.GetID(),
				common_service_models.OrganizationIdType(entityID),
			)
			if err != nil {
				return fmt.Errorf("check active organization membership: %w", err)
			}
			if !member {
				return serviceerrors.ErrUserNotMember
			}
			return nil
		},
	), nil
}
