// Package usertest holds shared user-lookup fixtures for tests. It must not be
// imported by production code. Package ports keeps its own copy because it
// cannot import this package without a cycle.
package usertest

import (
	"context"

	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/ports"
)

// LookupProvider adapts mock functions; production providers implement
// their scalar and batched persistence methods directly.
type LookupProvider[U common_service_models.UserLike] struct {
	Single ports.UserLookupFunc[U]
	Many   ports.UserLookupManyFunc[U]
}

// UserLookup delegates to Single.
func (p *LookupProvider[U]) UserLookup(ctx context.Context, id common_service_models.UserIdType) (U, error) {
	return p.Single(ctx, id)
}

// UserLookupMany delegates to Many.
func (p *LookupProvider[U]) UserLookupMany(ctx context.Context, ids []common_service_models.UserIdType) (map[common_service_models.UserIdType]U, error) {
	return p.Many(ctx, ids)
}

// BulkLookup adapts scalar mock fixtures; production adapters must use
// actual batched retrieval.
func BulkLookup[U common_service_models.UserLike](lookup ports.UserLookupFunc[U]) ports.UserLookupManyFunc[U] {
	return func(ctx context.Context, ids []common_service_models.UserIdType) (map[common_service_models.UserIdType]U, error) {
		users := make(map[common_service_models.UserIdType]U, len(ids))
		for _, id := range ids {
			user, err := lookup(ctx, id)
			if err != nil {
				return nil, err
			}
			users[id] = user
		}
		return users, nil
	}
}

// CallbackConfig builds a user callback config backed by a scalar lookup.
func CallbackConfig[U common_service_models.UserLike](lookup ports.UserLookupFunc[U]) *ports.UserCallbackConfig[U] {
	return ports.NewUserCallbackConfig[U](&LookupProvider[U]{Single: lookup, Many: BulkLookup(lookup)})
}
