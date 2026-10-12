package ports

import (
	"context"
	"errors"
	"maps"
	"reflect"

	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
)

// MaxUserLookupBatchSize bounds the distinct IDs passed to an application callback.
const MaxUserLookupBatchSize = 64

// ErrInvalidUserLookupResult indicates an application user callback contract
// violation from UserLookup, UserLookupMany, or OnUserCreated.
var ErrInvalidUserLookupResult = errors.New("invalid application user callback result")

// LookupUsers deduplicates and bounds application lookups. Results are scoped to
// this call; missing users are omitted and no scalar fallback is permitted.
func (c *UserCallbackConfig[U]) LookupUsers(ctx context.Context, ids []sm.UserIdType) (map[sm.UserIdType]U, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	unique := make([]sm.UserIdType, 0, len(ids))
	seen := make(map[sm.UserIdType]bool, len(ids))
	for _, id := range ids {
		if !seen[id] {
			seen[id] = true
			unique = append(unique, id)
		}
	}
	result := make(map[sm.UserIdType]U, len(unique))
	for start := 0; start < len(unique); start += MaxUserLookupBatchSize {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		end := min(start+MaxUserLookupBatchSize, len(unique))
		users, err := c.UserLookupMany(ctx, append([]sm.UserIdType(nil), unique[start:end]...))
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		maps.Copy(result, users)
	}
	return result, nil
}

// validateUserLookupMany enforces the UserLookupManyFunc result contract so
// callers can invoke methods on every returned user.
func validateUserLookupMany[U sm.UserLike](ids []sm.UserIdType, users map[sm.UserIdType]U) error {
	requested := make(map[sm.UserIdType]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}
	for id, user := range users {
		if !requested[id] || isNilUser(user) || user.GetID() != id {
			return ErrInvalidUserLookupResult
		}
	}
	return nil
}

// isNilUser reports a nil interface or a typed nil. UserLike carries no nil
// predicate and U need not be comparable, so reflection is the only general
// test; a typed-nil pointer would otherwise panic in GetID's receiver.
func isNilUser[U sm.UserLike](user U) bool {
	v := reflect.ValueOf(user)
	switch v.Kind() {
	case reflect.Invalid:
		return true
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}
