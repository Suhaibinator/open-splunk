package alerts

import (
	"context"
	"strconv"
	"strings"
)

// AccessScope is supplied by freshly authenticated server policy, never by a
// browser request body. Site administrators may select an organization or all.
type AccessScope struct {
	OwnerID           string
	OrganizationID    string
	SiteAdministrator bool
}
type accessScopeKey struct{}

func NormalizeAccessScope(scope AccessScope) (AccessScope, error) {
	if (!scope.SiteAdministrator && scope.OwnerID == "") || len(scope.OwnerID) > maximumOwnerIDBytes || strings.TrimSpace(scope.OwnerID) != scope.OwnerID || strings.ContainsRune(scope.OwnerID, 0) {
		return AccessScope{}, ErrInvalidArgument
	}
	if scope.OrganizationID == "" {
		if scope.SiteAdministrator {
			return scope, nil
		}
		return AccessScope{}, ErrInvalidArgument
	}
	organization, err := strconv.ParseInt(scope.OrganizationID, 10, 64)
	if err != nil || organization <= 0 || strconv.FormatInt(organization, 10) != scope.OrganizationID {
		return AccessScope{}, ErrInvalidArgument
	}
	return scope, nil
}
func WithAccessScope(ctx context.Context, scope AccessScope) (context.Context, error) {
	if ctx == nil {
		return nil, ErrInvalidArgument
	}
	scope, err := NormalizeAccessScope(scope)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, accessScopeKey{}, scope), nil
}
func AccessScopeFromContext(ctx context.Context) (AccessScope, bool) {
	if ctx == nil {
		return AccessScope{}, false
	}
	scope, ok := ctx.Value(accessScopeKey{}).(AccessScope)
	if !ok {
		return AccessScope{}, false
	}
	normalized, err := NormalizeAccessScope(scope)
	return normalized, err == nil
}
