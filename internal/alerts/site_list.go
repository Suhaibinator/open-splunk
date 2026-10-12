package alerts

import (
	"context"
	"fmt"
	"strings"
)

// ListPageRequest is the site administrator's bounded catalog page.
type ListPageRequest struct {
	Limit        int
	AfterAlertID string
	Application  string
	Text         string
	IncludeTotal bool
}
type ListPage struct {
	Alerts      []AlertSummary
	NextAlertID string
	TotalSize   *uint64
}

type pagedRepository interface {
	ListPage(context.Context, string, ListPageRequest) (ListPage, error)
}

func (service *Service) ListPage(ctx context.Context, scope AccessScope, request ListPageRequest) (ListPage, error) {
	scoped, err := WithAccessScope(ctx, scope)
	if err != nil {
		return ListPage{}, err
	}
	if !scope.SiteAdministrator || request.Limit < 1 || request.Limit > MaximumAlertsPerOwner || len(request.AfterAlertID) > 128 || len(request.Application) > 128 || len(request.Text) > 1024 {
		return ListPage{}, ErrInvalidArgument
	}
	repository, ok := service.repository.(pagedRepository)
	if !ok {
		return ListPage{}, fmt.Errorf("%w: native catalog pagination unavailable", ErrInvalidArgument)
	}
	request.Application = strings.TrimSpace(request.Application)
	request.Text = strings.ToLower(strings.TrimSpace(request.Text))
	return repository.ListPage(scoped, scope.OwnerID, request)
}
