package server

import (
	"context"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// sanitizeSetSavedSearchScheduleRequest requires the whole identity the route
// addresses - a bounded saved-search ID, a nonzero expected saved-search
// version and a schedule body - and keeps the two schedule versions consistent:
// a schedule that names its own config version must name the one the caller
// expects, so the handler never reconciles them against the store.
func sanitizeSetSavedSearchScheduleRequest(ctx context.Context, request *opensplunk.SetSavedSearchScheduleRequest) (*opensplunk.SetSavedSearchScheduleRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil || request.GetExpectedVersion() == 0 || request.GetSchedule() == nil {
		return request, badRequestError("saved search ID, expected version, and schedule are required")
	}
	request.SavedSearchId = id
	schedule := request.GetSchedule()
	if schedule.GetConfigVersion() != 0 && schedule.GetConfigVersion() != request.GetExpectedScheduleVersion() {
		return request, badRequestError("schedule config version must match expected schedule version")
	}
	return request, nil
}

func sanitizeRunSavedSearchRequest(ctx context.Context, request *opensplunk.RunSavedSearchRequest) (*opensplunk.RunSavedSearchRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	return request, nil
}

func (handler *apiHandler) sanitizeListScheduledSearchRunsRequest(ctx context.Context, request *opensplunk.ListScheduledSearchRunsRequest) (*opensplunk.ListScheduledSearchRunsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	id, err := savedSearchID(request.GetSavedSearchId())
	if err != nil {
		return request, badRequestError(err.Error())
	}
	request.SavedSearchId = id
	page, err := handler.sanitizedAlertFamilyPage(
		request.GetPage(), "scheduled run", scheduledReportRunPageSize, scheduledReportRunPageSize,
	)
	if err != nil {
		return request, err
	}
	request.Page = page
	return request, nil
}

func sanitizeValidateScheduleRequest(ctx context.Context, request *opensplunk.ValidateScheduleRequest) (*opensplunk.ValidateScheduleRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if _, err := scheduleValidationMode(request.GetMode()); err != nil {
		return request, err
	}
	return request, nil
}
