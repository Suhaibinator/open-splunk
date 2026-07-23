// Package searchjobproto projects retained search-job intent and provenance
// into the protobuf representation shared by live-search and history APIs.
package searchjobproto

import (
	"errors"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

// TimeRange preserves reusable time intent while returning its effective
// timezone for the separately resolved execution interval.
func TimeRange(job searchjobs.Job) (*opensplunkv1.TimeRangeSpec, string, error) {
	intent := job.TimeRange
	if intent == (searchtime.Intent{}) {
		earliest := job.Earliest.UTC().Format(time.RFC3339Nano)
		latest := job.Latest.UTC().Format(time.RFC3339Nano)
		timezone := "UTC"
		return &opensplunkv1.TimeRangeSpec{
			Earliest: &earliest,
			Latest:   &latest,
			Timezone: &timezone,
		}, timezone, nil
	}
	if err := searchtime.ValidateIntent(intent); err != nil {
		return nil, "", errors.New("invalid search-job time-range intent")
	}
	result := &opensplunkv1.TimeRangeSpec{
		Earliest: stringPointer(intent.Earliest),
		Latest:   stringPointer(intent.Latest),
	}
	if intent.TimezoneSpecified {
		result.Timezone = stringPointer(intent.Timezone)
	}
	return result, intent.Timezone, nil
}

// Source maps normalized internal provenance to its mutually exclusive wire
// representation. A zero value remains a compatibility alias for ad hoc.
func Source(source searchjobs.JobSource) (*opensplunkv1.SearchJobSource, error) {
	source, err := searchjobs.CanonicalJobSource(source)
	if err != nil {
		return nil, errors.New("invalid search-job source metadata")
	}
	result := &opensplunkv1.SearchJobSource{}
	switch source.Origin {
	case searchjobs.JobOriginAdHoc:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_AD_HOC
	case searchjobs.JobOriginSavedSearch:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_SAVED_SEARCH
		result.SavedSearchId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginHistoryRerun:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_HISTORY_RERUN
		result.HistorySearchId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginDashboard:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_DASHBOARD
		result.DashboardId = stringPointer(source.ObjectID)
	case searchjobs.JobOriginAPI:
		result.Origin = opensplunkv1.SearchJobOrigin_SEARCH_JOB_ORIGIN_API
	default:
		return nil, errors.New("invalid search-job source origin")
	}
	return result, nil
}

func stringPointer(value string) *string { return &value }
