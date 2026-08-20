// Package exportjobproto projects export-job metadata into the protobuf
// representation shared by HTTP and live-search APIs.
package exportjobproto

import (
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
)

// State projects an internal lifecycle state to its wire representation.
// Values unknown to this binary fail closed as UNSPECIFIED so callers can
// reject snapshots whose state cannot be represented safely.
func State(state exportjobs.State) opensplunk.ExportJobState {
	switch state {
	case exportjobs.StateQueued:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_QUEUED
	case exportjobs.StateRunning:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_RUNNING
	case exportjobs.StateCompleted:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_COMPLETED
	case exportjobs.StateFailed:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_FAILED
	case exportjobs.StateCanceled:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_CANCELED
	case exportjobs.StateExpired:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_EXPIRED
	default:
		return opensplunk.ExportJobState_EXPORT_JOB_STATE_UNSPECIFIED
	}
}

// FailureCode projects an internal failure code to its wire representation.
// Values unknown to this binary remain UNSPECIFIED for caller-side validation.
func FailureCode(code exportjobs.FailureCode) opensplunk.ExportFailureCode {
	switch code {
	case exportjobs.FailureRowLimit:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_ROW_LIMIT
	case exportjobs.FailureByteLimit:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_BYTE_LIMIT
	case exportjobs.FailureSourceUnavailable:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_SEARCH_UNAVAILABLE
	case exportjobs.FailureStorageUnavailable:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_STORAGE_UNAVAILABLE
	case exportjobs.FailureInternal:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_INTERNAL
	default:
		return opensplunk.ExportFailureCode_EXPORT_FAILURE_CODE_UNSPECIFIED
	}
}
