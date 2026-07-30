package export

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// CanonicalFailure returns the stable, non-sensitive public representation of
// an export failure code. The manager and every transport projection share
// this definition so a safe wording or retryability change cannot make
// otherwise valid retained jobs unlistable.
func CanonicalFailure(code FailureCode) (Failure, bool) {
	switch code {
	case FailureRowLimit:
		return Failure{
			Code:    code,
			Message: "export exceeded its row limit",
		}, true
	case FailureByteLimit:
		return Failure{
			Code:    code,
			Message: "export exceeded its byte limit",
		}, true
	case FailureSourceUnavailable:
		return Failure{
			Code:    code,
			Message: "export source results became unavailable",
		}, true
	case FailureStorageUnavailable:
		return Failure{
			Code:      code,
			Message:   "export artifact storage is unavailable",
			Retryable: true,
		}, true
	case FailureInternal:
		return Failure{
			Code:    code,
			Message: "export serialization failed",
		}, true
	default:
		return Failure{}, false
	}
}

// CanonicalArtifactMetadata returns the public filename and media type for one
// valid export format.
func CanonicalArtifactMetadata(
	id string,
	format Format,
) (fileName string, mediaType string, ok bool) {
	switch format {
	case FormatCSV:
		return id + ".csv", "text/csv; charset=utf-8", true
	case FormatJSONLines:
		return id + ".jsonl", "application/x-ndjson; charset=utf-8", true
	default:
		return "", "", false
	}
}

// ValidSearchJobID reports whether an exact search-job selector can be
// retained by an export and safely rebound to a list cursor.
func ValidSearchJobID(value string) bool {
	return validExportMetadataIdentifier(value, maximumSearchIDBytes)
}

// ValidPublicJob verifies the complete detached Job contract shared by the
// manager and its transports. It intentionally does not compare timestamps to
// wall-clock time: lifecycle evaluation belongs to the manager that produced
// the snapshot, and transport clocks may be skewed.
func ValidPublicJob(job Job) bool {
	if job.Version == 0 ||
		!validID(job.ID) ||
		!ValidSearchJobID(job.SearchJobID) ||
		job.RowLimit == 0 ||
		job.RowLimit > hardMaximumRowLimit ||
		job.ByteLimit == 0 ||
		job.ByteLimit > hardMaximumByteLimit ||
		job.Progress.RowsWritten > job.RowLimit ||
		job.Progress.BytesWritten > job.ByteLimit ||
		job.CreatedAt.IsZero() ||
		job.Progress.UpdatedAt.IsZero() ||
		job.Progress.UpdatedAt.Before(job.CreatedAt) ||
		!validPublicExportDefinition(job) ||
		!validPublicExportTimes(job) ||
		!validPublicExportFailure(job.State, job.Failure) {
		return false
	}

	switch job.State {
	case StateQueued:
		return job.Progress.RowsWritten == 0 &&
			job.Progress.BytesWritten == 0 &&
			job.StartedAt.IsZero() &&
			job.FinishedAt.IsZero() &&
			job.ExpiresAt.IsZero() &&
			job.Artifact == nil
	case StateRunning:
		return !job.StartedAt.IsZero() &&
			job.FinishedAt.IsZero() &&
			job.ExpiresAt.IsZero() &&
			job.Artifact == nil
	case StateCompleted:
		return !job.StartedAt.IsZero() &&
			!job.FinishedAt.IsZero() &&
			!job.ExpiresAt.IsZero() &&
			validPublicExportArtifact(job)
	case StateFailed, StateCanceled:
		return !job.FinishedAt.IsZero() &&
			!job.ExpiresAt.IsZero() &&
			job.Artifact == nil
	case StateExpired:
		return !job.FinishedAt.IsZero() &&
			!job.ExpiresAt.IsZero() &&
			job.Artifact == nil
	default:
		return false
	}
}

func validPublicExportDefinition(job Job) bool {
	if len(job.Columns) > maximumColumns {
		return false
	}
	columnBytes := 0
	seenColumns := make(map[string]struct{}, len(job.Columns))
	for _, column := range job.Columns {
		if len(column) > maximumColumnBytes-columnBytes ||
			!validPublicExportColumnName(column) {
			return false
		}
		if _, exists := seenColumns[column]; exists {
			return false
		}
		seenColumns[column] = struct{}{}
		columnBytes += len(column)
	}
	switch job.Format {
	case FormatCSV:
		return job.CSV.HeaderMode <= CSVHeaderNone &&
			job.JSONLines == (JSONLinesOptions{})
	case FormatJSONLines:
		return job.JSONLines.IntegerEncoding <= JSONIntegerString &&
			job.CSV == (CSVOptions{})
	default:
		return false
	}
}

func validPublicExportColumnName(value string) bool {
	return value != "" &&
		utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}

func validPublicExportTimes(job Job) bool {
	return (job.StartedAt.IsZero() ||
		!job.StartedAt.Before(job.CreatedAt) &&
			!job.Progress.UpdatedAt.Before(job.StartedAt)) &&
		(job.FinishedAt.IsZero() ||
			!job.FinishedAt.Before(job.CreatedAt) &&
				(job.StartedAt.IsZero() ||
					!job.FinishedAt.Before(job.StartedAt)) &&
				!job.Progress.UpdatedAt.Before(job.FinishedAt)) &&
		(job.ExpiresAt.IsZero() ||
			!job.FinishedAt.IsZero() &&
				!job.ExpiresAt.Before(job.FinishedAt))
}

func validPublicExportFailure(state State, failure *Failure) bool {
	if failure == nil {
		return state != StateFailed
	}
	if state != StateFailed && state != StateExpired {
		return false
	}
	canonical, ok := CanonicalFailure(failure.Code)
	return ok && *failure == canonical
}

func validPublicExportArtifact(job Job) bool {
	if job.Artifact == nil ||
		job.Artifact.ExpiresAt.IsZero() ||
		job.Artifact.SizeBytes != job.Progress.BytesWritten ||
		job.Artifact.RowCount != job.Progress.RowsWritten ||
		!job.Artifact.ExpiresAt.Equal(job.ExpiresAt) {
		return false
	}
	fileName, mediaType, ok := CanonicalArtifactMetadata(job.ID, job.Format)
	return ok &&
		job.Artifact.FileName == fileName &&
		job.Artifact.MediaType == mediaType
}
