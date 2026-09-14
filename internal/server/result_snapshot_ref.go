package server

import (
	"errors"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

const (
	resultSnapshotRefDomain  = "search-result-snapshot"
	resultSnapshotRefVersion = 1
	resultSnapshotRefBytes   = 1024
)

type resultSnapshotPayload struct {
	JobID      string `json:"job_id"`
	Generation uint64 `json:"generation"`
}

func (handler *apiHandler) resultSnapshotRef(jobID string, generation uint64) (string, error) {
	if jobID == "" || generation == 0 {
		return "", errors.New("result snapshot identity is invalid")
	}
	return cursorcodec.Encode(
		handler.searchArtifactCursorKey[:],
		resultSnapshotRefDomain,
		resultSnapshotRefVersion,
		resultSnapshotRefBytes,
		resultSnapshotPayload{JobID: jobID, Generation: generation},
	)
}

func (handler *apiHandler) parseResultSnapshotRef(jobID, ref string) (uint64, error) {
	var payload resultSnapshotPayload
	if jobID == "" || ref == "" || cursorcodec.Decode(
		handler.searchArtifactCursorKey[:],
		resultSnapshotRefDomain,
		resultSnapshotRefVersion,
		resultSnapshotRefBytes,
		ref,
		&payload,
	) != nil || payload.JobID != jobID || payload.Generation == 0 {
		return 0, searchjobs.ErrInvalidCursor
	}
	return payload.Generation, nil
}
