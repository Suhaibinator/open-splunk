package patterns

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func (service *Service) encodeListCursor(access searchjobs.AccessScope, request ListRequest, offset int) (string, error) {
	return cursorcodec.Encode(service.cursorKey, listCursorDomain, cursorVersion, MaximumCursorBytes, listCursor{
		Version: cursorVersion, Scope: scopeDigest(access), JobID: request.SearchJobID,
		Generation: request.Generation, Sensitivity: request.Sensitivity,
		Offset: offset,
	})
}

func (service *Service) decodeListCursor(access searchjobs.AccessScope, request ListRequest) (listCursor, error) {
	var cursor listCursor
	if err := cursorcodec.Decode(service.cursorKey, listCursorDomain, cursorVersion, MaximumCursorBytes, request.PageToken, &cursor); err != nil ||
		cursor.Version != cursorVersion || cursor.Scope != scopeDigest(access) || cursor.JobID != request.SearchJobID ||
		cursor.Generation != request.Generation || cursor.Sensitivity != request.Sensitivity || cursor.Offset <= 0 {
		return listCursor{}, ErrInvalidCursor
	}
	return cursor, nil
}

func (service *Service) encodeMemberCursor(
	access searchjobs.AccessScope,
	request MemberRequest,
	projection string,
	afterOrdinal uint64,
) (string, error) {
	return cursorcodec.Encode(service.cursorKey, memberCursorDomain, cursorVersion, MaximumCursorBytes, memberCursor{
		Version: cursorVersion, Scope: scopeDigest(access), JobID: request.SearchJobID,
		Generation: request.Generation, Sensitivity: request.Sensitivity, PatternID: request.PatternID,
		Projection: projection, AfterOrdinal: afterOrdinal,
	})
}

func (service *Service) decodeMemberCursor(
	access searchjobs.AccessScope,
	request MemberRequest,
	projection string,
) (uint64, error) {
	if request.PageToken == "" {
		return 0, nil
	}
	var cursor memberCursor
	if err := cursorcodec.Decode(service.cursorKey, memberCursorDomain, cursorVersion, MaximumCursorBytes, request.PageToken, &cursor); err != nil ||
		cursor.Version != cursorVersion || cursor.Scope != scopeDigest(access) || cursor.JobID != request.SearchJobID ||
		cursor.Generation != request.Generation || cursor.Sensitivity != request.Sensitivity ||
		cursor.PatternID != request.PatternID || cursor.Projection != projection {
		return 0, ErrInvalidCursor
	}
	return cursor.AfterOrdinal, nil
}

func scopeDigest(access searchjobs.AccessScope) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte("open-splunk/pattern-scope/v1\x00"))
	_, _ = digest.Write([]byte(access.TenantID))
	_, _ = digest.Write([]byte{'\x00'})
	_, _ = digest.Write([]byte(access.OwnerID))
	return hex.EncodeToString(digest.Sum(nil))
}
