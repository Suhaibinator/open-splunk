package export

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/nilcheck"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func normalizePatternSource(request *CreateRequest) error {
	if request.SourceKind == "" {
		request.SourceKind = SourceOrdinary
	}
	if request.SourceKind == SourceOrdinary {
		if request.Pattern != nil {
			return ErrInvalidRequest
		}
		return nil
	}
	if request.SourceKind != SourcePatternSummary && request.SourceKind != SourcePatternMembers {
		return ErrInvalidRequest
	}
	if request.Pattern == nil {
		return ErrInvalidRequest
	}
	pattern := *request.Pattern
	if pattern.SearchJobID != request.SearchJobID || pattern.Generation == 0 || !validExportMetadataIdentifier(pattern.SnapshotRef, 4096) || pattern.Sensitivity < patterns.Precise || pattern.Sensitivity > patterns.Broad {
		return fmt.Errorf("%w: invalid pattern source", ErrInvalidRequest)
	}
	if request.SourceKind == SourcePatternMembers && !validExportMetadataIdentifier(pattern.PatternID, 4096) {
		return ErrInvalidRequest
	}
	if request.SourceKind == SourcePatternSummary && pattern.PatternID != "" {
		return ErrInvalidRequest
	}
	pattern.SearchJobID = strings.Clone(pattern.SearchJobID)
	pattern.SnapshotRef = strings.Clone(pattern.SnapshotRef)
	pattern.PatternID = strings.Clone(pattern.PatternID)
	request.Pattern = &pattern
	return nil
}

func patternMetadataBytes(pattern *patterns.ExportRequest) uint64 {
	if pattern == nil {
		return 0
	}
	return uint64(unsafe.Sizeof(*pattern)) + uint64(len(pattern.SearchJobID)+len(pattern.SnapshotRef)+len(pattern.PatternID))
}

func (manager *Manager) acquireSource(ctx context.Context, access searchjobs.AccessScope, request CreateRequest) (searchjobs.ResultLease, error) {
	if request.SourceKind == SourceOrdinary {
		return manager.source.AcquireResultsFor(ctx, access, request.SearchJobID)
	}
	if nilcheck.IsNil(manager.patternSource) || request.Pattern == nil {
		return nil, ErrSourceUnavailable
	}
	var lease searchjobs.ResultLease
	var err error
	switch request.SourceKind {
	case SourcePatternSummary:
		lease, err = manager.patternSource.AcquirePatternSummary(ctx, access, *request.Pattern)
	case SourcePatternMembers:
		lease, err = manager.patternSource.AcquirePatternMembers(ctx, access, *request.Pattern)
	default:
		return nil, ErrInvalidRequest
	}
	if errors.Is(err, patterns.ErrCapacity) || errors.Is(err, patterns.ErrLimit) {
		return lease, searchjobs.ErrCapacity
	}
	return lease, err
}
