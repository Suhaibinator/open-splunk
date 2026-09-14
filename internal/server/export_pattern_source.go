package server

import (
	"errors"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
)

// exportPatternSourceFromProto decodes client intent only. The fresh-admission
// handler resolves SnapshotRef to Generation after receipt lookup; replay never
// reparses a prior process's public snapshot reference.
func exportPatternSourceFromProto(definition *opensplunk.ExportDefinition) (exportjobs.SourceKind, *patterns.ExportRequest, error) {
	if definition == nil {
		return "", nil, errors.New("export definition is required")
	}
	var result patterns.ExportRequest
	var sensitivity opensplunk.PatternSensitivity
	var kind exportjobs.SourceKind
	switch source := definition.GetSource().(type) {
	case nil:
		return exportjobs.SourceOrdinary, nil, nil
	case *opensplunk.ExportDefinition_PatternSummary:
		if source.PatternSummary == nil {
			return "", nil, errors.New("pattern summary source is required")
		}
		kind = exportjobs.SourcePatternSummary
		result.SearchJobID = source.PatternSummary.GetSearchJobId()
		result.SnapshotRef = source.PatternSummary.GetSnapshotRef()
		sensitivity = source.PatternSummary.GetSensitivity()
	case *opensplunk.ExportDefinition_PatternMembers:
		if source.PatternMembers == nil {
			return "", nil, errors.New("pattern member source is required")
		}
		kind = exportjobs.SourcePatternMembers
		result.SearchJobID = source.PatternMembers.GetSearchJobId()
		result.SnapshotRef = source.PatternMembers.GetSnapshotRef()
		result.PatternID = source.PatternMembers.GetPatternId()
		sensitivity = source.PatternMembers.GetSensitivity()
		if len(result.PatternID) == 0 || len(result.PatternID) > 4096 {
			return "", nil, errors.New("pattern ID is invalid")
		}
	default:
		return "", nil, errors.New("export source is invalid")
	}
	if result.SearchJobID != definition.GetSearchJobId() || result.SearchJobID == "" || len(result.SnapshotRef) == 0 || len(result.SnapshotRef) > 4096 {
		return "", nil, errors.New("pattern source identity is invalid")
	}
	switch sensitivity {
	case opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE:
		result.Sensitivity = patterns.Precise
	case opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED:
		result.Sensitivity = patterns.Balanced
	case opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BROAD:
		result.Sensitivity = patterns.Broad
	default:
		return "", nil, errors.New("pattern sensitivity is required")
	}
	return kind, &result, nil
}

func applyExportPatternSourceToProto(definition *opensplunk.ExportDefinition, job exportjobs.Job) error {
	if job.SourceKind == "" || job.SourceKind == exportjobs.SourceOrdinary {
		if job.Pattern != nil {
			return errors.New("ordinary export has a pattern source")
		}
		return nil
	}
	source := job.Pattern
	if source == nil || source.SearchJobID != job.SearchJobID || source.SnapshotRef == "" || source.Generation == 0 {
		return errors.New("retained pattern export identity is invalid")
	}
	var sensitivity opensplunk.PatternSensitivity
	switch source.Sensitivity {
	case patterns.Precise:
		sensitivity = opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE
	case patterns.Balanced:
		sensitivity = opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED
	case patterns.Broad:
		sensitivity = opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BROAD
	default:
		return errors.New("retained pattern export sensitivity is invalid")
	}
	switch job.SourceKind {
	case exportjobs.SourcePatternSummary:
		if source.PatternID != "" {
			return errors.New("summary export has a member identity")
		}
		definition.Source = &opensplunk.ExportDefinition_PatternSummary{PatternSummary: &opensplunk.PatternSummaryExportSource{SearchJobId: source.SearchJobID, SnapshotRef: source.SnapshotRef, Sensitivity: sensitivity}}
	case exportjobs.SourcePatternMembers:
		if source.PatternID == "" {
			return errors.New("member export has no pattern identity")
		}
		definition.Source = &opensplunk.ExportDefinition_PatternMembers{PatternMembers: &opensplunk.PatternMemberExportSource{SearchJobId: source.SearchJobID, SnapshotRef: source.SnapshotRef, Sensitivity: sensitivity, PatternId: source.PatternID}}
	default:
		return errors.New("retained export source is invalid")
	}
	return nil
}
