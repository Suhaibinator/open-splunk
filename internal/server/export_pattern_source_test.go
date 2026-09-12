package server

import (
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestExportPatternSourceRoundTripsExactClientIdentity(t *testing.T) {
	for _, members := range []bool{false, true} {
		definition := &opensplunk.ExportDefinition{SearchJobId: "job"}
		if members {
			definition.Source = &opensplunk.ExportDefinition_PatternMembers{PatternMembers: &opensplunk.PatternMemberExportSource{SearchJobId: "job", SnapshotRef: "immutable-ref", Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE, PatternId: "exact-pattern"}}
		} else {
			definition.Source = &opensplunk.ExportDefinition_PatternSummary{PatternSummary: &opensplunk.PatternSummaryExportSource{SearchJobId: "job", SnapshotRef: "immutable-ref", Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED}}
		}
		kind, source, err := exportPatternSourceFromProto(definition)
		if err != nil {
			t.Fatal(err)
		}
		if source.Generation != 0 {
			t.Fatal("wire decoder resolved dynamic snapshot state")
		}
		source.Generation = 42
		roundTrip := &opensplunk.ExportDefinition{SearchJobId: "job"}
		if err := applyExportPatternSourceToProto(roundTrip, exportjobs.Job{SearchJobID: "job", SourceKind: kind, Pattern: source}); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(definition, roundTrip) {
			t.Fatalf("source identity changed: %v", roundTrip)
		}
		definition.SearchJobId = "another"
		if _, _, err := exportPatternSourceFromProto(definition); err == nil {
			t.Fatal("mixed source identity accepted")
		}
	}
}
func TestExportPatternSourceRejectsUnknownSensitivityAndMissingRelation(t *testing.T) {
	for _, source := range []*opensplunk.PatternMemberExportSource{nil, {SearchJobId: "job", SnapshotRef: "ref", Sensitivity: 999, PatternId: "pattern"}, {SearchJobId: "job", SnapshotRef: "ref", Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE}} {
		if _, _, err := exportPatternSourceFromProto(&opensplunk.ExportDefinition{SearchJobId: "job", Source: &opensplunk.ExportDefinition_PatternMembers{PatternMembers: source}}); err == nil {
			t.Fatalf("invalid source accepted: %v", source)
		}
	}
}
