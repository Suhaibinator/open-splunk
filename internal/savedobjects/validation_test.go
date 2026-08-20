package savedobjects

import (
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestValidationDiagnosticsAreDeterministic(t *testing.T) {
	t.Parallel()
	empty := ""
	invalid := "bad\x00value"
	definition := savedSearchDefinition("Errors", "app-main")
	definition.Search.TimeRange = &opensplunk.TimeRangeSpec{Earliest: &empty, Latest: &empty}
	for iteration := range 50 {
		_, _, _, err := normalizeAndEncodeDefinition(definition, "owner")
		if err == nil || !strings.Contains(err.Error(), "earliest time") {
			t.Fatalf("iteration %d error = %v, want earliest-time error", iteration, err)
		}
	}

	definition = savedSearchDefinition("Errors", "app-main")
	definition.Search.Visualization = &opensplunk.VisualizationSpec{
		Type:  opensplunk.VisualizationType_VISUALIZATION_TYPE_TABLE,
		Title: &invalid, XField: &invalid,
	}
	for iteration := range 50 {
		_, _, _, err := normalizeAndEncodeDefinition(definition, "owner")
		if err == nil || !strings.Contains(err.Error(), "visualization title") {
			t.Fatalf("iteration %d error = %v, want title error", iteration, err)
		}
	}
}

func TestUpdateMaskWildcardStillValidatesEveryPath(t *testing.T) {
	t.Parallel()
	for _, paths := range [][]string{{"*", "bogus"}, {"bogus", "*"}} {
		if _, err := normalizeUpdateMask(&fieldmaskpb.FieldMask{Paths: paths}); err == nil {
			t.Fatalf("normalizeUpdateMask(%v) succeeded", paths)
		}
	}
	for _, paths := range [][]string{{"*", "name"}, {"name", "*"}} {
		mask, err := normalizeUpdateMask(&fieldmaskpb.FieldMask{Paths: paths})
		if err != nil || !mask.full {
			t.Fatalf("normalizeUpdateMask(%v) = (%+v, %v)", paths, mask, err)
		}
	}
}
