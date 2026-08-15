package searchinspection

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestV03AllTenCommandsHaveACompleteRedactedInspectionProjection(t *testing.T) {
	t.Parallel()

	const source = `index=gradethis` +
		` | regex message!="(?i)inspection_secret_拒否"` +
		` | sort 0 +event_id` +
		` | accum bytes AS running` +
		` | strcat allrequired=true host "inspection_secret_💥" route endpoint` +
		` | addinfo` +
		` | fillnull value="inspection_secret_界" optional` +
		` | addtotals fieldname=total bytes running` +
		` | delta running AS step p=2` +
		` | makemv delim="💥界" allowempty=true tags` +
		` | mvexpand tags limit=3` +
		` | reverse` +
		` | table event_id tags running endpoint optional total step info_sid`
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	visibility := uint64(77)
	earliest := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	latest := earliest.Add(time.Hour)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          "tenant-inspection",
		AuthorizedIndexes: []string{"gradethis"},
		SearchJobID:       "inspection_secret_sid",
		Earliest:          earliest,
		Latest:            latest,
		SearchStart:       latest,
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   latest.Add(time.Second),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	projected, err := projectLogicalPlan(context.Background(), logical, source)
	if err != nil {
		t.Fatalf("projectLogicalPlan() error = %v", err)
	}

	wantStages := []struct {
		operator string
		inputs   []string
		outputs  []string
	}{
		{operator: "Scan"},
		{operator: "Filter", inputs: []string{"index"}},
		{operator: "RegexFilter", inputs: []string{"message"}},
		{operator: "Sort", inputs: []string{"event_id"}},
		{operator: "StreamAggregate", inputs: []string{"bytes"}, outputs: []string{"running"}},
		{operator: "Strcat", inputs: []string{"host", "route"}, outputs: []string{"endpoint"}},
		{operator: "Extend", outputs: []string{"info_max_time", "info_min_time", "info_search_time", "info_sid"}},
		{operator: "FillNull", inputs: []string{"optional"}, outputs: []string{"optional"}},
		{operator: "RowTotal", inputs: []string{"bytes", "running"}, outputs: []string{"total"}},
		{operator: "OrderedDelta", inputs: []string{"running"}, outputs: []string{"step"}},
		{operator: "MakeMultivalue", inputs: []string{"tags"}, outputs: []string{"tags"}},
		{operator: "ExpandMultivalue", inputs: []string{"tags"}, outputs: []string{"tags"}},
		{operator: "Reverse"},
		{operator: "Project", inputs: []string{
			"endpoint", "event_id", "info_sid", "optional", "running", "step", "tags", "total",
		}, outputs: []string{
			"endpoint", "event_id", "info_sid", "optional", "running", "step", "tags", "total",
		}},
	}
	if len(projected.Stages) != len(wantStages) {
		t.Fatalf("projected stages = %d, want %d", len(projected.Stages), len(wantStages))
	}
	for index, want := range wantStages {
		stage := projected.Stages[index]
		if stage.Index != uint32(index) || stage.Operator != want.operator {
			t.Fatalf("stage %d identity = %#v, want operator %q", index, stage, want.operator)
		}
		if !slices.Equal(stage.InputFields, want.inputs) {
			t.Fatalf("stage %d (%s) inputs = %v, want %v", index, want.operator, stage.InputFields, want.inputs)
		}
		if !slices.Equal(stage.OutputFields, want.outputs) {
			t.Fatalf("stage %d (%s) outputs = %v, want %v", index, want.operator, stage.OutputFields, want.outputs)
		}
		if stage.SourceRange == nil {
			t.Fatalf("stage %d (%s) omitted its authored source range", index, want.operator)
		}
		if len(stage.KnowledgeObjects) != 0 || len(stage.OutputProvenance) != 0 {
			t.Fatalf("authored stage %d minted knowledge provenance: %#v", index, stage)
		}
	}

	wantFinal := []string{"event_id", "tags", "running", "endpoint", "optional", "total", "step", "info_sid"}
	if projected.Output.Kind != OutputKindStatic || !slices.Equal(projected.Output.Fields, wantFinal) {
		t.Fatalf("output shape = %#v, want static %v", projected.Output, wantFinal)
	}
	wantReads := []string{
		"bytes", "endpoint", "event_id", "host", "index", "info_sid", "message", "optional",
		"route", "running", "step", "tags", "total",
	}
	if !slices.Equal(projected.ReferencedFields, wantReads) {
		t.Fatalf("projected reads = %v, want %v", projected.ReferencedFields, wantReads)
	}

	rendered := strings.ToLower(strings.Join(append(append([]string(nil), projected.ReferencedFields...), projected.Output.Fields...), "\x00"))
	for _, forbidden := range []string{"inspection_secret", "拒否", "💥", "__os_"} {
		if strings.Contains(rendered, strings.ToLower(forbidden)) {
			t.Fatalf("inspection projection leaked %q: %q", forbidden, rendered)
		}
	}
	// The projection is detached: mutating it cannot alter the retained plan.
	projected.Stages[2].InputFields[0] = "tampered"
	projected.Output.Fields[0] = "tampered"
	if logical.Operators[2].(*plan.RegexFilter).Input.Name != "message" || logical.OutputFields[0] != "event_id" {
		t.Fatalf("detached projection mutation reached logical authority: %#v", logical)
	}
}

func TestV03MissingMultivalueFieldKeepsInspectionAndLogicalShapeAligned(t *testing.T) {
	t.Parallel()
	for _, command := range []string{"makemv missing", "mvexpand missing"} {
		t.Run(command, func(t *testing.T) {
			t.Parallel()
			source := `index=gradethis | table event_id | ` + command
			parsed, err := spl.Parse(source)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			visibility := uint64(1)
			at := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
			logical, err := plan.Build(parsed, plan.Scope{
				TenantID: "tenant", AuthorizedIndexes: []string{"gradethis"},
				SearchJobID: "job", Earliest: at, Latest: at.Add(time.Hour),
				SearchStart: at, SearchTimezone: "UTC", IndexTimeCutoff: at,
				VisibilityCutoff: &visibility,
			})
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			projected, err := projectLogicalPlan(context.Background(), logical, source)
			if err != nil {
				t.Fatalf("projectLogicalPlan: %v", err)
			}
			want := []string{"event_id", "missing"}
			if !slices.Equal(logical.OutputFields, want) ||
				projected.Output.Kind != OutputKindStatic ||
				!slices.Equal(projected.Output.Fields, want) {
				t.Fatalf("logical/inspection shapes = %v/%#v, want %v", logical.OutputFields, projected.Output, want)
			}
		})
	}
}
