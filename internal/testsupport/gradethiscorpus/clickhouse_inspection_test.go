package gradethiscorpus

import (
	"strings"
	"testing"
	"time"
)

func TestValidateInspectionPartLayoutPinsExactGeometry(t *testing.T) {
	t.Parallel()

	profile := Fixture()
	valid := []InspectionPart{
		{
			Partition: profile.BaseTime.Add(-24 * time.Hour).
				Format("200601"),
			Parts:    1,
			Rows:     InspectionLoadRowsPerCohort,
			Marks:    2,
			MaxLevel: 0,
		},
		{
			Partition: profile.BaseTime.Format("200601"),
			Parts:     4,
			Rows: uint64(len(profile.Events)) +
				3*InspectionLoadRowsPerCohort,
			Marks:    8,
			MaxLevel: 0,
		},
	}
	if err := ValidateInspectionPartLayout(profile, valid); err != nil {
		t.Fatal(err)
	}

	const private = "private-partition-7f2c"
	tests := []struct {
		name   string
		mutate func([]InspectionPart) []InspectionPart
	}{
		{
			name: "missing partition",
			mutate: func(layout []InspectionPart) []InspectionPart {
				return layout[:1]
			},
		},
		{
			name: "private partition",
			mutate: func(layout []InspectionPart) []InspectionPart {
				layout[0].Partition = private
				return layout
			},
		},
		{
			name: "part count",
			mutate: func(layout []InspectionPart) []InspectionPart {
				layout[0].Parts++
				return layout
			},
		},
		{
			name: "row count",
			mutate: func(layout []InspectionPart) []InspectionPart {
				layout[0].Rows++
				return layout
			},
		},
		{
			name: "mark count",
			mutate: func(layout []InspectionPart) []InspectionPart {
				layout[0].Marks++
				return layout
			},
		},
		{
			name: "merge level",
			mutate: func(layout []InspectionPart) []InspectionPart {
				layout[0].MaxLevel++
				return layout
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			layout := append([]InspectionPart(nil), valid...)
			err := ValidateInspectionPartLayout(
				profile,
				test.mutate(layout),
			)
			if err == nil {
				t.Fatal("invalid part layout passed validation")
			}
			if strings.Contains(err.Error(), private) {
				t.Fatal("part-layout error disclosed metadata")
			}
		})
	}
}
