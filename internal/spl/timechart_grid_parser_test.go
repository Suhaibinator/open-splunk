package spl

import "testing"

func TestParseTimechartEnhancedGrid(t *testing.T) {
	for _, span := range []string{"1us", "125usec", "250ms", "25cs", "2ds", "49h", "3days", "2weeks", "5months", "2qtr", "4years"} {
		if _, err := Parse("index=main | timechart span=" + span + " cont=false partial=false fixedrange=false aligntime=earliest count"); err != nil {
			t.Errorf("span %s: %v", span, err)
		}
	}
	for _, span := range []string{"3ms", "1000ms", "100cs", "11ds", "0us", "18446744073709551615h"} {
		if _, err := Parse("index=main | timechart span=" + span + " count"); err == nil {
			t.Errorf("span %s accepted", span)
		}
	}
	for _, options := range []string{"cont=false cont=true", "partial=maybe", "fixedrange=0", "aligntime=earliest aligntime=latest"} {
		if _, err := Parse("index=main | timechart " + options + " count"); err == nil {
			t.Errorf("options %s accepted", options)
		}
	}
}
