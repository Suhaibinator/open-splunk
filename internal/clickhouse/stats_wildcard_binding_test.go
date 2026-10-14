package clickhouse

import (
	"slices"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

func TestWildcardInventoryBindsUInt8ArraysAsNumericSlices(t *testing.T) {
	t.Parallel()
	for _, source := range []string{
		`index=gradethis | stats count(f*)`,
		`index=gradethis | convert num(f*)`,
		`index=gradethis | convert num(_*) none(_raw)`,
		`index=gradethis | convert num(epoch) AS newepoch num(new*) none(f*)`,
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			parsed, err := spl.Parse(source)
			if err != nil {
				t.Fatal(err)
			}
			preparation, err := plan.PrepareStatsWildcard(parsed, testChartScope())
			if err != nil {
				t.Fatal(err)
			}
			compiled, err := (Compiler{}).CompileStatsWildcardInventory(preparation.Prefix(), preparation.Request())
			if err != nil {
				t.Fatal(err)
			}
			clone, ok := compiled.CloneForExecution()
			if !ok {
				t.Fatal("compiled inventory cannot be cloned")
			}
			numericArrays := 0
			emptyArrays := 0
			for index, argument := range compiled.Args {
				if _, binary := argument.([]uint8); binary {
					t.Fatalf("argument %d is an ambiguous binary slice: %#v", index, argument)
				}
				values, numeric := argument.([]int64)
				if !numeric {
					continue
				}
				numericArrays++
				if len(values) == 0 {
					emptyArrays++
					continue
				}
				copied, ok := clone.Args[index].([]int64)
				if !ok || !slices.Equal(copied, values) {
					t.Fatal("clone changed numeric array argument")
				}
				copied[0]++
				if copied[0] == values[0] {
					t.Fatal("clone shares numeric array backing")
				}
				tampered := compiled
				tampered.Args = slices.Clone(compiled.Args)
				binary := make([]uint8, len(values))
				for i, value := range values {
					binary[i] = uint8(value)
				}
				tampered.Args[index] = binary
				if tampered.HasValidExecutionSeal() {
					t.Fatal("binary array substitution retained compiler authority")
				}
			}
			if numericArrays == 0 {
				t.Fatal("inventory has no numeric array arguments")
			}
			if source == `index=gradethis | convert num(f*)` && emptyArrays != 2 {
				t.Fatalf("empty internal/exclusion arrays = %d, want 2", emptyArrays)
			}
			if !compiled.HasValidExecutionSeal() {
				t.Fatal("clone mutation changed original compiler authority")
			}
		})
	}
}
