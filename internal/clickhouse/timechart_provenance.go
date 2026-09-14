package clickhouse

import "slices"

// ResultTimeBucketOutput describes presentation provenance on an ordinary
// typed relation. It is independent from the physical Timechart decoder.
type ResultTimeBucketOutput struct{ TimeIndex int }

const ResultTimeBucketEndColumn = "__os_result_time_bucket_end"

func resultTimeBucketOutput(state compileState, fields []string) *ResultTimeBucketOutput {
	field, ok := state.visible["_time"]
	index := slices.Index(fields, "_time")
	if !ok || index < 0 || field.kind != fieldKindTime || field.timeBucketEndSQL == "" {
		return nil
	}
	return &ResultTimeBucketOutput{TimeIndex: index}
}

func validResultTimeBucketOutput(compiled CompiledQuery) bool {
	if compiled.TimeBucket == nil {
		return true
	}
	return compiled.Timechart == nil && compiled.Chart == nil && compiled.TimeBucket.TimeIndex >= 0 &&
		compiled.TimeBucket.TimeIndex < len(compiled.OutputFields) && compiled.OutputFields[compiled.TimeBucket.TimeIndex] == "_time"
}

// HasTimechartStage reports sealed chart origin even after ordinary suffixes.
func (compiled CompiledQuery) HasTimechartStage() bool { return compiled.hasTimechartStage }
