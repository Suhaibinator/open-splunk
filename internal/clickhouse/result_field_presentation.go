package clickhouse

import (
	"errors"
	"strings"
	"unicode/utf8"
)

const (
	// MaximumResultFieldFlatDelimiterBytes is an independent defensive ceiling
	// for one presentation-only delimiter. Authored stats source is already
	// bounded to 16 KiB, but the executable contract revalidates forged plans
	// and retained clones without trusting parser provenance.
	MaximumResultFieldFlatDelimiterBytes = 16 << 10
)

var errInvalidResultFieldPresentation = errors.New(
	"compile ClickHouse query: result field presentation metadata is invalid",
)

// ResultFieldPresentation is presentation metadata for the public result
// field at the same ordinal in CompiledQuery.OutputFields. It never changes
// the typed result value. In particular, list/values and stats sparklines
// remain Array(String) for search-job storage and exports.
//
// HasFlatMultivalueDelimiter distinguishes an effective delimiter (including
// an authored empty delimiter) from a field with no delimiter metadata.
type ResultFieldPresentation struct {
	FlatMultivalueDelimiter    string
	HasFlatMultivalueDelimiter bool
	StatsSparkline             bool
}

func validResultFieldPresentations(compiled CompiledQuery) bool {
	if len(compiled.OutputPresentations) == 0 {
		return true
	}
	if len(compiled.OutputPresentations) != len(compiled.OutputFields) {
		return false
	}
	for _, presentation := range compiled.OutputPresentations {
		if presentation.StatsSparkline && presentation.HasFlatMultivalueDelimiter {
			return false
		}
		if !presentation.HasFlatMultivalueDelimiter {
			if presentation.FlatMultivalueDelimiter != "" {
				return false
			}
			continue
		}
		if len(presentation.FlatMultivalueDelimiter) > MaximumResultFieldFlatDelimiterBytes ||
			!utf8.ValidString(presentation.FlatMultivalueDelimiter) {
			return false
		}
	}
	return true
}

func cloneResultFieldPresentations(
	input []ResultFieldPresentation,
) []ResultFieldPresentation {
	if input == nil {
		return nil
	}
	result := make([]ResultFieldPresentation, len(input))
	for index, presentation := range input {
		result[index] = presentation
		result[index].FlatMultivalueDelimiter = strings.Clone(
			presentation.FlatMultivalueDelimiter,
		)
	}
	return result
}

// ValidatedResultFieldPresentations returns a detached ordinal presentation
// contract. A nil slice is the canonical absence form retained for compiled
// queries that do not publish presentation metadata.
func (compiled CompiledQuery) ValidatedResultFieldPresentations() (
	[]ResultFieldPresentation,
	bool,
) {
	if !validResultFieldPresentations(compiled) {
		return nil, false
	}
	return cloneResultFieldPresentations(compiled.OutputPresentations), true
}

func resultFieldPresentations(
	state compileState,
	outputFields []string,
) ([]ResultFieldPresentation, error) {
	result := make([]ResultFieldPresentation, len(outputFields))
	present := false
	for index, name := range outputFields {
		field, visible := state.visible[name]
		if !visible || (!field.hasFlatMultivalueDelimiter && !field.statsSparkline) {
			continue
		}
		if field.kind != fieldKindStringArray ||
			(field.hasFlatMultivalueDelimiter && field.statsSparkline) ||
			(field.hasFlatMultivalueDelimiter &&
				(len(field.flatMultivalueDelimiter) > MaximumResultFieldFlatDelimiterBytes ||
					!utf8.ValidString(field.flatMultivalueDelimiter))) {
			return nil, errInvalidResultFieldPresentation
		}
		result[index] = ResultFieldPresentation{
			FlatMultivalueDelimiter:    strings.Clone(field.flatMultivalueDelimiter),
			HasFlatMultivalueDelimiter: true,
			StatsSparkline:             field.statsSparkline,
		}
		if !field.hasFlatMultivalueDelimiter {
			result[index].HasFlatMultivalueDelimiter = false
		}
		present = true
	}
	if !present {
		return nil, nil
	}
	return result, nil
}
