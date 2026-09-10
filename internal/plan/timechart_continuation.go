package plan

import (
	"errors"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// TimechartContinuation retains parser-owned suffix text and the immutable
// admission scope. The exact runtime pivot schema is supplied only after the
// complete upstream transport has passed validation.
type TimechartContinuation struct {
	source      string
	nextCommand int
	scope       Scope
}

func newTimechartContinuation(query *spl.Query, scope Scope, next int) (TimechartContinuation, error) {
	source, ok := query.ParsedSource()
	if !ok || next < 0 || next >= len(query.Commands) {
		return TimechartContinuation{}, errors.New("plan timechart continuation: parser-owned source is required")
	}
	start := query.Commands[next].SourceRange().Start.Offset
	if start < 0 || start >= len(source) {
		return TimechartContinuation{}, errors.New("plan timechart continuation: source range is invalid")
	}
	scope.AuthorizedIndexes = slices.Clone(scope.AuthorizedIndexes)
	scope.RequestedIndexes = slices.Clone(scope.RequestedIndexes)
	if scope.VisibilityCutoff != nil {
		value := *scope.VisibilityCutoff
		scope.VisibilityCutoff = &value
	}
	return TimechartContinuation{source: strings.Clone(source), nextCommand: next, scope: scope}, nil
}

// Source identifies the exact suffix for compiler execution authentication.
func (continuation TimechartContinuation) Source() string { return continuation.source }

// Build resumes ordinary SPL planning against an exact closed schema.
func (continuation TimechartContinuation) Build(fields []string) (*Query, error) {
	if continuation.source == "" || fields == nil {
		return nil, errors.New("plan timechart continuation: source and schema are required")
	}
	query, err := spl.Parse(continuation.source)
	if err != nil {
		return nil, err
	}
	// The synthetic base search is identity over the external relation.
	return buildWithRelationStart(query, continuation.scope, fields, continuation.nextCommand)
}

// TimechartContinuationAt returns immutable continuation authority for a chart
// operator. Its private members cannot be assembled from user-provided SQL.
func (query *Query) TimechartContinuationAt(index int) (TimechartContinuation, bool) {
	if query == nil {
		return TimechartContinuation{}, false
	}
	continuation, ok := query.timechartContinuations[index]
	return continuation, ok
}

// StartCommand identifies the suffix boundary within the original source.
func (continuation TimechartContinuation) StartCommand() int { return continuation.nextCommand }

// HasTimechartContinuation reports retained parser-owned chart suffix authority.
func (query *Query) HasTimechartContinuation() bool {
	return query != nil && len(query.timechartContinuations) != 0
}

// LookupContracts returns ordered authored lookup contracts in the suffix.
// Their event fields are literal names in the closed timechart relation.
func (continuation TimechartContinuation) LookupContracts() ([]Lookup, error) {
	query, err := spl.Parse(continuation.source)
	if err != nil {
		return nil, err
	}
	var contracts []Lookup
	for _, command := range query.Commands[continuation.nextCommand:] {
		if lookup, ok := command.(*spl.LookupCommand); ok {
			contract, err := buildLookupCommand(lookup, true)
			if err != nil {
				return nil, err
			}
			contracts = append(contracts, *contract)
		}
	}
	return contracts, nil
}

func shiftedTimechartContinuations(source map[int]TimechartContinuation, at, count int) map[int]TimechartContinuation {
	if source == nil {
		return nil
	}
	result := make(map[int]TimechartContinuation, len(source))
	for index, continuation := range source {
		if index >= at {
			index += count
		}
		result[index] = continuation
	}
	return result
}

// RetainedBytes accounts for the detached parser source and admission snapshot.
func (continuation TimechartContinuation) RetainedBytes() (uint64, bool) {
	total := uint64(unsafe.Sizeof(continuation))
	add := func(charge uint64) bool {
		if charge > math.MaxUint64-total {
			return false
		}
		total += charge
		return true
	}
	for _, value := range []string{continuation.source, continuation.scope.TenantID, continuation.scope.SearchJobID, continuation.scope.SearchTimezone} {
		if !add(uint64(len(value))) {
			return 0, false
		}
	}
	for _, values := range [][]string{continuation.scope.AuthorizedIndexes, continuation.scope.RequestedIndexes} {
		if uint64(cap(values)) > math.MaxUint64/uint64(unsafe.Sizeof("")) || !add(uint64(cap(values))*uint64(unsafe.Sizeof(""))) {
			return 0, false
		}
		for _, value := range values {
			if !add(uint64(len(value))) {
				return 0, false
			}
		}
	}
	if continuation.scope.VisibilityCutoff != nil && !add(uint64(unsafe.Sizeof(uint64(0)))) {
		return 0, false
	}
	return total, true
}
