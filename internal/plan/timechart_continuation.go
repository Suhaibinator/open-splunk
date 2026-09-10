package plan

import (
	"errors"
	"slices"
	"strings"

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
