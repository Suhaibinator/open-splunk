package ingest

import (
	"fmt"
	"regexp"
	"slices"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/tokenconstraint"
)

const (
	maximumEventAuthorizationRegexesPerDimension = tokenconstraint.MaximumPatternsPerDimension
	maximumEventAuthorizationRegexBytes          = tokenconstraint.MaximumPatternBytes
)

// eventAuthorizationMatcher is an immutable, precompiled authorization
// snapshot. A dimension with no expressions is unrestricted. Expressions are
// evaluated independently so their named captures and flags cannot interfere.
type eventAuthorizationMatcher struct {
	hosts   []*regexp.Regexp
	sources []*regexp.Regexp
}

func compileEventAuthorization(
	authorization Authorization,
) (eventAuthorizationMatcher, error) {
	hosts, err := compileEventAuthorizationDimension(
		"host",
		authorization.AllowedHostRegexes,
	)
	if err != nil {
		return eventAuthorizationMatcher{}, err
	}
	sources, err := compileEventAuthorizationDimension(
		"source",
		authorization.AllowedSourceRegexes,
	)
	if err != nil {
		return eventAuthorizationMatcher{}, err
	}
	return eventAuthorizationMatcher{hosts: hosts, sources: sources}, nil
}

// refreshEventAuthorization recompiles only changed dimensions. An exact
// slice match is safe to reuse because currentAuthorization and current were
// installed together only after bounded validation and compilation succeeded.
func refreshEventAuthorization(
	current eventAuthorizationMatcher,
	currentAuthorization Authorization,
	nextAuthorization Authorization,
) (eventAuthorizationMatcher, error) {
	next := current
	if !slices.Equal(
		currentAuthorization.AllowedHostRegexes,
		nextAuthorization.AllowedHostRegexes,
	) {
		hosts, err := compileEventAuthorizationDimension(
			"host",
			nextAuthorization.AllowedHostRegexes,
		)
		if err != nil {
			return eventAuthorizationMatcher{}, err
		}
		next.hosts = hosts
	}
	if !slices.Equal(
		currentAuthorization.AllowedSourceRegexes,
		nextAuthorization.AllowedSourceRegexes,
	) {
		sources, err := compileEventAuthorizationDimension(
			"source",
			nextAuthorization.AllowedSourceRegexes,
		)
		if err != nil {
			return eventAuthorizationMatcher{}, err
		}
		next.sources = sources
	}
	return next, nil
}

func compileEventAuthorizationDimension(
	dimension string,
	patterns []string,
) ([]*regexp.Regexp, error) {
	if err := tokenconstraint.ValidateNormalized(patterns); err != nil {
		return nil, fmt.Errorf("invalid %s event authority", dimension)
	}
	compiled := make([]*regexp.Regexp, 0, len(patterns))
	for index, pattern := range patterns {
		expression, err := regexp.Compile(`\A(?:` + pattern + `)\z`)
		if err != nil {
			// Do not reflect a credential-owned expression into logs or RPCs.
			return nil, fmt.Errorf("invalid %s event authority expression %d", dimension, index)
		}
		compiled = append(compiled, expression)
	}
	return compiled, nil
}

func (matcher eventAuthorizationMatcher) allows(host, source string) bool {
	return eventAuthorizationDimensionAllows(matcher.hosts, host) &&
		eventAuthorizationDimensionAllows(matcher.sources, source)
}

func eventAuthorizationDimensionAllows(
	patterns []*regexp.Regexp,
	value string,
) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, pattern := range patterns {
		if pattern.MatchString(value) {
			return true
		}
	}
	return false
}

func (matcher eventAuthorizationMatcher) rejection(event *StoredEvent) *EventError {
	if event == nil || event.Event == nil {
		return nil
	}
	if !eventAuthorizationDimensionAllows(matcher.hosts, event.Event.GetHost()) {
		return eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_HOST,
			"token is not authorized for the event host",
			"host",
			"unauthorized_host",
		)
	}
	if !eventAuthorizationDimensionAllows(matcher.sources, event.Event.GetSource()) {
		return eventFailure(
			opensplunkv1.EventRejectionCode_EVENT_REJECTION_CODE_UNAUTHORIZED_SOURCE,
			"token is not authorized for the event source",
			"source",
			"unauthorized_source",
		)
	}
	return nil
}
