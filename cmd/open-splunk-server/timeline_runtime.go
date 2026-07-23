package main

import (
	"errors"
	"fmt"

	"github.com/Suhaibinator/open-splunk/internal/searchanalysis"
	"github.com/Suhaibinator/open-splunk/internal/server"
)

// newRuntimeHTTPHandler attaches the enforcing timeline service to the browser
// handler. Timeline analysis is synchronous and owns no background workers or
// connections, so it needs no additional close step in the runtime's carefully
// ordered transport, export, search-job, and ClickHouse shutdown sequence.
func newRuntimeHTTPHandler(config server.Config, timelineConfig searchanalysis.Config) (*server.Handler, error) {
	if config.SearchTimelines != nil {
		return nil, errors.New("compose HTTP runtime: search timeline service is already configured")
	}
	timelines, err := searchanalysis.New(timelineConfig)
	if err != nil {
		return nil, fmt.Errorf("compose HTTP runtime: %w", err)
	}
	config.SearchTimelines = timelines
	return server.NewHandler(config)
}
