package server

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestSearchReadMapsFreshAuthorizationFailures(t *testing.T) {
	for _, mapper := range []struct {
		name     string
		mapError func(error) error
	}{
		{"live job", mapSearchJobError},
		{"retained artifact", mapSearchArtifactError},
	} {
		t.Run(mapper.name, func(t *testing.T) {
			for _, failure := range []struct {
				name   string
				err    error
				status int
			}{
				{"expired session", authorization.ErrUnauthorized, http.StatusUnauthorized},
				{"revoked source", fmt.Errorf("fresh source: %w", authorization.ErrForbidden), http.StatusForbidden},
				{"removed membership", authorization.ErrUnassigned, http.StatusForbidden},
			} {
				t.Run(failure.name, func(t *testing.T) {
					assertHTTPErrorStatus(t, mapper.mapError(failure.err), failure.status)
				})
			}
		})
	}
	assertHTTPErrorStatus(t, mapSearchJobError(searchjobs.ErrSourceForbidden), http.StatusForbidden)
}
