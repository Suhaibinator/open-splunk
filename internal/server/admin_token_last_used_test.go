package server

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
)

func TestIngestionTokenLastUsedProjection(t *testing.T) {
	t.Parallel()

	record := adminLastUsedToken("tok_projected", "projected", testNow.Add(-time.Hour))
	converted, err := tokenToProto(record)
	if err != nil {
		t.Fatalf("tokenToProto: %v", err)
	}
	if converted.GetLastUsedAt() == nil ||
		!converted.GetLastUsedAt().AsTime().Equal(record.LastUsedAt) {
		t.Fatalf(
			"last_used_at = %v, want %v",
			converted.GetLastUsedAt(),
			record.LastUsedAt,
		)
	}

	record.LastUsedAt = time.Time{}
	converted, err = tokenToProto(record)
	if err != nil {
		t.Fatalf("tokenToProto(unused): %v", err)
	}
	if converted.GetLastUsedAt() != nil {
		t.Fatalf("unused last_used_at = %v, want nil", converted.GetLastUsedAt())
	}
}

func TestIngestionTokenLastUsedProjectionRejectsImpossibleTimestamp(t *testing.T) {
	t.Parallel()

	record := adminLastUsedToken("tok_invalid", "invalid", testNow.Add(-time.Hour))
	record.LastUsedAt = record.CreatedAt.Add(-time.Microsecond)
	if _, err := tokenToProto(record); err == nil {
		t.Fatal("tokenToProto accepted last use before token creation")
	}
}

func TestIngestionTokenLastUsedSortIsDeterministic(t *testing.T) {
	t.Parallel()

	early := testNow.Add(-2 * time.Hour)
	later := testNow.Add(-time.Hour)
	input := []auth.CollectorToken{
		adminLastUsedToken("tok_unused_a", "unused a", time.Time{}),
		adminLastUsedToken("tok_tie_b", "tie b", later),
		adminLastUsedToken("tok_early", "early", early),
		adminLastUsedToken("tok_unused_b", "unused b", time.Time{}),
		adminLastUsedToken("tok_tie_a", "tie a", later),
	}

	tests := []struct {
		name      string
		direction opensplunkv1.SortDirection
		wantIDs   []string
	}{
		{
			name:      "ascending",
			direction: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
			wantIDs: []string{
				"tok_early",
				"tok_tie_a",
				"tok_tie_b",
				"tok_unused_a",
				"tok_unused_b",
			},
		},
		{
			name:      "descending",
			direction: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
			wantIDs: []string{
				"tok_unused_b",
				"tok_unused_a",
				"tok_tie_b",
				"tok_tie_a",
				"tok_early",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			sorted := filterAndSortTokens(
				input,
				nil,
				"",
				"",
				opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
				test.direction,
			)
			gotIDs := make([]string, 0, len(sorted))
			for _, record := range sorted {
				gotIDs = append(gotIDs, record.ID)
			}
			if !slices.Equal(gotIDs, test.wantIDs) {
				t.Fatalf("IDs = %q, want %q", gotIDs, test.wantIDs)
			}
		})
	}
}

func TestIngestionTokenLastUsedListPaginationIsBoundAndStaleSafe(t *testing.T) {
	t.Parallel()

	early := testNow.Add(-2 * time.Hour)
	later := testNow.Add(-time.Hour)
	tokens := &mutableTokenAdministration{records: []auth.CollectorToken{
		adminLastUsedToken("tok_unused", "unused", time.Time{}),
		adminLastUsedToken("tok_later", "later", later),
		adminLastUsedToken("tok_early", "early", early),
	}}
	handler := newAdminTokenHandler(t, tokens)
	pageSize := uint32(1)
	request := &opensplunkv1.ListIngestionTokensRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		},
		SortBy: opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
	}

	response := postProto(t, handler, "/api/v1/ingestion-tokens/list", request)
	if response.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body = %s", response.Code, response.Body)
	}
	var first opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &first)
	if len(first.GetIngestionTokens()) != 1 ||
		first.GetIngestionTokens()[0].GetIngestionTokenId() != "tok_early" ||
		first.GetIngestionTokens()[0].GetLastUsedAt() == nil ||
		first.GetPage().GetTotalSize() != 3 ||
		!first.GetPage().GetTotalSizeExact() ||
		first.GetPage().GetNextPageToken() == "" {
		t.Fatalf("first page = %+v", &first)
	}
	cursor := first.GetPage().GetNextPageToken()

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: "tok_early",
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body)
	}
	var got opensplunkv1.GetIngestionTokenResponse
	unmarshalResponse(t, response, &got)
	if got.GetIngestionToken().GetLastUsedAt() == nil ||
		!got.GetIngestionToken().GetLastUsedAt().AsTime().Equal(early) {
		t.Fatalf("get token = %+v", got.GetIngestionToken())
	}

	request.Page.PageToken = &cursor
	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", request)
	if response.Code != http.StatusOK {
		t.Fatalf("second page status = %d, body = %s", response.Code, response.Body)
	}
	var second opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &second)
	if len(second.GetIngestionTokens()) != 1 ||
		second.GetIngestionTokens()[0].GetIngestionTokenId() != "tok_later" {
		t.Fatalf("second page = %+v", &second)
	}

	descending := &opensplunkv1.ListIngestionTokensRequest{
		Page: &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			PageToken:        &cursor,
			IncludeTotalSize: true,
		},
		SortBy:        opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	}
	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		descending,
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"cross-direction cursor status = %d, body = %s",
			response.Code,
			response.Body,
		)
	}

	// Advance a use timestamp without changing the token's version or its
	// position in the sorted result. The cursor must still become stale because
	// the projected metadata changed.
	tokens.setLastUsedAt("tok_later", testNow.Add(-30*time.Minute))
	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"stale last-used cursor status = %d, body = %s",
			response.Code,
			response.Body,
		)
	}
}

type mutableTokenAdministration struct {
	mu      sync.Mutex
	records []auth.CollectorToken
}

func (administration *mutableTokenAdministration) CreateCollectorToken(
	context.Context,
	auth.CreateCollectorTokenRequest,
) (auth.IssuedCollectorToken, error) {
	return auth.IssuedCollectorToken{}, errors.New("unexpected token creation")
}

func (administration *mutableTokenAdministration) GetCollectorToken(
	_ context.Context,
	id string,
) (auth.CollectorToken, error) {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	for _, record := range administration.records {
		if record.ID == id {
			return cloneAdminToken(record), nil
		}
	}
	return auth.CollectorToken{}, errors.New("token not found")
}

func (administration *mutableTokenAdministration) ListCollectorTokens(
	context.Context,
) ([]auth.CollectorToken, error) {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	result := make([]auth.CollectorToken, 0, len(administration.records))
	for _, record := range administration.records {
		result = append(result, cloneAdminToken(record))
	}
	return result, nil
}

func (administration *mutableTokenAdministration) UpdateCollectorToken(
	context.Context,
	string,
	uint64,
	auth.UpdateCollectorTokenRequest,
) (auth.CollectorToken, error) {
	return auth.CollectorToken{}, errors.New("unexpected token update")
}

func (administration *mutableTokenAdministration) SetCollectorTokenEnabled(
	context.Context,
	string,
	uint64,
	bool,
) (auth.CollectorToken, error) {
	return auth.CollectorToken{}, errors.New("unexpected token state update")
}

func (administration *mutableTokenAdministration) RevokeCollectorToken(
	context.Context,
	string,
	uint64,
) (auth.CollectorToken, error) {
	return auth.CollectorToken{}, errors.New("unexpected token revocation")
}

func (administration *mutableTokenAdministration) setLastUsedAt(
	id string,
	lastUsedAt time.Time,
) {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	for index := range administration.records {
		if administration.records[index].ID == id {
			administration.records[index].LastUsedAt = lastUsedAt
			return
		}
	}
}

func (administration *mutableTokenAdministration) setBoundCollectorID(
	id string,
	boundCollectorID string,
) {
	administration.mu.Lock()
	defer administration.mu.Unlock()
	for index := range administration.records {
		if administration.records[index].ID == id {
			administration.records[index].BoundCollectorID = boundCollectorID
			return
		}
	}
}

func cloneAdminToken(record auth.CollectorToken) auth.CollectorToken {
	record.AllowedIndexNames = slices.Clone(record.AllowedIndexNames)
	record.AllowedHostRegexes = slices.Clone(record.AllowedHostRegexes)
	record.AllowedSourceRegexes = slices.Clone(record.AllowedSourceRegexes)
	return record
}

func adminLastUsedToken(
	id string,
	name string,
	lastUsedAt time.Time,
) auth.CollectorToken {
	createdAt := testNow.Add(-24 * time.Hour)
	return auth.CollectorToken{
		ID:                id,
		Version:           1,
		Name:              name,
		Prefix:            "ost_v1_safe",
		State:             auth.CollectorTokenStateActive,
		AllowedIndexNames: []string{"main"},
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
		LastUsedAt:        lastUsedAt,
	}
}

func newAdminTokenHandler(
	t *testing.T,
	tokens IngestionTokenAdministration,
) *adminIntegrationHandler {
	t.Helper()
	browserAuthenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(adminIntegrationBearerToken),
		"default",
		"single-user",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("auth.NewBearerTokenAuthenticator: %v", err)
	}
	raw := newTestHandler(t, Config{
		SearchJobs:           &fakeSearchJobs{},
		Indexes:              fakeIndexCatalog{},
		IngestionTokens:      tokens,
		WebUI:                testUI(),
		Now:                  func() time.Time { return testNow },
		BrowserAuthenticator: browserAuthenticator,
	})
	return &adminIntegrationHandler{
		raw:   raw,
		token: adminIntegrationBearerToken,
	}
}
