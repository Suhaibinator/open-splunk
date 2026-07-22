package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIndexAdministrationLifecycleAgainstSQLite(t *testing.T) {
	t.Parallel()

	handler, _, _ := newAdminIntegrationHandler(t)
	description := "production logs"
	response := postProto(t, handler, "/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{Definition: &opensplunkv1.IndexDefinition{
		Name: " GRADETHIS-PROD ", DisplayName: "GradeThis production", Description: &description,
		RetentionPeriod: durationpb.New(30 * 24 * time.Hour),
		IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
	}})
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("secret-safe cache headers = %v", response.Header())
	}
	var created opensplunkv1.CreateIndexResponse
	unmarshalResponse(t, response, &created)
	index := created.GetIndex()
	if index.GetIndexId() == "" || index.GetVersion() != 1 || index.GetDefinition().GetName() != "gradethis-prod" ||
		index.GetDefinition().GetDescription() != description || index.GetDefinition().GetLimits() != nil || index.GetDefinition().GetDefaultSourcetype() != "" {
		t.Fatalf("created index = %+v", index)
	}

	response = postProto(t, handler, "/api/v1/indexes/get", &opensplunkv1.GetIndexRequest{Selector: &opensplunkv1.IndexSelector{
		Selector: &opensplunkv1.IndexSelector_IndexName{IndexName: "GRADETHIS-PROD"},
	}})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunkv1.GetIndexResponse
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetIndex(), index) {
		t.Fatalf("get index = %+v, want %+v", got.GetIndex(), index)
	}

	updatedDescription := "retained application logs"
	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: index.GetIndexId()}},
		ExpectedVersion: index.GetVersion(),
		Definition:      &opensplunkv1.IndexDefinition{Description: &updatedDescription},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIndexResponse
	unmarshalResponse(t, response, &updated)
	if updated.GetIndex().GetVersion() != 2 || updated.GetIndex().GetDefinition().GetDescription() != updatedDescription ||
		updated.GetIndex().GetDefinition().GetName() != "gradethis-prod" ||
		updated.GetIndex().GetDefinition().GetIngestionAccess() != opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED {
		t.Fatalf("updated index = %+v", updated.GetIndex())
	}

	response = postProto(t, handler, "/api/v1/indexes/update", &opensplunkv1.UpdateIndexRequest{
		Selector:        &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: index.GetIndexId()}},
		ExpectedVersion: 1,
		Definition:      &opensplunkv1.IndexDefinition{Description: &description},
		UpdateMask:      &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("stale update status = %d, body = %s", response.Code, response.Body.String())
	}

	response = postProto(t, handler, "/api/v1/indexes/state/set", &opensplunkv1.SetIndexStateRequest{
		Selector:        &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: index.GetIndexId()}},
		ExpectedVersion: 2,
		State:           opensplunkv1.IndexState_INDEX_STATE_ARCHIVED,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("state status = %d, body = %s", response.Code, response.Body.String())
	}
	var state opensplunkv1.SetIndexStateResponse
	unmarshalResponse(t, response, &state)
	if state.GetIndex().GetVersion() != 3 || state.GetIndex().GetState() != opensplunkv1.IndexState_INDEX_STATE_ARCHIVED {
		t.Fatalf("state response = %+v", state.GetIndex())
	}
}

func TestAdministrativeListPaginationIsBoundAndTamperSafe(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	ctx := context.Background()
	for _, name := range []string{"alpha", "bravo", "charlie"} {
		if _, err := db.CreateIndex(ctx, adminTestIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	pageSize := uint32(1)
	response := postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("first list status = %d, body = %s", response.Code, response.Body.String())
	}
	var first opensplunkv1.ListIndexesResponse
	unmarshalResponse(t, response, &first)
	if len(first.GetIndexes()) != 1 || first.GetIndexes()[0].GetIndex().GetDefinition().GetName() != "alpha" ||
		first.GetPage().GetTotalSize() != 3 || !first.GetPage().GetTotalSizeExact() || first.GetPage().GetNextPageToken() == "" {
		t.Fatalf("first page = %+v", &first)
	}
	token := first.GetPage().GetNextPageToken()

	tampered := token[:len(token)-1] + "A"
	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &tampered, IncludeTotalSize: true},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("tampered cursor status = %d, body = %s", response.Code, response.Body.String())
	}

	filter := "bravo"
	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &token, IncludeTotalSize: true}, TextFilter: &filter,
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("cross-filter cursor status = %d, body = %s", response.Code, response.Body.String())
	}

	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &token, IncludeTotalSize: true},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("second list status = %d, body = %s", response.Code, response.Body.String())
	}
	var second opensplunkv1.ListIndexesResponse
	unmarshalResponse(t, response, &second)
	if len(second.GetIndexes()) != 1 || second.GetIndexes()[0].GetIndex().GetDefinition().GetName() != "bravo" {
		t.Fatalf("second page = %+v", &second)
	}

	if _, err := db.CreateIndex(ctx, adminTestIndex("delta")); err != nil {
		t.Fatalf("CreateIndex(delta): %v", err)
	}
	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize, PageToken: &token, IncludeTotalSize: true},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("stale cursor status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestIngestionTokenLifecycleReturnsPlaintextOnlyAtCreation(t *testing.T) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandler(t)
	ctx := context.Background()
	for _, name := range []string{"main", "audit"} {
		if _, err := db.CreateIndex(ctx, adminTestIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	description := "application collector"
	expires := timestamppb.New(time.Now().UTC().Add(24 * time.Hour))
	response := postProto(t, handler, "/api/v1/ingestion-tokens/create", &opensplunkv1.CreateIngestionTokenRequest{
		Definition: &opensplunkv1.IngestionTokenDefinition{
			Name: "production", Description: &description,
			Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"main"}}, ExpiresAt: expires,
		},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("create token status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("token response cache headers = %v", response.Header())
	}
	var created opensplunkv1.CreateIngestionTokenResponse
	unmarshalResponse(t, response, &created)
	plaintext := created.GetPlaintextToken()
	token := created.GetIngestionToken()
	if plaintext == "" || token.GetIngestionTokenId() == "" || token.GetVersion() != 1 || token.GetTokenPrefix() == plaintext ||
		!strings.HasPrefix(plaintext, token.GetTokenPrefix()) {
		t.Fatalf("created token metadata = %+v, plaintext length = %d", token, len(plaintext))
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "main"); err != nil {
		t.Fatalf("Authorize(main): %v", err)
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/get", &opensplunkv1.GetIngestionTokenRequest{IngestionTokenId: token.GetIngestionTokenId()})
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(plaintext)) {
		t.Fatalf("get token response leaked plaintext: status %d body %x", response.Code, response.Body.Bytes())
	}
	var got opensplunkv1.GetIngestionTokenResponse
	unmarshalResponse(t, response, &got)
	if got.GetIngestionToken().GetTokenPrefix() != token.GetTokenPrefix() {
		t.Fatalf("get token = %+v", got.GetIngestionToken())
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/update", &opensplunkv1.UpdateIngestionTokenRequest{
		IngestionTokenId: token.GetIngestionTokenId(), ExpectedVersion: 1,
		Definition: &opensplunkv1.IngestionTokenDefinition{Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"audit"}}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"constraints"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("update token status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIngestionTokenResponse
	unmarshalResponse(t, response, &updated)
	if updated.GetIngestionToken().GetVersion() != 2 || !proto.Equal(updated.GetIngestionToken().GetConstraints(), &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"audit"}}) {
		t.Fatalf("updated token = %+v", updated.GetIngestionToken())
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "main"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Authorize(old scope) error = %v, want ErrUnauthorized", err)
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "audit"); err != nil {
		t.Fatalf("Authorize(new scope): %v", err)
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", &opensplunkv1.ListIngestionTokensRequest{})
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(plaintext)) {
		t.Fatalf("list token response leaked plaintext: status %d body %x", response.Code, response.Body.Bytes())
	}
	var listed opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIngestionTokens()) != 1 || listed.GetIngestionTokens()[0].GetIngestionTokenId() != token.GetIngestionTokenId() {
		t.Fatalf("listed tokens = %+v", listed.GetIngestionTokens())
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/revoke", &opensplunkv1.RevokeIngestionTokenRequest{
		IngestionTokenId: token.GetIngestionTokenId(), ExpectedVersion: 2,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("revoke token status = %d, body = %s", response.Code, response.Body.String())
	}
	var revoked opensplunkv1.RevokeIngestionTokenResponse
	unmarshalResponse(t, response, &revoked)
	if revoked.GetIngestionToken().GetVersion() != 3 || revoked.GetIngestionToken().GetState() != opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED ||
		revoked.GetIngestionToken().GetRevokedAt() == nil {
		t.Fatalf("revoked token = %+v", revoked.GetIngestionToken())
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "audit"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Authorize(revoked) error = %v, want ErrUnauthorized", err)
	}
}

func TestIngestionTokenListFiltersSortsAndReportsExactTotals(t *testing.T) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandler(t)
	ctx := context.Background()
	for _, name := range []string{"main", "audit"} {
		if _, err := db.CreateIndex(ctx, adminTestIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%q): %v", name, err)
		}
	}
	alpha, err := tokenStore.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{Name: "Alpha", AllowedIndexNames: []string{"main"}})
	if err != nil {
		t.Fatalf("CreateCollectorToken(alpha): %v", err)
	}
	if _, err := tokenStore.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{Name: "Beta audit", Description: "secondary", AllowedIndexNames: []string{"audit"}}); err != nil {
		t.Fatalf("CreateCollectorToken(beta): %v", err)
	}
	if _, err := tokenStore.RevokeCollectorToken(ctx, alpha.Token.ID, alpha.Token.Version); err != nil {
		t.Fatalf("RevokeCollectorToken(alpha): %v", err)
	}

	indexFilter := "AUDIT"
	textFilter := "beta"
	pageSize := uint32(1)
	response := postProto(t, handler, "/api/v1/ingestion-tokens/list", &opensplunkv1.ListIngestionTokensRequest{
		Page:            &opensplunkv1.PageRequest{PageSize: &pageSize, IncludeTotalSize: true},
		StateFilters:    []opensplunkv1.IngestionTokenState{opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE},
		IndexNameFilter: &indexFilter, TextFilter: &textFilter,
		SortBy:        opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("list token status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIngestionTokens()) != 1 || listed.GetIngestionTokens()[0].GetName() != "Beta audit" ||
		listed.GetPage().GetTotalSize() != 1 || !listed.GetPage().GetTotalSizeExact() || listed.GetPage().GetNextPageToken() != "" {
		t.Fatalf("listed tokens = %+v", &listed)
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", &opensplunkv1.ListIngestionTokensRequest{
		SortBy: opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_LAST_USED_AT,
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported last-used sort status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestAdministrativeValidationAndStatusMapping(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	ctx := context.Background()
	if _, err := db.CreateIndex(ctx, adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}

	tests := []struct {
		name    string
		path    string
		request proto.Message
		status  int
	}{
		{
			name: "missing index", path: "/api/v1/indexes/get",
			request: &opensplunkv1.GetIndexRequest{Selector: &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexName{IndexName: "missing"}}},
			status:  http.StatusNotFound,
		},
		{
			name: "unspecified access", path: "/api/v1/indexes/create",
			request: &opensplunkv1.CreateIndexRequest{Definition: &opensplunkv1.IndexDefinition{Name: "invalid"}},
			status:  http.StatusBadRequest,
		},
		{
			name: "duplicate index", path: "/api/v1/indexes/create",
			request: &opensplunkv1.CreateIndexRequest{Definition: adminTestIndexProto("main")},
			status:  http.StatusConflict,
		},
		{
			name: "unsupported stats", path: "/api/v1/indexes/list",
			request: &opensplunkv1.ListIndexesRequest{IncludeStats: true},
			status:  http.StatusBadRequest,
		},
		{
			name: "present empty index idempotency key", path: "/api/v1/indexes/create",
			request: &opensplunkv1.CreateIndexRequest{
				Definition: adminTestIndexProto("empty-index-idempotency"), ClientRequestId: stringPointer(""),
			},
			status: http.StatusBadRequest,
		},
		{
			name: "unenforced default sourcetype", path: "/api/v1/indexes/create",
			request: func() proto.Message {
				definition := adminTestIndexProto("sourcetype-policy")
				definition.DefaultSourcetype = stringPointer("go:zap:json")
				return &opensplunkv1.CreateIndexRequest{Definition: definition}
			}(),
			status: http.StatusBadRequest,
		},
		{
			name: "unenforced per-index limits", path: "/api/v1/indexes/create",
			request: func() proto.Message {
				definition := adminTestIndexProto("limits-policy")
				definition.Limits = &opensplunkv1.IndexLimits{MaxEventBytes: uint64Pointer(1024)}
				return &opensplunkv1.CreateIndexRequest{Definition: definition}
			}(),
			status: http.StatusBadRequest,
		},
		{
			name: "unsupported token constraints", path: "/api/v1/ingestion-tokens/create",
			request: &opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: "bad", Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"main"}, AllowedHostRegexes: []string{".*"}},
			}},
			status: http.StatusBadRequest,
		},
		{
			name: "present empty token idempotency key", path: "/api/v1/ingestion-tokens/create",
			request: &opensplunkv1.CreateIngestionTokenRequest{
				Definition: &opensplunkv1.IngestionTokenDefinition{
					Name: "empty-token-idempotency", Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"main"}},
				},
				ClientRequestId: stringPointer(""),
			},
			status: http.StatusBadRequest,
		},
		{
			name: "present empty revocation reason", path: "/api/v1/ingestion-tokens/revoke",
			request: &opensplunkv1.RevokeIngestionTokenRequest{
				IngestionTokenId: "tok_missing", ExpectedVersion: 1, Reason: stringPointer(""),
			},
			status: http.StatusBadRequest,
		},
		{
			name: "missing token", path: "/api/v1/ingestion-tokens/get",
			request: &opensplunkv1.GetIngestionTokenRequest{IngestionTokenId: "tok_missing"},
			status:  http.StatusNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := postProto(t, handler, test.path, test.request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, test.status, response.Body.String())
			}
		})
	}
}

func TestAdministrativeCapabilitiesDoNotOverstatePartialRouteFamilies(t *testing.T) {
	t.Parallel()

	handler, _, _ := newAdminIntegrationHandler(t)
	response := postProto(t, handler, "/api/v1/system/bootstrap", &opensplunkv1.GetSystemBootstrapRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("bootstrap status = %d, body = %s", response.Code, response.Body.String())
	}
	var bootstrap opensplunkv1.GetSystemBootstrapResponse
	unmarshalResponse(t, response, &bootstrap)
	if containsFeature(bootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_INDEX_ADMIN) ||
		containsFeature(bootstrap.GetFeatures(), opensplunkv1.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN) {
		t.Fatalf("bootstrap features = %v", bootstrap.GetFeatures())
	}
}

func TestCommittedAdministrativeSuccessWinsContextCancellationRace(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapAdministrativeCallError(ctx, nil, "ingestion token"); err != nil {
		t.Fatalf("committed operation mapped to error = %v", err)
	}
}

func TestAdministrativeRoutesRejectDNSRebindingAndCrossOriginBrowsers(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	if _, err := db.CreateIndex(context.Background(), adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	requestMessage := &opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
		Name: "browser", Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"main"}},
	}}

	for name, headers := range map[string]map[string]string{
		"dns rebinding host": {"Host": "attacker.example", "Origin": "http://attacker.example"},
		"foreign origin":     {"Host": "example.com", "Origin": "http://attacker.example"},
		"cross-site fetch":   {"Host": "example.com", "Origin": "http://example.com", "Sec-Fetch-Site": "cross-site"},
	} {
		t.Run(name, func(t *testing.T) {
			response := postProtoHeaders(t, handler, "/api/v1/ingestion-tokens/create", requestMessage, headers)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}

	response := postProtoHeaders(t, handler, "/api/v1/ingestion-tokens/create", requestMessage, map[string]string{
		"Host": "example.com", "Origin": "http://example.com", "Sec-Fetch-Site": "same-origin",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("same-origin status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestHandlerRejectsTenantIdentityBeyondDurableStorageLimit(t *testing.T) {
	t.Parallel()

	_, err := NewHandler(Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
		WebUI: testUI(), TenantID: strings.Repeat("t", maximumIdentityBytes+1),
	})
	if err == nil || !strings.Contains(err.Error(), "identity is invalid") {
		t.Fatalf("NewHandler oversized tenant error = %v", err)
	}
}

func newAdminIntegrationHandler(t *testing.T) (http.Handler, *control.DB, *auth.Store) {
	t.Helper()
	db, err := control.Open(context.Background(), t.TempDir()+"/control.sqlite")
	if err != nil {
		t.Fatalf("control.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("control DB close: %v", err)
		}
	})
	tokens, err := auth.NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("auth.NewStore: %v", err)
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: db, IngestionTokens: tokens,
		SavedSearches: &fakeSavedSearches{}, WebUI: testUI(), Now: func() time.Time { return testNow },
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	return handler, db, tokens
}

func adminTestIndex(name string) control.IndexDefinition {
	return control.IndexDefinition{Name: name, DisplayName: name, IngestionEnabled: true, SearchEnabled: true}
}

func adminTestIndexProto(name string) *opensplunkv1.IndexDefinition {
	return &opensplunkv1.IndexDefinition{
		Name: name, DisplayName: name,
		IngestionAccess: opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
		SearchAccess:    opensplunkv1.IndexAccessState_INDEX_ACCESS_STATE_ENABLED,
	}
}

func containsFeature(features []opensplunkv1.ServerFeature, target opensplunkv1.ServerFeature) bool {
	for _, feature := range features {
		if feature == target {
			return true
		}
	}
	return false
}

func postProtoHeaders(t *testing.T, handler http.Handler, path string, message proto.Message, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	request.Header.Set("Content-Type", "application/x-protobuf")
	for name, value := range headers {
		if name == "Host" {
			request.Host = value
		} else {
			request.Header.Set(name, value)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}
