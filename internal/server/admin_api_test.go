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

	response = postProto(t, handler, "/api/v1/indexes/delete", &opensplunkv1.DeleteIndexRequest{
		Selector: &opensplunkv1.IndexSelector{
			Selector: &opensplunkv1.IndexSelector_IndexName{IndexName: "GRADETHIS-PROD"},
		},
		ExpectedVersion:  3,
		DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
		ConfirmationName: "gradethis-prod",
	})
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}
	var deleted opensplunkv1.DeleteIndexResponse
	unmarshalResponse(t, response, &deleted)
	if deleted.GetIndexId() != index.GetIndexId() || deleted.DeletionOperationId != nil {
		t.Fatalf("delete response = %+v", &deleted)
	}

	response = postProto(t, handler, "/api/v1/indexes/get", &opensplunkv1.GetIndexRequest{Selector: &opensplunkv1.IndexSelector{
		Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: index.GetIndexId()},
	}})
	if response.Code != http.StatusNotFound {
		t.Fatalf("deleted get status = %d, body = %s", response.Code, response.Body.String())
	}
	response = postProto(t, handler, "/api/v1/indexes/list", &opensplunkv1.ListIndexesRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("post-delete list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListIndexesResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIndexes()) != 0 {
		t.Fatalf("post-delete indexes = %+v", listed.GetIndexes())
	}
	response = postProto(t, handler, "/api/v1/indexes/create", &opensplunkv1.CreateIndexRequest{
		Definition: adminTestIndexProto("gradethis-prod"),
	})
	if response.Code != http.StatusConflict {
		t.Fatalf("reused name status = %d, body = %s", response.Code, response.Body.String())
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

	replacement := "A"
	if token[len(token)-1] == 'A' {
		replacement = "B"
	}
	tampered := token[:len(token)-1] + replacement
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

func TestDecodeAdminBase64RejectsNonCanonicalTrailingBits(t *testing.T) {
	t.Parallel()

	// AA is the canonical RawURL encoding of one zero byte. AB decodes to the
	// same byte when unused trailing bits are ignored, but cursors accept only
	// one canonical spelling so an altered token can never verify unchanged.
	if decoded, err := decodeAdminBase64("AA"); err != nil || !bytes.Equal(decoded, []byte{0}) {
		t.Fatalf("decode canonical base64 = %v, %v", decoded, err)
	}
	if _, err := decodeAdminBase64("AB"); err == nil {
		t.Fatal("non-canonical base64 with altered trailing bits was accepted")
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
	boundCollectorID := "collector-production"
	expires := timestamppb.New(time.Now().UTC().Add(24 * time.Hour))
	response := postProto(t, handler, "/api/v1/ingestion-tokens/create", &opensplunkv1.CreateIngestionTokenRequest{
		Definition: &opensplunkv1.IngestionTokenDefinition{
			Name: "production", Description: &description,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"main"}, BoundCollectorId: &boundCollectorID,
			},
			ExpiresAt: expires,
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
		!strings.HasPrefix(plaintext, token.GetTokenPrefix()) ||
		token.GetConstraints().GetBoundCollectorId() != boundCollectorID {
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
	if got.GetIngestionToken().GetTokenPrefix() != token.GetTokenPrefix() ||
		got.GetIngestionToken().GetConstraints().GetBoundCollectorId() != boundCollectorID {
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
	if updated.GetIngestionToken().GetVersion() != 2 || !proto.Equal(
		updated.GetIngestionToken().GetConstraints(),
		&opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"audit"}, BoundCollectorId: &boundCollectorID,
		},
	) {
		t.Fatalf("updated token = %+v", updated.GetIngestionToken())
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "main"); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf("Authorize(old scope) error = %v, want ErrUnauthorized", err)
	}
	if _, err := tokenStore.Authorize(ctx, plaintext, "audit"); err != nil {
		t.Fatalf("Authorize(new scope): %v", err)
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/update", &opensplunkv1.UpdateIngestionTokenRequest{
		IngestionTokenId: token.GetIngestionTokenId(), ExpectedVersion: 2,
		Definition: &opensplunkv1.IngestionTokenDefinition{Constraints: &opensplunkv1.IngestionTokenConstraints{
			BoundCollectorId: &boundCollectorID,
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"constraints.bound_collector_id"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("same-binding update status = %d, body = %s", response.Code, response.Body.String())
	}
	var rebound opensplunkv1.UpdateIngestionTokenResponse
	unmarshalResponse(t, response, &rebound)
	if rebound.GetIngestionToken().GetVersion() != 3 ||
		rebound.GetIngestionToken().GetConstraints().GetBoundCollectorId() != boundCollectorID ||
		len(rebound.GetIngestionToken().GetConstraints().GetAllowedIndexNames()) != 1 ||
		rebound.GetIngestionToken().GetConstraints().GetAllowedIndexNames()[0] != "audit" {
		t.Fatalf("same-binding update = %+v", rebound.GetIngestionToken())
	}

	differentCollectorID := "collector-other"
	response = postProto(t, handler, "/api/v1/ingestion-tokens/update", &opensplunkv1.UpdateIngestionTokenRequest{
		IngestionTokenId: token.GetIngestionTokenId(), ExpectedVersion: 3,
		Definition: &opensplunkv1.IngestionTokenDefinition{Constraints: &opensplunkv1.IngestionTokenConstraints{
			BoundCollectorId: &differentCollectorID,
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"constraints.bound_collector_id"}},
	})
	if response.Code != http.StatusConflict ||
		bytes.Contains(response.Body.Bytes(), []byte(boundCollectorID)) ||
		bytes.Contains(response.Body.Bytes(), []byte(differentCollectorID)) {
		t.Fatalf("immutable-binding response = status %d body %q", response.Code, response.Body.String())
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", &opensplunkv1.ListIngestionTokensRequest{})
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(plaintext)) {
		t.Fatalf("list token response leaked plaintext: status %d body %x", response.Code, response.Body.Bytes())
	}
	var listed opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIngestionTokens()) != 1 ||
		listed.GetIngestionTokens()[0].GetIngestionTokenId() != token.GetIngestionTokenId() ||
		listed.GetIngestionTokens()[0].GetConstraints().GetBoundCollectorId() != boundCollectorID {
		t.Fatalf("listed tokens = %+v", listed.GetIngestionTokens())
	}

	response = postProto(t, handler, "/api/v1/ingestion-tokens/revoke", &opensplunkv1.RevokeIngestionTokenRequest{
		IngestionTokenId: token.GetIngestionTokenId(), ExpectedVersion: 3,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("revoke token status = %d, body = %s", response.Code, response.Body.String())
	}
	var revoked opensplunkv1.RevokeIngestionTokenResponse
	unmarshalResponse(t, response, &revoked)
	if revoked.GetIngestionToken().GetVersion() != 4 || revoked.GetIngestionToken().GetState() != opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED ||
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
	alpha, err := tokenStore.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name: "Alpha", BoundCollectorID: "collector-alpha", AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(alpha): %v", err)
	}
	if _, err := tokenStore.CreateCollectorToken(ctx, auth.CreateCollectorTokenRequest{
		Name: "Beta audit", Description: "secondary", BoundCollectorID: "collector-beta",
		AllowedIndexNames: []string{"audit"},
	}); err != nil {
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
}

func TestPrunedIngestionTokenTombstonesDisappearFromAdministrativeCatalog(t *testing.T) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandlerWithTokenOptions(
		t,
		auth.StoreOptions{RetainedRevokedTokenLimit: 3},
	)
	ctx := context.Background()
	if _, err := db.CreateIndex(ctx, adminTestIndex("audit")); err != nil {
		t.Fatalf("CreateIndex(audit): %v", err)
	}

	createToken := func(name, description, collectorID string) auth.IssuedCollectorToken {
		t.Helper()
		issued, err := tokenStore.CreateCollectorToken(
			ctx,
			auth.CreateCollectorTokenRequest{
				Name:              name,
				Description:       description,
				BoundCollectorID:  collectorID,
				AllowedIndexNames: []string{"audit"},
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, err)
		}
		return issued
	}

	matching := []auth.IssuedCollectorToken{
		createToken("Bravo retention", "retention fixture", "collector-bravo"),
		createToken("Charlie retention", "retention fixture", "collector-charlie"),
		createToken("Delta retention", "retention fixture", "collector-delta"),
	}
	trigger := createToken(
		"Echo pruning trigger",
		"cursor-excluded fixture",
		"collector-echo",
	)
	for tokenIndex := range matching {
		if _, err := tokenStore.RevokeCollectorToken(
			ctx,
			matching[tokenIndex].Token.ID,
			matching[tokenIndex].Token.Version,
		); err != nil {
			t.Fatalf(
				"RevokeCollectorToken(%s): %v",
				matching[tokenIndex].Token.Name,
				err,
			)
		}
	}

	indexFilter := "AUDIT"
	textFilter := "RETENTION FIXTURE"
	pageSize := uint32(1)
	states := []opensplunkv1.IngestionTokenState{
		opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED,
	}
	listRequest := func(pageToken string) *opensplunkv1.ListIngestionTokensRequest {
		page := &opensplunkv1.PageRequest{
			PageSize:         &pageSize,
			IncludeTotalSize: true,
		}
		if pageToken != "" {
			page.PageToken = &pageToken
		}
		return &opensplunkv1.ListIngestionTokensRequest{
			Page:            page,
			StateFilters:    states,
			IndexNameFilter: &indexFilter,
			TextFilter:      &textFilter,
			SortBy:          opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME,
			SortDirection:   opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		}
	}

	response := postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		listRequest(""),
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"list before prune status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var before opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &before)
	if len(before.GetIngestionTokens()) != 1 ||
		before.GetIngestionTokens()[0].GetIngestionTokenId() !=
			matching[0].Token.ID ||
		before.GetPage().GetTotalSize() != uint64(len(matching)) ||
		!before.GetPage().GetTotalSizeExact() ||
		before.GetPage().GetNextPageToken() == "" {
		t.Fatalf("list before prune = %+v", &before)
	}
	staleCursor := before.GetPage().GetNextPageToken()

	revokedTrigger, err := tokenStore.RevokeCollectorToken(
		ctx,
		trigger.Token.ID,
		trigger.Token.Version,
	)
	if err != nil {
		t.Fatalf("RevokeCollectorToken(trigger): %v", err)
	}
	if revokedTrigger.ID != trigger.Token.ID ||
		revokedTrigger.State != auth.CollectorTokenStateRevoked {
		t.Fatalf("revoked current token = %#v", revokedTrigger)
	}

	retainedMatching := make([]auth.IssuedCollectorToken, 0, 2)
	prunedMatching := make([]auth.IssuedCollectorToken, 0, 1)
	for tokenIndex := range matching {
		response = postProto(
			t,
			handler,
			"/api/v1/ingestion-tokens/get",
			&opensplunkv1.GetIngestionTokenRequest{
				IngestionTokenId: matching[tokenIndex].Token.ID,
			},
		)
		switch response.Code {
		case http.StatusOK:
			var retained opensplunkv1.GetIngestionTokenResponse
			unmarshalResponse(t, response, &retained)
			if retained.GetIngestionToken().GetIngestionTokenId() !=
				matching[tokenIndex].Token.ID ||
				retained.GetIngestionToken().GetState() !=
					opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED {
				t.Fatalf(
					"retained matching token = %+v",
					retained.GetIngestionToken(),
				)
			}
			retainedMatching = append(retainedMatching, matching[tokenIndex])
		case http.StatusNotFound:
			prunedMatching = append(prunedMatching, matching[tokenIndex])
		default:
			t.Fatalf(
				"get matching token %q status = %d, body = %s",
				matching[tokenIndex].Token.Name,
				response.Code,
				response.Body.String(),
			)
		}
	}
	if len(prunedMatching) != 1 || len(retainedMatching) != 2 {
		t.Fatalf(
			"post-prune matching tokens = %d pruned/%d retained, want 1/2",
			len(prunedMatching),
			len(retainedMatching),
		)
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: trigger.Token.ID,
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"get retained trigger token status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var retainedTrigger opensplunkv1.GetIngestionTokenResponse
	unmarshalResponse(t, response, &retainedTrigger)
	if retainedTrigger.GetIngestionToken().GetIngestionTokenId() !=
		trigger.Token.ID ||
		retainedTrigger.GetIngestionToken().GetState() !=
			opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED {
		t.Fatalf(
			"retained trigger token = %+v",
			retainedTrigger.GetIngestionToken(),
		)
	}

	// The trigger token is excluded by the unchanged text filter both before
	// and after its revocation. The cursor's offset (1) also remains within the
	// two-row result, so only physical removal of one matching tombstone can
	// make this otherwise valid cursor stale.
	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		listRequest(staleCursor),
	)
	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"stale pre-prune cursor status = %d, want %d; body = %s",
			response.Code,
			http.StatusBadRequest,
			response.Body.String(),
		)
	}

	cursor := ""
	for pageIndex := range retainedMatching {
		response = postProto(
			t,
			handler,
			"/api/v1/ingestion-tokens/list",
			listRequest(cursor),
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"fresh page %d status = %d, body = %s",
				pageIndex,
				response.Code,
				response.Body.String(),
			)
		}
		var page opensplunkv1.ListIngestionTokensResponse
		unmarshalResponse(t, response, &page)
		if len(page.GetIngestionTokens()) != 1 ||
			page.GetIngestionTokens()[0].GetIngestionTokenId() !=
				retainedMatching[pageIndex].Token.ID ||
			page.GetPage().GetTotalSize() != uint64(len(retainedMatching)) ||
			!page.GetPage().GetTotalSizeExact() {
			t.Fatalf("fresh page %d = %+v", pageIndex, &page)
		}
		cursor = page.GetPage().GetNextPageToken()
		if (pageIndex < len(retainedMatching)-1) != (cursor != "") {
			t.Fatalf(
				"fresh page %d next cursor presence = %t",
				pageIndex,
				cursor != "",
			)
		}
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		&opensplunkv1.ListIngestionTokensRequest{
			Page: &opensplunkv1.PageRequest{
				IncludeTotalSize: true,
			},
			StateFilters:    states,
			IndexNameFilter: &indexFilter,
			TextFilter:      &textFilter,
			SortBy:          opensplunkv1.IngestionTokenSortBy_INGESTION_TOKEN_SORT_BY_NAME,
			SortDirection:   opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"revoked filter status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var revokedOnly opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &revokedOnly)
	if len(revokedOnly.GetIngestionTokens()) != len(retainedMatching) ||
		revokedOnly.GetPage().GetTotalSize() != uint64(len(retainedMatching)) ||
		!revokedOnly.GetPage().GetTotalSizeExact() ||
		revokedOnly.GetPage().GetNextPageToken() != "" {
		t.Fatalf("revoked filtered list = %+v", &revokedOnly)
	}
	for tokenIndex := range retainedMatching {
		descendingIndex := len(retainedMatching) - 1 - tokenIndex
		if revokedOnly.GetIngestionTokens()[tokenIndex].
			GetIngestionTokenId() !=
			retainedMatching[descendingIndex].Token.ID {
			t.Fatalf("revoked filtered list = %+v", &revokedOnly)
		}
	}
}

func TestAdministrativeIngestionTokenCapacityIsSanitizedAndRecoverable(
	t *testing.T,
) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandlerWithTokenOptions(
		t,
		auth.StoreOptions{
			RetainedRevokedTokenLimit: 1,
			TotalTokenRecordLimit:     2,
		},
	)
	ctx := context.Background()
	if _, err := db.CreateIndex(ctx, adminTestIndex("audit")); err != nil {
		t.Fatalf("CreateIndex(audit): %v", err)
	}
	createDirect := func(name, collectorID string) auth.IssuedCollectorToken {
		t.Helper()
		issued, err := tokenStore.CreateCollectorToken(
			ctx,
			auth.CreateCollectorTokenRequest{
				Name:              name,
				BoundCollectorID:  collectorID,
				AllowedIndexNames: []string{"audit"},
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, err)
		}
		return issued
	}
	first := createDirect("first capacity fixture", "collector-first")
	second := createDirect("second capacity fixture", "collector-second")

	request := &opensplunkv1.CreateIngestionTokenRequest{
		Definition: &opensplunkv1.IngestionTokenDefinition{
			Name: "replacement capacity fixture",
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"audit"},
				BoundCollectorId:  stringPointer("collector-replacement"),
			},
		},
	}
	response := postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/create",
		request,
	)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"full-catalog create status = %d, want %d; body = %s",
			response.Code,
			http.StatusTooManyRequests,
			response.Body.String(),
		)
	}
	body := response.Body.String()
	if !strings.Contains(body, "ingestion token capacity is exhausted") {
		t.Fatalf("full-catalog response is not generic: %q", body)
	}
	for _, sensitive := range []string{
		first.Token.ID,
		first.Token.Prefix,
		first.Secret.Plaintext(),
		second.Token.ID,
		second.Token.Prefix,
		second.Secret.Plaintext(),
		"ingestion_tokens",
		"SQLite",
		"database",
	} {
		if strings.Contains(body, sensitive) {
			t.Fatalf(
				"full-catalog response disclosed %q: %q",
				sensitive,
				body,
			)
		}
	}

	if _, err := tokenStore.RevokeCollectorToken(
		ctx,
		first.Token.ID,
		first.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}
	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/create",
		request,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"recovered create status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var created opensplunkv1.CreateIngestionTokenResponse
	unmarshalResponse(t, response, &created)
	if created.GetIngestionToken().GetName() !=
		request.GetDefinition().GetName() ||
		created.GetPlaintextToken() == "" {
		t.Fatalf("recovered create = %+v", &created)
	}
	if _, err := tokenStore.GetCollectorToken(
		ctx,
		first.Token.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetCollectorToken(compacted first) error = %v, want ErrNotFound",
			err,
		)
	}
	if _, err := tokenStore.GetCollectorToken(ctx, second.Token.ID); err != nil {
		t.Fatalf("GetCollectorToken(second retained): %v", err)
	}
}

func TestAdministrativeRevokePruneFailureIsSanitizedAndRollsBack(
	t *testing.T,
) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandlerWithTokenOptions(
		t,
		auth.StoreOptions{RetainedRevokedTokenLimit: 1},
	)
	ctx := context.Background()
	if _, err := db.CreateIndex(ctx, adminTestIndex("audit")); err != nil {
		t.Fatalf("CreateIndex(audit): %v", err)
	}
	createToken := func(name, collectorID string) auth.IssuedCollectorToken {
		t.Helper()
		issued, err := tokenStore.CreateCollectorToken(
			ctx,
			auth.CreateCollectorTokenRequest{
				Name:              name,
				Description:       "prune rollback fixture",
				BoundCollectorID:  collectorID,
				AllowedIndexNames: []string{"audit"},
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, err)
		}
		return issued
	}
	first := createToken("first revoked", "collector-first")
	current := createToken("current rollback", "collector-current")
	if _, err := tokenStore.RevokeCollectorToken(
		ctx,
		first.Token.ID,
		first.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}

	const pruneFailureDetail = "forced token-prune database-secret"
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER reject_administrative_revoked_token_prune
		BEFORE DELETE ON ingestion_tokens
		WHEN OLD.state = 'revoked'
		BEGIN
			SELECT RAISE(ABORT, 'forced token-prune database-secret');
		END`); err != nil {
		t.Fatalf("create prune failure trigger: %v", err)
	}

	response := postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/revoke",
		&opensplunkv1.RevokeIngestionTokenRequest{
			IngestionTokenId: current.Token.ID,
			ExpectedVersion:  current.Token.Version,
		},
	)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf(
			"failed-prune revoke status = %d, want %d; body = %s",
			response.Code,
			http.StatusServiceUnavailable,
			response.Body.String(),
		)
	}
	body := response.Body.String()
	if !strings.Contains(body, "ingestion token service is unavailable") {
		t.Fatalf("failed-prune response is not generic: %q", body)
	}
	for _, sensitive := range []string{
		pruneFailureDetail,
		"reject_administrative_revoked_token_prune",
		"SQLite",
		"database",
		first.Token.ID,
		first.Token.Prefix,
		first.Secret.Plaintext(),
		current.Token.ID,
		current.Token.Prefix,
		current.Secret.Plaintext(),
	} {
		if strings.Contains(body, sensitive) {
			t.Fatalf(
				"failed-prune response disclosed %q: %q",
				sensitive,
				body,
			)
		}
	}

	firstAfter, err := tokenStore.GetCollectorToken(ctx, first.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(first after rollback): %v", err)
	}
	if firstAfter.State != auth.CollectorTokenStateRevoked ||
		firstAfter.Version != first.Token.Version+1 ||
		firstAfter.RevokedAt.IsZero() {
		t.Fatalf("first tombstone after rollback = %#v", firstAfter)
	}
	currentAfter, err := tokenStore.GetCollectorToken(ctx, current.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(current after rollback): %v", err)
	}
	if currentAfter.State != auth.CollectorTokenStateActive ||
		currentAfter.Version != current.Token.Version ||
		!currentAfter.RevokedAt.IsZero() {
		t.Fatalf("current token after rollback = %#v", currentAfter)
	}
	if _, err := tokenStore.Authorize(
		ctx,
		first.Secret.Plaintext(),
		"audit",
	); !errors.Is(err, auth.ErrUnauthorized) {
		t.Fatalf(
			"Authorize(first after rollback) error = %v, want ErrUnauthorized",
			err,
		)
	}
	if _, err := tokenStore.Authorize(
		ctx,
		current.Secret.Plaintext(),
		"audit",
	); err != nil {
		t.Fatalf("Authorize(current after rollback): %v", err)
	}
	for _, table := range []string{
		"ingestion_tokens",
		"ingestion_token_indexes",
	} {
		var rows int64
		query := db.GORMDB().WithContext(ctx).Table(table).Count(&rows)
		if query.Error != nil {
			t.Fatalf("count %s after rollback: %v", table, query.Error)
		}
		if rows != 2 {
			t.Fatalf("%s rows after rollback = %d, want 2", table, rows)
		}
	}
}

func TestAdministrativeValidationAndStatusMapping(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	ctx := context.Background()
	if _, err := db.CreateIndex(ctx, adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	deletionTarget, err := db.CreateIndex(ctx, adminTestIndex("delete-target"))
	if err != nil {
		t.Fatalf("CreateIndex(delete-target): %v", err)
	}
	deletionTarget, err = db.SetIndexState(
		ctx,
		deletionTarget.ID,
		deletionTarget.Version,
		control.IndexStateArchived,
	)
	if err != nil {
		t.Fatalf("archive delete-target: %v", err)
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
			name: "sub-millisecond index retention", path: "/api/v1/indexes/create",
			request: func() proto.Message {
				definition := adminTestIndexProto("sub-millisecond-retention")
				definition.RetentionPeriod = durationpb.New(time.Nanosecond)
				return &opensplunkv1.CreateIndexRequest{Definition: definition}
			}(),
			status: http.StatusBadRequest,
		},
		{
			name: "unspecified index deletion mode", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:        &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: deletionTarget.ID}},
				ExpectedVersion: deletionTarget.Version, ConfirmationName: deletionTarget.Definition.Name,
			},
			status: http.StatusBadRequest,
		},
		{
			name: "physical index deletion is unavailable", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:         &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: deletionTarget.ID}},
				ExpectedVersion:  deletionTarget.Version,
				DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_DELETE_DATA,
				ConfirmationName: deletionTarget.Definition.Name,
			},
			status: http.StatusBadRequest,
		},
		{
			name: "noncanonical index delete confirmation", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:         &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: deletionTarget.ID}},
				ExpectedVersion:  deletionTarget.Version,
				DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
				ConfirmationName: " DELETE-TARGET ",
			},
			status: http.StatusBadRequest,
		},
		{
			name: "wrong index delete confirmation", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:         &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: deletionTarget.ID}},
				ExpectedVersion:  deletionTarget.Version,
				DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
				ConfirmationName: "main",
			},
			status: http.StatusBadRequest,
		},
		{
			name: "stale index deletion", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:         &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexId{IndexId: deletionTarget.ID}},
				ExpectedVersion:  deletionTarget.Version - 1,
				DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
				ConfirmationName: deletionTarget.Definition.Name,
			},
			status: http.StatusConflict,
		},
		{
			name: "active index deletion", path: "/api/v1/indexes/delete",
			request: &opensplunkv1.DeleteIndexRequest{
				Selector:         &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexName{IndexName: "main"}},
				ExpectedVersion:  1,
				DataDeletionMode: opensplunkv1.IndexDataDeletionMode_INDEX_DATA_DELETION_MODE_KEEP_DATA,
				ConfirmationName: "main",
			},
			status: http.StatusConflict,
		},
		{
			name: "managed deleting state", path: "/api/v1/indexes/state/set",
			request: &opensplunkv1.SetIndexStateRequest{
				Selector:        &opensplunkv1.IndexSelector{Selector: &opensplunkv1.IndexSelector_IndexName{IndexName: "main"}},
				ExpectedVersion: 1,
				State:           opensplunkv1.IndexState_INDEX_STATE_DELETING,
			},
			status: http.StatusBadRequest,
		},
		{
			name: "unsupported token constraints", path: "/api/v1/ingestion-tokens/create",
			request: &opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: "bad", Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"}, AllowedHostRegexes: []string{".*"},
					BoundCollectorId: stringPointer("collector-bad"),
				},
			}},
			status: http.StatusBadRequest,
		},
		{
			name: "missing token collector binding", path: "/api/v1/ingestion-tokens/create",
			request: &opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: "unbound", Constraints: &opensplunkv1.IngestionTokenConstraints{AllowedIndexNames: []string{"main"}},
			}},
			status: http.StatusBadRequest,
		},
		{
			name: "present empty token idempotency key", path: "/api/v1/ingestion-tokens/create",
			request: &opensplunkv1.CreateIngestionTokenRequest{
				Definition: &opensplunkv1.IngestionTokenDefinition{
					Name: "empty-token-idempotency", Constraints: &opensplunkv1.IngestionTokenConstraints{
						AllowedIndexNames: []string{"main"}, BoundCollectorId: stringPointer("collector-idempotency"),
					},
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
	assertHTTPErrorStatus(
		t,
		mapAdministrativeCallError(
			context.Background(),
			control.ErrDependencyConflict,
			"index",
		),
		http.StatusConflict,
	)
}

func TestAdministrativeRoutesRejectDNSRebindingAndCrossOriginBrowsers(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	if _, err := db.CreateIndex(context.Background(), adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	requestMessage := &opensplunkv1.CreateIngestionTokenRequest{Definition: &opensplunkv1.IngestionTokenDefinition{
		Name: "browser", Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"main"}, BoundCollectorId: stringPointer("collector-browser"),
		},
	}}

	for name, headers := range map[string]map[string]string{
		"dns rebinding host":    {"Host": "attacker.example", "Origin": "http://attacker.example"},
		"foreign origin":        {"Host": "example.com", "Origin": "http://attacker.example"},
		"cross-site fetch":      {"Host": "example.com", "Origin": "http://example.com", "Sec-Fetch-Site": "cross-site"},
		"opaque origin":         {"Host": "example.com", "Origin": "null"},
		"different port":        {"Host": "example.com", "Origin": "http://example.com:8080"},
		"different scheme":      {"Host": "example.com", "Origin": "https://example.com"},
		"empty query delimiter": {"Host": "example.com", "Origin": "http://example.com?"},
		"empty fragment":        {"Host": "example.com", "Origin": "http://example.com#"},
		"empty port":            {"Host": "example.com:", "Origin": "http://example.com:"},
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

	payload, err := proto.Marshal(requestMessage)
	if err != nil {
		t.Fatalf("marshal duplicate-origin request: %v", err)
	}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/v1/ingestion-tokens/create", bytes.NewReader(payload))
	request.Host = "example.com"
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Add("Origin", "http://example.com")
	request.Header.Add("Origin", "http://example.com")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("duplicate-origin status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestHandlerRejectsInvalidTenantIdentity(t *testing.T) {
	t.Parallel()

	for name, tenantID := range map[string]string{
		"oversized":     strings.Repeat("t", maximumIdentityBytes+1),
		"control":       "tenant\x00boundary",
		"invalid UTF-8": string([]byte{0xff}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewHandler(Config{
				SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SavedSearches: &fakeSavedSearches{},
				WebUI: testUI(), TenantID: tenantID,
			})
			if err == nil || !strings.Contains(err.Error(), "identity is invalid") {
				t.Fatalf("NewHandler tenant error = %v", err)
			}
		})
	}
}

const adminIntegrationBearerToken = "open-splunk-administrator-test-token-0123456789"

type adminIntegrationHandler struct {
	raw   http.Handler
	token string
}

func (handler *adminIntegrationHandler) ServeHTTP(
	response http.ResponseWriter,
	request *http.Request,
) {
	request = request.Clone(request.Context())
	request.Header = request.Header.Clone()
	request.Header.Set("Authorization", "Bearer "+handler.token)
	handler.raw.ServeHTTP(response, request)
}

func newAdminIntegrationHandler(t *testing.T) (*adminIntegrationHandler, *control.DB, *auth.Store) {
	t.Helper()
	return newAdminIntegrationHandlerWithTokenOptions(t, auth.StoreOptions{})
}

func newAdminIntegrationHandlerWithTokenOptions(
	t *testing.T,
	options auth.StoreOptions,
) (*adminIntegrationHandler, *control.DB, *auth.Store) {
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
	digestKey := []byte("0123456789abcdef0123456789abcdef")
	tokens, err := auth.NewStoreWithOptions(db, digestKey, options)
	if err != nil {
		t.Fatalf("auth.NewStoreWithOptions: %v", err)
	}
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
		SearchJobs: &fakeSearchJobs{}, Indexes: db, IngestionTokens: tokens,
		SavedSearches: &fakeSavedSearches{}, WebUI: testUI(), Now: func() time.Time { return testNow },
		AdministrativeAllowedHosts: []string{"example.com"},
		BrowserAuthenticator:       browserAuthenticator,
	})
	return &adminIntegrationHandler{
		raw:   raw,
		token: adminIntegrationBearerToken,
	}, db, tokens
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
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, bytes.NewReader(payload))
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
