package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestIngestionTokenPurposeParsingPreservesLegacyNativeWithoutInferringHEC(t *testing.T) {
	t.Parallel()

	binding := "collector-legacy-wire"
	legacy, err := tokenDefinitionFromProto(&opensplunkv1.IngestionTokenDefinition{
		Name: "legacy native",
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"main"},
			BoundCollectorId:  &binding,
		},
	})
	if err != nil {
		t.Fatalf("tokenDefinitionFromProto(legacy native): %v", err)
	}
	if legacy.Purpose != auth.IngestionTokenPurposeNativeCollector ||
		legacy.BoundCollectorID != binding ||
		legacy.HECProfile != (auth.HECTokenProfile{}) {
		t.Fatalf("legacy native definition = %#v", legacy)
	}

	if _, err := tokenDefinitionFromProto(&opensplunkv1.IngestionTokenDefinition{
		Name: "ambiguous unbound",
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"main"},
		},
		HecProfile: &opensplunkv1.IngestionTokenHecProfile{},
	}); err == nil || !strings.Contains(err.Error(), "purpose is required") {
		t.Fatalf("unspecified unbound purpose error = %v", err)
	}

	defaultIndex := "main"
	defaultHost := "hec-producer"
	defaultSource := "/var/log/app.json"
	defaultSourcetype := "_json"
	hec, err := tokenDefinitionFromProto(&opensplunkv1.IngestionTokenDefinition{
		Name:    "HEC",
		Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"main", "audit"},
		},
		HecProfile: &opensplunkv1.IngestionTokenHecProfile{
			DefaultIndexName:      &defaultIndex,
			DefaultHost:           &defaultHost,
			DefaultSource:         &defaultSource,
			DefaultSourcetype:     &defaultSourcetype,
			IndexerAcknowledgment: true,
		},
	})
	if err != nil {
		t.Fatalf("tokenDefinitionFromProto(HEC): %v", err)
	}
	if hec.Purpose != auth.IngestionTokenPurposeHEC ||
		hec.BoundCollectorID != "" ||
		hec.HECProfile.DefaultIndexName != defaultIndex ||
		!hec.HECProfile.IndexerAcknowledgment {
		t.Fatalf("parsed HEC definition = %#v", hec)
	}

	for label, definition := range map[string]*opensplunkv1.IngestionTokenDefinition{
		"HEC binding": {
			Name:    "HEC bound",
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"main"},
				BoundCollectorId:  &binding,
			},
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{},
		},
		"HEC missing profile": {
			Name:    "HEC missing profile",
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"main"},
			},
		},
		"native profile": {
			Name:    "native profile",
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"main"},
				BoundCollectorId:  &binding,
			},
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{},
		},
	} {
		if _, err := tokenDefinitionFromProto(definition); err == nil {
			t.Fatalf("tokenDefinitionFromProto(%s) unexpectedly succeeded", label)
		}
	}
}

func TestHECIngestionTokenAdministrativeHTTPVertical(t *testing.T) {
	t.Parallel()

	handler, db, tokenStore := newAdminIntegrationHandler(t)
	ctx := context.Background()
	for _, name := range []string{"main", "audit"} {
		if _, err := db.CreateIndex(ctx, adminTestIndex(name)); err != nil {
			t.Fatalf("CreateIndex(%s): %v", name, err)
		}
	}
	defaultIndex := "main"
	defaultHost := "hec-producer"
	defaultSource := "/var/log/app.json"
	defaultSourcetype := "_json"
	create := &opensplunkv1.CreateIngestionTokenRequest{
		Definition: &opensplunkv1.IngestionTokenDefinition{
			Name:    "HEC production",
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC,
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"audit", "main"},
			},
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{
				DefaultIndexName:      &defaultIndex,
				DefaultHost:           &defaultHost,
				DefaultSource:         &defaultSource,
				DefaultSourcetype:     &defaultSourcetype,
				IndexerAcknowledgment: true,
			},
		},
	}
	response := postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/create",
		create,
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunkv1.CreateIngestionTokenResponse
	unmarshalResponse(t, response, &created)
	token := created.GetIngestionToken()
	if token.GetPurpose() != opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC ||
		token.GetConstraints().BoundCollectorId != nil ||
		token.GetHecProfile() == nil ||
		token.GetHecProfile().GetDefaultIndexName() != defaultIndex ||
		!token.GetHecProfile().GetIndexerAcknowledgment() ||
		created.GetPlaintextToken() == "" {
		t.Fatalf("created HEC token = %#v", &created)
	}
	plaintext := created.GetPlaintextToken()

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/get",
		&opensplunkv1.GetIngestionTokenRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC get status = %d, body = %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), plaintext) {
		t.Fatal("HEC get response disclosed plaintext credential")
	}
	var got opensplunkv1.GetIngestionTokenResponse
	unmarshalResponse(t, response, &got)
	if got.GetIngestionToken().GetPurpose() != token.GetPurpose() ||
		got.GetIngestionToken().GetHecProfile().GetDefaultHost() != defaultHost {
		t.Fatalf("HEC get response = %#v", &got)
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/list",
		&opensplunkv1.ListIngestionTokensRequest{},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIngestionTokens()) != 1 ||
		listed.GetIngestionTokens()[0].GetPurpose() !=
			opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC {
		t.Fatalf("HEC list response = %#v", &listed)
	}

	replacementHost := "hec-producer-v2"
	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  token.GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				HecProfile: &opensplunkv1.IngestionTokenHecProfile{
					DefaultHost: &replacementHost,
				},
			},
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"hec_profile.default_host"},
			},
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunkv1.UpdateIngestionTokenResponse
	unmarshalResponse(t, response, &updated)
	if updated.GetIngestionToken().GetVersion() != token.GetVersion()+1 ||
		updated.GetIngestionToken().GetHecProfile().GetDefaultHost() != replacementHost ||
		updated.GetIngestionToken().GetHecProfile().GetDefaultSource() != defaultSource ||
		!updated.GetIngestionToken().GetHecProfile().GetIndexerAcknowledgment() {
		t.Fatalf("updated HEC token = %#v", &updated)
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/state/set",
		&opensplunkv1.SetIngestionTokenEnabledRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  updated.GetIngestionToken().GetVersion(),
			Enabled:          false,
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC disable status = %d, body = %s", response.Code, response.Body.String())
	}
	var disabled opensplunkv1.SetIngestionTokenEnabledResponse
	unmarshalResponse(t, response, &disabled)
	if disabled.GetIngestionToken().GetVersion() !=
		updated.GetIngestionToken().GetVersion()+1 ||
		disabled.GetIngestionToken().GetState() !=
			opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_DISABLED ||
		disabled.GetIngestionToken().GetRevokedAt() != nil {
		t.Fatalf("disabled HEC token = %#v", &disabled)
	}
	if _, err := tokenStore.AuthenticateHEC(ctx, plaintext); !errors.Is(err, auth.ErrInactiveToken) {
		t.Fatalf("AuthenticateHEC(disabled) error = %v, want ErrInactiveToken", err)
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/state/set",
		&opensplunkv1.SetIngestionTokenEnabledRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  disabled.GetIngestionToken().GetVersion(),
			Enabled:          true,
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC re-enable status = %d, body = %s", response.Code, response.Body.String())
	}
	var enabled opensplunkv1.SetIngestionTokenEnabledResponse
	unmarshalResponse(t, response, &enabled)
	if enabled.GetIngestionToken().GetVersion() !=
		disabled.GetIngestionToken().GetVersion()+1 ||
		enabled.GetIngestionToken().GetState() !=
			opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_ACTIVE ||
		enabled.GetIngestionToken().GetRevokedAt() != nil {
		t.Fatalf("re-enabled HEC token = %#v", &enabled)
	}
	if _, err := tokenStore.AuthenticateHEC(ctx, plaintext); err != nil {
		t.Fatalf("AuthenticateHEC(re-enabled): %v", err)
	}

	ackDisabled := tokenHECProfileToProto(auth.HECTokenProfile{
		DefaultIndexName:      defaultIndex,
		DefaultHost:           replacementHost,
		DefaultSource:         defaultSource,
		DefaultSourcetype:     defaultSourcetype,
		IndexerAcknowledgment: false,
	})
	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/update",
		&opensplunkv1.UpdateIngestionTokenRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  updated.GetIngestionToken().GetVersion(),
			Definition: &opensplunkv1.IngestionTokenDefinition{
				HecProfile: ackDisabled,
			},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"hec_profile"}},
		},
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("HEC acknowledgment mutation status = %d, body = %s", response.Code, response.Body.String())
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/revoke",
		&opensplunkv1.RevokeIngestionTokenRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  enabled.GetIngestionToken().GetVersion(),
		},
	)
	if response.Code != http.StatusOK {
		t.Fatalf("HEC revoke status = %d, body = %s", response.Code, response.Body.String())
	}
	var revoked opensplunkv1.RevokeIngestionTokenResponse
	unmarshalResponse(t, response, &revoked)
	if revoked.GetIngestionToken().GetState() !=
		opensplunkv1.IngestionTokenState_INGESTION_TOKEN_STATE_REVOKED ||
		revoked.GetIngestionToken().GetPurpose() !=
			opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_HEC ||
		revoked.GetIngestionToken().GetHecProfile().GetDefaultHost() != replacementHost {
		t.Fatalf("revoked HEC token = %#v", &revoked)
	}

	response = postProto(
		t,
		handler,
		"/api/v1/ingestion-tokens/state/set",
		&opensplunkv1.SetIngestionTokenEnabledRequest{
			IngestionTokenId: token.GetIngestionTokenId(),
			ExpectedVersion:  revoked.GetIngestionToken().GetVersion(),
			Enabled:          true,
		},
	)
	if response.Code != http.StatusConflict {
		t.Fatalf("HEC revoked re-enable status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestApplyHECTokenUpdateFencesImmutablePurposeAndAcknowledgment(t *testing.T) {
	t.Parallel()

	current := auth.CollectorToken{
		Name:              "HEC producer",
		Purpose:           auth.IngestionTokenPurposeHEC,
		AllowedIndexNames: []string{"main"},
		HECProfile: auth.HECTokenProfile{
			DefaultIndexName:      "main",
			IndexerAcknowledgment: true,
		},
	}
	if _, err := applyTokenUpdate(
		current,
		&opensplunkv1.IngestionTokenDefinition{
			Purpose: opensplunkv1.IngestionTokenPurpose_INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
			Name:    "changed",
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"main"},
			},
		},
		nil,
	); !errors.Is(err, errImmutableTokenPurpose) {
		t.Fatalf("purpose mutation error = %v, want immutable purpose", err)
	}
	if _, err := applyTokenUpdate(
		current,
		&opensplunkv1.IngestionTokenDefinition{
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{},
		},
		&fieldmaskpb.FieldMask{Paths: []string{"hec_profile"}},
	); !errors.Is(err, errImmutableTokenHECAcknowledgment) {
		t.Fatalf("acknowledgment mutation error = %v, want immutable acknowledgment", err)
	}
}

func TestApplyHECTokenUpdateMovesDefaultWithScopeRegardlessOfMaskOrder(t *testing.T) {
	t.Parallel()

	current := auth.CollectorToken{
		Name:              "HEC producer",
		Purpose:           auth.IngestionTokenPurposeHEC,
		AllowedIndexNames: []string{"main"},
		HECProfile: auth.HECTokenProfile{
			DefaultIndexName:      "main",
			DefaultHost:           "producer",
			IndexerAcknowledgment: true,
		},
	}
	audit := "audit"
	updated, err := applyTokenUpdate(
		current,
		&opensplunkv1.IngestionTokenDefinition{
			Constraints: &opensplunkv1.IngestionTokenConstraints{
				AllowedIndexNames: []string{"audit"},
			},
			HecProfile: &opensplunkv1.IngestionTokenHecProfile{
				DefaultIndexName: &audit,
			},
		},
		&fieldmaskpb.FieldMask{Paths: []string{
			"hec_profile.default_index_name",
			"constraints",
		}},
	)
	if err != nil {
		t.Fatalf("applyTokenUpdate(move default and scope): %v", err)
	}
	if len(updated.AllowedIndexNames) != 1 || updated.AllowedIndexNames[0] != "audit" ||
		updated.HECProfile.DefaultIndexName != "audit" ||
		updated.HECProfile.DefaultHost != "producer" ||
		!updated.HECProfile.IndexerAcknowledgment {
		t.Fatalf("combined HEC update = %#v", updated)
	}
}

func TestTokenHECMetadataDefaultPreservesNonASCIIEdgeWhitespace(t *testing.T) {
	t.Parallel()
	value := "\u00a0producer\u00a0"
	got, err := tokenHECMetadataDefault(&value, "host")
	if err != nil || got != value {
		t.Fatalf("tokenHECMetadataDefault(non-ASCII edge) = %q, %v", got, err)
	}
}
