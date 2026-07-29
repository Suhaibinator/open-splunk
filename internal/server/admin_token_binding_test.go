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

func TestIngestionTokenCollectorBindingValidation(t *testing.T) {
	t.Parallel()

	maximumLengthID := "a" + strings.Repeat("Z0._:-", 21) + "b"
	if len(maximumLengthID) != maximumCollectorIDBytes {
		t.Fatalf("maximum-length test ID has length %d", len(maximumLengthID))
	}
	tests := []struct {
		name    string
		binding *string
		want    string
	}{
		{name: "missing"},
		{name: "empty", binding: stringPointer("")},
		{name: "leading punctuation", binding: stringPointer("-collector")},
		{name: "space", binding: stringPointer("collector one")},
		{name: "slash", binding: stringPointer("collector/one")},
		{name: "non ASCII", binding: stringPointer("collectör")},
		{name: "too long", binding: stringPointer("c" + strings.Repeat("x", maximumCollectorIDBytes))},
		{name: "minimum", binding: stringPointer("7"), want: "7"},
		{name: "allowed punctuation", binding: stringPointer("Collector-01.eu_west:blue"), want: "Collector-01.eu_west:blue"},
		{name: "maximum length", binding: &maximumLengthID, want: maximumLengthID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := tokenDefinitionFromProto(&opensplunkv1.IngestionTokenDefinition{
				Name: "token",
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"},
					BoundCollectorId:  test.binding,
				},
			})
			if test.want == "" {
				if err == nil {
					t.Fatalf("tokenDefinitionFromProto accepted binding %v", test.binding)
				}
				return
			}
			if err != nil {
				t.Fatalf("tokenDefinitionFromProto: %v", err)
			}
			if parsed.BoundCollectorID != test.want {
				t.Fatalf("bound collector ID = %q, want %q", parsed.BoundCollectorID, test.want)
			}
		})
	}

	for name, constraints := range map[string]*opensplunkv1.IngestionTokenConstraints{
		"host": {
			AllowedIndexNames: []string{"main"}, AllowedHostRegexes: []string{".*"},
			BoundCollectorId: stringPointer("collector-host"),
		},
		"source": {
			AllowedIndexNames: []string{"main"}, AllowedSourceRegexes: []string{".*"},
			BoundCollectorId: stringPointer("collector-source"),
		},
	} {
		t.Run("unsupported "+name+" regex", func(t *testing.T) {
			t.Parallel()
			if _, err := tokenDefinitionFromProto(&opensplunkv1.IngestionTokenDefinition{
				Name: "token", Constraints: constraints,
			}); err == nil {
				t.Fatalf("%s regex was accepted", name)
			}
		})
	}
}

func TestApplyIngestionTokenUpdatePreservesAndFencesCollectorBinding(t *testing.T) {
	t.Parallel()

	current := adminLastUsedToken("tok_binding", "token", testNow)
	current.BoundCollectorID = "collector-current"

	full, err := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
		Name: "replacement",
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"audit"},
		},
	}, nil)
	if err != nil {
		t.Fatalf("full update: %v", err)
	}
	if full.BoundCollectorID != current.BoundCollectorID ||
		len(full.AllowedIndexNames) != 1 || full.AllowedIndexNames[0] != "audit" {
		t.Fatalf("full update = %+v", full)
	}

	sameBinding := current.BoundCollectorID
	fullRoundTrip, err := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
		Name: "replacement",
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"audit"},
			BoundCollectorId:  &sameBinding,
		},
	}, nil)
	if err != nil {
		t.Fatalf("full same-binding update: %v", err)
	}
	if fullRoundTrip.BoundCollectorID != current.BoundCollectorID ||
		len(fullRoundTrip.AllowedIndexNames) != 1 || fullRoundTrip.AllowedIndexNames[0] != "audit" {
		t.Fatalf("full same-binding update = %+v", fullRoundTrip)
	}

	wholeConstraints, err := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
		Constraints: &opensplunkv1.IngestionTokenConstraints{
			AllowedIndexNames: []string{"audit"},
		},
	}, &fieldmaskpb.FieldMask{Paths: []string{"constraints"}})
	if err != nil {
		t.Fatalf("whole constraints update: %v", err)
	}
	if wholeConstraints.BoundCollectorID != current.BoundCollectorID ||
		len(wholeConstraints.AllowedIndexNames) != 1 || wholeConstraints.AllowedIndexNames[0] != "audit" {
		t.Fatalf("whole constraints update = %+v", wholeConstraints)
	}

	masked, err := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
		Constraints: &opensplunkv1.IngestionTokenConstraints{BoundCollectorId: &sameBinding},
	}, &fieldmaskpb.FieldMask{Paths: []string{"definition.constraints.bound_collector_id"}})
	if err != nil {
		t.Fatalf("masked same-binding update: %v", err)
	}
	if masked.BoundCollectorID != current.BoundCollectorID ||
		len(masked.AllowedIndexNames) != 1 || masked.AllowedIndexNames[0] != "main" {
		t.Fatalf("masked same-binding update = %+v", masked)
	}

	legacy := current
	legacy.BoundCollectorID = ""
	newBinding := "collector-enrolled"
	bound, err := applyTokenUpdate(legacy, &opensplunkv1.IngestionTokenDefinition{
		Constraints: &opensplunkv1.IngestionTokenConstraints{BoundCollectorId: &newBinding},
	}, &fieldmaskpb.FieldMask{Paths: []string{"constraints.bound_collector_id"}})
	if err != nil {
		t.Fatalf("legacy one-way bind: %v", err)
	}
	if bound.BoundCollectorID != newBinding ||
		len(bound.AllowedIndexNames) != 1 || bound.AllowedIndexNames[0] != "main" {
		t.Fatalf("legacy one-way bind = %+v", bound)
	}

	for name, candidate := range map[string]string{
		"clear":  "",
		"change": "collector-other",
	} {
		t.Run(name, func(t *testing.T) {
			_, updateErr := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
				Constraints: &opensplunkv1.IngestionTokenConstraints{BoundCollectorId: &candidate},
			}, &fieldmaskpb.FieldMask{Paths: []string{"constraints.bound_collector_id"}})
			if !errors.Is(updateErr, errImmutableTokenCollectorBinding) {
				t.Fatalf("error = %v, want immutable binding conflict", updateErr)
			}
		})
	}

	if _, err := applyTokenUpdate(current, &opensplunkv1.IngestionTokenDefinition{
		Constraints: &opensplunkv1.IngestionTokenConstraints{},
	}, &fieldmaskpb.FieldMask{Paths: []string{"constraints.bound_collector_id"}}); err == nil {
		t.Fatal("masked update accepted an absent bound collector ID")
	}
}

func TestIngestionTokenCollectorBindingProjectionSupportsLegacyRows(t *testing.T) {
	t.Parallel()

	legacy := adminLastUsedToken("tok_legacy", "legacy", testNow)
	converted, err := tokenToProto(legacy)
	if err != nil {
		t.Fatalf("legacy tokenToProto: %v", err)
	}
	if converted.GetConstraints().BoundCollectorId != nil {
		t.Fatalf("legacy binding = %q, want absent", converted.GetConstraints().GetBoundCollectorId())
	}

	bound := adminLastUsedToken("tok_bound", "bound", testNow)
	bound.BoundCollectorID = "collector-bound"
	converted, err = tokenToProto(bound)
	if err != nil {
		t.Fatalf("bound tokenToProto: %v", err)
	}
	if converted.GetConstraints().BoundCollectorId == nil ||
		converted.GetConstraints().GetBoundCollectorId() != bound.BoundCollectorID {
		t.Fatalf("projected binding = %q", converted.GetConstraints().GetBoundCollectorId())
	}

	bound.BoundCollectorID = "collector invalid"
	if _, err := tokenToProto(bound); err == nil {
		t.Fatal("tokenToProto accepted a corrupt collector binding")
	}
}

func TestIngestionTokenAllowsCredentialRotationForSameCollector(t *testing.T) {
	t.Parallel()

	handler, db, _ := newAdminIntegrationHandler(t)
	if _, err := db.CreateIndex(context.Background(), adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	binding := "collector-rotation"
	ids := make(map[string]struct{}, 2)
	for _, name := range []string{"current", "replacement"} {
		response := postProto(t, handler, "/api/v1/ingestion-tokens/create", &opensplunkv1.CreateIngestionTokenRequest{
			Definition: &opensplunkv1.IngestionTokenDefinition{
				Name: name,
				Constraints: &opensplunkv1.IngestionTokenConstraints{
					AllowedIndexNames: []string{"main"},
					BoundCollectorId:  &binding,
				},
			},
		})
		if response.Code != http.StatusOK {
			t.Fatalf("create %s token status = %d, body = %s", name, response.Code, response.Body)
		}
		var created opensplunkv1.CreateIngestionTokenResponse
		unmarshalResponse(t, response, &created)
		token := created.GetIngestionToken()
		if created.GetPlaintextToken() == "" ||
			token.GetConstraints().GetBoundCollectorId() != binding ||
			token.GetIngestionTokenId() == "" {
			t.Fatalf("created %s token = %+v", name, &created)
		}
		ids[token.GetIngestionTokenId()] = struct{}{}
	}
	if len(ids) != 2 {
		t.Fatalf("rotation token IDs = %v, want two distinct credentials", ids)
	}
}

func TestIngestionTokenCollectorBindingInvalidatesPaginationSnapshot(t *testing.T) {
	t.Parallel()

	tokens := &mutableTokenAdministration{records: []auth.CollectorToken{
		adminLastUsedToken("tok_alpha", "alpha", testNow),
		adminLastUsedToken("tok_bravo", "bravo", testNow),
	}}
	handler := newAdminTokenHandler(t, tokens)
	pageSize := uint32(1)
	request := &opensplunkv1.ListIngestionTokensRequest{
		Page: &opensplunkv1.PageRequest{PageSize: &pageSize},
	}
	response := postProto(t, handler, "/api/v1/ingestion-tokens/list", request)
	if response.Code != http.StatusOK {
		t.Fatalf("first page status = %d, body = %s", response.Code, response.Body)
	}
	var first opensplunkv1.ListIngestionTokensResponse
	unmarshalResponse(t, response, &first)
	cursor := first.GetPage().GetNextPageToken()
	if cursor == "" {
		t.Fatalf("first page = %+v, want continuation cursor", &first)
	}

	tokens.setBoundCollectorID("tok_bravo", "collector-bravo")
	request.Page.PageToken = &cursor
	response = postProto(t, handler, "/api/v1/ingestion-tokens/list", request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("stale binding cursor status = %d, body = %s", response.Code, response.Body)
	}
}
