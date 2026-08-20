package server

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/tokenconstraint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestIngestionTokenConstraintParsingIsCanonicalAndDetached(t *testing.T) {
	t.Parallel()

	input := &opensplunk.IngestionTokenDefinition{
		Name: "constrained",
		Constraints: &opensplunk.IngestionTokenConstraints{
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^z$", "^a$", "^z$"},
			AllowedSourceRegexes: []string{`^/var/log/[^/]+$`, `.*`, `.*`},
			BoundCollectorId:     new("collector-constrained"),
		},
	}
	parsed, err := tokenDefinitionFromProto(input)
	if err != nil {
		t.Fatalf("tokenDefinitionFromProto: %v", err)
	}
	wantHosts := []string{"^a$", "^z$"}
	wantSources := []string{`.*`, `^/var/log/[^/]+$`}
	if !slices.Equal(parsed.AllowedHostRegexes, wantHosts) ||
		!slices.Equal(parsed.AllowedSourceRegexes, wantSources) {
		t.Fatalf(
			"parsed constraints = hosts %q sources %q, want hosts %q sources %q",
			parsed.AllowedHostRegexes,
			parsed.AllowedSourceRegexes,
			wantHosts,
			wantSources,
		)
	}

	input.Constraints.AllowedHostRegexes[1] = "mutated"
	input.Constraints.AllowedSourceRegexes[0] = "mutated"
	if !slices.Equal(parsed.AllowedHostRegexes, wantHosts) ||
		!slices.Equal(parsed.AllowedSourceRegexes, wantSources) {
		t.Fatalf("input mutation changed parsed constraints: %+v", parsed)
	}
}

func TestIngestionTokenConstraintParsingEnforcesPublicBounds(t *testing.T) {
	t.Parallel()

	tooMany := make([]string, tokenconstraint.MaximumPatternsPerDimension+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("^host-%02d$", index)
	}
	overDimensionBytes := make([]string, 9)
	for index := range overDimensionBytes {
		overDimensionBytes[index] = strings.Repeat("a", tokenconstraint.MaximumPatternBytes-1) +
			string(rune('a'+index))
	}
	tests := []struct {
		name        string
		hosts       []string
		sources     []string
		wantMessage string
	}{
		{name: "empty", hosts: []string{""}, wantMessage: "ingestion token host constraints are invalid"},
		{name: "invalid UTF-8", hosts: []string{string([]byte{0xff})}, wantMessage: "ingestion token host constraints are invalid"},
		{name: "embedded NUL", hosts: []string{"^private\x00host$"}, wantMessage: "ingestion token host constraints are invalid"},
		{name: "invalid RE2", hosts: []string{"^private-host["}, wantMessage: "ingestion token host constraints are invalid"},
		{
			name:        "compiled program too large",
			hosts:       []string{strings.Repeat("a{1000}", 5)},
			wantMessage: "ingestion token host constraints are invalid",
		},
		{
			name:        "pattern too large",
			hosts:       []string{strings.Repeat("a", tokenconstraint.MaximumPatternBytes+1)},
			wantMessage: "ingestion token host constraints are invalid",
		},
		{name: "too many patterns", hosts: tooMany, wantMessage: "ingestion token host constraints are invalid"},
		{
			name: "dimension too large", sources: overDimensionBytes,
			wantMessage: "ingestion token source constraints are invalid",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := tokenDefinitionFromProto(tokenConstraintDefinition(test.hosts, test.sources))
			if err == nil || err.Error() != test.wantMessage {
				t.Fatalf("error = %v, want %q", err, test.wantMessage)
			}
		})
	}

	maximumDimension := make([]string, 8)
	for index := range maximumDimension {
		maximumDimension[index] = strings.Repeat("b", tokenconstraint.MaximumPatternBytes-1) +
			string(rune('a'+index))
	}
	maximumCount := make([]string, tokenconstraint.MaximumPatternsPerDimension)
	for index := range maximumCount {
		maximumCount[index] = fmt.Sprintf("^source-%02d$", index)
	}
	parsed, err := tokenDefinitionFromProto(tokenConstraintDefinition(maximumDimension, maximumCount))
	if err != nil {
		t.Fatalf("exact public boundaries were rejected: %v", err)
	}
	if len(parsed.AllowedHostRegexes) != len(maximumDimension) ||
		len(parsed.AllowedSourceRegexes) != tokenconstraint.MaximumPatternsPerDimension {
		t.Fatalf("boundary constraints = %+v", parsed)
	}

	duplicates := make([]string, tokenconstraint.MaximumPatternsPerDimension+1)
	for index := range duplicates {
		duplicates[index] = strings.Repeat("d", tokenconstraint.MaximumPatternBytes)
	}
	parsed, err = tokenDefinitionFromProto(tokenConstraintDefinition(duplicates, nil))
	if err != nil {
		t.Fatalf("duplicate constraints were bounded before canonicalization: %v", err)
	}
	if len(parsed.AllowedHostRegexes) != 1 || parsed.AllowedHostRegexes[0] != duplicates[0] {
		t.Fatalf("duplicate constraints = %q", parsed.AllowedHostRegexes)
	}
}

func TestApplyIngestionTokenUpdatePreservesAndReplacesConstraints(t *testing.T) {
	t.Parallel()

	current := adminLastUsedToken("tok_constraints", "current", testNow)
	current.BoundCollectorID = "collector-current"
	current.AllowedHostRegexes = []string{"^current-host$"}
	current.AllowedSourceRegexes = []string{"^current-source$"}

	masked, err := applyTokenUpdate(
		current,
		&opensplunk.IngestionTokenDefinition{Name: "renamed"},
		&fieldmaskpb.FieldMask{Paths: []string{"name"}},
	)
	if err != nil {
		t.Fatalf("masked update: %v", err)
	}
	if !slices.Equal(masked.AllowedHostRegexes, current.AllowedHostRegexes) ||
		!slices.Equal(masked.AllowedSourceRegexes, current.AllowedSourceRegexes) {
		t.Fatalf("masked update lost constraints: %+v", masked)
	}
	masked.AllowedHostRegexes[0] = "mutated"
	masked.AllowedSourceRegexes[0] = "mutated"
	if current.AllowedHostRegexes[0] != "^current-host$" ||
		current.AllowedSourceRegexes[0] != "^current-source$" {
		t.Fatal("masked update aliases current token constraints")
	}

	replaced, err := applyTokenUpdate(
		current,
		&opensplunk.IngestionTokenDefinition{Constraints: &opensplunk.IngestionTokenConstraints{
			AllowedIndexNames:    []string{"audit"},
			AllowedHostRegexes:   []string{"^z$", "^a$", "^z$"},
			AllowedSourceRegexes: []string{"^new-source$"},
		}},
		&fieldmaskpb.FieldMask{Paths: []string{"constraints"}},
	)
	if err != nil {
		t.Fatalf("whole constraints update: %v", err)
	}
	if replaced.BoundCollectorID != current.BoundCollectorID ||
		!slices.Equal(replaced.AllowedIndexNames, []string{"audit"}) ||
		!slices.Equal(replaced.AllowedHostRegexes, []string{"^a$", "^z$"}) ||
		!slices.Equal(replaced.AllowedSourceRegexes, []string{"^new-source$"}) {
		t.Fatalf("whole constraints update = %+v", replaced)
	}
}

func TestIngestionTokenConstraintProjectionIsCanonicalAndDetached(t *testing.T) {
	t.Parallel()

	record := adminLastUsedToken("tok_projection", "projection", testNow)
	record.AllowedHostRegexes = []string{"^a$", "^z$"}
	record.AllowedSourceRegexes = []string{"^source$"}
	converted, err := tokenToProto(record)
	if err != nil {
		t.Fatalf("tokenToProto: %v", err)
	}
	constraints := converted.GetConstraints()
	if !slices.Equal(constraints.GetAllowedHostRegexes(), record.AllowedHostRegexes) ||
		!slices.Equal(constraints.GetAllowedSourceRegexes(), record.AllowedSourceRegexes) {
		t.Fatalf("projected constraints = %+v", constraints)
	}
	constraints.AllowedHostRegexes[0] = "mutated"
	constraints.AllowedSourceRegexes[0] = "mutated"
	if record.AllowedHostRegexes[0] != "^a$" || record.AllowedSourceRegexes[0] != "^source$" {
		t.Fatal("projected constraints alias the token record")
	}

	tests := []struct {
		name    string
		hosts   []string
		sources []string
	}{
		{name: "unsorted host", hosts: []string{"^z$", "^a$"}},
		{name: "duplicate host", hosts: []string{"^a$", "^a$"}},
		{name: "invalid source", sources: []string{"["}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			corrupt := adminLastUsedToken("tok_corrupt", "corrupt", testNow)
			corrupt.AllowedHostRegexes = test.hosts
			corrupt.AllowedSourceRegexes = test.sources
			if _, err := tokenToProto(corrupt); err == nil {
				t.Fatal("tokenToProto accepted corrupt token constraints")
			}
		})
	}
}

func TestIngestionTokenConstraintAdministrativeRoundTrip(t *testing.T) {
	t.Parallel()

	handler, database, _ := newAdminIntegrationHandler(t)
	if _, err := database.CreateIndex(context.Background(), adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	binding := "collector-round-trip"
	response := postProto(t, handler, "/api/ingestion-tokens/create", &opensplunk.CreateIngestionTokenRequest{
		Definition: &opensplunk.IngestionTokenDefinition{
			Name: "round trip",
			Constraints: &opensplunk.IngestionTokenConstraints{
				AllowedIndexNames:    []string{"main"},
				AllowedHostRegexes:   []string{"^z$", "^a$", "^z$"},
				AllowedSourceRegexes: []string{"^source$"},
				BoundCollectorId:     &binding,
			},
		},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("create status = %d, body = %s", response.Code, response.Body.String())
	}
	var created opensplunk.CreateIngestionTokenResponse
	unmarshalResponse(t, response, &created)
	wantCreatedConstraints := &opensplunk.IngestionTokenConstraints{
		AllowedIndexNames:    []string{"main"},
		AllowedHostRegexes:   []string{"^a$", "^z$"},
		AllowedSourceRegexes: []string{"^source$"},
		BoundCollectorId:     &binding,
	}
	if !proto.Equal(created.GetIngestionToken().GetConstraints(), wantCreatedConstraints) {
		t.Fatalf("created constraints = %+v", created.GetIngestionToken().GetConstraints())
	}
	tokenID := created.GetIngestionToken().GetIngestionTokenId()

	response = postProto(t, handler, "/api/ingestion-tokens/get", &opensplunk.GetIngestionTokenRequest{
		IngestionTokenId: tokenID,
	})
	if response.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", response.Code, response.Body.String())
	}
	var got opensplunk.GetIngestionTokenResponse
	unmarshalResponse(t, response, &got)
	if !proto.Equal(got.GetIngestionToken().GetConstraints(), wantCreatedConstraints) {
		t.Fatalf("get constraints = %+v", got.GetIngestionToken().GetConstraints())
	}

	response = postProto(t, handler, "/api/ingestion-tokens/list", &opensplunk.ListIngestionTokensRequest{})
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", response.Code, response.Body.String())
	}
	var listed opensplunk.ListIngestionTokensResponse
	unmarshalResponse(t, response, &listed)
	if len(listed.GetIngestionTokens()) != 1 ||
		!proto.Equal(listed.GetIngestionTokens()[0].GetConstraints(), wantCreatedConstraints) {
		t.Fatalf("listed tokens = %+v", listed.GetIngestionTokens())
	}

	response = postProto(t, handler, "/api/ingestion-tokens/update", &opensplunk.UpdateIngestionTokenRequest{
		IngestionTokenId: tokenID,
		ExpectedVersion:  created.GetIngestionToken().GetVersion(),
		Definition: &opensplunk.IngestionTokenDefinition{Constraints: &opensplunk.IngestionTokenConstraints{
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^new-host$"},
			AllowedSourceRegexes: []string{"^z$", "^a$"},
		}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"constraints"}},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", response.Code, response.Body.String())
	}
	var updated opensplunk.UpdateIngestionTokenResponse
	unmarshalResponse(t, response, &updated)
	wantUpdatedConstraints := &opensplunk.IngestionTokenConstraints{
		AllowedIndexNames:    []string{"main"},
		AllowedHostRegexes:   []string{"^new-host$"},
		AllowedSourceRegexes: []string{"^a$", "^z$"},
		BoundCollectorId:     &binding,
	}
	if !proto.Equal(updated.GetIngestionToken().GetConstraints(), wantUpdatedConstraints) {
		t.Fatalf("updated constraints = %+v", updated.GetIngestionToken().GetConstraints())
	}
}

func TestIngestionTokenConstraintHTTPErrorIsGeneric(t *testing.T) {
	t.Parallel()

	handler, database, _ := newAdminIntegrationHandler(t)
	if _, err := database.CreateIndex(context.Background(), adminTestIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	sensitivePattern := "private-tenant-source["
	response := postProto(t, handler, "/api/ingestion-tokens/create", &opensplunk.CreateIngestionTokenRequest{
		Definition: tokenConstraintDefinition(nil, []string{sensitivePattern}),
	})
	if response.Code != http.StatusBadRequest ||
		!strings.Contains(response.Body.String(), "ingestion token source constraints are invalid") ||
		strings.Contains(response.Body.String(), sensitivePattern) {
		t.Fatalf("invalid constraint response = status %d body %q", response.Code, response.Body.String())
	}
}

func tokenConstraintDefinition(hosts, sources []string) *opensplunk.IngestionTokenDefinition {
	return &opensplunk.IngestionTokenDefinition{
		Name: "constraint test",
		Constraints: &opensplunk.IngestionTokenConstraints{
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   hosts,
			AllowedSourceRegexes: sources,
			BoundCollectorId:     new("collector-constraint-test"),
		},
	}
}
