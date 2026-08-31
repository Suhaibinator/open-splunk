package server

import (
	"slices"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

func appSanitizerSelector() *opensplunk.AppSelector {
	return &opensplunk.AppSelector{
		Selector: &opensplunk.AppSelector_AppId{AppId: "app-1"},
	}
}

func appSanitizerDefinition() *opensplunk.AppDefinition {
	return &opensplunk.AppDefinition{Slug: "operations", DisplayName: "Operations"}
}

func TestSanitizeCreateAppRequestCanonicalizesTheDefinition(t *testing.T) {
	t.Parallel()

	request := &opensplunk.CreateAppRequest{
		Definition: &opensplunk.AppDefinition{
			Slug:              "  Operations  ",
			DisplayName:       "  Operations Workspace  ",
			Description:       new("  Shared dashboards  "),
			DefaultIndexNames: []string{"secondary", "main", "main"},
			DefaultTimeRange: &opensplunk.TimeRangeSpec{
				Earliest: new("-24h"),
				Latest:   new("now"),
			},
		},
	}
	got, err := sanitizeCreateAppRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if got != request {
		t.Fatalf("sanitizer returned %p, want %p", got, request)
	}
	definition := got.GetDefinition()
	if definition.GetSlug() != "operations" ||
		definition.GetDisplayName() != "Operations Workspace" ||
		definition.GetDescription() != "Shared dashboards" ||
		!slices.Equal(definition.GetDefaultIndexNames(), []string{"main", "secondary"}) {
		t.Fatalf("canonical definition = %v", definition)
	}
}

func TestSanitizeCreateAppRequestDropsABlankDescription(t *testing.T) {
	t.Parallel()

	definition := appSanitizerDefinition()
	definition.Description = new("   ")
	got, err := sanitizeCreateAppRequest(
		t.Context(),
		&opensplunk.CreateAppRequest{Definition: definition},
	)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if got.GetDefinition().Description != nil {
		t.Fatalf("description = %q, want absent", got.GetDefinition().GetDescription())
	}
}

func TestSanitizeCreateAppRequestRejectsUnsupportedShapes(t *testing.T) {
	t.Parallel()

	badSlug := appSanitizerDefinition()
	badSlug.Slug = "-operations"
	emptyName := appSanitizerDefinition()
	emptyName.DisplayName = "   "
	oversizedName := appSanitizerDefinition()
	oversizedName.DisplayName = strings.Repeat("n", maximumAppAdministrationDisplayName+1)
	oversizedDescription := appSanitizerDefinition()
	oversizedDescription.Description = new(
		strings.Repeat("d", maximumAppAdministrationDescription+1),
	)
	tooManyIndexes := appSanitizerDefinition()
	tooManyIndexes.DefaultIndexNames = make(
		[]string,
		maximumAppAdministrationIndexes+1,
	)
	badIndex := appSanitizerDefinition()
	badIndex.DefaultIndexNames = []string{"not a name"}
	untrimmedRange := appSanitizerDefinition()
	untrimmedRange.DefaultTimeRange = &opensplunk.TimeRangeSpec{
		Earliest: new(" -24h"),
	}
	unresolvableRange := appSanitizerDefinition()
	unresolvableRange.DefaultTimeRange = &opensplunk.TimeRangeSpec{
		Earliest: new("not-a-time"),
	}

	tests := map[string]struct {
		request *opensplunk.CreateAppRequest
		message string
	}{
		"idempotency key": {
			request: &opensplunk.CreateAppRequest{
				Definition:      appSanitizerDefinition(),
				ClientRequestId: new("retry-1"),
			},
			message: "client request idempotency is not supported",
		},
		"absent definition": {
			request: &opensplunk.CreateAppRequest{},
			message: "app definition is invalid",
		},
		"invalid slug": {
			request: &opensplunk.CreateAppRequest{Definition: badSlug},
			message: "app definition is invalid",
		},
		"blank display name": {
			request: &opensplunk.CreateAppRequest{Definition: emptyName},
			message: "app definition is invalid",
		},
		"oversized display name": {
			request: &opensplunk.CreateAppRequest{Definition: oversizedName},
			message: "app definition is invalid",
		},
		"oversized description": {
			request: &opensplunk.CreateAppRequest{Definition: oversizedDescription},
			message: "app definition is invalid",
		},
		"too many indexes": {
			request: &opensplunk.CreateAppRequest{Definition: tooManyIndexes},
			message: "app definition is invalid",
		},
		"invalid index": {
			request: &opensplunk.CreateAppRequest{Definition: badIndex},
			message: "app definition is invalid",
		},
		"untrimmed time range": {
			request: &opensplunk.CreateAppRequest{Definition: untrimmedRange},
			message: "app definition is invalid",
		},
		"unresolvable time range": {
			request: &opensplunk.CreateAppRequest{Definition: unresolvableRange},
			message: "app definition is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeCreateAppRequest(t.Context(), test.request)
			assertSanitizerRejection(t, err, test.message)
		})
	}
}

func TestSanitizeGetAppRequestRequiresOneCanonicalSelector(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		selector *opensplunk.AppSelector
		message  string
	}{
		"app ID": {selector: appSanitizerSelector()},
		"slug": {selector: &opensplunk.AppSelector{
			Selector: &opensplunk.AppSelector_Slug{Slug: "operations"},
		}},
		"absent": {message: "app selector is invalid"},
		"unset": {
			selector: &opensplunk.AppSelector{},
			message:  "app selector is invalid",
		},
		"empty app ID": {
			selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_AppId{AppId: ""},
			},
			message: "app selector is invalid",
		},
		"untrimmed app ID": {
			selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_AppId{AppId: " app-1"},
			},
			message: "app selector is invalid",
		},
		"oversized app ID": {
			selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_AppId{
					AppId: strings.Repeat("a", maximumAppAdministrationIDBytes+1),
				},
			},
			message: "app selector is invalid",
		},
		"non-canonical slug": {
			selector: &opensplunk.AppSelector{
				Selector: &opensplunk.AppSelector_Slug{Slug: "Operations"},
			},
			message: "app selector is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.GetAppRequest{Selector: test.selector}
			got, err := sanitizeGetAppRequest(t.Context(), request)
			if got != request {
				t.Fatalf("sanitizer returned %p, want %p", got, request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			if canonicalAppAdministrationSelector(got.GetSelector()) ==
				(AppAdministrationSelector{}) {
				t.Fatal("canonical selector is empty")
			}
		})
	}
}

func TestSanitizeListAppsRequestBoundsTokenAndFilters(t *testing.T) {
	t.Parallel()

	emptyToken := ""
	untrimmedToken := " token "
	oversizedToken := strings.Repeat("t", maximumPageTokenBytes+1)
	controlText := "needle\ncontrol"
	oversizedText := strings.Repeat("t", maximumAppAdministrationTextFilter+1)
	tests := map[string]struct {
		request *opensplunk.ListAppsRequest
		message string
	}{
		"empty":    {request: &opensplunk.ListAppsRequest{}},
		"one page": {request: &opensplunk.ListAppsRequest{Page: &opensplunk.PageRequest{}}},
		"empty token": {
			request: &opensplunk.ListAppsRequest{
				Page: &opensplunk.PageRequest{PageToken: &emptyToken},
			},
			message: "page token is invalid",
		},
		"untrimmed token": {
			request: &opensplunk.ListAppsRequest{
				Page: &opensplunk.PageRequest{PageToken: &untrimmedToken},
			},
			message: "page token is invalid",
		},
		"oversized token": {
			request: &opensplunk.ListAppsRequest{
				Page: &opensplunk.PageRequest{PageToken: &oversizedToken},
			},
			message: "page token is invalid",
		},
		"too many state filters": {
			request: &opensplunk.ListAppsRequest{
				StateFilters: make(
					[]opensplunk.AppState,
					maximumAppAdministrationStateFilters+1,
				),
			},
			message: "app list request is invalid",
		},
		"control text filter": {
			request: &opensplunk.ListAppsRequest{TextFilter: &controlText},
			message: "app list request is invalid",
		},
		"oversized text filter": {
			request: &opensplunk.ListAppsRequest{TextFilter: &oversizedText},
			message: "app list request is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := sanitizeListAppsRequest(t.Context(), test.request)
			if got != test.request {
				t.Fatalf("sanitizer returned %p, want %p", got, test.request)
			}
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeListAppsRequestTrimsTheTextFilter(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		filter *string
		want   *string
	}{
		"absent":     {},
		"padded":     {filter: new("  needle  "), want: new("needle")},
		"whitespace": {filter: new("   "), want: new("")},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			request := &opensplunk.ListAppsRequest{TextFilter: test.filter}
			got, err := sanitizeListAppsRequest(t.Context(), request)
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
			switch {
			case test.want == nil && got.TextFilter != nil:
				t.Fatalf("text filter = %q, want absent", got.GetTextFilter())
			case test.want != nil && got.GetTextFilter() != *test.want:
				t.Fatalf("text filter = %q, want %q", got.GetTextFilter(), *test.want)
			}
		})
	}
}

func TestSanitizeUpdateAppRequestRequiresVersionAndDefinition(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request *opensplunk.UpdateAppRequest
		message string
	}{
		"canonical": {request: &opensplunk.UpdateAppRequest{
			Selector:        appSanitizerSelector(),
			ExpectedVersion: 2,
			Definition:      appSanitizerDefinition(),
		}},
		"absent selector": {
			request: &opensplunk.UpdateAppRequest{
				ExpectedVersion: 2,
				Definition:      appSanitizerDefinition(),
			},
			message: "app selector is invalid",
		},
		"zero version": {
			request: &opensplunk.UpdateAppRequest{
				Selector:   appSanitizerSelector(),
				Definition: appSanitizerDefinition(),
			},
			message: "app expected version is invalid",
		},
		"absent definition": {
			request: &opensplunk.UpdateAppRequest{
				Selector:        appSanitizerSelector(),
				ExpectedVersion: 2,
			},
			message: "app definition is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeUpdateAppRequest(t.Context(), test.request)
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeSetAppStateRequestRequiresSelectorAndVersion(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request *opensplunk.SetAppStateRequest
		message string
	}{
		"canonical": {request: &opensplunk.SetAppStateRequest{
			Selector:        appSanitizerSelector(),
			ExpectedVersion: 5,
			State:           opensplunk.AppState_APP_STATE_ARCHIVED,
		}},
		"absent selector": {
			request: &opensplunk.SetAppStateRequest{ExpectedVersion: 5},
			message: "app selector is invalid",
		},
		"zero version": {
			request: &opensplunk.SetAppStateRequest{Selector: appSanitizerSelector()},
			message: "app expected version is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeSetAppStateRequest(t.Context(), test.request)
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeDeleteAppRequestRequiresACanonicalConfirmation(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request *opensplunk.DeleteAppRequest
		message string
	}{
		"canonical": {request: &opensplunk.DeleteAppRequest{
			Selector:         appSanitizerSelector(),
			ExpectedVersion:  9,
			ConfirmationSlug: "operations",
		}},
		"absent selector": {
			request: &opensplunk.DeleteAppRequest{
				ExpectedVersion:  9,
				ConfirmationSlug: "operations",
			},
			message: "app selector is invalid",
		},
		"zero version": {
			request: &opensplunk.DeleteAppRequest{
				Selector:         appSanitizerSelector(),
				ConfirmationSlug: "operations",
			},
			message: "app expected version is invalid",
		},
		"empty confirmation": {
			request: &opensplunk.DeleteAppRequest{
				Selector:        appSanitizerSelector(),
				ExpectedVersion: 9,
			},
			message: "app delete confirmation is invalid",
		},
		"uppercase confirmation": {
			request: &opensplunk.DeleteAppRequest{
				Selector:         appSanitizerSelector(),
				ExpectedVersion:  9,
				ConfirmationSlug: "Operations",
			},
			message: "app delete confirmation is invalid",
		},
		"padded confirmation": {
			request: &opensplunk.DeleteAppRequest{
				Selector:         appSanitizerSelector(),
				ExpectedVersion:  9,
				ConfirmationSlug: " operations ",
			},
			message: "app delete confirmation is invalid",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := sanitizeDeleteAppRequest(t.Context(), test.request)
			if test.message != "" {
				assertSanitizerRejection(t, err, test.message)
				return
			}
			if err != nil {
				t.Fatalf("sanitize = %v", err)
			}
		})
	}
}

func TestSanitizeAppRequestsTolerateUnknownFields(t *testing.T) {
	t.Parallel()

	topLevel := futureProtobufField("future-app")
	nested := futureProtobufField("future-selector")
	request := &opensplunk.GetAppRequest{Selector: appSanitizerSelector()}
	request.ProtoReflect().SetUnknown(topLevel)
	request.Selector.ProtoReflect().SetUnknown(nested)
	got, err := sanitizeGetAppRequest(t.Context(), request)
	if err != nil {
		t.Fatalf("sanitize = %v", err)
	}
	if got.GetSelector().GetAppId() != "app-1" {
		t.Fatalf("app ID = %q, want %q", got.GetSelector().GetAppId(), "app-1")
	}
	assertUnknownFieldTolerated(t, got, topLevel)
	assertUnknownFieldTolerated(t, got.GetSelector(), nested)
}
