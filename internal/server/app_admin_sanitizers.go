package server

import (
	"context"
	"errors"
	"slices"
	"strings"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
)

// This file holds one sanitizer per app administration route, in
// route-registration order. Every sanitizer first discards fields unknown to
// this server, then normalizes and bounds the request in place, so an app
// handler only ever sees a canonical message: exactly one selector key that is
// already trimmed and bounded, a positive expected version, and a create
// definition whose slug, display name, description and default indexes are
// canonical. Mapping an enumeration to its stored value stays in the handler,
// because the handler needs the converted value and the conversion is the
// check; so does any rule that depends on the stored app.

// sanitizeCreateAppRequest rejects idempotency keys this server does not
// implement, then bounds and canonicalizes the definition it will persist.
func sanitizeCreateAppRequest(
	ctx context.Context,
	request *opensplunk.CreateAppRequest,
) (*opensplunk.CreateAppRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if request.ClientRequestId != nil {
		return request, badRequestError("client request idempotency is not supported")
	}
	definition, err := appAdministrationDefinition(request.GetDefinition())
	if err != nil {
		return request, badRequestError("app definition is invalid")
	}
	writeCanonicalAppDefinition(request.GetDefinition(), definition)
	return request, nil
}

func sanitizeGetAppRequest(
	ctx context.Context,
	request *opensplunk.GetAppRequest,
) (*opensplunk.GetAppRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := validAppAdministrationSelector(request.GetSelector()); err != nil {
		return request, err
	}
	return request, nil
}

// sanitizeListAppsRequest bounds the continuation token and the filters the
// catalog page is built from, and rewrites the text filter in its trimmed
// form. Whether a well-formed token still matches this tenant's filters and
// catalog revision is stored state and stays in the handler.
func sanitizeListAppsRequest(
	ctx context.Context,
	request *opensplunk.ListAppsRequest,
) (*opensplunk.ListAppsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if page := request.GetPage(); page != nil && page.PageToken != nil {
		token := page.GetPageToken()
		if token == "" ||
			strings.TrimSpace(token) != token ||
			len(token) > maximumPageTokenBytes {
			return request, badRequestError("page token is invalid")
		}
	}
	if len(request.GetStateFilters()) > maximumAppAdministrationStateFilters {
		return request, badRequestError("app list request is invalid")
	}
	if request.TextFilter == nil {
		return request, nil
	}
	textFilter, err := normalizeAppAdministrationTextFilter(request.TextFilter)
	if err != nil {
		return request, badRequestError("app list request is invalid")
	}
	request.TextFilter = new(textFilter)
	return request, nil
}

func sanitizeUpdateAppRequest(
	ctx context.Context,
	request *opensplunk.UpdateAppRequest,
) (*opensplunk.UpdateAppRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := validAppAdministrationSelector(request.GetSelector()); err != nil {
		return request, err
	}
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError("app expected version is invalid")
	}
	if request.GetDefinition() == nil {
		return request, badRequestError("app definition is invalid")
	}
	return request, nil
}

func sanitizeSetAppStateRequest(
	ctx context.Context,
	request *opensplunk.SetAppStateRequest,
) (*opensplunk.SetAppStateRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := validAppAdministrationSelector(request.GetSelector()); err != nil {
		return request, err
	}
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError("app expected version is invalid")
	}
	return request, nil
}

// sanitizeDeleteAppRequest requires the confirmation slug to be canonical.
// Whether it names the app being deleted depends on the stored record and
// stays in the handler, next to the archived-state and version rules.
func sanitizeDeleteAppRequest(
	ctx context.Context,
	request *opensplunk.DeleteAppRequest,
) (*opensplunk.DeleteAppRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := validAppAdministrationSelector(request.GetSelector()); err != nil {
		return request, err
	}
	if err := administrationExpectedVersion(request.GetExpectedVersion()); err != nil {
		return request, badRequestError("app expected version is invalid")
	}
	confirmation := request.GetConfirmationSlug()
	if canonical, err := normalizeAppAdministrationSlug(confirmation); err != nil ||
		canonical != confirmation {
		return request, badRequestError("app delete confirmation is invalid")
	}
	return request, nil
}

func appAdministrationSelector(
	input *opensplunk.AppSelector,
) (AppAdministrationSelector, error) {
	if input == nil {
		return AppAdministrationSelector{}, errors.New("selector is required")
	}
	switch selected := input.GetSelector().(type) {
	case *opensplunk.AppSelector_AppId:
		appID := strings.TrimSpace(selected.AppId)
		if appID != selected.AppId ||
			validateBoundedIdentifier(
				appID,
				maximumAppAdministrationIDBytes,
				false,
			) != nil {
			return AppAdministrationSelector{}, errors.New("app ID is invalid")
		}
		return AppAdministrationSelector{AppID: strings.Clone(appID)}, nil
	case *opensplunk.AppSelector_Slug:
		slug, err := normalizeAppAdministrationSlug(selected.Slug)
		if err != nil || slug != selected.Slug {
			return AppAdministrationSelector{}, errors.New("app slug is invalid")
		}
		return AppAdministrationSelector{Slug: strings.Clone(slug)}, nil
	default:
		return AppAdministrationSelector{}, errors.New("selector is required")
	}
}

func validAppAdministrationSelector(input *opensplunk.AppSelector) error {
	if _, err := appAdministrationSelector(input); err != nil {
		return badRequestError("app selector is invalid")
	}
	return nil
}

// canonicalAppAdministrationSelector reads the one lookup key a sanitized
// selector carries. An unset selector cannot reach a handler, and the empty
// selector it would produce matches no stored app.
func canonicalAppAdministrationSelector(
	input *opensplunk.AppSelector,
) AppAdministrationSelector {
	switch selected := input.GetSelector().(type) {
	case *opensplunk.AppSelector_AppId:
		return AppAdministrationSelector{AppID: strings.Clone(selected.AppId)}
	case *opensplunk.AppSelector_Slug:
		return AppAdministrationSelector{Slug: strings.Clone(selected.Slug)}
	default:
		return AppAdministrationSelector{}
	}
}

// canonicalAppAdministrationDefinition reads a definition the create sanitizer
// has already canonicalized.
func canonicalAppAdministrationDefinition(
	input *opensplunk.AppDefinition,
) AppAdministrationDefinition {
	definition := AppAdministrationDefinition{
		Slug:              strings.Clone(input.GetSlug()),
		DisplayName:       strings.Clone(input.GetDisplayName()),
		Description:       cloneOptionalString(input.Description),
		DefaultIndexNames: slices.Clone(input.GetDefaultIndexNames()),
	}
	if timeRange := input.GetDefaultTimeRange(); timeRange != nil {
		definition.DefaultTimeRange = &AppAdministrationTimeRange{
			Earliest: cloneOptionalString(timeRange.Earliest),
			Latest:   cloneOptionalString(timeRange.Latest),
			Timezone: cloneOptionalString(timeRange.Timezone),
		}
	}
	return definition
}

// writeCanonicalAppDefinition replaces the caller's spelling of a definition
// with the canonical one, so the handler persists what the sanitizer accepted
// rather than re-deriving it.
func writeCanonicalAppDefinition(
	input *opensplunk.AppDefinition,
	canonical AppAdministrationDefinition,
) {
	input.Slug = strings.Clone(canonical.Slug)
	input.DisplayName = strings.Clone(canonical.DisplayName)
	input.Description = cloneOptionalString(canonical.Description)
	input.DefaultIndexNames = slices.Clone(canonical.DefaultIndexNames)
}

func normalizeAppAdministrationTextFilter(input *string) (string, error) {
	if input == nil {
		return "", nil
	}
	value := strings.TrimSpace(*input)
	if validateAdminText(
		value,
		maximumAppAdministrationTextFilter,
		true,
		false,
	) != nil {
		return "", errors.New("text filter is invalid")
	}
	return value, nil
}
