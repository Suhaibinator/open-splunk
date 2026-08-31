package server

import (
	"context"
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

// This file holds one sanitizer per collector administration route, in
// route-registration order. Every sanitizer first discards fields unknown to
// this server, then normalizes and bounds the request in place, so a collector
// handler only ever sees a canonical message: a collector ID that is a valid
// token identifier, a positive expected version, an exact single-path update
// mask, and list filters that already fit the fleet catalog's bounds. Mapping
// an enumeration to its fleet value stays in the handler, because the handler
// needs the converted value and the conversion is the check.

// sanitizeListCollectorsRequest bounds the page, the state filters, the index
// filter and the text filter, and rewrites the text filter in its trimmed
// form. A request that carries a continuation token reports every failure as
// an invalid page token so an opaque cursor never explains itself.
func (handler *apiHandler) sanitizeListCollectorsRequest(
	ctx context.Context,
	request *opensplunk.ListCollectorsRequest,
) (*opensplunk.ListCollectorsRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	if err := handler.boundCollectorListFilters(request); err != nil {
		if request.GetPage() != nil && request.GetPage().PageToken != nil {
			return request, badRequestError("page token is invalid")
		}
		return request, badRequestError("collector list request is invalid")
	}
	return request, nil
}

func sanitizeGetCollectorRequest(
	ctx context.Context,
	request *opensplunk.GetCollectorRequest,
) (*opensplunk.GetCollectorRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	collectorID, err := canonicalCollectorID(request.GetCollectorId())
	if err != nil {
		return request, badRequestError("collector ID is invalid")
	}
	request.CollectorId = collectorID
	return request, nil
}

func sanitizeUpdateCollectorRequest(
	ctx context.Context,
	request *opensplunk.UpdateCollectorRequest,
) (*opensplunk.UpdateCollectorRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	collectorID, expectedVersion, displayName, err :=
		normalizeCollectorDisplayNameUpdate(request)
	if err != nil {
		return request, badRequestError("collector update is invalid")
	}
	request.CollectorId = collectorID
	request.ExpectedVersion = expectedVersion
	request.DisplayName = displayName
	return request, nil
}

func sanitizeSetCollectorEnabledRequest(
	ctx context.Context,
	request *opensplunk.SetCollectorEnabledRequest,
) (*opensplunk.SetCollectorEnabledRequest, error) {
	request, err := discardUnknownProtoFields(ctx, request)
	if err != nil {
		return request, err
	}
	collectorID, err := canonicalCollectorID(request.GetCollectorId())
	if err != nil {
		return request, badRequestError("collector ID is invalid")
	}
	request.CollectorId = collectorID
	expectedVersion, err := collectorExpectedVersion(request.GetExpectedVersion())
	if err != nil {
		return request, badRequestError("collector expected version is invalid")
	}
	request.ExpectedVersion = expectedVersion
	return request, nil
}

func (handler *apiHandler) boundCollectorListFilters(
	request *opensplunk.ListCollectorsRequest,
) error {
	if page := request.GetPage(); page != nil {
		maximumPageSize := min(
			collectorfleet.MaximumCollectorListPageSize,
			handler.maximumPageSize,
		)
		if page.PageSize != nil &&
			(page.GetPageSize() == 0 || page.GetPageSize() > maximumPageSize) {
			return errors.New("collector page size is invalid")
		}
		if page.PageToken != nil {
			token := page.GetPageToken()
			if token == "" ||
				len(token) > collectorfleet.MaximumCollectorListCursorBytes ||
				!utf8.ValidString(token) ||
				strings.TrimSpace(token) != token {
				return errors.New("collector page token is invalid")
			}
		}
	}
	if len(request.GetStateFilters()) >
		collectorfleet.MaximumCollectorListStateFilters {
		return errors.New("too many collector state filters")
	}
	if err := validCollectorIndexFilter(request.IndexNameFilter); err != nil {
		return err
	}
	text, err := normalizeCollectorTextFilter(request.TextFilter)
	if err != nil {
		return err
	}
	request.TextFilter = text
	return nil
}

// validCollectorIndexFilter accepts only an index name that is already
// canonical, so the catalog never sees two spellings of one index.
func validCollectorIndexFilter(input *string) error {
	if input == nil {
		return nil
	}
	canonical, err := control.NormalizeIndexName(*input)
	if err != nil || canonical != *input {
		return errors.New("collector index filter is invalid")
	}
	return nil
}

// normalizeCollectorDisplayNameUpdate reports the canonical collector ID,
// expected version and display name of one update request. A nil display name
// clears the administrative override; the update mask must name exactly the
// one field this route may write.
func normalizeCollectorDisplayNameUpdate(
	input *opensplunk.UpdateCollectorRequest,
) (string, uint64, *string, error) {
	if input == nil {
		return "", 0, nil, errors.New("collector update is required")
	}
	collectorID, err := canonicalCollectorID(input.GetCollectorId())
	if err != nil {
		return "", 0, nil, err
	}
	expectedVersion, err := collectorExpectedVersion(input.GetExpectedVersion())
	if err != nil {
		return "", 0, nil, err
	}
	if input.GetUpdateMask() == nil ||
		!input.GetUpdateMask().IsValid(input) ||
		len(input.GetUpdateMask().GetPaths()) != 1 ||
		input.GetUpdateMask().GetPaths()[0] != "display_name" {
		return "", 0, nil, errors.New("collector update mask is invalid")
	}
	displayName, err := normalizeCollectorDisplayName(input.DisplayName)
	if err != nil {
		return "", 0, nil, err
	}
	return collectorID, expectedVersion, displayName, nil
}

func canonicalCollectorID(input string) (string, error) {
	if !validTokenCollectorID(input) {
		return "", errors.New("collector ID is invalid")
	}
	return strings.Clone(input), nil
}

func collectorExpectedVersion(input uint64) (uint64, error) {
	if err := administrationExpectedVersion(input); err != nil {
		return 0, errors.New("collector expected version is invalid")
	}
	return input, nil
}

func normalizeCollectorDisplayName(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*input)
	if validateAdminText(
		value,
		maximumCollectorDisplayNameBytes,
		false,
		false,
	) != nil {
		return nil, errors.New("collector display name is invalid")
	}
	return new(strings.Clone(value)), nil
}

func normalizeCollectorTextFilter(input *string) (*string, error) {
	if input == nil {
		return nil, nil
	}
	if len(*input) > collectorfleet.MaximumCollectorListTextBytes ||
		!utf8.ValidString(*input) {
		return nil, errors.New("collector text filter is invalid")
	}
	for _, character := range *input {
		if unicode.IsControl(character) {
			return nil, errors.New("collector text filter is invalid")
		}
	}
	value := strings.TrimSpace(*input)
	if len(value) > collectorfleet.MaximumCollectorListTextBytes {
		return nil, errors.New("collector text filter is invalid")
	}
	if value == "" {
		return nil, nil
	}
	return new(strings.Clone(value)), nil
}
