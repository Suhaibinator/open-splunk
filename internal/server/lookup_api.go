package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupdefinition"
	"github.com/Suhaibinator/open-splunk/internal/lookupservice"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	lookupCreateRoute   = "/knowledge/lookups/create"
	lookupGetRoute      = "/knowledge/lookups/get"
	lookupListRoute     = "/knowledge/lookups/list"
	lookupReplaceRoute  = "/knowledge/lookups/replace"
	lookupSetStateRoute = "/knowledge/lookups/state/set"
	lookupDeleteRoute   = "/knowledge/lookups/delete"
	lookupPreviewRoute  = "/knowledge/lookups/preview"

	lookupCreatePath   = apiPathPrefix + lookupCreateRoute
	lookupGetPath      = apiPathPrefix + lookupGetRoute
	lookupListPath     = apiPathPrefix + lookupListRoute
	lookupReplacePath  = apiPathPrefix + lookupReplaceRoute
	lookupSetStatePath = apiPathPrefix + lookupSetStateRoute
	lookupDeletePath   = apiPathPrefix + lookupDeleteRoute
	lookupPreviewPath  = apiPathPrefix + lookupPreviewRoute

	// The semantic maximum includes 8 MiB of CSV plus 20 canonical event paths,
	// 64 selector patterns, a description, and protobuf framing.
	maximumLookupMutationRequestBytes = int64(lookupasset.MaximumSourceBytes + 256<<10)
	maximumLookupSmallRequestBytes    = int64(16 << 10)
	maximumLookupResponseBytes        = 9 << 20
	maximumLookupProtoDepth           = 32
)

// LookupManagement is the complete lookup administrator surface. One dependency
// owns every route so partial configuration cannot expose a misleading API.
type LookupManagement interface {
	Ready() bool
	Create(context.Context, lookupservice.Scope, *opensplunk.CreateLookupRequest) (*opensplunk.CreateLookupResponse, error)
	Get(context.Context, lookupservice.Scope, *opensplunk.GetLookupRequest) (*opensplunk.GetLookupResponse, error)
	List(context.Context, lookupservice.Scope, *opensplunk.ListLookupsRequest) (*opensplunk.ListLookupsResponse, error)
	Replace(context.Context, lookupservice.Scope, *opensplunk.ReplaceLookupRequest) (*opensplunk.ReplaceLookupResponse, error)
	SetState(context.Context, lookupservice.Scope, *opensplunk.SetLookupStateRequest) (*opensplunk.SetLookupStateResponse, error)
	Delete(context.Context, lookupservice.Scope, *opensplunk.DeleteLookupRequest) (*opensplunk.DeleteLookupResponse, error)
	Preview(context.Context, lookupservice.Scope, *opensplunk.PreviewLookupRequest) (*opensplunk.PreviewLookupResponse, error)
}

var _ LookupManagement = (*lookupservice.Service)(nil)

type serializedCreateLookupResponse = boundedProtoResponse[*opensplunk.CreateLookupResponse]
type serializedGetLookupResponse = boundedProtoResponse[*opensplunk.GetLookupResponse]
type serializedListLookupsResponse = boundedProtoResponse[*opensplunk.ListLookupsResponse]
type serializedReplaceLookupResponse = boundedProtoResponse[*opensplunk.ReplaceLookupResponse]
type serializedSetLookupStateResponse = boundedProtoResponse[*opensplunk.SetLookupStateResponse]
type serializedDeleteLookupResponse = boundedProtoResponse[*opensplunk.DeleteLookupResponse]
type serializedPreviewLookupResponse = boundedProtoResponse[*opensplunk.PreviewLookupResponse]

func newLookupBoundedCodec[Request any, Message proto.Message](inner codec.Codec[Request, Message], operation string) *boundedProtoCodec[Request, Message] {
	return newBoundedProtoCodec(inner, boundedProtoCodecOptions{
		stateError: "lookup " + operation + " response state is invalid", messageError: "lookup " + operation + " response is absent",
		maximumBytes: maximumLookupResponseBytes, sizeError: "lookup " + operation + " response exceeds its byte limit",
	})
}

func (handler *apiHandler) lookupManagementConfigured() bool {
	return handler != nil && !isNilDependency(handler.lookupManagement) && handler.lookupManagement.Ready()
}

func (handler *apiHandler) lookupManagementRoutes(noAuth router.AuthLevel) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[*opensplunk.CreateLookupRequest, *serializedCreateLookupResponse]{
			Path: lookupCreateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.CreateLookupRequest, *opensplunk.CreateLookupResponse](), "create"), Handler: handler.createLookup,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupMutationRequestBytes},
			Sanitizer: sanitizeCreateLookupRequest,
		},
		router.RouteConfig[*opensplunk.GetLookupRequest, *serializedGetLookupResponse]{
			Path: lookupGetRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.GetLookupRequest, *opensplunk.GetLookupResponse](), "get"), Handler: handler.getLookup,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupSmallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.ListLookupsRequest, *serializedListLookupsResponse]{
			Path: lookupListRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.ListLookupsRequest, *opensplunk.ListLookupsResponse](), "list"), Handler: handler.listLookups,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupSmallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.ReplaceLookupRequest, *serializedReplaceLookupResponse]{
			Path: lookupReplaceRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.ReplaceLookupRequest, *opensplunk.ReplaceLookupResponse](), "replace"), Handler: handler.replaceLookup,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupMutationRequestBytes},
			Sanitizer: sanitizeReplaceLookupRequest,
		},
		router.RouteConfig[*opensplunk.SetLookupStateRequest, *serializedSetLookupStateResponse]{
			Path: lookupSetStateRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.SetLookupStateRequest, *opensplunk.SetLookupStateResponse](), "set state"), Handler: handler.setLookupState,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupSmallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.DeleteLookupRequest, *serializedDeleteLookupResponse]{
			Path: lookupDeleteRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.DeleteLookupRequest, *opensplunk.DeleteLookupResponse](), "delete"), Handler: handler.deleteLookup,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupSmallRequestBytes},
			Sanitizer: discardUnknownProtoFields,
		},
		router.RouteConfig[*opensplunk.PreviewLookupRequest, *serializedPreviewLookupResponse]{
			Path: lookupPreviewRoute, Methods: []router.HttpMethod{router.MethodPost}, AuthLevel: &noAuth,
			Codec: newLookupBoundedCodec(codec.NewProtoCodec[*opensplunk.PreviewLookupRequest, *opensplunk.PreviewLookupResponse](), "preview"), Handler: handler.previewLookup,
			SourceType: router.Body, Overrides: sroutercommon.RouteOverrides{MaxBodySize: maximumLookupMutationRequestBytes},
			Sanitizer: sanitizePreviewLookupRequest,
		},
	}
}

func (handler *apiHandler) createLookup(request *http.Request, input *opensplunk.CreateLookupRequest) (*serializedCreateLookupResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.Create, func(response *opensplunk.CreateLookupResponse, scope lookupservice.Scope) bool {
		return response != nil && validLookupProjection(response.GetLookup(), scope) && response.GetLookup().GetVersion() == 1 && response.GetLookup().GetState() == opensplunk.LookupState_LOOKUP_STATE_ACTIVE
	})
}

func (handler *apiHandler) getLookup(request *http.Request, input *opensplunk.GetLookupRequest) (*serializedGetLookupResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.Get, func(response *opensplunk.GetLookupResponse, scope lookupservice.Scope) bool {
		return response != nil && validLookupProjection(response.GetLookup(), scope) && response.GetLookup().GetLookupId() == input.GetLookupId() && (input.Version == nil || response.GetLookup().GetVersion() == input.GetVersion())
	})
}

func (handler *apiHandler) listLookups(request *http.Request, input *opensplunk.ListLookupsRequest) (*serializedListLookupsResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.List, func(response *opensplunk.ListLookupsResponse, scope lookupservice.Scope) bool {
		if response == nil || response.GetPage() == nil || len(response.GetLookups()) > lookupservice.MaximumPageSize {
			return false
		}
		for _, lookup := range response.GetLookups() {
			if !validLookupProjection(lookup, scope) {
				return false
			}
		}
		page := response.GetPage()
		if page.TotalSize == nil {
			if page.GetTotalSizeExact() {
				return false
			}
		} else if !page.GetTotalSizeExact() || page.GetTotalSize() < uint64(len(response.GetLookups())) ||
			page.GetTotalSize() > lookupcatalog.MaximumManagedLookups {
			return false
		}
		if page.GetNextPageToken() == "" {
			return true
		}
		pageSize := uint32(lookupservice.DefaultPageSize)
		if input.GetPage() != nil && input.GetPage().PageSize != nil {
			pageSize = input.GetPage().GetPageSize()
		}
		return len(page.GetNextPageToken()) <= lookupservice.MaximumPageToken &&
			pageSize > 0 && len(response.GetLookups()) == int(pageSize)
	})
}

func (handler *apiHandler) replaceLookup(request *http.Request, input *opensplunk.ReplaceLookupRequest) (*serializedReplaceLookupResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.Replace, func(response *opensplunk.ReplaceLookupResponse, scope lookupservice.Scope) bool {
		return response != nil && validLookupProjection(response.GetLookup(), scope) && response.GetLookup().GetLookupId() == input.GetLookupId() && response.GetLookup().GetVersion() == input.GetExpectedVersion()+1
	})
}

func (handler *apiHandler) setLookupState(request *http.Request, input *opensplunk.SetLookupStateRequest) (*serializedSetLookupStateResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.SetState, func(response *opensplunk.SetLookupStateResponse, scope lookupservice.Scope) bool {
		return response != nil && validLookupProjection(response.GetLookup(), scope) && response.GetLookup().GetLookupId() == input.GetLookupId() && response.GetLookup().GetVersion() == input.GetExpectedVersion()+1 && response.GetLookup().GetState() == input.GetState()
	})
}

func (handler *apiHandler) deleteLookup(request *http.Request, input *opensplunk.DeleteLookupRequest) (*serializedDeleteLookupResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.Delete, func(response *opensplunk.DeleteLookupResponse, _ lookupservice.Scope) bool {
		return response != nil && response.GetLookupId() == input.GetLookupId() && response.GetVersion() == input.GetExpectedVersion()+1
	})
}

func (handler *apiHandler) previewLookup(request *http.Request, input *opensplunk.PreviewLookupRequest) (*serializedPreviewLookupResponse, error) {
	return invokeLookup(handler, request, input, handler.lookupManagement.Preview, func(response *opensplunk.PreviewLookupResponse, _ lookupservice.Scope) bool {
		return validLookupPreview(response)
	})
}

func invokeLookup[Request proto.Message, Response proto.Message](handler *apiHandler, request *http.Request, input Request, call func(context.Context, lookupservice.Scope, Request) (Response, error), validate func(Response, lookupservice.Scope) bool) (*boundedProtoResponse[Response], error) {
	if handler == nil || request == nil || isNilDependency(input) || call == nil || validate == nil {
		return nil, internalError()
	}
	release, ok := handler.acquireSerialization()
	if !ok {
		return nil, router.NewHTTPError(http.StatusTooManyRequests, "lookup response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	scope, err := handler.lookupScope(request)
	if err != nil {
		return nil, err
	}
	detachedInput, ok := proto.Clone(input).(Request)
	if !ok {
		return nil, internalError()
	}
	response, err := call(request.Context(), scope, detachedInput)
	if mapped := mapLookupCallError(request.Context(), err); mapped != nil {
		return nil, mapped
	}
	if isNilDependency(response) || !validate(response, scope) || proto.Size(response) > maximumLookupResponseBytes || !lookupMessageKnownAndBounded(response) {
		return nil, unavailableError("lookup service returned an invalid response")
	}
	detachedResponse, ok := proto.Clone(response).(Response)
	if !ok || !validate(detachedResponse, scope) {
		return nil, unavailableError("lookup service returned an invalid response")
	}
	transferred = true
	return &boundedProtoResponse[Response]{message: detachedResponse, ctx: request.Context(), release: release}, nil
}

func (handler *apiHandler) lookupScope(request *http.Request) (lookupservice.Scope, error) {
	principal, err := handler.administratorPrincipal(request)
	if err != nil {
		return lookupservice.Scope{}, err
	}
	return lookupservice.Scope{TenantID: principal.TenantID(), OwnerID: principal.OwnerID()}, nil
}

func mapLookupCallError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if requestContextFailure(ctx, err) != nil {
		return router.NewHTTPError(http.StatusRequestTimeout, "lookup request was canceled")
	}
	switch {
	case errors.Is(err, lookupservice.ErrInvalid):
		return badRequestError("lookup request is invalid")
	case errors.Is(err, lookupservice.ErrNotFound):
		return router.NewHTTPError(http.StatusNotFound, "lookup not found")
	case errors.Is(err, lookupservice.ErrConflict):
		return router.NewHTTPError(http.StatusConflict, "lookup version or state conflict")
	case errors.Is(err, lookupservice.ErrResourceLimit):
		return router.NewHTTPError(http.StatusRequestEntityTooLarge, "lookup resource limit exceeded")
	default:
		return unavailableError("lookup service is unavailable")
	}
}

// boundedLookupDefinitionRepeatedShape runs before reflection, cloning, or
// normalization at configurable boundaries. A raw protobuf byte ceiling does
// not bound the heap represented by repeated empty messages.
func boundedLookupDefinitionRepeatedShape(definition *opensplunk.LookupDefinition) bool {
	if definition == nil {
		return true
	}
	if len(definition.GetKeyMappings()) > lookupdefinition.MaximumKeyMappings ||
		len(definition.GetOutputMappings()) > lookupdefinition.MaximumOutputMappings {
		return false
	}
	selector := definition.GetSelector()
	return selector == nil ||
		len(selector.GetIndexPatterns()) <= knowledge.MaximumSelectorPatternsPerDimension &&
			len(selector.GetHostPatterns()) <= knowledge.MaximumSelectorPatternsPerDimension &&
			len(selector.GetSourcePatterns()) <= knowledge.MaximumSelectorPatternsPerDimension &&
			len(selector.GetSourcetypePatterns()) <= knowledge.MaximumSelectorPatternsPerDimension
}

// sanitizeCreateLookupRequest rejects unknown fields inside the persisted lookup
// definition and then discards unknown fields from the rest of the request
// envelope. Replace and Preview share the rule.
func sanitizeCreateLookupRequest(
	ctx context.Context,
	request *opensplunk.CreateLookupRequest,
) (*opensplunk.CreateLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return discardUnknownProtoFields(ctx, request)
}

func sanitizeReplaceLookupRequest(
	ctx context.Context,
	request *opensplunk.ReplaceLookupRequest,
) (*opensplunk.ReplaceLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return discardUnknownProtoFields(ctx, request)
}

func sanitizePreviewLookupRequest(
	ctx context.Context,
	request *opensplunk.PreviewLookupRequest,
) (*opensplunk.PreviewLookupRequest, error) {
	if err := rejectUnknownLookupDefinition(request.GetDefinition()); err != nil {
		return request, err
	}
	return discardUnknownProtoFields(ctx, request)
}

// rejectUnknownLookupDefinition prevents a newer client's persisted semantic
// fields from being silently discarded while retaining forward compatibility
// for unknown request-envelope fields.
func rejectUnknownLookupDefinition(definition *opensplunk.LookupDefinition) error {
	if definition == nil {
		return nil
	}
	if !boundedLookupDefinitionRepeatedShape(definition) {
		return badRequestError("lookup definition exceeds its entry limit")
	}
	if !lookupMessageKnownAndBounded(definition) {
		return badRequestError("lookup definition contains unsupported fields")
	}
	return nil
}

func validLookupProjection(lookup *opensplunk.Lookup, scope lookupservice.Scope) bool {
	if lookup == nil || lookup.GetDefinition() == nil || lookup.GetTenantId() != scope.TenantID || lookup.GetOwnerId() != scope.OwnerID ||
		lookup.GetVersion() == 0 || lookup.GetVersion() > math.MaxInt64 || !validLookupManagementIdentity(lookup.GetLookupId(), 128) ||
		!boundedLookupDefinitionRepeatedShape(lookup.GetDefinition()) || !validLookupColumns(lookup.GetColumns(), false) ||
		lookup.GetRowCount() > lookupasset.MaximumRows || lookup.GetCanonicalSizeBytes() == 0 || lookup.GetCanonicalSizeBytes() > lookupasset.MaximumSourceBytes ||
		len(lookup.GetSourceSha256()) != 32 || len(lookup.GetContentSha256()) != 32 || lookup.GetCreatedAt() == nil || lookup.GetUpdatedAt() == nil ||
		!lookup.GetCreatedAt().IsValid() || !lookup.GetUpdatedAt().IsValid() || lookup.GetUpdatedAt().AsTime().Before(lookup.GetCreatedAt().AsTime()) {
		return false
	}
	normalized, err := lookupdefinition.Normalize(lookup.GetDefinition(), lookup.GetColumns())
	if err != nil || !proto.Equal(normalized.Definition, lookup.GetDefinition()) {
		return false
	}
	switch lookup.GetState() {
	case opensplunk.LookupState_LOOKUP_STATE_ACTIVE:
		return lookup.DisabledAt == nil && lookup.DeletedAt == nil
	case opensplunk.LookupState_LOOKUP_STATE_DISABLED:
		return validLookupLifecycleTime(lookup.GetDisabledAt(), lookup.GetCreatedAt(), lookup.GetUpdatedAt()) && lookup.DeletedAt == nil
	case opensplunk.LookupState_LOOKUP_STATE_DELETED:
		return validLookupLifecycleTime(lookup.GetDisabledAt(), lookup.GetCreatedAt(), lookup.GetUpdatedAt()) &&
			validLookupLifecycleTime(lookup.GetDeletedAt(), lookup.GetDisabledAt(), lookup.GetUpdatedAt())
	default:
		return false
	}
}

func validLookupLifecycleTime(value, earliest, latest *timestamppb.Timestamp) bool {
	return value != nil && earliest != nil && latest != nil && value.IsValid() && earliest.IsValid() && latest.IsValid() &&
		!value.AsTime().Before(earliest.AsTime()) && !value.AsTime().After(latest.AsTime())
}

func validLookupColumns(columns []string, allowEmpty bool) bool {
	if len(columns) == 0 {
		return allowEmpty
	}
	if len(columns) > lookupasset.MaximumColumns {
		return false
	}
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" || len(column) > lookupasset.MaximumHeaderBytes || !utf8.ValidString(column) ||
			strings.TrimSpace(column) != column || strings.IndexByte(column, 0) >= 0 ||
			strings.ContainsFunc(column, func(character rune) bool {
				return unicode.IsControl(character) || unicode.Is(unicode.Cf, character)
			}) {
			return false
		}
		if _, duplicate := seen[column]; duplicate {
			return false
		}
		seen[column] = struct{}{}
	}
	return true
}

func validLookupManagementIdentity(value string, maximum int) bool {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) {
		return false
	}
	for index := range len(value) {
		character := value[index]
		alphaNumeric := character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9'
		if !alphaNumeric && (index == 0 || character != '.' && character != '_' && character != ':' && character != '-') {
			return false
		}
	}
	return true
}

func validLookupPreview(response *opensplunk.PreviewLookupResponse) bool {
	if response == nil || !validLookupColumns(response.GetColumns(), true) ||
		len(response.GetRows()) > lookupservice.MaximumPreviewRows || len(response.GetViolations()) > 8 ||
		response.GetTotalRows() > lookupasset.MaximumRows || len(response.GetSourceSha256()) != 32 {
		return false
	}
	if len(response.GetViolations()) != 0 {
		for _, violation := range response.GetViolations() {
			if violation == nil || violation.GetFieldPath() == "" || len(violation.GetFieldPath()) > 255 ||
				violation.GetCode() == "" || len(violation.GetCode()) > 128 ||
				violation.GetMessage() == "" || len(violation.GetMessage()) > 4<<10 {
				return false
			}
		}
		if len(response.GetRows()) != 0 || response.GetTruncated() {
			return false
		}
		if len(response.GetColumns()) == 0 {
			return response.GetTotalRows() == 0 && len(response.GetContentSha256()) == 0
		}
		return len(response.GetContentSha256()) == 32
	}
	if len(response.GetColumns()) == 0 || len(response.GetContentSha256()) != 32 || response.GetTotalRows() < uint64(len(response.GetRows())) {
		return false
	}
	totalCellBytes := 0
	for _, row := range response.GetRows() {
		if row == nil || len(row.GetValues()) != len(response.GetColumns()) {
			return false
		}
		rowBytes := 0
		for _, value := range row.GetValues() {
			if len(value) > lookupasset.MaximumCellBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
				return false
			}
			rowBytes += len(value)
			if rowBytes > lookupasset.MaximumRowBytes || len(value) > lookupasset.MaximumSourceBytes-totalCellBytes {
				return false
			}
			totalCellBytes += len(value)
		}
	}
	return response.GetTruncated() == (response.GetTotalRows() > uint64(len(response.GetRows())))
}

func lookupMessageKnownAndBounded(message proto.Message) bool {
	if isNilDependency(message) {
		return false
	}
	type pending struct {
		message protoreflect.Message
		depth   int
	}
	queue := []pending{{message: message.ProtoReflect()}}
	for len(queue) != 0 {
		current := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if current.depth > maximumLookupProtoDepth || !current.message.IsValid() || len(current.message.GetUnknown()) != 0 {
			return false
		}
		valid := true
		current.message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.IsMap() {
				valid = false
				return false
			}
			if field.IsList() && field.Kind() == protoreflect.MessageKind {
				list := value.List()
				for index := 0; index < list.Len(); index++ {
					queue = append(queue, pending{message: list.Get(index).Message(), depth: current.depth + 1})
				}
			} else if !field.IsList() && field.Kind() == protoreflect.MessageKind {
				queue = append(queue, pending{message: value.Message(), depth: current.depth + 1})
			}
			return true
		})
		if !valid {
			return false
		}
	}
	return true
}
