package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"fortio.org/safecast"
	"github.com/Suhaibinator/SRouter/pkg/codec"
	sroutercommon "github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

const (
	defaultAppAdministrationPageSize    = 50
	maximumAppAdministrationRows        = 64
	maximumAppAdministrationResponse    = 8 << 20
	maximumAppAdministrationIDBytes     = 128
	maximumAppAdministrationSlugBytes   = 128
	maximumAppAdministrationDisplayName = 255
	maximumAppAdministrationDescription = 16 << 10
	maximumAppAdministrationTextFilter  = 255
	maximumAppAdministrationIndexes     = 128
	maximumAppAdministrationPageCursor  = 2 << 10
	appAdministrationCursorVersion      = 1
)

var (
	// ErrAppAdministrationInvalidArgument is returned by an implementation
	// when an already transport-validated request cannot be represented.
	ErrAppAdministrationInvalidArgument = errors.New("app administration: invalid argument")
	// ErrAppAdministrationNotFound intentionally does not reveal whether a
	// selector exists in another tenant.
	ErrAppAdministrationNotFound = errors.New("app administration: app not found")
	// ErrAppAdministrationAlreadyExists covers tenant-local ID or slug
	// uniqueness conflicts.
	ErrAppAdministrationAlreadyExists = errors.New("app administration: app already exists")
	// ErrAppAdministrationConflict covers optimistic-version, lifecycle,
	// immutable-slug, and dependent-object conflicts.
	ErrAppAdministrationConflict = errors.New("app administration: version or state conflict")
	// ErrAppAdministrationInvalidPageToken covers malformed, filter-mismatched,
	// and stale-revision continuation state without exposing catalog details.
	ErrAppAdministrationInvalidPageToken = errors.New("app administration: invalid page token")
	// ErrAppAdministrationCapacity means the tenant app catalog reached its
	// configured hard bound.
	ErrAppAdministrationCapacity = errors.New("app administration: capacity exceeded")
)

var appAdministrationUpdatePaths = map[string]string{
	"display_name":                   "display_name",
	"definition.display_name":        "display_name",
	"description":                    "description",
	"definition.description":         "description",
	"default_index_names":            "default_index_names",
	"definition.default_index_names": "default_index_names",
	"default_time_range":             "default_time_range",
	"definition.default_time_range":  "default_time_range",
}

// AppAdministrationScope is derived only from an authenticated administrator.
// ActorID is the principal owner identity and is never accepted on the wire.
type AppAdministrationScope struct {
	TenantID string
	ActorID  string
}

// AppAdministrationSelector contains exactly one canonical lookup key.
type AppAdministrationSelector struct {
	AppID string
	Slug  string
}

// AppAdministrationState is the persistence-independent app lifecycle state.
type AppAdministrationState string

const (
	AppAdministrationStateActive   AppAdministrationState = "active"
	AppAdministrationStateArchived AppAdministrationState = "archived"
)

// AppAdministrationTimeRange preserves normalized reusable SPL time intent.
type AppAdministrationTimeRange struct {
	Earliest *string
	Latest   *string
	Timezone *string
}

// AppAdministrationDefinition is one detached, canonical workspace
// definition. Slug is immutable after creation.
type AppAdministrationDefinition struct {
	Slug              string
	DisplayName       string
	Description       *string
	DefaultIndexNames []string
	DefaultTimeRange  *AppAdministrationTimeRange
}

// AppAdministrationWorkspace is one optimistic-versioned app record.
type AppAdministrationWorkspace struct {
	AppID      string
	Version    uint64
	Definition AppAdministrationDefinition
	State      AppAdministrationState
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// AppAdministrationSort is a stable storage sort key.
type AppAdministrationSort uint8

const (
	AppAdministrationSortDisplayName AppAdministrationSort = iota + 1
	AppAdministrationSortCreatedAt
	AppAdministrationSortUpdatedAt
)

// AppAdministrationListRequest is a bounded storage-level page request.
// PageToken is opaque and implementations must bind it to scope, filters,
// ordering, and a stable snapshot.
type AppAdministrationListRequest struct {
	PageSize                uint32
	PageCursor              string
	RequiredCatalogRevision *uint64
	IncludeTotal            bool
	StateFilters            []AppAdministrationState
	TextFilter              string
	SortBy                  AppAdministrationSort
	SortDescending          bool
}

// AppAdministrationListResult is one storage-bounded page. TotalSize may be
// present only when IncludeTotal was requested.
type AppAdministrationListResult struct {
	Apps            []AppAdministrationWorkspace
	CatalogRevision uint64
	NextPageCursor  string
	TotalSize       *uint64
	TotalSizeExact  bool
}

func (handler *apiHandler) appAdministrationRoutes(
	noAuth router.AuthLevel,
	requestBytes int64,
	smallRequestBytes int64,
) []router.RouteDefinition {
	return []router.RouteDefinition{
		router.RouteConfig[
			*opensplunk.CreateAppRequest,
			*serializedCreateAppResponse,
		]{
			Path:       "/apps/create",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedCreateAppCodec(),
			Handler:    handler.createApp,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
		router.RouteConfig[
			*opensplunk.GetAppRequest,
			*serializedGetAppResponse,
		]{
			Path:       "/apps/get",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedGetAppCodec(),
			Handler:    handler.getApp,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
		router.RouteConfig[
			*opensplunk.ListAppsRequest,
			*serializedListAppsResponse,
		]{
			Path:       "/apps/list",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedListAppsCodec(),
			Handler:    handler.listApps,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
		router.RouteConfig[
			*opensplunk.UpdateAppRequest,
			*serializedUpdateAppResponse,
		]{
			Path:       "/apps/update",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedUpdateAppCodec(),
			Handler:    handler.updateApp,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: requestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
		router.RouteConfig[
			*opensplunk.SetAppStateRequest,
			*serializedSetAppStateResponse,
		]{
			Path:       "/apps/state/set",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedSetAppStateCodec(),
			Handler:    handler.setAppState,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
		router.RouteConfig[
			*opensplunk.DeleteAppRequest,
			*serializedDeleteAppResponse,
		]{
			Path:       "/apps/delete",
			Methods:    []router.HttpMethod{router.MethodPost},
			AuthLevel:  &noAuth,
			Codec:      newSerializedDeleteAppCodec(),
			Handler:    handler.deleteApp,
			SourceType: router.Body,
			Overrides:  sroutercommon.RouteOverrides{MaxBodySize: smallRequestBytes},
			Sanitizer:  discardUnknownProtoFields,
		},
	}
}

func (handler *apiHandler) createApp(
	request *http.Request,
	input *opensplunk.CreateAppRequest,
) (*serializedCreateAppResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	if input.ClientRequestId != nil {
		return nil, badRequestError("client request idempotency is not supported")
	}
	definition, err := handler.appAdministrationDefinition(input.GetDefinition())
	if err != nil {
		return nil, badRequestError("app definition is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("administrative response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	expectedDefinition := cloneAppAdministrationDefinition(definition)
	record, operationErr := handler.appAdmin.CreateApp(
		request.Context(),
		scope,
		cloneAppAdministrationDefinition(definition),
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	converted, err := handler.appAdministrationWorkspaceToProto(record)
	if err != nil ||
		converted.GetVersion() != 1 ||
		record.State != AppAdministrationStateActive ||
		!equalAppAdministrationDefinition(
			record.Definition,
			expectedDefinition,
		) ||
		!record.CreatedAt.Equal(record.UpdatedAt) {
		return nil, internalError()
	}
	message := &opensplunk.CreateAppResponse{App: converted}
	if proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedCreateAppResponse{
		message: message,
		// A committed mutation wins a cancellation racing after the service
		// returns successfully.
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) getApp(
	request *http.Request,
	input *opensplunk.GetAppRequest,
) (*serializedGetAppResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	selector, err := appAdministrationSelector(input.GetSelector())
	if err != nil {
		return nil, badRequestError("app selector is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("administrative response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	record, operationErr := handler.appAdmin.GetApp(
		request.Context(),
		scope,
		selector,
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	if !appAdministrationSelectorMatches(selector, record) {
		return nil, internalError()
	}
	converted, err := handler.appAdministrationWorkspaceToProto(record)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunk.GetAppResponse{App: converted}
	if proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedGetAppResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) listApps(
	request *http.Request,
	input *opensplunk.ListAppsRequest,
) (*serializedListAppsResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	listRequest, err := handler.appAdministrationListRequest(input)
	if err != nil {
		if errors.Is(err, ErrAppAdministrationInvalidPageToken) {
			return nil, badRequestError("page token is invalid")
		}
		return nil, badRequestError("app list request is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return nil, unavailableError("administrative response capacity is exhausted")
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()

	result, operationErr := handler.appAdmin.ListApps(
		request.Context(),
		scope,
		cloneAppAdministrationListRequest(listRequest),
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	message, err := handler.appAdministrationListToProto(listRequest, result)
	if errors.Is(err, ErrAppAdministrationInvalidPageToken) {
		return nil, badRequestError("page token is invalid")
	}
	if err != nil || proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedListAppsResponse{
		message: message,
		ctx:     request.Context(),
		release: release,
	}, nil
}

func (handler *apiHandler) updateApp(
	request *http.Request,
	input *opensplunk.UpdateAppRequest,
) (*serializedUpdateAppResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	selector, err := appAdministrationSelector(input.GetSelector())
	if err != nil {
		return nil, badRequestError("app selector is invalid")
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError("app expected version is invalid")
	}
	if input.GetDefinition() == nil {
		return nil, badRequestError("app definition is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	current, release, err := handler.beginAppAdministrationMutation(
		request,
		scope,
		selector,
		input.GetExpectedVersion(),
	)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	replacement, err := handler.applyAppAdministrationUpdate(
		current.Definition,
		input.GetDefinition(),
		input.GetUpdateMask(),
	)
	if errors.Is(err, errAppAdministrationImmutableSlug) {
		return nil, appAdministrationConflictError()
	}
	if err != nil {
		return nil, badRequestError("app update is invalid")
	}
	byID := AppAdministrationSelector{AppID: strings.Clone(current.AppID)}
	expectedReplacement := cloneAppAdministrationDefinition(replacement)
	record, operationErr := handler.appAdmin.UpdateApp(
		request.Context(),
		scope,
		byID,
		input.GetExpectedVersion(),
		cloneAppAdministrationDefinition(replacement),
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	converted, err := handler.appAdministrationWorkspaceToProto(record)
	if err != nil ||
		record.AppID != current.AppID ||
		record.Version != input.GetExpectedVersion()+1 ||
		record.Definition.Slug != current.Definition.Slug ||
		!equalAppAdministrationDefinition(
			record.Definition,
			expectedReplacement,
		) ||
		record.State != current.State ||
		!record.CreatedAt.Equal(current.CreatedAt) ||
		record.UpdatedAt.Before(current.UpdatedAt) {
		return nil, internalError()
	}
	message := &opensplunk.UpdateAppResponse{App: converted}
	if proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedUpdateAppResponse{
		message: message,
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) setAppState(
	request *http.Request,
	input *opensplunk.SetAppStateRequest,
) (*serializedSetAppStateResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	selector, err := appAdministrationSelector(input.GetSelector())
	if err != nil {
		return nil, badRequestError("app selector is invalid")
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError("app expected version is invalid")
	}
	state, err := appAdministrationStateFromProto(input.GetState())
	if err != nil {
		return nil, badRequestError("app state is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	current, release, err := handler.beginAppAdministrationMutation(
		request,
		scope,
		selector,
		input.GetExpectedVersion(),
	)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	expectedDefinition := cloneAppAdministrationDefinition(current.Definition)
	byID := AppAdministrationSelector{AppID: strings.Clone(current.AppID)}
	record, operationErr := handler.appAdmin.SetAppState(
		request.Context(),
		scope,
		byID,
		input.GetExpectedVersion(),
		state,
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	if record.AppID != current.AppID ||
		record.Version != input.GetExpectedVersion()+1 ||
		record.State != state ||
		!equalAppAdministrationDefinition(
			record.Definition,
			expectedDefinition,
		) ||
		!record.CreatedAt.Equal(current.CreatedAt) ||
		record.UpdatedAt.Before(current.UpdatedAt) {
		return nil, internalError()
	}
	converted, err := handler.appAdministrationWorkspaceToProto(record)
	if err != nil {
		return nil, internalError()
	}
	message := &opensplunk.SetAppStateResponse{App: converted}
	if proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedSetAppStateResponse{
		message: message,
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) deleteApp(
	request *http.Request,
	input *opensplunk.DeleteAppRequest,
) (*serializedDeleteAppResponse, error) {
	scope, err := handler.appAdministrationAccess(request)
	if err != nil {
		return nil, err
	}
	if invalidAppAdministrationRequest(input) {
		return nil, badRequestError("app request is invalid")
	}
	if handler.appAdmin == nil {
		return nil, unavailableError("app service is unavailable")
	}
	selector, err := appAdministrationSelector(input.GetSelector())
	if err != nil {
		return nil, badRequestError("app selector is invalid")
	}
	if err := administrationExpectedVersion(input.GetExpectedVersion()); err != nil {
		return nil, badRequestError("app expected version is invalid")
	}
	confirmation := input.GetConfirmationSlug()
	if canonical, err := normalizeAppAdministrationSlug(confirmation); err != nil ||
		canonical != confirmation {
		return nil, badRequestError("app delete confirmation is invalid")
	}
	if err := appAdministrationContextError(request.Context()); err != nil {
		return nil, err
	}
	current, release, err := handler.beginAppAdministrationMutation(
		request,
		scope,
		selector,
		input.GetExpectedVersion(),
	)
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	if current.State != AppAdministrationStateArchived {
		return nil, appAdministrationConflictError()
	}
	// Confirmation is checked before the destructive call, then also passed to
	// the service so an implementation can include it in the same transaction
	// as the version, archived-state, and dependent-object checks.
	if current.Definition.Slug != confirmation {
		return nil, badRequestError("app delete confirmation is invalid")
	}
	byID := AppAdministrationSelector{AppID: strings.Clone(current.AppID)}
	deletedID, operationErr := handler.appAdmin.DeleteApp(
		request.Context(),
		scope,
		byID,
		input.GetExpectedVersion(),
		confirmation,
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		return nil, mapped
	}
	if deletedID != current.AppID {
		return nil, internalError()
	}
	message := &opensplunk.DeleteAppResponse{AppId: strings.Clone(deletedID)}
	if proto.Size(message) > maximumAppAdministrationResponse {
		return nil, internalError()
	}
	transferred = true
	return &serializedDeleteAppResponse{
		message: message,
		ctx:     nil,
		release: release,
	}, nil
}

func (handler *apiHandler) appAdministrationAccess(
	request *http.Request,
) (AppAdministrationScope, error) {
	principal, err := handler.administratorPrincipal(request)
	if err != nil {
		return AppAdministrationScope{}, err
	}
	return AppAdministrationScope{
		TenantID: principal.TenantID(),
		ActorID:  principal.OwnerID(),
	}, nil
}

// beginAppAdministrationMutation acquires one serialization permit and loads
// the app a mutation will replace. On any error it releases the permit
// itself; on success the caller owns release and must transfer it into the
// response.
func (handler *apiHandler) beginAppAdministrationMutation(
	request *http.Request,
	scope AppAdministrationScope,
	selector AppAdministrationSelector,
	expectedVersion uint64,
) (AppAdministrationWorkspace, func(), error) {
	release, acquired := handler.acquireSerialization()
	if !acquired {
		return AppAdministrationWorkspace{}, nil, unavailableError("administrative response capacity is exhausted")
	}
	current, operationErr := handler.appAdmin.GetApp(
		request.Context(),
		scope,
		selector,
	)
	if mapped := mapAppAdministrationCallError(
		request.Context(),
		operationErr,
	); mapped != nil {
		release()
		return AppAdministrationWorkspace{}, nil, mapped
	}
	if !appAdministrationSelectorMatches(selector, current) {
		release()
		return AppAdministrationWorkspace{}, nil, internalError()
	}
	if _, err := handler.appAdministrationWorkspaceToProto(current); err != nil {
		release()
		return AppAdministrationWorkspace{}, nil, internalError()
	}
	if current.Version != expectedVersion {
		release()
		return AppAdministrationWorkspace{}, nil, appAdministrationConflictError()
	}
	return current, release, nil
}

func invalidAppAdministrationRequest(message proto.Message) bool {
	return isNilDependency(message) ||
		protostrict.ContainsUnknown(message.ProtoReflect())
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

func normalizeAppAdministrationSlug(input string) (string, error) {
	slug := strings.ToLower(strings.TrimSpace(input))
	if len(slug) == 0 ||
		len(slug) > maximumAppAdministrationSlugBytes ||
		!utf8.ValidString(slug) {
		return "", errors.New("app slug is invalid")
	}
	for index, character := range []byte(slug) {
		if (character >= 'a' && character <= 'z') ||
			(character >= '0' && character <= '9') ||
			(index > 0 && (character == '-' || character == '_')) {
			continue
		}
		return "", errors.New("app slug is invalid")
	}
	return slug, nil
}

func (handler *apiHandler) appAdministrationDefinition(
	input *opensplunk.AppDefinition,
) (AppAdministrationDefinition, error) {
	if input == nil {
		return AppAdministrationDefinition{}, errors.New("definition is required")
	}
	slug, err := normalizeAppAdministrationSlug(input.GetSlug())
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	displayName, err := normalizeAppAdministrationDisplayName(
		input.GetDisplayName(),
	)
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	description, err := normalizeAppAdministrationDescription(
		input.Description,
	)
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	indexNames, err := normalizeAppAdministrationIndexes(
		input.GetDefaultIndexNames(),
	)
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	defaultRange, err := handler.normalizeAppAdministrationTimeRange(
		input.GetDefaultTimeRange(),
	)
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	return AppAdministrationDefinition{
		Slug:              slug,
		DisplayName:       displayName,
		Description:       description,
		DefaultIndexNames: indexNames,
		DefaultTimeRange:  defaultRange,
	}, nil
}

func normalizeAppAdministrationDisplayName(input string) (string, error) {
	value := strings.TrimSpace(input)
	if validateAdminText(
		value,
		maximumAppAdministrationDisplayName,
		false,
		false,
	) != nil {
		return "", errors.New("display name is invalid")
	}
	return value, nil
}

func normalizeAppAdministrationDescription(
	input *string,
) (*string, error) {
	if input == nil {
		return nil, nil
	}
	value := strings.TrimSpace(*input)
	if validateAdminText(
		value,
		maximumAppAdministrationDescription,
		true,
		true,
	) != nil {
		return nil, errors.New("description is invalid")
	}
	if value == "" {
		return nil, nil
	}
	return new(value), nil
}

func normalizeAppAdministrationIndexes(input []string) ([]string, error) {
	if len(input) > maximumAppAdministrationIndexes {
		return nil, errors.New("too many default indexes")
	}
	result := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, candidate := range input {
		name, err := control.NormalizeIndexName(candidate)
		if err != nil {
			return nil, errors.New("default index name is invalid")
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, name)
	}
	slices.Sort(result)
	return result, nil
}

func (handler *apiHandler) normalizeAppAdministrationTimeRange(
	input *opensplunk.TimeRangeSpec,
) (*AppAdministrationTimeRange, error) {
	if input == nil {
		return nil, nil
	}
	earliest := cloneOptionalString(input.Earliest)
	latest := cloneOptionalString(input.Latest)
	timezone := cloneOptionalString(input.Timezone)
	intent := searchtime.Intent{
		Earliest: "-24h",
		Latest:   "now",
		Timezone: "UTC",
	}
	result := &AppAdministrationTimeRange{}
	if earliest != nil {
		if strings.TrimSpace(*earliest) != *earliest {
			return nil, errors.New("default time range is not canonical")
		}
		intent.Earliest = strings.Clone(*earliest)
		result.Earliest = cloneOptionalString(earliest)
	}
	if latest != nil {
		if strings.TrimSpace(*latest) != *latest {
			return nil, errors.New("default time range is not canonical")
		}
		intent.Latest = strings.Clone(*latest)
		result.Latest = cloneOptionalString(latest)
	}
	if input.Timezone != nil {
		if strings.TrimSpace(*timezone) != *timezone {
			return nil, errors.New("default time range is not canonical")
		}
		intent.Timezone = strings.Clone(*timezone)
		intent.TimezoneSpecified = true
		result.Timezone = cloneOptionalString(timezone)
	}
	if err := searchtime.ValidateIntent(intent); err != nil {
		return nil, errors.New("default time range is invalid")
	}
	return result, nil
}

func cloneOptionalString(input *string) *string {
	if input == nil {
		return nil
	}
	return new(strings.Clone(*input))
}

var errAppAdministrationImmutableSlug = errors.New("app slug is immutable")

func (handler *apiHandler) applyAppAdministrationUpdate(
	current AppAdministrationDefinition,
	input *opensplunk.AppDefinition,
	mask *fieldmaskpb.FieldMask,
) (AppAdministrationDefinition, error) {
	if input == nil {
		return AppAdministrationDefinition{}, errors.New("definition is required")
	}
	if input.GetSlug() != "" {
		slug, err := normalizeAppAdministrationSlug(input.GetSlug())
		if err != nil {
			return AppAdministrationDefinition{}, err
		}
		if slug != current.Slug {
			return AppAdministrationDefinition{}, errAppAdministrationImmutableSlug
		}
	}
	paths, full, err := normalizeUpdateMask(
		mask,
		appAdministrationUpdatePaths,
	)
	if err != nil {
		return AppAdministrationDefinition{}, err
	}
	if full {
		cloned := proto.Clone(input).(*opensplunk.AppDefinition)
		cloned.Slug = strings.Clone(current.Slug)
		return handler.appAdministrationDefinition(cloned)
	}

	result := cloneAppAdministrationDefinition(current)
	for _, path := range paths {
		switch path {
		case "display_name":
			result.DisplayName, err = normalizeAppAdministrationDisplayName(
				input.GetDisplayName(),
			)
		case "description":
			result.Description, err = normalizeAppAdministrationDescription(
				input.Description,
			)
		case "default_index_names":
			result.DefaultIndexNames, err = normalizeAppAdministrationIndexes(
				input.GetDefaultIndexNames(),
			)
		case "default_time_range":
			result.DefaultTimeRange, err =
				handler.normalizeAppAdministrationTimeRange(
					input.GetDefaultTimeRange(),
				)
		}
		if err != nil {
			return AppAdministrationDefinition{}, err
		}
	}
	return result, nil
}

func cloneAppAdministrationDefinition(
	input AppAdministrationDefinition,
) AppAdministrationDefinition {
	result := AppAdministrationDefinition{
		Slug:              strings.Clone(input.Slug),
		DisplayName:       strings.Clone(input.DisplayName),
		Description:       cloneOptionalString(input.Description),
		DefaultIndexNames: slices.Clone(input.DefaultIndexNames),
	}
	if input.DefaultTimeRange != nil {
		result.DefaultTimeRange = &AppAdministrationTimeRange{
			Earliest: cloneOptionalString(input.DefaultTimeRange.Earliest),
			Latest:   cloneOptionalString(input.DefaultTimeRange.Latest),
			Timezone: cloneOptionalString(input.DefaultTimeRange.Timezone),
		}
	}
	return result
}

func cloneAppAdministrationListRequest(
	input AppAdministrationListRequest,
) AppAdministrationListRequest {
	result := input
	result.PageCursor = strings.Clone(input.PageCursor)
	result.StateFilters = slices.Clone(input.StateFilters)
	result.TextFilter = strings.Clone(input.TextFilter)
	if input.RequiredCatalogRevision != nil {
		revision := *input.RequiredCatalogRevision
		result.RequiredCatalogRevision = &revision
	}
	return result
}

func (handler *apiHandler) appAdministrationListRequest(
	input *opensplunk.ListAppsRequest,
) (AppAdministrationListRequest, error) {
	if page := input.GetPage(); page != nil && page.PageToken != nil {
		rawToken := page.GetPageToken()
		if rawToken == "" ||
			strings.TrimSpace(rawToken) != rawToken ||
			len(rawToken) > maximumPageTokenBytes {
			return AppAdministrationListRequest{},
				ErrAppAdministrationInvalidPageToken
		}
	}
	pageSize, pageToken, includeTotal, err := handler.pageRequest(
		input.GetPage(),
	)
	if err != nil {
		return AppAdministrationListRequest{}, err
	}
	if pageSize == 0 {
		pageSize = min(
			defaultAppAdministrationPageSize,
			maximumAppAdministrationRows,
			int(handler.maximumPageSize),
		)
	}
	pageSize = min(pageSize, maximumAppAdministrationRows)
	states, err := normalizeAppAdministrationStateFilters(
		input.GetStateFilters(),
	)
	if err != nil {
		return AppAdministrationListRequest{}, err
	}
	textFilter, err := normalizeAppAdministrationTextFilter(
		input.TextFilter,
	)
	if err != nil {
		return AppAdministrationListRequest{}, err
	}
	sortBy, descending, err := normalizeAppAdministrationSort(
		input.GetSortBy(),
		input.GetSortDirection(),
	)
	if err != nil {
		return AppAdministrationListRequest{}, err
	}
	fingerprint := appAdministrationListFingerprint(
		handler.tenantID,
		pageSize,
		includeTotal,
		states,
		textFilter,
		sortBy,
		descending,
	)
	var pageCursor string
	var requiredRevision *uint64
	if pageToken != "" {
		if input.GetPage() == nil ||
			input.GetPage().PageToken == nil ||
			input.GetPage().GetPageToken() != pageToken {
			return AppAdministrationListRequest{},
				ErrAppAdministrationInvalidPageToken
		}
		cursor, err := decodeAppAdministrationCursor(
			handler.appCursorKey,
			pageToken,
		)
		if err != nil ||
			cursor.Fingerprint != fingerprint ||
			cursor.PageCursor == "" {
			return AppAdministrationListRequest{},
				ErrAppAdministrationInvalidPageToken
		}
		pageCursor = strings.Clone(cursor.PageCursor)
		revision := cursor.CatalogRevision
		requiredRevision = &revision
	}

	pageSizeUint32 := safecast.MustConv[uint32](pageSize)
	return AppAdministrationListRequest{
		PageSize:                pageSizeUint32,
		PageCursor:              pageCursor,
		RequiredCatalogRevision: requiredRevision,
		IncludeTotal:            includeTotal,
		StateFilters:            slices.Clone(states),
		TextFilter:              strings.Clone(textFilter),
		SortBy:                  sortBy,
		SortDescending:          descending,
	}, nil
}

func appAdministrationListFingerprint(
	tenantID string,
	pageSize int,
	includeTotal bool,
	states []AppAdministrationState,
	textFilter string,
	sortBy AppAdministrationSort,
	descending bool,
) string {
	direction := int32(
		opensplunk.SortDirection_SORT_DIRECTION_ASCENDING,
	)
	if descending {
		direction = int32(
			opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
		)
	}
	base := adminFilterFingerprint(
		"apps",
		pageSize,
		includeTotal,
		states,
		textFilter,
		int32(sortBy),
		direction,
		"",
	)
	digest := sha256.Sum256(
		[]byte(strings.Clone(tenantID) + "\x00" + base),
	)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

type appAdministrationCursor struct {
	Version         int    `json:"v"`
	Fingerprint     string `json:"f"`
	CatalogRevision uint64 `json:"r"`
	PageCursor      string `json:"c"`
}

func encodeAppAdministrationCursor(
	key []byte,
	cursor appAdministrationCursor,
) (string, error) {
	if len(key) < 32 || len(key) > 1<<10 ||
		cursor.Version != appAdministrationCursorVersion ||
		cursor.Fingerprint == "" ||
		cursor.CatalogRevision == 0 ||
		cursor.PageCursor == "" ||
		len(cursor.PageCursor) > maximumAppAdministrationPageCursor ||
		!utf8.ValidString(cursor.PageCursor) {
		return "", errors.New("app cursor is invalid")
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	signature := signAppAdministrationCursor(key, payload)
	encoded := base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(signature)
	if len(encoded) > maximumPageTokenBytes {
		return "", errors.New("app cursor exceeds its transport bound")
	}
	return encoded, nil
}

func decodeAppAdministrationCursor(
	key []byte,
	token string,
) (appAdministrationCursor, error) {
	invalid := func() (appAdministrationCursor, error) {
		return appAdministrationCursor{},
			ErrAppAdministrationInvalidPageToken
	}
	if len(key) < 32 || len(key) > 1<<10 ||
		token == "" ||
		len(token) > maximumPageTokenBytes ||
		strings.TrimSpace(token) != token ||
		strings.Count(token, ".") != 1 {
		return invalid()
	}
	parts := strings.SplitN(token, ".", 2)
	payload, err := decodeAdminBase64(parts[0])
	if err != nil {
		return invalid()
	}
	signature, err := decodeAdminBase64(parts[1])
	if err != nil ||
		len(signature) != sha256.Size ||
		!hmac.Equal(
			signature,
			signAppAdministrationCursor(key, payload),
		) {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var cursor appAdministrationCursor
	if err := decoder.Decode(&cursor); err != nil {
		return invalid()
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return invalid()
	}
	canonical, err := json.Marshal(cursor)
	if err != nil ||
		!bytes.Equal(canonical, payload) ||
		cursor.Version != appAdministrationCursorVersion ||
		cursor.Fingerprint == "" ||
		cursor.CatalogRevision == 0 ||
		cursor.PageCursor == "" ||
		len(cursor.PageCursor) > maximumAppAdministrationPageCursor ||
		!utf8.ValidString(cursor.PageCursor) {
		return invalid()
	}
	return cursor, nil
}

func signAppAdministrationCursor(key, payload []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte("open-splunk/app-list-cursor/v1\x00"))
	_, _ = mac.Write(payload)
	return mac.Sum(nil)
}

func normalizeAppAdministrationStateFilters(
	input []opensplunk.AppState,
) ([]AppAdministrationState, error) {
	if len(input) > 2 {
		return nil, errors.New("too many app state filters")
	}
	result := make([]AppAdministrationState, 0, len(input))
	for _, state := range input {
		converted, err := appAdministrationStateFromProto(state)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	slices.Sort(result)
	return slices.Compact(result), nil
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

func normalizeAppAdministrationSort(
	sortBy opensplunk.AppSortBy,
	direction opensplunk.SortDirection,
) (AppAdministrationSort, bool, error) {
	var result AppAdministrationSort
	switch sortBy {
	case opensplunk.AppSortBy_APP_SORT_BY_UNSPECIFIED,
		opensplunk.AppSortBy_APP_SORT_BY_DISPLAY_NAME:
		result = AppAdministrationSortDisplayName
	case opensplunk.AppSortBy_APP_SORT_BY_CREATED_AT:
		result = AppAdministrationSortCreatedAt
	case opensplunk.AppSortBy_APP_SORT_BY_UPDATED_AT:
		result = AppAdministrationSortUpdatedAt
	default:
		return 0, false, errors.New("app sort is invalid")
	}
	normalizedDirection, err := normalizeSortDirection(direction)
	if err != nil {
		return 0, false, err
	}
	return result,
		normalizedDirection ==
			opensplunk.SortDirection_SORT_DIRECTION_DESCENDING,
		nil
}

func appAdministrationStateFromProto(
	input opensplunk.AppState,
) (AppAdministrationState, error) {
	switch input {
	case opensplunk.AppState_APP_STATE_ACTIVE:
		return AppAdministrationStateActive, nil
	case opensplunk.AppState_APP_STATE_ARCHIVED:
		return AppAdministrationStateArchived, nil
	default:
		return "", errors.New("app state is invalid")
	}
}

func appAdministrationStateToProto(
	input AppAdministrationState,
) (opensplunk.AppState, error) {
	switch input {
	case AppAdministrationStateActive:
		return opensplunk.AppState_APP_STATE_ACTIVE, nil
	case AppAdministrationStateArchived:
		return opensplunk.AppState_APP_STATE_ARCHIVED, nil
	default:
		return opensplunk.AppState_APP_STATE_UNSPECIFIED,
			errors.New("app state is invalid")
	}
}

func appAdministrationSelectorMatches(
	selector AppAdministrationSelector,
	record AppAdministrationWorkspace,
) bool {
	return (selector.AppID != "" && selector.Slug == "" &&
		record.AppID == selector.AppID) ||
		(selector.Slug != "" && selector.AppID == "" &&
			record.Definition.Slug == selector.Slug)
}

func (handler *apiHandler) appAdministrationWorkspaceToProto(
	record AppAdministrationWorkspace,
) (*opensplunk.AppWorkspace, error) {
	if validateBoundedIdentifier(
		record.AppID,
		maximumAppAdministrationIDBytes,
		false,
	) != nil ||
		record.Version == 0 ||
		record.Version > math.MaxInt64 {
		return nil, errors.New("app record is invalid")
	}
	definition, err := handler.appAdministrationDefinitionToProto(
		record.Definition,
	)
	if err != nil {
		return nil, err
	}
	state, err := appAdministrationStateToProto(record.State)
	if err != nil {
		return nil, err
	}
	createdAt, err := validTimestamp(record.CreatedAt)
	if err != nil {
		return nil, err
	}
	updatedAt, err := validTimestamp(record.UpdatedAt)
	if err != nil ||
		updatedAt.AsTime().Before(createdAt.AsTime()) {
		return nil, errors.New("app timestamps are invalid")
	}
	return &opensplunk.AppWorkspace{
		AppId:      strings.Clone(record.AppID),
		Version:    record.Version,
		Definition: definition,
		State:      state,
		CreatedAt:  createdAt,
		UpdatedAt:  updatedAt,
	}, nil
}

func (handler *apiHandler) appAdministrationDefinitionToProto(
	input AppAdministrationDefinition,
) (*opensplunk.AppDefinition, error) {
	slug, err := normalizeAppAdministrationSlug(input.Slug)
	if err != nil || slug != input.Slug {
		return nil, errors.New("app slug is invalid")
	}
	displayName, err := normalizeAppAdministrationDisplayName(
		input.DisplayName,
	)
	if err != nil || displayName != input.DisplayName {
		return nil, errors.New("app display name is invalid")
	}
	description, err := normalizeAppAdministrationDescription(
		input.Description,
	)
	if err != nil || !equalOptionalString(description, input.Description) {
		return nil, errors.New("app description is invalid")
	}
	indexNames, err := normalizeAppAdministrationIndexes(
		input.DefaultIndexNames,
	)
	if err != nil || !slices.Equal(indexNames, input.DefaultIndexNames) {
		return nil, errors.New("app default indexes are invalid")
	}
	rangeProto := appAdministrationTimeRangeToProto(input.DefaultTimeRange)
	normalizedRange, err := handler.normalizeAppAdministrationTimeRange(
		rangeProto,
	)
	if err != nil ||
		!equalAppAdministrationTimeRange(
			normalizedRange,
			input.DefaultTimeRange,
		) {
		return nil, errors.New("app default time range is invalid")
	}
	return &opensplunk.AppDefinition{
		Slug:              strings.Clone(input.Slug),
		DisplayName:       strings.Clone(input.DisplayName),
		Description:       cloneOptionalString(input.Description),
		DefaultIndexNames: slices.Clone(input.DefaultIndexNames),
		DefaultTimeRange:  rangeProto,
	}, nil
}

func appAdministrationTimeRangeToProto(
	input *AppAdministrationTimeRange,
) *opensplunk.TimeRangeSpec {
	if input == nil {
		return nil
	}
	return &opensplunk.TimeRangeSpec{
		Earliest: cloneOptionalString(input.Earliest),
		Latest:   cloneOptionalString(input.Latest),
		Timezone: cloneOptionalString(input.Timezone),
	}
}

func equalOptionalString(left, right *string) bool {
	return (left == nil && right == nil) ||
		(left != nil && right != nil && *left == *right)
}

func equalAppAdministrationTimeRange(
	left, right *AppAdministrationTimeRange,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return equalOptionalString(left.Earliest, right.Earliest) &&
		equalOptionalString(left.Latest, right.Latest) &&
		equalOptionalString(left.Timezone, right.Timezone)
}

func equalAppAdministrationDefinition(
	left, right AppAdministrationDefinition,
) bool {
	return left.Slug == right.Slug &&
		left.DisplayName == right.DisplayName &&
		equalOptionalString(left.Description, right.Description) &&
		slices.Equal(left.DefaultIndexNames, right.DefaultIndexNames) &&
		equalAppAdministrationTimeRange(
			left.DefaultTimeRange,
			right.DefaultTimeRange,
		)
}

func (handler *apiHandler) appAdministrationListToProto(
	request AppAdministrationListRequest,
	result AppAdministrationListResult,
) (*opensplunk.ListAppsResponse, error) {
	if len(result.Apps) > int(request.PageSize) ||
		len(result.Apps) > maximumAppAdministrationRows {
		return nil, errors.New("app page exceeds its bound")
	}
	if request.RequiredCatalogRevision != nil &&
		result.CatalogRevision != *request.RequiredCatalogRevision {
		return nil, ErrAppAdministrationInvalidPageToken
	}
	if result.NextPageCursor != "" &&
		(strings.TrimSpace(result.NextPageCursor) != result.NextPageCursor ||
			len(result.NextPageCursor) > maximumAppAdministrationPageCursor ||
			!utf8.ValidString(result.NextPageCursor) ||
			result.CatalogRevision == 0 ||
			len(result.Apps) == 0) {
		return nil, errors.New("app continuation cursor is invalid")
	}
	if !request.IncludeTotal &&
		(result.TotalSize != nil || result.TotalSizeExact) {
		return nil, errors.New("unrequested app total was returned")
	}
	if result.TotalSize == nil && result.TotalSizeExact {
		return nil, errors.New("app total exactness is invalid")
	}
	if result.TotalSize != nil &&
		*result.TotalSize < uint64(len(result.Apps)) {
		return nil, errors.New("app total is invalid")
	}
	apps := make(
		[]*opensplunk.AppWorkspace,
		0,
		len(result.Apps),
	)
	seenIDs := make(map[string]struct{}, len(result.Apps))
	seenSlugs := make(map[string]struct{}, len(result.Apps))
	for _, record := range result.Apps {
		if _, duplicate := seenIDs[record.AppID]; duplicate {
			return nil, errors.New("app page contains a duplicate")
		}
		if _, duplicate := seenSlugs[record.Definition.Slug]; duplicate {
			return nil, errors.New("app page contains a duplicate")
		}
		converted, err := handler.appAdministrationWorkspaceToProto(record)
		if err != nil {
			return nil, err
		}
		seenIDs[record.AppID] = struct{}{}
		seenSlugs[record.Definition.Slug] = struct{}{}
		apps = append(apps, converted)
	}
	page := &opensplunk.PageResponse{
		TotalSizeExact: result.TotalSizeExact,
	}
	if result.NextPageCursor != "" {
		token, err := encodeAppAdministrationCursor(
			handler.appCursorKey,
			appAdministrationCursor{
				Version: appAdministrationCursorVersion,
				Fingerprint: appAdministrationListFingerprint(
					handler.tenantID,
					int(request.PageSize),
					request.IncludeTotal,
					request.StateFilters,
					request.TextFilter,
					request.SortBy,
					request.SortDescending,
				),
				CatalogRevision: result.CatalogRevision,
				PageCursor: strings.Clone(
					result.NextPageCursor,
				),
			},
		)
		if err != nil {
			return nil, err
		}
		page.NextPageToken = new(token)
	}
	if result.TotalSize != nil {
		total := *result.TotalSize
		page.TotalSize = &total
	}
	return &opensplunk.ListAppsResponse{
		Apps: apps,
		Page: page,
	}, nil
}

func appAdministrationContextError(ctx context.Context) error {
	return canceledRequestError(ctx, "app administration request was canceled")
}

func mapAppAdministrationCallError(
	ctx context.Context,
	operationErr error,
) error {
	if operationErr == nil {
		// Store success is authoritative for mutations. A cancellation racing
		// after commit must not turn a successful operation into a reported
		// failure which encourages an unsafe retry.
		return nil
	}
	if requestContextFailure(ctx, operationErr) != nil {
		return router.NewHTTPError(
			http.StatusRequestTimeout,
			"app administration request was canceled",
		)
	}
	switch {
	case errors.Is(
		operationErr,
		ErrAppAdministrationInvalidArgument,
	):
		return badRequestError("app request is invalid")
	case errors.Is(operationErr, ErrAppAdministrationNotFound):
		return router.NewHTTPError(http.StatusNotFound, "app not found")
	case errors.Is(operationErr, ErrAppAdministrationAlreadyExists):
		return router.NewHTTPError(
			http.StatusConflict,
			"app already exists",
		)
	case errors.Is(operationErr, ErrAppAdministrationConflict):
		return appAdministrationConflictError()
	case errors.Is(
		operationErr,
		ErrAppAdministrationInvalidPageToken,
	):
		return badRequestError("page token is invalid")
	case errors.Is(operationErr, ErrAppAdministrationCapacity):
		return router.NewHTTPError(
			http.StatusTooManyRequests,
			"app capacity is exhausted",
		)
	default:
		return unavailableError("app service is unavailable")
	}
}

func appAdministrationConflictError() error {
	return router.NewHTTPError(
		http.StatusConflict,
		"app version or state conflict",
	)
}

type serializedCreateAppResponse = boundedProtoResponse[*opensplunk.CreateAppResponse]

type serializedCreateAppCodec = boundedProtoCodec[
	*opensplunk.CreateAppRequest,
	*opensplunk.CreateAppResponse,
]

func newSerializedCreateAppCodec() *serializedCreateAppCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.CreateAppRequest,
			*opensplunk.CreateAppResponse,
		](),
	)
}

type serializedGetAppResponse = boundedProtoResponse[*opensplunk.GetAppResponse]

type serializedGetAppCodec = boundedProtoCodec[
	*opensplunk.GetAppRequest,
	*opensplunk.GetAppResponse,
]

func newSerializedGetAppCodec() *serializedGetAppCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.GetAppRequest,
			*opensplunk.GetAppResponse,
		](),
	)
}

type serializedListAppsResponse = boundedProtoResponse[*opensplunk.ListAppsResponse]

type serializedListAppsCodec = boundedProtoCodec[
	*opensplunk.ListAppsRequest,
	*opensplunk.ListAppsResponse,
]

func newSerializedListAppsCodec() *serializedListAppsCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.ListAppsRequest,
			*opensplunk.ListAppsResponse,
		](),
	)
}

type serializedUpdateAppResponse = boundedProtoResponse[*opensplunk.UpdateAppResponse]

type serializedUpdateAppCodec = boundedProtoCodec[
	*opensplunk.UpdateAppRequest,
	*opensplunk.UpdateAppResponse,
]

func newSerializedUpdateAppCodec() *serializedUpdateAppCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.UpdateAppRequest,
			*opensplunk.UpdateAppResponse,
		](),
	)
}

type serializedSetAppStateResponse = boundedProtoResponse[*opensplunk.SetAppStateResponse]

type serializedSetAppStateCodec = boundedProtoCodec[
	*opensplunk.SetAppStateRequest,
	*opensplunk.SetAppStateResponse,
]

func newSerializedSetAppStateCodec() *serializedSetAppStateCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.SetAppStateRequest,
			*opensplunk.SetAppStateResponse,
		](),
	)
}

type serializedDeleteAppResponse = boundedProtoResponse[*opensplunk.DeleteAppResponse]

type serializedDeleteAppCodec = boundedProtoCodec[
	*opensplunk.DeleteAppRequest,
	*opensplunk.DeleteAppResponse,
]

func newSerializedDeleteAppCodec() *serializedDeleteAppCodec {
	return newAppAdministrationCodec(
		codec.NewProtoCodec[
			*opensplunk.DeleteAppRequest,
			*opensplunk.DeleteAppResponse,
		](),
	)
}

func newAppAdministrationCodec[
	Request any,
	Response proto.Message,
](
	inner codec.Codec[Request, Response],
) *boundedProtoCodec[Request, Response] {
	return newBoundedProtoCodec(
		inner,
		boundedProtoCodecOptions{
			stateError:   "app response serialization state is invalid",
			messageError: "app response message is invalid",
			maximumBytes: maximumAppAdministrationResponse,
			sizeError:    "app response exceeds its byte limit",
		},
	)
}
