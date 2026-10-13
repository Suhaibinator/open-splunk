package identity

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"path"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_api/middleware"
	sm "github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/common_service/permission"
	pb "github.com/Suhaibinator/go-common/pkg/proto_models"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const organizationMutationBodyLimit = 1 << 20

type organizationRoleRequest interface {
	proto.Message
	GetRole() pb.OrganizationRole
}

// Only role-bearing native mutations are decoded here. Native codecs, request
// sanitizers, authentication, permissions and transaction handlers remain the
// implementation of those operations. Other requests keep their original stream.
func organizationExposureGuard(next, adminRoleUpdate http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		route := path.Clean(request.URL.Path)
		if route == Prefix+"/organizations" || route == Prefix+"/admin/organizations/create" {
			http.NotFound(w, request)
			return
		}
		decode := organizationRoleDecoder(route, request.Method)
		if decode == nil {
			next.ServeHTTP(w, request)
			return
		}
		if request.ContentLength > organizationMutationBodyLimit {
			http.Error(w, "Organization mutation request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		original := request.Body
		if original == nil {
			http.Error(w, "Organization role is required", http.StatusBadRequest)
			return
		}
		defer func() { _ = original.Close() }()
		body, err := io.ReadAll(io.LimitReader(original, organizationMutationBodyLimit+1))
		if len(body) > organizationMutationBodyLimit {
			http.Error(w, "Organization mutation request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err != nil {
			http.Error(w, "Organization mutation request is invalid", http.StatusBadRequest)
			return
		}
		input, err := decode(body)
		if err != nil || !authorization.ValidRole(int32(input.GetRole())) {
			http.Error(w, "Organization role must be Viewer, Admin or Owner", http.StatusBadRequest)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		if route == Prefix+"/admin/organizations/members/update-role" && adminRoleUpdate != nil {
			adminRoleUpdate.ServeHTTP(w, request)
			return
		}
		next.ServeHTTP(w, request)
	})
}

func organizationRoleDecoder(route, method string) func([]byte) (organizationRoleRequest, error) {
	if method == http.MethodPost {
		switch route {
		case Prefix + "/organizations/members/add":
			return nativeOrganizationRoleDecoder[*pb.AddUserToOrganizationRequest]()
		case Prefix + "/organizations/members/add-by-identifier":
			return nativeOrganizationRoleDecoder[*pb.AddUserToOrganizationByIdentifierRequest]()
		case Prefix + "/organizations/members/invite":
			return nativeOrganizationRoleDecoder[*pb.InviteUserToOrganizationRequest]()
		case Prefix + "/admin/organizations/members/add":
			return nativeOrganizationRoleDecoder[*pb.AdminAddUserToOrganizationRequest]()
		case Prefix + "/admin/organizations/members/add-by-identifier":
			return nativeOrganizationRoleDecoder[*pb.AdminAddUserToOrganizationByIdentifierRequest]()
		}
	}
	if method == http.MethodPut {
		switch route {
		case Prefix + "/organizations/members/update-role":
			return nativeOrganizationRoleDecoder[*pb.UpdateUserRoleInOrganizationRequest]()
		case Prefix + "/admin/organizations/members/update-role":
			return nativeOrganizationRoleDecoder[*pb.AdminUpdateUserRoleInOrganizationRequest]()
		}
	}
	return nil
}

func nativeOrganizationRoleDecoder[T interface {
	organizationRoleRequest
	*M
}, M any]() func([]byte) (organizationRoleRequest, error) {
	return func(body []byte) (organizationRoleRequest, error) {
		return codec.NewProtoCodec[T, T]().DecodeBytes(body)
	}
}

// The native privileged admin role-update method deliberately skips the last
// Owner guard. Keep its public codec and site permission boundary, but compose
// the ordinary native generic operation so ownership integrity is preserved.
func (r *Runtime) registerGuardedAdminRoleRoute(logger *zap.Logger) error {
	admin := router.NewRouter(router.RouterConfig{Logger: logger, AddUserObjectToCtx: true, GlobalMaxBodySize: organizationMutationBodyLimit}, router.RouterDependencies[sm.UserIdType, sm.User]{
		Authenticate: func(ctx context.Context, raw string) (*sm.User, bool) {
			user, err := r.authenticateSession(ctx, raw)
			return user, err == nil && user != nil
		},
		UserID: func(user *sm.User) sm.UserIdType { return user.ID },
	})
	group := admin.Group(Prefix + "/admin").AuthToken(&common.AuthTokenConfig{Source: common.AuthTokenSourceCookie, CookieName: r.cookieName})
	group.Use(r.api.GetSessionRefreshMiddleware())
	group.Route(router.RouteConfig[*pb.AdminUpdateUserRoleInOrganizationRequest, *pb.AdminUpdateUserRoleInOrganizationResponse]{
		Path: "/organizations/members/update-role", Methods: []router.HttpMethod{http.MethodPut, http.MethodOptions}, AuthLevel: new(router.AuthRequired), SourceType: router.Body,
		Codec:       codec.NewProtoCodec[*pb.AdminUpdateUserRoleInOrganizationRequest, *pb.AdminUpdateUserRoleInOrganizationResponse](),
		Sanitizer:   r.sanitizer.SanitizeAdminUpdateUserRoleInOrganizationRequest,
		Middlewares: []common.Middleware{middleware.AdminAuthMiddlewareWithPermission[sm.User](r.service.GetPermissionHandler(), permission.PermissionAdminManageOrganizationMembers, logger), r.api.GetTransactionMiddleware()},
		Handler: func(request *http.Request, input *pb.AdminUpdateUserRoleInOrganizationRequest) (*pb.AdminUpdateUserRoleInOrganizationResponse, error) {
			response, err := r.api.UpdateUserRoleInOrganizationGeneric(request, &pb.UpdateUserRoleInOrganizationRequest{OrganizationId: input.OrganizationId, UserId: input.UserId, Role: input.Role})
			if err != nil {
				return nil, err
			}
			return &pb.AdminUpdateUserRoleInOrganizationResponse{OrganizationUser: response.OrganizationUser}, nil
		},
	})
	if err := admin.Build(); err != nil {
		return err
	}
	r.adminRoleUpdate = admin
	return nil
}
