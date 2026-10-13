package server

import (
	"context"
	"net/http"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
)

func (handler *apiHandler) registerIndexPermissionRoutes(group *apiRouteGroup, maximumBytes int64) {
	if handler.authorization == nil {
		return
	}
	group.Route(
		sizedProtoPostRoute("/indexes/permissions/get", maximumBytes, handler.getIndexPermissions, sanitizeGetIndexPermissions),
		sizedProtoPostRoute("/indexes/permissions/set", maximumBytes, handler.setIndexPermission, sanitizeSetIndexPermission),
		sizedProtoPostRoute("/indexes/permissions/delete", maximumBytes, handler.deleteIndexPermission, sanitizeDeleteIndexPermission),
		sizedProtoPostRoute("/indexes/organization/set", maximumBytes, handler.setIndexOrganization, sanitizeSetIndexOrganization),
	)
}
func permissionIndexID(id string) error {
	if validateBoundedIdentifier(id, maximumAdminObjectIDBytes, false) != nil {
		return badRequestError("index ID is invalid")
	}
	return nil
}
func sanitizeGetIndexPermissions(_ context.Context, r *opensplunk.GetIndexPermissionsRequest) (*opensplunk.GetIndexPermissionsRequest, error) {
	return r, permissionIndexID(r.GetIndexId())
}
func sanitizeSetIndexOrganization(_ context.Context, r *opensplunk.SetIndexOrganizationRequest) (*opensplunk.SetIndexOrganizationRequest, error) {
	if err := permissionIndexID(r.GetIndexId()); err != nil {
		return r, err
	}
	if r.GetOrganizationId() <= 0 || r.GetExpectedRevision() > 1<<63-1 {
		return r, badRequestError("organization ID is invalid")
	}
	return r, nil
}
func sanitizePermission(p *opensplunk.IndexPermission, remove bool) error {
	if p == nil || p.GetSubjectId() == 0 || p.GetSubjectId() > 1<<63-1 || !authorization.Capability(p.GetCapability()).Valid() {
		return badRequestError("index permission is invalid")
	}
	if p.GetSubjectKind() != "user" && p.GetSubjectKind() != "role" {
		return badRequestError("permission subject is invalid")
	}
	if p.GetSubjectKind() == "role" && p.GetSubjectId() != 1 && p.GetSubjectId() != 23 && p.GetSubjectId() != 44 {
		return badRequestError("permission role is invalid")
	}
	if !remove && p.GetEffect() != "allow" && p.GetEffect() != "deny" {
		return badRequestError("permission effect is invalid")
	}
	return nil
}
func sanitizeSetIndexPermission(_ context.Context, r *opensplunk.SetIndexPermissionRequest) (*opensplunk.SetIndexPermissionRequest, error) {
	if err := permissionIndexID(r.GetIndexId()); err != nil {
		return r, err
	}
	if r.GetExpectedRevision() == 0 || r.GetExpectedRevision() > 1<<63-1 {
		return r, badRequestError("policy revision is required")
	}
	return r, sanitizePermission(r.GetPermission(), false)
}
func sanitizeDeleteIndexPermission(_ context.Context, r *opensplunk.DeleteIndexPermissionRequest) (*opensplunk.DeleteIndexPermissionRequest, error) {
	if err := permissionIndexID(r.GetIndexId()); err != nil {
		return r, err
	}
	if r.GetExpectedRevision() == 0 || r.GetExpectedRevision() > 1<<63-1 {
		return r, badRequestError("policy revision is required")
	}
	return r, sanitizePermission(r.GetPermission(), true)
}
func (handler *apiHandler) getIndexPermissions(request *http.Request, input *opensplunk.GetIndexPermissionsRequest) (*opensplunk.GetIndexPermissionsResponse, error) {
	scope, ok := authorization.FromContext(request.Context())
	if !ok {
		return nil, mapAuthorizationError(authorization.ErrUnauthorized)
	}
	policy, decisions, err := handler.authorization.GetPolicy(request.Context(), scope.Subject, scope.OrganizationID, input.GetIndexId())
	if err != nil {
		return nil, mapAuthorizationError(err)
	}
	response := &opensplunk.GetIndexPermissionsResponse{OrganizationId: policy.OrganizationID, Revision: policy.Revision, Rules: make([]*opensplunk.IndexPermission, 0, len(policy.Rules)), Effective: make([]*opensplunk.IndexCapabilityDecision, 0, len(decisions))}
	for _, r := range policy.Rules {
		response.Rules = append(response.Rules, &opensplunk.IndexPermission{SubjectKind: r.SubjectKind, SubjectId: r.SubjectID, Capability: string(r.Capability), Effect: r.Effect})
	}
	for _, d := range decisions {
		response.Effective = append(response.Effective, &opensplunk.IndexCapabilityDecision{Capability: string(d.Capability), Allowed: d.Allowed, Reason: d.Reason})
	}
	return response, nil
}
func permissionRule(indexID string, p *opensplunk.IndexPermission, remove bool) authorization.Rule {
	effect := p.GetEffect()
	if remove {
		effect = "allow"
	}
	return authorization.Rule{IndexID: indexID, SubjectKind: p.GetSubjectKind(), SubjectID: p.GetSubjectId(), Capability: authorization.Capability(p.GetCapability()), Effect: effect}
}
func (handler *apiHandler) setIndexPermission(request *http.Request, input *opensplunk.SetIndexPermissionRequest) (*opensplunk.SetIndexPermissionResponse, error) {
	scope, ok := authorization.FromContext(request.Context())
	if !ok {
		return nil, mapAuthorizationError(authorization.ErrUnauthorized)
	}
	revision, err := handler.authorization.ChangeRuleExpected(request.Context(), scope.Subject, scope.OrganizationID, permissionRule(input.GetIndexId(), input.GetPermission(), false), input.GetExpectedRevision(), false)
	if err != nil {
		return nil, mapAuthorizationError(err)
	}
	return &opensplunk.SetIndexPermissionResponse{Success: true, Revision: revision}, nil
}
func (handler *apiHandler) deleteIndexPermission(request *http.Request, input *opensplunk.DeleteIndexPermissionRequest) (*opensplunk.DeleteIndexPermissionResponse, error) {
	scope, ok := authorization.FromContext(request.Context())
	if !ok {
		return nil, mapAuthorizationError(authorization.ErrUnauthorized)
	}
	revision, err := handler.authorization.ChangeRuleExpected(request.Context(), scope.Subject, scope.OrganizationID, permissionRule(input.GetIndexId(), input.GetPermission(), true), input.GetExpectedRevision(), true)
	if err != nil {
		return nil, mapAuthorizationError(err)
	}
	return &opensplunk.DeleteIndexPermissionResponse{Success: true, Revision: revision}, nil
}
func (handler *apiHandler) setIndexOrganization(request *http.Request, input *opensplunk.SetIndexOrganizationRequest) (*opensplunk.SetIndexOrganizationResponse, error) {
	scope, ok := authorization.FromContext(request.Context())
	if !ok {
		return nil, mapAuthorizationError(authorization.ErrUnauthorized)
	}
	if _, err := handler.indexAdmin.GetIndex(request.Context(), input.GetIndexId()); err != nil {
		return nil, mapAdministrativeCallError(request.Context(), err, "index")
	}
	revision, err := handler.authorization.AssignIndexExpected(request.Context(), scope.Subject, input.GetIndexId(), input.GetOrganizationId(), input.GetExpectedRevision())
	if err != nil {
		return nil, mapAuthorizationError(err)
	}
	return &opensplunk.SetIndexOrganizationResponse{Success: true, Revision: revision}, nil
}
