package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/scontext"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

func (h *CommonAPIHandler[U]) AdminGetOrganizationDeletionImpactGeneric(r *http.Request, req *proto_models.AdminGetOrganizationDeletionImpactRequest) (*proto_models.AdminGetOrganizationDeletionImpactResponse, error) {
	if _, found := scontext.GetUserID[common_service_models.UserIdType](r.Context()); !found {
		return nil, ErrUnauthorized
	}
	organizations, hasMore, err := h.serviceHandler.GetOrganizationHandler().GetOrganizationDeletionImpactPage(r.Context(), req.OrganizationId, req.AfterOrganizationId, int(req.Limit))
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "get_organization_deletion_impact"), zap.Int64(appLogger.KeyOrganizationID, req.OrganizationId))
	}
	response := &proto_models.AdminGetOrganizationDeletionImpactResponse{HasMore: hasMore}
	for _, organization := range organizations {
		response.Organizations = append(response.Organizations, h.serviceHandler.GetConverter().ServiceOrganizationToProto(organization))
	}
	return response, nil
}
