package common_api

import (
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/logkeys"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"github.com/Suhaibinator/go-common/pkg/common_service/common_service_models"
	"github.com/Suhaibinator/go-common/pkg/config"
	appLogger "github.com/Suhaibinator/go-common/pkg/logger"
	"github.com/Suhaibinator/go-common/pkg/proto_models"
	"go.uber.org/zap"
)

// HandleFinalizeAttachments HandleFinalizeAttachments handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) HandleFinalizeAttachments(r *http.Request, req *proto_models.FinalizeAttachmentsRequest) (*proto_models.FinalizeAttachmentsResponse, error) {
	// Input is already sanitized by SRouter

	// Convert attachment IDs
	attachmentIDs := req.AttachmentIds

	// Call service layer - that's it!
	results, err := h.serviceHandler.FinalizeAttachments(
		r.Context(),
		attachmentIDs,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "finalize_attachments"),
			zap.Int(appLogger.KeyAttachmentCount, len(attachmentIDs)))
	}

	return &proto_models.FinalizeAttachmentsResponse{
		Results: results,
	}, nil
}

// RegisterAttachmentRoutes RegisterAttachmentRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterAttachmentRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableAttachments)
	// Get transaction middleware for write operations. This unary route uses the
	// default commit-before-flush middleware; its post-commit hooks run after commit.
	transactionMiddleware := h.GetTransactionMiddleware()

	// Define route for finalizing uploaded attachments
	finalizeAttachmentsRoute := router.RouteConfig[*proto_models.FinalizeAttachmentsRequest, *proto_models.FinalizeAttachmentsResponse]{
		Path:        "/attachments/finalize",
		Methods:     []router.HttpMethod{router.MethodPost},
		AuthLevel:   new(router.AuthRequired), // Requires authentication
		Codec:       codec.NewProtoCodec[*proto_models.FinalizeAttachmentsRequest, *proto_models.FinalizeAttachmentsResponse](),
		Handler:     h.HandleFinalizeAttachments,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeFinalizeAttachmentsRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(finalizeAttachmentsRoute)
}
