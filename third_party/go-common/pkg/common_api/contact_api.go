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

// RegisterContactRoutes RegisterContactRoutes registers API routes and wires this capability into the backend request flow during handler setup.
func (h *CommonAPIHandler[U]) RegisterContactRoutes(group *router.RouteGroup[common_service_models.UserIdType, U]) {
	group = h.featureGroup(group, config.APIView.EnableContacts)
	// Get transaction middleware for write operations
	transactionMiddleware := h.GetTransactionMiddleware()

	contactSubmitRoute := router.RouteConfig[*proto_models.ContactRequest, *proto_models.ContactResponse]{
		Path:        "/contact/submit",
		Methods:     []router.HttpMethod{http.MethodPost, http.MethodOptions},
		AuthLevel:   new(router.NoAuth), // Allow anonymous submissions
		Codec:       codec.NewProtoCodec[*proto_models.ContactRequest, *proto_models.ContactResponse](),
		Handler:     h.SubmitContact,
		SourceType:  router.Body,
		Sanitizer:   h.sanitizer.SanitizeContactRequest,
		Middlewares: []common.Middleware{transactionMiddleware},
	}
	group.Route(contactSubmitRoute)

	h.logger.Named(commonAPILoggerName).Debug("Contact routes registered to route group")
}

// SubmitContact SubmitContact handles a backend operation in the API/request flow and delegates business logic to the service layer.
func (h *CommonAPIHandler[U]) SubmitContact(
	r *http.Request,
	req *proto_models.ContactRequest,
) (*proto_models.ContactResponse, error) {
	// Input is already sanitized by SRouter

	// Call service layer - that's it!
	referenceID, err := h.serviceHandler.SubmitContactMessage(
		r.Context(),
		req.Name,
		req.Email,
		req.Subject,
		req.Message,
	)
	if err != nil {
		return nil, MapServiceErrorWithFields(err,
			zap.String(logkeys.Operation, "submit_contact_message"),
			zap.String(appLogger.KeyEmail, req.Email),
			zap.String(appLogger.KeySubject, req.Subject))
	}

	return &proto_models.ContactResponse{
		ReferenceId: referenceID,
	}, nil
}
