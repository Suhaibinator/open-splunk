package server

import (
	"context"
	"errors"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"

	"github.com/Suhaibinator/open-splunk/internal/audit"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
)

// mutationIntent seals the caller-authored, already sanitized request before
// generated IDs, resolved clocks, live authority, or capacity defaults enter
// the operation. canonical must be a detached clone with client_request_id
// cleared so the same logical intent can be retried under a new key.
func (handler *apiHandler) mutationIntent(
	ctx context.Context,
	route string,
	clientRequestID *string,
	canonical proto.Message,
) (*requestidempotency.Intent, error) {
	if clientRequestID == nil {
		return nil, nil
	}
	actor, ok := audit.ActorFromContext(ctx)
	if !ok || !actor.Valid() {
		return nil, internalError()
	}
	intent, err := requestidempotency.NewIntent(
		handler.tenantID,
		string(actor.Kind),
		actor.ID,
		route,
		*clientRequestID,
		canonical,
	)
	if err != nil {
		return nil, badRequestError(err.Error())
	}
	return &intent, nil
}

func mapRequestIdempotencyError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, requestidempotency.ErrInvalid):
		return badRequestError("client request ID is invalid")
	case errors.Is(err, requestidempotency.ErrConflict):
		return router.NewHTTPError(
			http.StatusConflict,
			"client request ID was already used for different intent",
		)
	case errors.Is(err, requestidempotency.ErrCapacity):
		return unavailableError("request idempotency capacity is exhausted")
	case errors.Is(err, requestidempotency.ErrUnavailable):
		return unavailableError("idempotent mutation outcome is unavailable")
	default:
		return internalError()
	}
}

func isRequestIdempotencyError(err error) bool {
	return errors.Is(err, requestidempotency.ErrInvalid) ||
		errors.Is(err, requestidempotency.ErrConflict) ||
		errors.Is(err, requestidempotency.ErrCapacity) ||
		errors.Is(err, requestidempotency.ErrUnavailable) ||
		errors.Is(err, requestidempotency.ErrCorrupt)
}
