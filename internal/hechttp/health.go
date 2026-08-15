package hechttp

import (
	"errors"
	"net/http"

	"github.com/Suhaibinator/open-splunk/internal/hec"
)

func (handler *Handler) serveHealth(response http.ResponseWriter, request *http.Request) {
	if len(request.RequestURI) > handler.limits.MaximumRequestTargetBytes {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil))
		return
	}
	if err := hec.ValidateConsumedHeaderBytes(
		handler.limits.MaximumHeaderBytes,
		request.Header.Values("Authorization"),
		request.Header.Values("Content-Type"),
		request.Header.Values("Content-Encoding"),
	); err != nil {
		handler.writeError(response, err)
		return
	}
	if queryAuthorizationPresent(request.URL.RawQuery) {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorQueryAuthorizationDisabled, nil))
		return
	}
	releaseHealth, acquired := handler.beginHealth()
	if !acquired {
		handler.writeResponse(response, hec.NewResponse(hec.ResultUnhealthyQueuesFull), 0, 0)
		return
	}
	defer releaseHealth()
	if len(request.Header.Values("Authorization")) != 0 {
		_, authErr := handler.authenticate(request)
		if authErr != nil {
			var failure *hec.ProtocolError
			if errors.As(authErr, &failure) && failure.Kind == hec.ErrorTokenDisabled {
				handler.writeResponse(response, hec.NewResponse(hec.ResultHealthTokenDisabled), 0, 0)
				return
			}
			handler.writeResponse(response, hec.NewResponse(hec.ResultHealthInvalidToken), 0, 0)
			return
		}
	}
	if hasHealthBody(request) {
		handler.writeError(response, hec.NewProtocolError(hec.ErrorInvalidDataFormat, nil))
		return
	}
	query, err := parseEndpointQuery(request.URL.RawQuery, hec.EndpointHealth, handler.limits)
	if err != nil {
		handler.writeError(response, err)
		return
	}
	acknowledgmentRequested := query.acknowledgmentHealthCheck
	snapshot, healthErr := handler.health.HECHealth(request.Context())
	if healthErr != nil {
		snapshot = HealthSnapshot{}
	}
	if !handler.lifecycle.isAccepting() {
		snapshot.QueueAvailable = false
	}
	code := hec.ResultHealthy
	queueUnavailable := !snapshot.QueueAvailable
	ackUnavailable := acknowledgmentRequested && !snapshot.AcknowledgmentAvailable
	switch {
	case queueUnavailable && ackUnavailable:
		code = hec.ResultUnhealthyQueuesAndAck
	case queueUnavailable:
		code = hec.ResultUnhealthyQueuesFull
	case ackUnavailable:
		code = hec.ResultUnhealthyAcknowledgment
	}
	handler.writeResponse(response, hec.NewResponse(code), 0, 0)
}

func hasHealthBody(request *http.Request) bool {
	return request.ContentLength != 0 || len(request.TransferEncoding) != 0 ||
		request.Header.Get("Transfer-Encoding") != ""
}

func (gate *lifecycleGate) isAccepting() bool {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	return gate.accepting
}
