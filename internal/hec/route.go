package hec

import "net/http"

// Endpoint is one exact route in the bounded HEC surface.
type Endpoint uint8

const (
	EndpointOutside Endpoint = iota
	EndpointUnknown
	EndpointEvent
	EndpointRaw
	EndpointAcknowledgment
	EndpointHealth
)

// Route describes an exact path and method classification without cleaning,
// redirecting, or falling through to another router.
type Route struct {
	Endpoint      Endpoint
	HECNamespace  bool
	KnownPath     bool
	Method        string
	Allow         string
	MethodAllowed bool
}

// ClassifyRoute classifies the path byte-for-byte. Unknown paths under the HEC
// namespace stay inside that namespace so the top-level server can return JSON
// rather than the browser SPA.
func ClassifyRoute(method, path string) Route {
	route := Route{Endpoint: EndpointOutside, Method: method}
	switch path {
	case "/services/collector", "/services/collector/event":
		route.Endpoint, route.KnownPath = EndpointEvent, true
	case "/services/collector/raw":
		route.Endpoint, route.KnownPath = EndpointRaw, true
	case "/services/collector/ack":
		route.Endpoint, route.KnownPath = EndpointAcknowledgment, true
	case "/services/collector/health":
		route.Endpoint, route.KnownPath = EndpointHealth, true
	default:
		if path == "/services/collector/" || len(path) > len("/services/collector/") && path[:len("/services/collector/")] == "/services/collector/" {
			route.Endpoint = EndpointUnknown
			route.HECNamespace = true
		}
		return route
	}
	route.HECNamespace = true
	if route.Endpoint == EndpointHealth {
		route.Allow = http.MethodGet
	} else {
		route.Allow = http.MethodPost
	}
	route.MethodAllowed = method == route.Allow
	return route
}

// ProtocolError returns the routing failure, if any.
func (route Route) ProtocolError() error {
	if !route.HECNamespace {
		return nil
	}
	if !route.KnownPath {
		return NewProtocolError(ErrorUnknownPath, nil)
	}
	if !route.MethodAllowed {
		return NewProtocolError(ErrorMethodNotAllowed, nil)
	}
	return nil
}
