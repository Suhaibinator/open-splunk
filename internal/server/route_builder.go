package server

import (
	"context"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/codec"
	"github.com/Suhaibinator/SRouter/pkg/common"
	"github.com/Suhaibinator/SRouter/pkg/router"
	"google.golang.org/protobuf/proto"
)

// apiRouteGroup keeps every server route registrar bound to the same user ID
// and user types as the owning router.
type apiRouteGroup = router.RouteGroup[string, struct{}]

// protoRequest constrains a route request to a pointer-to-struct protobuf
// message so the protobuf codec can allocate a fresh value without reflection.
type protoRequest[Message any] interface {
	proto.Message
	*Message
}

// postRoute assembles the shared typed POST route shape. Authentication,
// middleware, and the default body limit are inherited from the API group.
func postRoute[Request any, Response any](
	path string,
	routeCodec codec.Codec[Request, Response],
	handler router.GenericHandler[Request, Response],
	sanitizer func(context.Context, Request) (Request, error),
) router.RouteConfig[Request, Response] {
	return router.RouteConfig[Request, Response]{
		Path:       path,
		Methods:    []router.HttpMethod{router.MethodPost},
		Codec:      routeCodec,
		Handler:    handler,
		SourceType: router.Body,
		Sanitizer:  sanitizer,
	}
}

// sizedPostRoute is postRoute with an endpoint-specific request body limit.
func sizedPostRoute[Request any, Response any](
	path string,
	maximumRequestBytes int64,
	routeCodec codec.Codec[Request, Response],
	handler router.GenericHandler[Request, Response],
	sanitizer func(context.Context, Request) (Request, error),
) router.RouteConfig[Request, Response] {
	route := postRoute(path, routeCodec, handler, sanitizer)
	route.Overrides = common.RouteOverrides{MaxBodySize: maximumRequestBytes}
	return route
}

// protoPostRoute is the ordinary protobuf POST route used by most endpoints.
func protoPostRoute[
	Request protoRequest[RequestMessage],
	Response proto.Message,
	RequestMessage any,
](
	path string,
	handler router.GenericHandler[Request, Response],
	sanitizer func(context.Context, Request) (Request, error),
) router.RouteConfig[Request, Response] {
	return postRoute(
		path,
		codec.NewProtoCodec[Request, Response](),
		handler,
		sanitizer,
	)
}

// sizedProtoPostRoute is protoPostRoute with an endpoint-specific body limit.
func sizedProtoPostRoute[
	Request protoRequest[RequestMessage],
	Response proto.Message,
	RequestMessage any,
](
	path string,
	maximumRequestBytes int64,
	handler router.GenericHandler[Request, Response],
	sanitizer func(context.Context, Request) (Request, error),
) router.RouteConfig[Request, Response] {
	return sizedPostRoute(
		path,
		maximumRequestBytes,
		codec.NewProtoCodec[Request, Response](),
		handler,
		sanitizer,
	)
}

// rawGetRoute registers a non-typed GET endpoint on the API group. Long-lived
// and streaming handlers explicitly disable SRouter's timeout wrapper.
func rawGetRoute(
	path string,
	handler http.HandlerFunc,
	disableTimeout bool,
	middlewares ...common.Middleware,
) router.RouteConfigBase {
	return router.RouteConfigBase{
		Path:           path,
		Methods:        []router.HttpMethod{router.MethodGet},
		Handler:        handler,
		Middlewares:    middlewares,
		DisableTimeout: disableTimeout,
	}
}
