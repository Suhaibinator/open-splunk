export {
  DEFAULT_REQUEST_TIMEOUT_MS,
  HttpError,
  PROTOBUF_CONTENT_TYPE,
  ProtobufProtocolError,
  ProtobufTransport,
  defineProtobufRoute,
  isHttpError,
  isHttpStatus,
} from "./protobuf-transport";
export type {
  ProtobufCodec,
  ProtobufRequestOptions,
  ProtobufRoute,
  ProtobufTransportOptions,
} from "./protobuf-transport";

export { OpenSplunkApiClient, createOpenSplunkApiClient } from "./open-splunk-client";

export {
  exportRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  openSplunkRoutes,
  savedSearchRoutes,
  searchRoutes,
  systemRoutes,
} from "./routes";
export type { RouteRequest, RouteResponse } from "./routes";

export {
  adaptSystemBootstrap,
  getSystemBootstrap,
  indexSelectorsFromSPL,
  splIndexScopeIsExhaustive,
  IndexScopeResolutionError,
  resolveExactIndexScope,
  supportsServerFeature,
} from "./system-bootstrap";
export type {
  BrowserApiLimitsModel,
  BrowserIndexModel,
  ResolveIndexScopeOptions,
  SystemBootstrapModel,
} from "./system-bootstrap";

export {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  isOptionalRouteUnavailable,
  optionalRouteUnavailable,
} from "./optional-feature";

export {
  recordNextPageToken,
  RepeatedPageCursorError,
} from "./pagination";
export type {
  OptionalFeatureResult,
  OptionalFeatureUnavailableReason,
} from "./optional-feature";

export {
  DEFAULT_SEARCH_WEBSOCKET_PATH,
  SearchWebSocketClient,
  exportJobTarget,
  resolveSearchWebSocketUrl,
  searchJobTarget,
  searchWebSocketTargetKey,
} from "./search-websocket";
export type {
  SearchWebSocketClientOptions,
  SearchWebSocketConnectionState,
  SearchWebSocketErrorListener,
  SearchWebSocketEventListener,
  SearchWebSocketProtocolErrorListener,
  SearchWebSocketResynchronizationListener,
  SearchWebSocketResynchronizationNotice,
  SearchWebSocketSequenceGap,
  SearchWebSocketSequenceGapListener,
  SearchWebSocketStateListener,
  SearchWebSocketSubscriptionOptions,
  WebSocketFactory,
} from "./search-websocket";
