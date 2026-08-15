export {
  DEFAULT_REQUEST_TIMEOUT_MS,
  HttpError,
  MAXIMUM_ERROR_RESPONSE_BYTES,
  PROTOBUF_CONTENT_TYPE,
  ProtobufProtocolError,
  ProtobufResponseTooLargeError,
  ProtobufTransport,
  defineProtobufRoute,
  isHttpError,
  isHttpStatus,
} from "./protobuf-transport";

export {
  MAXIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES,
  MINIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES,
  clearAdministratorBearerToken,
  hasAdministratorBearerToken,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "./administrator-session";
export type {
  ProtobufCodec,
  ProtobufRequestOptions,
  ProtobufRoute,
  ProtobufRouteOptions,
  ProtobufTransportOptions,
} from "./protobuf-transport";

export { OpenSplunkApiClient, createOpenSplunkApiClient } from "./open-splunk-client";

export {
  appRoutes,
  auditEventRoutes,
  collectorRoutes,
  exportRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  openSplunkRoutes,
  savedSearchRoutes,
  searchRoutes,
  searchAttemptAuditRoutes,
  systemRoutes,
} from "./routes";
export type { RouteRequest, RouteResponse } from "./routes";

export {
  analyzeSPLIndexScope,
  adaptSystemBootstrap,
  getSystemBootstrap,
  indexSelectorsFromSPL,
  splIndexScopeIsExhaustive,
  IndexScopeResolutionError,
  resolveExactIndexScope,
  supportsServerFeature,
  MAXIMUM_BROWSER_BOOTSTRAP_APPS,
} from "./system-bootstrap";
export type {
  BrowserApiLimitsModel,
  BrowserIndexModel,
  ResolveIndexScopeOptions,
  SPLIndexScopeAnalysis,
  SystemBootstrapModel,
} from "./system-bootstrap";

export {
  featureNotAdvertised,
  isAdvertisedFeatureRouteUnavailable,
  isOptionalRouteUnavailable,
  optionalRouteUnavailable,
} from "./optional-feature";

export {
  assertBrowserResultColumnCount,
  assertBrowserResultPageBounds,
  MAXIMUM_BROWSER_RESULT_COLUMNS,
  pruneCursorChainFrom,
  recordNextPageToken,
  RepeatedPageCursorError,
  validateBrowserResultColumnCount,
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
