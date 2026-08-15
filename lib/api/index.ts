export {
  DEFAULT_REQUEST_TIMEOUT_MS,
  HttpError,
  isHttpError,
  isHttpStatus,
} from "./protobuf-transport";

export {
  clearAdministratorBearerToken,
  hasAdministratorBearerToken,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "./administrator-session";
export type { ProtobufRequestOptions } from "./protobuf-transport";

export { OpenSplunkApiClient, createOpenSplunkApiClient } from "./open-splunk-client";

export {
  analyzeSPLIndexScope,
  getSystemBootstrap,
  resolveExactIndexScope,
  supportsServerFeature,
} from "./system-bootstrap";
export type {
  BrowserIndexModel,
  SystemBootstrapModel,
} from "./system-bootstrap";

export {
  isAdvertisedFeatureRouteUnavailable,
  isOptionalRouteUnavailable,
} from "./optional-feature";

export {
  assertBrowserResultPageBounds,
  pruneCursorChainFrom,
  recordNextPageToken,
  RepeatedPageCursorError,
} from "./pagination";
export type { OptionalFeatureResult } from "./optional-feature";

export { SearchWebSocketClient, searchJobTarget } from "./search-websocket";
