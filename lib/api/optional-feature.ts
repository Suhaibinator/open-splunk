import { isHttpStatus } from "./protobuf-transport";

export type OptionalFeatureUnavailableReason =
  | "feature-not-advertised"
  | "route-unavailable";

export type OptionalFeatureResult<T> =
  | { status: "available"; value: T }
  | { status: "unavailable"; reason: OptionalFeatureUnavailableReason };

/** Optional handlers are omitted from the server allowlist when disabled. */
export function isOptionalRouteUnavailable(error: unknown): boolean {
  return isHttpStatus(error, 404) || isHttpStatus(error, 405) || isHttpStatus(error, 501);
}

/**
 * Once bootstrap advertises a feature, a 404 identifies a missing resource
 * (for example an expired search), not an absent route.
 */
export function isAdvertisedFeatureRouteUnavailable(error: unknown): boolean {
  return isHttpStatus(error, 405) || isHttpStatus(error, 501);
}

export const featureNotAdvertised = {
  status: "unavailable",
  reason: "feature-not-advertised",
} as const satisfies OptionalFeatureResult<never>;

export const optionalRouteUnavailable = {
  status: "unavailable",
  reason: "route-unavailable",
} as const satisfies OptionalFeatureResult<never>;
