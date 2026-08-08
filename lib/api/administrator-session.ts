/**
 * Browser administrator credential held only for the lifetime of this
 * JavaScript realm. It is deliberately never written to storage, cookies,
 * URLs, logs, or React-readable application state outside the sign-in form.
 */

export const MINIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES = 32;
export const MAXIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES = 512;

let administratorBearerToken: string | null = null;

const ADMINISTRATOR_ROUTE_PATHS: ReadonlySet<string> = new Set([
  "/api/v1/apps/create",
  "/api/v1/apps/delete",
  "/api/v1/apps/get",
  "/api/v1/apps/list",
  "/api/v1/apps/state/set",
  "/api/v1/apps/update",
  "/api/v1/audit/events/list",
  "/api/v1/audit/search-attempts/list",
  "/api/v1/collectors/get",
  "/api/v1/collectors/list",
  "/api/v1/collectors/state/set",
  "/api/v1/collectors/update",
  "/api/v1/indexes/create",
  "/api/v1/indexes/delete",
  "/api/v1/indexes/fields/list",
  "/api/v1/indexes/get",
  "/api/v1/indexes/list",
  "/api/v1/indexes/state/set",
  "/api/v1/indexes/stats/get",
  "/api/v1/indexes/update",
  "/api/v1/ingestion-tokens/create",
  "/api/v1/ingestion-tokens/get",
  "/api/v1/ingestion-tokens/list",
  "/api/v1/ingestion-tokens/revoke",
  "/api/v1/ingestion-tokens/update",
  "/api/v1/knowledge/objects/get",
  "/api/v1/knowledge/objects/list",
  "/api/v1/search/jobs/inspect",
]);

function isBearerCharacter(character: string): boolean {
  return /^[A-Za-z0-9._~+/=-]$/.test(character);
}

/** Mirrors the backend's bounded RFC 6750 bearer-token syntax admission. */
export function isValidAdministratorBearerToken(token: string): boolean {
  if (
    token.length < MINIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES
    || token.length > MAXIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES
  ) {
    return false;
  }

  let padding = false;
  for (let index = 0; index < token.length; index += 1) {
    const character = token[index] ?? "";
    if (character === "=") {
      if (index === 0) return false;
      padding = true;
      continue;
    }
    if (padding || !isBearerCharacter(character)) return false;
  }
  return true;
}

/** Replaces the current memory-only administrator credential. */
export function setAdministratorBearerToken(token: string): void {
  if (!isValidAdministratorBearerToken(token)) {
    throw new TypeError("Administrator bearer token is invalid.");
  }
  administratorBearerToken = token;
}

/** Returns the current credential for protected transport calls only. */
export function getAdministratorBearerToken(): string | null {
  return administratorBearerToken;
}

/** Removes the credential from the current JavaScript realm. */
export function clearAdministratorBearerToken(): void {
  administratorBearerToken = null;
}

export function hasAdministratorBearerToken(): boolean {
  return administratorBearerToken !== null;
}

/** Keeps bearer attachment on the backend's exact protected-route allowlist. */
export function isAdministratorRoutePath(path: string): boolean {
  return ADMINISTRATOR_ROUTE_PATHS.has(path);
}
