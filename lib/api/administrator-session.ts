/**
 * Browser administrator credential held only for the lifetime of this
 * JavaScript realm. It is deliberately never written to storage, cookies,
 * URLs, logs, or React-readable application state outside the sign-in form.
 */

export const MINIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES = 32;
export const MAXIMUM_ADMINISTRATOR_BEARER_TOKEN_BYTES = 512;

let administratorBearerToken: string | null = null;

const ADMINISTRATOR_ROUTE_PATHS: ReadonlySet<string> = new Set([
  "/api/alerts/create",
  "/api/alerts/delete",
  "/api/alerts/get",
  "/api/alerts/list",
  "/api/alerts/run",
  "/api/alerts/runs/list",
  "/api/alerts/secret/rotate",
  "/api/alerts/state/set",
  "/api/alerts/update",
  "/api/alerts/webhook/test",
  "/api/apps/create",
  "/api/apps/delete",
  "/api/apps/get",
  "/api/apps/list",
  "/api/apps/state/set",
  "/api/apps/update",
  "/api/audit/events/list",
  "/api/audit/search-attempts/list",
  "/api/collectors/get",
  "/api/collectors/list",
  "/api/collectors/state/set",
  "/api/collectors/update",
  "/api/hec/operations/get",
  "/api/indexes/create",
  "/api/indexes/delete",
  "/api/indexes/fields/list",
  "/api/indexes/get",
  "/api/indexes/list",
  "/api/indexes/state/set",
  "/api/indexes/stats/get",
  "/api/indexes/update",
  "/api/ingestion-tokens/create",
  "/api/ingestion-tokens/get",
  "/api/ingestion-tokens/list",
  "/api/ingestion-tokens/revoke",
  "/api/ingestion-tokens/state/set",
  "/api/ingestion-tokens/update",
  "/api/knowledge/objects/create",
  "/api/knowledge/objects/delete",
  "/api/knowledge/objects/dependencies",
  "/api/knowledge/objects/dependents",
  "/api/knowledge/objects/get",
  "/api/knowledge/objects/list",
  "/api/knowledge/objects/preview",
  "/api/knowledge/objects/quarantine",
  "/api/knowledge/objects/quarantine/prepare",
  "/api/knowledge/objects/set-state",
  "/api/knowledge/objects/update",
  "/api/knowledge/objects/validate",
  "/api/knowledge/lookups/create",
  "/api/knowledge/lookups/delete",
  "/api/knowledge/lookups/get",
  "/api/knowledge/lookups/list",
  "/api/knowledge/lookups/preview",
  "/api/knowledge/lookups/replace",
  "/api/knowledge/lookups/state/set",
  "/api/search/jobs/inspect",
  "/api/server/settings/get",
  "/api/server/settings/update",
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
