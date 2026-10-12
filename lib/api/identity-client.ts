import { GoCommonClient, ApiError, go_common } from "@suhaibinator/go-common-core";
import { currentIdentity, currentOrganizationId, isSiteAdministrator, publishIdentity, clearIdentitySession, reportSessionUnauthorized, type IdentityOrganization } from "./identity-session";

export function createIdentityClient(baseUrl = ""): GoCommonClient {
  return new GoCommonClient({
    baseUrl, apiPrefix: "/api/identity", credentials: "include",
    onUnauthorized: reportSessionUnauthorized,
    headersProvider: (): Record<string, string> => {
      const id = currentOrganizationId();
      return id === null ? {} : { "X-OpenSplunk-Organization-ID": id.toString() };
    },
  });
}

const teamOrganizationType = go_common.v1.OrganizationType.ORGANIZATION_TYPE_BUSINESS_TIER_0;

let refreshGeneration = 0;
export async function refreshIdentity(client: GoCommonClient): Promise<void> {
  const generation = ++refreshGeneration;
  try {
    const response = await client.user.getProfile();
    if (!response.user) throw new Error("The server did not return your account.");
    const source = response.user;
    const user = { id: source.id, email: source.email, name: [source.firstName, source.lastName].filter(Boolean).join(" ") || source.email, userType: source.userType };
    const organizations: IdentityOrganization[] = [];
    async function readPage(page: number): Promise<void> {
      const result = isSiteAdministrator(user)
        ? await client.admin.organizations.listAll({ page }, undefined, undefined, teamOrganizationType)
        : await client.organizations.list({ page }, undefined, undefined, undefined, teamOrganizationType);
      for (const row of result.organizations) {
        // Admin listings contain organizations directly; member listings wrap
        // the organization and its effective membership role.
        const organization = "organization" in row ? row.organization : row;
        if (organization && "id" in organization && !organizations.some((org) => org.id === organization.id)) {
          organizations.push({ id: organization.id, name: organization.name, role: "effectiveRole" in row ? row.effectiveRole : 44 });
        }
      }
      if (result.pagination?.hasNextPage) await readPage(page + 1);
    }
    await readPage(1);
    if (generation !== refreshGeneration) return;
    const previous = currentIdentity();
    const organizationId = previous.user?.id === user.id && organizations.some((org) => org.id === previous.organizationId)
      ? previous.organizationId : organizations[0]?.id ?? null;
    publishIdentity({ status: "authenticated", user, organizations, organizationId, error: null });
  } catch (error) {
    if (generation !== refreshGeneration) return;
    if (error instanceof ApiError && (error.statusCode === 401 || error.statusCode === 404)) clearIdentitySession();
    else {
      const previous = currentIdentity();
      publishIdentity({ ...previous, status: previous.user ? previous.status : "error", error: error instanceof Error ? error.message : "Unable to verify your session." });
    }
  }
}

export async function logoutIdentity(client: GoCommonClient): Promise<void> {
  ++refreshGeneration;
  await client.auth.logout();
  clearIdentitySession();
}

export function localReturnUrl(value: string | null | undefined): string {
  if (!value || !value.startsWith("/") || value.startsWith("//") || value.includes("\\")) return "/";
  if (Array.from(value).some((character) => character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127)) return "/";
  const base = new URL("https://open-splunk.invalid/");
  if (new URL(value, base).origin !== base.origin) return "/";
  return value;
}

export async function startGoogleSignIn(client: GoCommonClient, returnUrl = "/"): Promise<void> {
  const result = await client.auth.listOAuthProviders();
  const provider = result.providers.find((item) => item.slug === "google");
  if (!provider) throw new Error("Google sign-in is unavailable on this server.");
  const state = await client.auth.createOAuthState({ providerSlug: provider.slug, returnUrl: localReturnUrl(returnUrl), redirectUri: `${window.location.origin}/auth/oauth/callback/` });
  const authorizationUrl = new URL(state.authorizationUrl);
  if (!state.state || authorizationUrl.protocol !== "https:") throw new Error("The server returned an invalid Google sign-in redirect.");
  window.location.assign(authorizationUrl.toString());
}

const exchanges = new Map<string, Promise<string>>();
export function finishGoogleSignIn(client: GoCommonClient, code: string, state: string): Promise<string> {
  const pending = exchanges.get(state);
  if (pending) return pending;
  const exchange = (async () => {
    if (!code || !state) throw new Error("Google sign-in did not include a code and state.");
    let completed: { state?: string; userId?: string; returnUrl?: string } | null = null;
    try {
      const stored: unknown = JSON.parse(sessionStorage.getItem("open-splunk.completed-oauth") ?? "null");
      if (typeof stored === "object" && stored !== null && "state" in stored && "userId" in stored && "returnUrl" in stored
        && typeof stored.state === "string" && typeof stored.userId === "string" && typeof stored.returnUrl === "string") {
        completed = { state: stored.state, userId: stored.userId, returnUrl: stored.returnUrl };
      }
    } catch { /* Storage is optional; the native exchange still works. */ }
    if (completed?.state === state) {
      await refreshIdentity(client);
      if (currentIdentity().user?.id.toString() === completed.userId) return localReturnUrl(completed.returnUrl);
    }
    const result = await client.auth.oauthCallback({ code, state });
    await refreshIdentity(client);
    if (currentIdentity().status !== "authenticated") throw new Error("Unable to resolve the signed-in account.");
    const returnUrl = localReturnUrl(result.returnUrl);
    try { sessionStorage.setItem("open-splunk.completed-oauth", JSON.stringify({ state, userId: currentIdentity().user?.id.toString(), returnUrl })); } catch { /* An unavailable storage area cannot undo successful sign-in. */ }
    return returnUrl;
  })();
  exchanges.set(state, exchange);
  return exchange;
}
