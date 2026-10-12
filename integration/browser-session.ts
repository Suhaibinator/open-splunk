import type { BrowserContext } from "@playwright/test";

/** Disposable integration identity issued by the real GoCommon session service. */
export const browserSessionCookie = requiredEnvironment("OPEN_SPLUNK_BROWSER_SESSION_COOKIE");
export const browserOrganizationId = requiredEnvironment("OPEN_SPLUNK_BROWSER_ORGANIZATION_ID");
if (browserSessionCookie.length < 32 || /[;\r\n]/u.test(browserSessionCookie)) {
  throw new Error("The integration session cookie is invalid.");
}
if (!/^[1-9][0-9]*$/u.test(browserOrganizationId)) {
  throw new Error("The integration organization ID must be positive.");
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

export async function installBrowserSession(context: BrowserContext, origin: string): Promise<void> {
  await context.addCookies([{
    name: "open_splunk_session",
    value: browserSessionCookie,
    url: new URL(origin).origin,
    httpOnly: true,
    sameSite: "Lax",
    secure: new URL(origin).protocol === "https:",
  }]);
}

export function browserSessionHeaders(): Record<string, string> {
  return {
    Cookie: `open_splunk_session=${browserSessionCookie}`,
    "X-OpenSplunk-Organization-ID": browserOrganizationId,
  };
}

export function requireBrowserSessionRequest(headers: Record<string, string>): void {
  if (headers.authorization !== undefined
    || headers["x-opensplunk-organization-id"] !== browserOrganizationId
    || !headers.cookie?.split(/;\s*/u).includes(`open_splunk_session=${browserSessionCookie}`)) {
    throw new Error("Browser mutation must use the native session cookie and selected organization.");
  }
}
