/**
 * Where a sign-in flow sends the browser once it is done.
 *
 * A `returnTo` value arrives in a query string anyone can write, so it is an
 * open-redirect vector until proven otherwise: only a same-origin absolute
 * path survives. Anything that could name another origin -- a scheme, a
 * protocol-relative `//host`, a backslash a browser reads as a slash, a
 * control character a browser strips before parsing, a dot segment that
 * collapses to `//host` -- falls back to the
 * product root, and so does a path back into the one-shot auth pages, which
 * would loop or replay a consumed token.
 */

export const DEFAULT_RETURN_TO = "/";
export const SIGN_IN_PATH = "/signin/";
export const PASSWORD_RESET_REQUEST_PATH = "/signin/reset/";
export const ORGANIZATION_INVITATION_PATH = "/auth/invitation/";

/** A throwaway origin to resolve against; a value that leaves it named another origin. */
const PROBE_ORIGIN = "https://return-to.invalid";

/** Pages a flow must never return to: the sign-in form, and pages that spend a one-time token. */
const NON_RETURNABLE_PREFIXES = ["/signin", "/auth/oauth", "/auth/email"];

function hasUnsafeCharacter(value: string): boolean {
  return [...value].some((character) => {
    const code = character.codePointAt(0) ?? 0;
    // Tab and newline are removed by the URL parser, so `/\t/host` would become `//host`.
    return code < 0x20 || code === 0x7f || character === "\\";
  });
}

function isNonReturnable(pathname: string): boolean {
  const lowered = pathname.toLowerCase();
  return NON_RETURNABLE_PREFIXES.some((prefix) => lowered === prefix || lowered.startsWith(`${prefix}/`));
}

/**
 * The same-origin path `raw` names, normalised, or `fallback` when it names
 * anything else. Pass the result straight to navigation.
 */
export function safeReturnTo(raw: string | null | undefined, fallback: string = DEFAULT_RETURN_TO): string {
  if (typeof raw !== "string" || raw.length === 0 || raw.length > 2048) return fallback;
  if (!raw.startsWith("/") || raw.startsWith("//") || hasUnsafeCharacter(raw)) return fallback;
  let url: URL;
  try {
    url = new URL(raw, PROBE_ORIGIN);
  } catch {
    return fallback;
  }
  if (url.origin !== PROBE_ORIGIN) return fallback;
  // Dot segments are resolved by the parse: `/.//host`, `/a/..//host` and
  // their percent-encoded forms all normalise to `//host`, which a browser
  // reads as another origin. Only the normalised path is ever returned, so it
  // is the one checked.
  if (url.pathname.startsWith("//") || url.pathname.includes("\\")) return fallback;
  if ([url.pathname, decodedPath(url.pathname)].some((candidate) => isNonReturnable(servedPath(candidate)))) return fallback;
  return `${url.pathname}${url.search}${url.hash}`;
}

/**
 * The page the server answers `pathname` with. It cleans the decoded request
 * path as Go's `path.Clean` does -- empty and `.` segments dropped, `..`
 * resolved -- so `/auth//oauth/callback/` and `/x/..%2fsignin/` (which
 * decodes to `/x/../signin/`) are the auth pages they reach.
 */
function servedPath(pathname: string): string {
  const segments: string[] = [];
  for (const segment of pathname.split("/")) {
    if (segment === "..") segments.pop();
    else if (segment !== "" && segment !== ".") segments.push(segment);
  }
  return `/${segments.join("/")}`;
}

/** The path with its percent escapes decoded, so `/%73ignin` is recognised as `/signin`; the raw path if it does not decode. */
function decodedPath(pathname: string): string {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return pathname;
  }
}

/** Builds a query string from the entries that have a value, with its `?`. */
function query(entries: Readonly<Record<string, string | null | undefined>>): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(entries)) {
    if (typeof value === "string" && value.length > 0) params.set(key, value);
  }
  const text = params.toString();
  return text.length === 0 ? "" : `?${text}`;
}

/** The sign-in page, told where to go afterwards and which address to prefill. */
export function signInHref(options: { readonly returnTo?: string | null; readonly email?: string | null } = {}): string {
  const returnTo = safeReturnTo(options.returnTo ?? null);
  return `${SIGN_IN_PATH}${query({ returnTo: returnTo === DEFAULT_RETURN_TO ? null : returnTo, email: options.email ?? null })}`;
}

/** The password-reset request page, with the address to prefill. */
export function passwordResetRequestHref(email?: string | null): string {
  return `${PASSWORD_RESET_REQUEST_PATH}${query({ email: email ?? null })}`;
}

/** An organization invitation page. */
export function organizationInvitationHref(invitationId: string, email?: string | null): string {
  return `${ORGANIZATION_INVITATION_PATH}${query({ id: invitationId, email: email ?? null })}`;
}

/** True for a URL a page may hand to client-side navigation; anything else needs a document load. */
export function isSameOriginPath(url: string): boolean {
  return url.startsWith("/") && !url.startsWith("//") && !url.includes("\\");
}

// --------------------------------------------------------------------------
// The return a flow resumes after an emailed link
// --------------------------------------------------------------------------

/**
 * Registration ends at an emailed verification link, which may open in a new
 * tab, so the invitation page that started it is remembered in localStorage
 * for a day. It is a convenience: losing it lands the user on the product
 * root, which lists their pending invitations anyway.
 */
export const PENDING_RETURN_STORAGE_KEY = "open-splunk.auth-pending-return";
export const PENDING_RETURN_TTL_MS = 24 * 60 * 60_000;

type ReturnStorage = Pick<Storage, "getItem" | "removeItem" | "setItem">;

function browserStorage(): ReturnStorage | null {
  try {
    return typeof window === "undefined" ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function rememberPendingReturn(path: string, now: number = Date.now(), storage: ReturnStorage | null = browserStorage()): void {
  const safe = safeReturnTo(path, "");
  if (safe.length === 0 || storage === null) return;
  try {
    storage.setItem(PENDING_RETURN_STORAGE_KEY, JSON.stringify({ v: 1, path: safe, expiresAt: now + PENDING_RETURN_TTL_MS }));
  } catch {
    // Blocked storage only costs the resume.
  }
}

/** Reads and forgets the remembered return; null when there is none, it expired, or it is not a safe path. */
export function takePendingReturn(now: number = Date.now(), storage: ReturnStorage | null = browserStorage()): string | null {
  if (storage === null) return null;
  try {
    const raw = storage.getItem(PENDING_RETURN_STORAGE_KEY);
    storage.removeItem(PENDING_RETURN_STORAGE_KEY);
    if (raw === null) return null;
    const parsed = JSON.parse(raw) as { v?: unknown; path?: unknown; expiresAt?: unknown };
    if (parsed.v !== 1 || typeof parsed.path !== "string" || typeof parsed.expiresAt !== "number" || parsed.expiresAt < now) {
      return null;
    }
    const safe = safeReturnTo(parsed.path, "");
    return safe.length === 0 ? null : safe;
  } catch {
    return null;
  }
}
