import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_RETURN_TO,
  PENDING_RETURN_STORAGE_KEY,
  PENDING_RETURN_TTL_MS,
  isSameOriginPath,
  organizationInvitationHref,
  passwordResetRequestHref,
  rememberPendingReturn,
  safeReturnTo,
  signInHref,
  takePendingReturn,
} from "./return-to";

test("a same-origin path survives, normalised, with its query and fragment", () => {
  assert.equal(safeReturnTo("/search/events/?q=index%3Dmain#top"), "/search/events/?q=index%3Dmain#top");
  assert.equal(safeReturnTo("/reports/../dashboards/"), "/dashboards/");
  assert.equal(safeReturnTo("/auth/invitation/?id=3&email=a%40b.test"), "/auth/invitation/?id=3&email=a%40b.test");
});

test("anything that could name another origin falls back to the product root", () => {
  const hostile = [
    "https://evil.example/",
    "http:/evil.example",
    "javascript:alert(1)",
    "data:text/html,hi",
    "//evil.example/path",
    "///evil.example",
    "/\\evil.example",
    "\\\\evil.example",
    "/\t/evil.example",
    "/\n/evil.example",
    "/\r//evil.example",
    "/\u0000/evil.example",
    "/\u007f/evil.example",
    // Dot segments resolve during the parse into a protocol-relative `//host`.
    "/.//evil.example",
    "/a/..//evil.example",
    "/%2e//evil.example",
    "/%2E%2E//evil.example",
    "/./\\evil.example",
    "search",
    " /search/",
    "",
    `/${"a".repeat(2048)}`,
  ];
  for (const raw of hostile) {
    assert.equal(safeReturnTo(raw), DEFAULT_RETURN_TO, JSON.stringify(raw));
  }
  assert.equal(safeReturnTo(null), DEFAULT_RETURN_TO);
  assert.equal(safeReturnTo(undefined), DEFAULT_RETURN_TO);
});

test("the sign-in and one-shot token pages are never a destination, however they are spelled", () => {
  for (const raw of [
    "/signin",
    "/signin/",
    "/SIGNIN/reset/",
    "/%73ignin/",
    "/auth/oauth/callback/?code=x&state=y",
    "/auth/email/reset/?token=abc",
    "/auth/email/verify/?token=abc",
    "/auth/email/accept-invitation/?token=abc",
  ]) {
    assert.equal(safeReturnTo(raw), DEFAULT_RETURN_TO, raw);
  }
  // A page that merely starts with the same letters is a different page.
  assert.equal(safeReturnTo("/signing-keys/"), "/signing-keys/");
});

test("a caller can choose its own fallback", () => {
  assert.equal(safeReturnTo("//evil.example", ""), "");
  assert.equal(safeReturnTo("/search/", ""), "/search/");
});

test("the sign-in link carries a safe return and a prefilled address", () => {
  assert.equal(signInHref(), "/signin/");
  assert.equal(signInHref({ returnTo: "/" }), "/signin/");
  assert.equal(signInHref({ returnTo: "//evil.example" }), "/signin/");
  assert.equal(signInHref({ returnTo: "/reports/", email: "nora@open-splunk.test" }), "/signin/?returnTo=%2Freports%2F&email=nora%40open-splunk.test");
  assert.equal(passwordResetRequestHref(), "/signin/reset/");
  assert.equal(passwordResetRequestHref("a b@c.test"), "/signin/reset/?email=a+b%40c.test");
  assert.equal(organizationInvitationHref("7", null), "/auth/invitation/?id=7");
  assert.equal(organizationInvitationHref("7", "x@y.test"), "/auth/invitation/?id=7&email=x%40y.test");
});

test("only a same-origin path is handed to client-side navigation", () => {
  assert.equal(isSameOriginPath("/auth/oauth/callback/?code=1"), true);
  assert.equal(isSameOriginPath("https://github.com/login/oauth/authorize"), false);
  assert.equal(isSameOriginPath("//github.com/"), false);
  assert.equal(isSameOriginPath("/\\github.com"), false);
});

function memoryStorage(): Map<string, string> & Pick<Storage, "getItem" | "removeItem" | "setItem"> {
  const map = new Map<string, string>() as Map<string, string> & Pick<Storage, "getItem" | "removeItem" | "setItem">;
  map.getItem = (key) => map.get(key) ?? null;
  map.removeItem = (key) => { map.delete(key); };
  map.setItem = (key, value) => { map.set(key, value); };
  return map;
}

test("a remembered return is read once, and only while it is fresh and safe", () => {
  const storage = memoryStorage();
  rememberPendingReturn("/auth/invitation/?id=3", 1_000, storage);
  assert.equal(takePendingReturn(1_000 + PENDING_RETURN_TTL_MS, storage), "/auth/invitation/?id=3");
  assert.equal(takePendingReturn(1_000, storage), null, "it is forgotten once read");

  rememberPendingReturn("/auth/invitation/?id=3", 1_000, storage);
  assert.equal(takePendingReturn(1_001 + PENDING_RETURN_TTL_MS, storage), null, "it expires");

  rememberPendingReturn("//evil.example", 1_000, storage);
  assert.equal(storage.has(PENDING_RETURN_STORAGE_KEY), false, "an unsafe path is never stored");

  storage.setItem(PENDING_RETURN_STORAGE_KEY, JSON.stringify({ v: 1, path: "/.//evil.example", expiresAt: 5_000 }));
  assert.equal(takePendingReturn(1_000, storage), null, "a tampered entry is re-validated");
  storage.setItem(PENDING_RETURN_STORAGE_KEY, "{not json");
  assert.equal(takePendingReturn(1_000, storage), null);
  storage.setItem(PENDING_RETURN_STORAGE_KEY, JSON.stringify({ v: 2, path: "/search/", expiresAt: 5_000 }));
  assert.equal(takePendingReturn(1_000, storage), null, "an unknown version is ignored");
});

test("blocked storage costs only the resume", () => {
  const blocked: Pick<Storage, "getItem" | "removeItem" | "setItem"> = {
    getItem() { throw new DOMException("denied", "SecurityError"); },
    removeItem() { throw new DOMException("denied", "SecurityError"); },
    setItem() { throw new DOMException("denied", "SecurityError"); },
  };
  assert.doesNotThrow(() => rememberPendingReturn("/search/", 1_000, blocked));
  assert.equal(takePendingReturn(1_000, blocked), null);
  assert.equal(takePendingReturn(1_000, null), null);
});
