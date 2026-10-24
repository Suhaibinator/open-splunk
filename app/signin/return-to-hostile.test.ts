import assert from "node:assert/strict";
import test from "node:test";

import { signInHref as shellSignInHref } from "@/lib/product-access";

import { DEFAULT_RETURN_TO, safeReturnTo, signInHref } from "./return-to";

// Open-redirect and loop attempts against `returnTo`, beyond the spellings
// return-to.test.ts lists: the paths the server cleans into an auth page, and
// a combinatorial corpus checked against invariants rather than one answer.

const ORIGIN = "https://app.invalid";

/** The page Go's file server answers: the decoded path, cleaned the way `path.Clean` does. */
function servedByServer(returned: string): string {
  const { pathname } = new URL(returned, ORIGIN);
  let decoded = pathname;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    // An undecodable path is answered as written (Go refuses it outright).
  }
  const kept: string[] = [];
  for (const segment of decoded.split("/")) {
    if (segment === "..") kept.pop();
    else if (segment !== "" && segment !== ".") kept.push(segment);
  }
  return `/${kept.join("/")}`.toLowerCase();
}

function reachesAuthPage(returned: string): boolean {
  const served = servedByServer(returned);
  return ["/signin", "/auth/oauth", "/auth/email"].some((prefix) => served === prefix || served.startsWith(`${prefix}/`));
}

test("a path the server cleans into the sign-in or a one-time token page is not a destination", () => {
  for (const raw of [
    "/auth//oauth/callback/?code=x&state=y",
    "/auth/oauth//callback/",
    "/auth///email/reset/?token=abc",
    "/signin//",
    "/x/..%2fsignin/",
    "/x/..%2Fauth/oauth/callback/?code=x",
    "/x/%2E%2E%2Fsignin/",
    "/auth/%2Foauth/callback/",
    "/auth%2F%2Foauth/callback/",
    "/a/b/..%2F..%2Fsignin",
    "/%2E%2E%2Fsignin",
    "/search/..%2F%2E%2E%2Fauth/email/verify/?token=t",
  ]) {
    assert.equal(safeReturnTo(raw), DEFAULT_RETURN_TO, raw);
  }
  // Pages that only resemble an auth page stay reachable.
  for (const raw of ["/auth/invitation/?id=1", "/auth/oauthx/", "/auth/emails/", "/x/..%2fsignins/", "/signing-keys/"]) {
    assert.equal(safeReturnTo(raw) === DEFAULT_RETURN_TO, false, raw);
  }
});

test("encoded slashes and backslashes stay inside the path instead of naming a host", () => {
  for (const raw of ["/%2F%2Fevil.example", "/%5Cevil.example", "/%5C%5Cevil.example", "/%09/evil.example", "/%2F/evil.example"]) {
    const returned = safeReturnTo(raw);
    assert.equal(new URL(returned, ORIGIN).origin, ORIGIN, raw);
    assert.equal(returned.startsWith("//"), false, raw);
  }
  // Raw spellings of the same thing never get that far.
  for (const raw of ["//evil.example", "/\\evil.example", "\\/evil.example", "/\t/evil.example", " //evil.example", "\u0000//evil.example"]) {
    assert.equal(safeReturnTo(raw), DEFAULT_RETURN_TO, JSON.stringify(raw));
  }
});

test("schemes in any case or disguise are refused, and a path that merely contains one is only a path", () => {
  for (const raw of [
    "javascript:alert(1)",
    "JAVASCRIPT:alert(1)",
    " javascript:alert(1)",
    "\tjavascript:alert(1)",
    "java\nscript:alert(1)",
    "javascript%3Aalert(1)",
    "vbscript:msgbox(1)",
    "data:text/html;base64,PHNjcmlwdD4=",
    "blob:https://evil.example/uuid",
    "file:///etc/passwd",
    "https:evil.example",
    "%2F%2Fevil.example",
    "%2f%2fevil.example",
  ]) {
    assert.equal(safeReturnTo(raw), DEFAULT_RETURN_TO, JSON.stringify(raw));
  }
  assert.equal(safeReturnTo("/javascript:alert(1)"), "/javascript:alert(1)");
  assert.equal(new URL(safeReturnTo("/javascript:alert(1)"), ORIGIN).protocol, "https:");
});

test("header-splitting and markup in a return path come back percent-encoded", () => {
  const returned = safeReturnTo("/search/?q=%0d%0aSet-Cookie:%20a=b#<script>");
  assert.equal(/[\r\n<>]/u.test(returned), false, returned);
  assert.equal(new URL(returned, ORIGIN).pathname, "/search/");
  assert.equal(safeReturnTo("/search/<img src=x onerror=alert(1)>").includes("<"), false);
});

const FRAGMENTS = [
  "/", "//", "\\", "/\\", ".", "..", "/.", "/..", "%2e", "%2E%2E", "%2f", "%2F", "%5c", "%5C", "%09", "%0a", "%00",
  "\t", "\n", "\u0000", " ", "@", ":", "evil.example", "signin", "auth", "oauth", "email", "callback", "?", "#",
  "javascript:", "https:", "%", "%25", "。", "／",
];

test("no combination of hostile fragments leaves the origin, reaches an auth page, or changes on a second pass", () => {
  let checked = 0;
  for (const first of FRAGMENTS) {
    for (const second of FRAGMENTS) {
      for (const third of FRAGMENTS) {
        for (const raw of [`${first}${second}${third}`, `/${first}${second}${third}`, `/${first}/${second}/${third}`]) {
          const returned = safeReturnTo(raw);
          const label = JSON.stringify(raw);
          checked += 1;
          if (returned === DEFAULT_RETURN_TO) continue;
          assert.equal(returned.startsWith("/") && !returned.startsWith("//"), true, label);
          assert.equal([...returned].some((character) => character === "\\" || (character.codePointAt(0) ?? 0) < 0x20 || character === "\u007f"), false, label);
          assert.equal(new URL(returned, ORIGIN).origin, ORIGIN, label);
          assert.equal(reachesAuthPage(returned), false, `${label} -> ${returned}`);
          assert.equal(safeReturnTo(returned), returned, `${label} is stable`);
        }
      }
    }
  }
  assert.equal(checked, FRAGMENTS.length ** 3 * 3);
});

test("whatever the product shell puts in the sign-in link, the sign-in page re-validates", () => {
  for (const raw of [
    "/search/?q=1",
    "//evil.example",
    "/\\evil.example",
    "/auth//oauth/callback/?code=x",
    "/x/..%2fsignin/",
    "/SIGNIN/",
    "javascript:alert(1)",
  ]) {
    const href = new URL(shellSignInHref(raw), ORIGIN);
    assert.equal(href.pathname, "/signin/", raw);
    const returned = safeReturnTo(href.searchParams.get("returnTo"));
    assert.equal(new URL(returned, ORIGIN).origin, ORIGIN, raw);
    assert.equal(reachesAuthPage(returned), false, raw);
  }
});

test("the sign-in link's own returnTo survives its round trip unchanged", () => {
  for (const raw of ["/search/?q=a%26b#frag", "/auth/invitation/?id=..%2F..%2Fsignin&email=a%40b.test", "/reports/%2F%2Fx/", "/%E2%9C%93/"]) {
    const returnTo = new URL(signInHref({ returnTo: raw }), ORIGIN).searchParams.get("returnTo");
    assert.equal(safeReturnTo(returnTo), safeReturnTo(raw), raw);
  }
});
