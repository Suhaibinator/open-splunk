import assert from "node:assert/strict";
import test from "node:test";

// React determines DOM availability at import time.
import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot } from "react-dom/client";

import { IngestionToken, IngestionTokenPurpose, IngestionTokenState } from "@/gen/ts/open_splunk/collector_admin";
import { CreateIngestionTokenRequest, CreateIngestionTokenResponse, ListIngestionTokensResponse } from "@/gen/ts/open_splunk/collector_admin_api";
import { ListIndexesResponse } from "@/gen/ts/open_splunk/index_api";
import { GetSystemBootstrapResponse, ServerFeature } from "@/gen/ts/open_splunk/system_api";
import { FakeElement, fakeEvent } from "@/lib/testing/fake-dom";

import { BackendAdminConsole } from "./backend-admin-console";
import { parsePersistedTokenCreateGuard, serializeTokenCreateGuard, tokenCreateGuardStorageKey } from "./token-creation";

Object.assign(FakeElement.prototype, {
  closest(this: FakeElement, selector: string) {
    if (this.tagName.toLowerCase() === selector) return this;
    let element = this.parentNode instanceof FakeElement ? this.parentNode : null;
    while (element !== null) {
      if (element.tagName.toLowerCase() === selector) return element;
      element = element.parentNode instanceof FakeElement ? element.parentNode : null;
    }
    return null;
  },
  matches(selector: string) {
    assert.equal(selector, ":popover-open");
    return false;
  },
});

Object.defineProperties(FakeElement.prototype, {
  parentElement: { configurable: true, get(this: FakeElement) {
    return this.parentNode instanceof FakeElement ? this.parentNode : null;
  } },
  children: { configurable: true, get(this: FakeElement) {
    return this.childNodes.filter((node) => node instanceof FakeElement);
  } },
  classList: { configurable: true, get(this: FakeElement) {
    return { contains: (name: string) => (this.getAttribute("class") ?? "").split(/\s+/u).includes(name) };
  } },
});

const baseUrl = "https://splunk.example/";
const serverNow = new Date("2026-09-12T12:00:00Z");
const originalFetch = globalThis.fetch;
const token = IngestionToken.fromPartial({
  ingestionTokenId: "existing-unusable-token", version: 3n, name: "Review this token",
  tokenPrefix: "ost_review", purpose: IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR,
  state: IngestionTokenState.INGESTION_TOKEN_STATE_ACTIVE,
  createdAt: new Date("2026-09-01T00:00:00Z"), updatedAt: serverNow,
});

function settle(): Promise<void> {
  return act(async () => { await new Promise((resolve) => setImmediate(resolve)); });
}

for (const kind of ["legacy", "aged", "current"] as const) {
  test(`${kind} token receipt retains identity and ownership across startup and asynchronous refresh`, async () => {
    browser.storage.clear();
    Object.assign(window.location, { origin: "https://splunk.example", href: `${baseUrl}admin/?section=collectors`, pathname: "/admin/", search: "?section=collectors" });
    Object.defineProperty(globalThis, "HTMLElement", { configurable: true, value: FakeElement });
    Object.assign(window, { requestAnimationFrame: () => 1, cancelAnimationFrame() {}, setTimeout: globalThis.setTimeout, clearTimeout: globalThis.clearTimeout,
      history: { state: null, replaceState() {}, pushState() {}, back() {} } });
    Object.defineProperty(document, "visibilityState", { configurable: true, value: "visible" });
    Object.defineProperty(navigator, "onLine", { configurable: true, value: true });
    let heldLocks = 0;
    Object.defineProperty(navigator, "locks", { configurable: true, value: {
      async request(_name: string, _options: LockOptions, callback: (lock: Lock) => Promise<void>) {
        heldLocks += 1;
        try { await callback({ name: "token-recovery", mode: "exclusive" }); }
        finally { heldLocks -= 1; }
      },
    } });
    const armed = serverNow.getTime() - (kind === "aged" ? 8 * 86_400_000 : 60_000);
    const guard = serializeTokenCreateGuard(baseUrl, {
      clientRequestId: kind === "legacy" ? undefined : `${kind}-logical-request`,
      attemptId: "retained-attempt", ownerId: "previous-tab", definition: {
        name: "Original request", description: "", boundCollectorId: "collector-1", allowedIndexNames: ["main"],
        purpose: IngestionTokenPurpose.INGESTION_TOKEN_PURPOSE_NATIVE_COLLECTOR, hecProfile: undefined,
        expiresAt: undefined, armedServerTimeMs: armed, dispatchedServerTimeMs: armed,
        outcomeObservedServerTimeMs: null, requestRoundTripMs: null, requestTimeoutMs: 30_000,
        clockUncertaintyMs: 10, outcomeKind: "ambiguous-failure",
      }, preexistingTokenIds: new Set(), confirmedRevokedTokenIds: new Set(),
      failureMessage: "Connection lost", candidates: [], reconciliationError: null,
    }, null);
    const storageKey = tokenCreateGuardStorageKey(baseUrl);
    assert.ok(parsePersistedTokenCreateGuard(JSON.stringify(guard), baseUrl), "fixture must be a valid persisted request");
    browser.storage.set(storageKey, JSON.stringify(guard));
    let creates = 0;
    let lists = 0;
    let releaseCreate: (() => void) | undefined;
    let releaseList: (() => void) | undefined;
    let delayLists = false;
    globalThis.fetch = async (input, options) => {
      const path = new URL(String(input)).pathname;
      let bytes: Uint8Array;
      if (path === "/api/system/bootstrap") {
        bytes = GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
          serverTime: serverNow, features: [ServerFeature.SERVER_FEATURE_COLLECTOR_ADMIN],
        })).finish();
      } else if (path === "/api/indexes/list") {
        bytes = ListIndexesResponse.encode(ListIndexesResponse.fromPartial({})).finish();
      } else if (path === "/api/ingestion-tokens/list") {
        lists += 1;
        if (delayLists) await new Promise<void>((resolve) => { releaseList = resolve; });
        bytes = ListIngestionTokensResponse.encode(ListIngestionTokensResponse.fromPartial({ ingestionTokens: [token], page: { totalSize: 1n, totalSizeExact: true } })).finish();
      } else if (path === "/api/ingestion-tokens/create") {
        creates += 1;
        assert.equal(kind, "current", "old requests must never be resubmitted");
        const request = CreateIngestionTokenRequest.decode(options?.body as Uint8Array);
        assert.equal(request.clientRequestId, guard.clientRequestId);
        assert.equal(request.definition?.name, guard.definition.name);
        assert.deepEqual(request.definition?.constraints?.allowedIndexNames, guard.definition.allowedIndexNames);
        assert.equal(request.definition?.constraints?.boundCollectorId, guard.definition.boundCollectorId);
        await new Promise<void>((resolve) => { releaseCreate = resolve; });
        bytes = CreateIngestionTokenResponse.encode(CreateIngestionTokenResponse.fromPartial({ ingestionToken: token, replayed: true })).finish();
      } else {
        throw new Error(`Unexpected request: ${path}`);
      }
      return new Response(bytes as BodyInit, { headers: { "content-type": "application/x-protobuf" } });
    };
    const container = browser.document.body.appendChild(browser.document.createElement("div"));
    const root = createRoot(container as unknown as Element);
    try {
      await act(async () => { root.render(<BackendAdminConsole apiBaseUrl={baseUrl} />); });
      await settle();
      await settle();
      if (kind === "current") {
        assert.equal(creates, 1, "clock readiness must resume the persisted request");
        assert.ok(releaseCreate);
        assert.equal(heldLocks, 1);
        await act(async () => { root.render(<BackendAdminConsole apiBaseUrl={baseUrl} />); });
        await settle();
        assert.equal(creates, 1, "rerender must not duplicate an in-flight receipt request");
        await act(async () => { releaseCreate?.(); });
        await settle();
        assert.equal(creates, 1);
        assert.match(browser.document.body.textContent, /secret|unusable/u);
        assert.equal(heldLocks, 1);
        return;
      }
      assert.equal(creates, 0);
      assert.ok(lists >= 2, `catalog reconciliation did not run: ${container.textContent}`);
      assert.equal(heldLocks, 1);
      const resolveButton = container.querySelectorAll("button").find((button) => button.textContent === "Resolve token creation");
      assert.ok(resolveButton, container.textContent);
      await act(async () => { resolveButton.dispatchEvent(fakeEvent("click")); });
      assert.match(browser.document.body.textContent, /Review unresolved token request/);
      assert.match(browser.document.body.textContent, /ost_review/);
      const saved = browser.storage.get(storageKey);
      assert.ok(saved);
      assert.equal(parsePersistedTokenCreateGuard(saved, baseUrl)?.recovery.attemptId, "retained-attempt");
      delayLists = true;
      const check = browser.document.body.querySelector("#reconcile-token-create");
      assert.ok(check);
      await act(async () => { check.dispatchEvent(fakeEvent("click")); });
      assert.ok(releaseList, `manual refresh did not retain recovery ownership: ${browser.document.body.textContent}`);
      assert.equal(heldLocks, 1);
      assert.equal(browser.storage.get(storageKey), saved);
      await act(async () => { releaseList?.(); });
      await settle();
      assert.equal(creates, 0);
      assert.equal(heldLocks, 1);
      assert.match(browser.document.body.textContent, /Review unresolved token request/);
      assert.equal(browser.storage.get(storageKey), saved);
    } finally {
      releaseCreate?.();
      releaseList?.();
      await act(async () => { root.unmount(); });
      assert.equal(heldLocks, 0);
      container.parentNode?.removeChild(container);
      globalThis.fetch = originalFetch;
    }
  });
}
