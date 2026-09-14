import assert from "node:assert/strict";
import test from "node:test";

import { browser } from "@/lib/testing/fake-browser";
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { AppState } from "@/gen/ts/open_splunk/app";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";
import {
  clearAdministratorBearerToken,
  invalidateAppCatalog,
  setAdministratorBearerToken,
  type OpenSplunkApiClient,
} from "@/lib/api";
import * as clientModule from "@/lib/api/open-splunk-client";

import { ProductShell, productMenuControlId, ThemeMenu } from "./product-shell";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function bootstrapWithApp(displayName: string): GetSystemBootstrapResponse {
  return GetSystemBootstrapResponse.fromPartial({
    apps: [{
      appId: "app-1",
      defaultIndexNames: [],
      displayName,
      slug: "search",
      state: AppState.APP_STATE_ACTIVE,
    }],
    selectedAppId: "app-1",
    serverTime: new Date("2026-09-12T00:00:00Z"),
  });
}

test("menu triggers control only their mounted popover", () => {
  for (const menu of ["apps", "help", "user"] as const) {
    assert.equal(productMenuControlId(null, menu), undefined);
    assert.equal(productMenuControlId(menu, menu), `suite-${menu === "apps" ? "app" : menu}-popover`);

    for (const other of ["apps", "help", "user"] as const) {
      if (other !== menu) assert.equal(productMenuControlId(other, menu), undefined);
    }
  }
});

test("the closed product shell exposes collapsed menu semantics without dangling controls", () => {
  const markup = renderToStaticMarkup(
    <ProductShell activeSection="home" appName="Search & Reporting" dataMode="demo">
      <p>Page content</p>
    </ProductShell>,
  );

  assert.equal(markup.match(/aria-haspopup="menu"/gu)?.length, 3);
  assert.equal(markup.match(/aria-haspopup="menu" aria-expanded="false"/gu)?.length, 3);
  assert.doesNotMatch(markup, /aria-controls=/u);
  assert.doesNotMatch(markup, /id="suite-(?:app|help|user)-popover"/u);
});

test("the theme menu checks exactly one radio for every preference", () => {
  for (const preference of ["system", "light", "dark"] as const) {
    const markup = renderToStaticMarkup(<ThemeMenu preference={preference} onSelect={() => undefined} />);

    assert.equal(markup.match(/role="menuitemradio"/gu)?.length, 3);
    assert.equal(markup.match(/aria-checked="true"/gu)?.length, 1);
    assert.equal(markup.match(/aria-checked="false"/gu)?.length, 2);
    // The checked entry is the one carrying the requested label, and the
    // legend names the group for assistive technology.
    const label = preference === "system" ? "System" : preference === "light" ? "Light" : "Dark";
    assert.match(markup, new RegExp(`aria-checked="true"[^>]*>(?:<svg[^]*?</svg>)?${label}</button>`, "u"));
    assert.match(markup, /<fieldset class="suite-theme-menu"><legend class="suite-menu-label">Theme<\/legend>/u);
  }
});

test("a controlled backend app catalog keeps dashboard navigation in place", () => {
  const markup = renderToStaticMarkup(
    <ProductShell
      activeSection="dashboards"
      appName="Dashboards"
      backendAppCatalog={{
        apps: [{ appId: "app-1", slug: "grade-this", displayName: "GradeThis", defaultIndexNames: [], state: AppState.APP_STATE_ACTIVE }],
        onSelect: () => undefined,
        selectedAppId: "app-1",
        state: "available",
      }}
      dataMode="backend"
    >
      <p>Dashboard content</p>
    </ProductShell>,
  );

  assert.match(markup, /App: <strong>GradeThis<\/strong>/u);
  assert.match(markup, /aria-label="Dashboards navigation"/u);
  assert.doesNotMatch(markup, /GradeThis Operations/u);
});

test("a cross-tab catalog invalidation refreshes an already mounted product shell", async () => {
  const realFactory = clientModule.createOpenSplunkApiClient;
  const answers = ["Before mutation", "After mutation"];
  let bootstrapCalls = 0;
  (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = (() => ({
    system: {
      bootstrap() {
        const displayName = answers[bootstrapCalls];
        bootstrapCalls += 1;
        assert.ok(displayName, "unexpected bootstrap request");
        return Promise.resolve(bootstrapWithApp(displayName));
      },
    },
  }) as unknown as OpenSplunkApiClient) as typeof realFactory;

  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => {
      root.render(
        <ProductShell activeSection="home" apiBaseUrl="https://catalog.test/api/" appName="Search" dataMode="backend">
          <p>Page content</p>
        </ProductShell>,
      );
    });
    await act(async () => new Promise((resolve) => setImmediate(resolve)));
    assert.equal(bootstrapCalls, 1);
    assert.match(container.textContent ?? "", /Before mutation/u);

    await act(async () => {
      browser.dispatchWindowEvent("storage", {
        key: "open-splunk.app-catalog-invalidation",
        newValue: "another-tab-nonce",
        oldValue: null,
        storageArea: null,
      });
    });
    await act(async () => new Promise((resolve) => setImmediate(resolve)));

    assert.equal(bootstrapCalls, 2);
    assert.match(container.textContent ?? "", /After mutation/u);
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
    (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = realFactory;
  }
});

test("an administrator session change hides the prior catalog and rejects its late refresh", async () => {
  const realFactory = clientModule.createOpenSplunkApiClient;
  const staleRefresh = deferred<GetSystemBootstrapResponse>();
  const nextSession = deferred<GetSystemBootstrapResponse>();
  let bootstrapCalls = 0;
  (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = (() => ({
    system: {
      bootstrap() {
        bootstrapCalls += 1;
        if (bootstrapCalls === 1) return Promise.resolve(bootstrapWithApp("Tenant A"));
        if (bootstrapCalls === 2) return staleRefresh.promise;
        if (bootstrapCalls === 3) return nextSession.promise;
        throw new Error("unexpected bootstrap request");
      },
    },
  }) as unknown as OpenSplunkApiClient) as typeof realFactory;

  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => {
      root.render(
        <ProductShell activeSection="home" apiBaseUrl="https://session-catalog.test" appName="Search" dataMode="backend">
          <p>Page content</p>
        </ProductShell>,
      );
    });
    await act(async () => new Promise((resolve) => setImmediate(resolve)));
    assert.match(container.textContent ?? "", /Tenant A/u);

    await act(async () => invalidateAppCatalog("https://session-catalog.test/"));
    assert.equal(bootstrapCalls, 2);
    await act(async () => setAdministratorBearerToken("a".repeat(32)));
    assert.equal(bootstrapCalls, 3);
    assert.doesNotMatch(container.textContent ?? "", /Tenant A/u);

    await act(async () => staleRefresh.resolve(bootstrapWithApp("Tenant A late")));
    await act(async () => new Promise((resolve) => setImmediate(resolve)));
    assert.doesNotMatch(container.textContent ?? "", /Tenant A/u);

    await act(async () => nextSession.resolve(bootstrapWithApp("Tenant B")));
    await act(async () => new Promise((resolve) => setImmediate(resolve)));
    assert.match(container.textContent ?? "", /Tenant B/u);
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
    clearAdministratorBearerToken();
    (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = realFactory;
  }
});

test("an authoritative empty catalog blocks a search from the stale URL preference", async () => {
  const realFactory = clientModule.createOpenSplunkApiClient;
  (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = (() => ({
    system: {
      bootstrap: () => Promise.resolve(GetSystemBootstrapResponse.fromPartial({
        serverTime: new Date("2026-09-12T00:00:00Z"),
      })),
    },
  }) as unknown as OpenSplunkApiClient) as typeof realFactory;
  window.location.search = "?appId=removed-app";
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => {
      root.render(
        <ProductShell activeSection="home" apiBaseUrl="https://empty-catalog.test" appName="Search" dataMode="backend">
          <p>Page content</p>
        </ProductShell>,
      );
    });
    await act(async () => new Promise((resolve) => setImmediate(resolve)));
    assert.equal(container.querySelector("#suite-find-input")?.hasAttribute("disabled"), true);
    assert.equal(container.querySelector('button[aria-label="Search"]')?.hasAttribute("disabled"), true);
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
    window.location.search = "";
    (clientModule as unknown as { createOpenSplunkApiClient: typeof realFactory }).createOpenSplunkApiClient = realFactory;
  }
});
