import assert from "node:assert/strict";
import test from "node:test";

import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent } from "@/lib/testing/fake-dom";
import { act } from "react";
import { createRoot } from "react-dom/client";

import { AppState } from "@/gen/ts/open_splunk/app";

import { ProductShell } from "./product-shell";

test("the shared Help menu opens bundled documentation with keyboard menu semantics", async () => {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => {
      root.render(<ProductShell activeSection="home" appName="Search & Reporting" dataMode="demo"><p>Content</p></ProductShell>);
    });
    const trigger = container.querySelectorAll("button").find((button) => button.textContent?.startsWith("Help"));
    assert.ok(trigger);
    await act(async () => trigger.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 })));
    const menu = container.querySelector("#suite-help-popover");
    assert.ok(menu);
    const links = menu.querySelectorAll('a[role="menuitem"]');
    assert.ok(links.some((link) => link.getAttribute("href")?.split("?")[0] === "/help/"), "Help must link to bundled /help/ documentation");
    assert.doesNotMatch(menu.textContent ?? "", /not bundled|frontend preview/u);
    assert.equal(trigger.getAttribute("aria-expanded"), "true");
    assert.equal(trigger.getAttribute("aria-controls"), menu.getAttribute("id"));
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
  }
});

test("backend Help navigation retains the selected application context", async () => {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  try {
    await act(async () => root.render(<ProductShell
      activeSection="home"
      appName="Search & Reporting"
      dataMode="backend"
      backendAppCatalog={{
        apps: [{ appId: "selected-app", slug: "selected", displayName: "Selected", defaultIndexNames: [], state: AppState.APP_STATE_ACTIVE }],
        onSelect: () => undefined,
        selectedAppId: "selected-app",
        state: "available",
      }}
    ><p>Content</p></ProductShell>));
    const trigger = container.querySelectorAll("button").find((button) => button.textContent?.startsWith("Help"));
    assert.ok(trigger);
    await act(async () => trigger.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 })));
    const links = container.querySelector("#suite-help-popover")?.querySelectorAll('a[role="menuitem"]') ?? [];
    assert.ok(links.length > 0);
    for (const link of links) {
      const url = new URL(link.getAttribute("href") ?? "", "https://workspace.test");
      assert.equal(url.searchParams.get("appId"), "selected-app");
    }
  } finally {
    await act(async () => root.unmount());
    container.parentNode?.removeChild(container);
  }
});
