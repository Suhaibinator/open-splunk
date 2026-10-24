import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import { createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";

import { IdentityProvider } from "./identity-provider";
import { ProductShell } from "./product-shell";

/*
 * Keyboard and screen-reader paths through the product shell's identity
 * surfaces: the organization menu walked with the arrow keys and left with
 * Escape, what it announces while a switch is pending and after, which
 * element each error describes, and how the forbidden state is labelled.
 * Assertions compare primitives only.
 */

interface FakeLocation {
  hash: string;
  href?: string;
  pathname: string;
  search: string;
  assign?: (href: string) => void;
  replace?: (href: string) => void;
}

const fakeWindow = window as unknown as {
  location: FakeLocation;
  requestAnimationFrame(callback: FrameRequestCallback): number;
};
fakeWindow.requestAnimationFrame = (callback) => {
  callback(0);
  return 1;
};
fakeWindow.location.assign = () => undefined;
fakeWindow.location.replace = () => undefined;

function at(pathname: string) {
  fakeWindow.location.pathname = pathname;
  fakeWindow.location.search = "";
  fakeWindow.location.hash = "";
  fakeWindow.location.href = `https://workspace.test${pathname}`;
}

async function settle(milliseconds = 10): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, milliseconds);
    });
  });
}

async function mount(node: ReactNode) {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  await settle();
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

function shell(client: IdentityClient, section: "datasets" | "admin" = "datasets") {
  return (
    <IdentityProvider client={client}>
      <ProductShell activeSection={section} appName="Search & Reporting" dataMode="demo"><p>Page content</p></ProductShell>
    </IdentityProvider>
  );
}

function signedIn(email: string): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email });
}

function trigger(container: FakeElement): FakeElement {
  const found = container.querySelectorAll("button").find((button) => button.getAttribute("aria-label")?.startsWith("Organization:"));
  assert.ok(found, "the organization trigger renders");
  return found;
}

/** A pointer click: `detail` 1, so opening a menu does not also move focus into it. */
async function click(element: FakeElement | undefined) {
  assert.ok(element, "the control is rendered");
  await act(async () => {
    element.focus();
    element.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 }));
  });
}

/** A key pressed while a menu is open: the shell listens on the document. */
async function press(key: string) {
  await act(async () => {
    browser.document.dispatchEvent(Object.assign(fakeEvent("keydown"), { key }));
  });
}

function choices(container: FakeElement): FakeElement[] {
  return container.querySelectorAll('[role="menuitemradio"]');
}

function choiceNamed(container: FakeElement, name: string): FakeElement {
  const found = choices(container).find((choice) => choice.querySelector("strong")?.textContent === name);
  assert.ok(found, `no organization choice ${name}`);
  return found;
}

function politeRegion(container: FakeElement): FakeElement {
  const region = container.querySelectorAll("span").find((span) => span.getAttribute("aria-live") === "polite");
  assert.ok(region, "the switcher keeps a polite region mounted");
  return region;
}

function textOfIds(ids: string | null): string {
  return (ids ?? "").split(/\s+/u).filter((id) => id.length > 0).map((id) => {
    const target = browser.document.getElementById(id);
    assert.ok(target, `#${id} is referenced, so it must exist`);
    return target.textContent.trim();
  }).join(" ");
}

test("the organization menu is walked with the arrows, Home and End, and Escape returns focus to its trigger", async () => {
  at("/datasets/");
  const view = await mount(shell(signedIn("owen@open-splunk.test")));
  try {
    await settle();
    const button = trigger(view.container);
    assert.equal(button.hasAttribute("aria-controls"), false, "nothing is controlled while the menu is closed");
    await click(button);
    const menu = view.container.querySelector('[data-suite-menu="org"]');
    assert.ok(menu);
    assert.equal(button.getAttribute("aria-controls"), menu.getAttribute("id"));
    assert.equal(menu.getAttribute("role"), "menu");
    assert.equal(menu.getAttribute("aria-label"), "Organizations");
    const names = choices(view.container).map((choice) => choice.querySelector("strong")?.textContent ?? "");
    assert.equal(names.length >= 2, true, `owen holds several organizations: ${names.join(", ")}`);

    await press("ArrowDown");
    assert.equal(browser.document.activeElement === choices(view.container)[0], true, "ArrowDown from outside the list lands on the first choice");
    await press("ArrowDown");
    assert.equal(browser.document.activeElement === choices(view.container)[1], true);
    await press("End");
    assert.equal(browser.document.activeElement === choices(view.container).at(-1), true);
    await press("ArrowDown");
    assert.equal(browser.document.activeElement === choices(view.container)[0], true, "the list wraps");
    await press("ArrowUp");
    assert.equal(browser.document.activeElement === choices(view.container).at(-1), true, "and wraps back");
    await press("Home");
    assert.equal(browser.document.activeElement === choices(view.container)[0], true);

    await press("Escape");
    assert.equal(view.container.querySelector('[data-suite-menu="org"]') === null, true);
    assert.equal(button.getAttribute("aria-expanded"), "false");
    assert.equal(browser.document.activeElement === button, true, "focus returns to the trigger");
  } finally {
    await view.unmount();
  }
});

test("a pending switch is announced and holds the choices; success closes the menu onto its trigger", async () => {
  at("/datasets/");
  const base = signedIn("ada@open-splunk.test");
  const client = Object.create(base) as IdentityClient;
  const control: { release?: () => void } = {};
  const gate = new Promise<void>((resolve) => { control.release = resolve; });
  client.setActiveOrganization = async (id) => {
    await gate;
    return base.setActiveOrganization(id);
  };
  const view = await mount(shell(client));
  try {
    await settle();
    await click(trigger(view.container));
    const platform = choiceNamed(view.container, "Acme Platform");
    await click(platform);
    const menu = view.container.querySelector('[data-suite-menu="org"]');
    assert.ok(menu, "the menu stays open while the switch is pending");
    assert.equal(menu.getAttribute("aria-busy"), "true");
    assert.equal(choices(view.container).every((choice) => choice.getAttribute("aria-disabled") === "true"), true, "every choice is held");
    assert.equal(choices(view.container).some((choice) => choice.hasAttribute("disabled")), false, "none is disabled, so focus stays put");
    assert.equal(browser.document.activeElement === platform, true);
    assert.equal(politeRegion(view.container).textContent, "Switching to Acme Platform…");

    control.release?.();
    await settle();
    assert.equal(view.container.querySelector('[data-suite-menu="org"]') === null, true);
    assert.equal(politeRegion(view.container).textContent, "Switched to Acme Platform.");
    assert.equal(browser.document.activeElement === trigger(view.container), true, "focus returns to the trigger, which names the new organization");
    assert.match(trigger(view.container).getAttribute("aria-label") ?? "", /^Organization: Acme Platform/u);
  } finally {
    await view.unmount();
  }
});

test("a refused switch is the menu's description and an alert, with focus left on the choice", async () => {
  at("/datasets/");
  const client = signedIn("owen@open-splunk.test");
  const view = await mount(shell(client));
  try {
    await settle();
    await click(trigger(view.container));
    client.failNext("setActiveOrganization", new IdentityError("rate_limited", "Slow down", { retryAfterMs: 5_000 }));
    const globex = choiceNamed(view.container, "Globex Security");
    await click(globex);
    await settle();
    const menu = view.container.querySelector('[data-suite-menu="org"]');
    assert.ok(menu);
    const error = textOfIds(menu.getAttribute("aria-describedby"));
    assert.equal(error, "Too many requests. Try switching again in 5 seconds.");
    const alert = view.container.querySelectorAll('[role="alert"]').find((element) => element.textContent === error);
    assert.ok(alert, "the error is announced as it appears");
    assert.equal(browser.document.activeElement === choiceNamed(view.container, "Globex Security"), true, "focus stays on the refused choice");
  } finally {
    await view.unmount();
  }
});

test("a failed sign-out describes the menu item that failed", async () => {
  at("/datasets/");
  const client = signedIn("alice@open-splunk.test");
  const view = await mount(shell(client));
  try {
    await settle();
    await click(view.container.querySelectorAll("button").find((button) => button.getAttribute("aria-label") === "Alice Abara menu"));
    client.failNext("logout", new IdentityError("network", "offline"));
    const signOut = view.container.querySelectorAll('[role="menuitem"]').find((item) => item.textContent === "Sign out");
    await click(signOut);
    await settle();
    const item = view.container.querySelectorAll('[role="menuitem"]').find((candidate) => candidate.textContent === "Sign out");
    assert.ok(item);
    assert.match(textOfIds(item.getAttribute("aria-describedby")), /still signed in/u);
    assert.equal(item.hasAttribute("disabled"), false);
  } finally {
    await view.unmount();
  }
});

test("the forbidden state is a labelled region whose heading names what was refused", async () => {
  at("/admin/");
  const view = await mount(shell(signedIn("mark@open-splunk.test"), "admin"));
  try {
    await settle();
    const state = view.container.querySelector('[data-state="forbidden"]');
    assert.ok(state, "a member opening administration sees the forbidden state");
    const label = textOfIds(state.getAttribute("aria-labelledby"));
    assert.equal(label, "You do not have access to Administration");
    const heading = view.container.querySelector("h1");
    assert.equal(heading?.textContent, label, "the region is named by the page's one h1");
    assert.equal(heading?.getAttribute("tabindex"), "-1", "the heading can take focus programmatically");
    assert.equal(view.container.querySelectorAll("h1").length, 1);
  } finally {
    await view.unmount();
  }
});
