import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { act, useEffect, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import { createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";

import { ADMIN_CONSOLE_ACCESS } from "@/lib/product-access";

import { AdminConsole } from "../admin/admin-console";
import { DatasetsConsole } from "../datasets/datasets-console";
import { HomeDashboard } from "../home-dashboard";
import { IdentityProvider } from "./identity-provider";
import { ShellOffer } from "./product-identity";
import { ProductShell } from "./product-shell";

type Section = Parameters<typeof ProductShell>[0]["activeSection"];

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

const navigations: Array<["assign" | "replace", string]> = [];
fakeWindow.location.assign = (href) => navigations.push(["assign", href]);
fakeWindow.location.replace = (href) => navigations.push(["replace", href]);

function at(pathname: string, search = "") {
  fakeWindow.location.pathname = pathname;
  fakeWindow.location.search = search;
  fakeWindow.location.hash = "";
  fakeWindow.location.href = `https://workspace.test${pathname}${search}`;
  navigations.length = 0;
}

let contentMounts = 0;

function PageContent() {
  useEffect(() => {
    contentMounts += 1;
  }, []);
  return <p data-page-content="">Page content</p>;
}

async function mount(node: ReactNode) {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

function shell(client: IdentityClient | null, section: Section, children: ReactNode = <PageContent />, dataMode: "demo" | "backend" = "demo") {
  const page = <ProductShell activeSection={section} appName="Search & Reporting" dataMode={dataMode}>{children}</ProductShell>;
  return client === null ? page : <IdentityProvider client={client}>{page}</IdentityProvider>;
}

async function settle(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 10);
    });
  });
}

function buttons(container: FakeElement, prefix: string): FakeElement[] {
  return container.querySelectorAll("button").filter((button) => button.textContent.startsWith(prefix));
}

function organizationTrigger(container: FakeElement): FakeElement | undefined {
  return container.querySelectorAll("button").find((button) => button.getAttribute("aria-label")?.startsWith("Organization:"));
}

async function click(element: FakeElement | undefined) {
  assert.ok(element, "the control is rendered");
  await act(async () => element.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 })));
}

function text(container: FakeElement): string {
  return container.textContent;
}

function links(container: FakeElement): string[] {
  return container.querySelectorAll("a").map((link) => link.textContent.trim());
}

function signedIn(email: string | null): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email });
}

test("a signed-in site admin sees its organizations, its account and every surface", async () => {
  at("/search/events/");
  const view = await mount(shell(signedIn("ada@open-splunk.test"), "search"));
  try {
    await settle();
    const trigger = organizationTrigger(view.container);
    assert.ok(trigger);
    assert.equal(trigger.getAttribute("aria-haspopup"), "menu");
    assert.equal(trigger.getAttribute("aria-expanded"), "false");
    assert.match(trigger.textContent, /Acme Operations/u);

    await click(trigger);
    const menu = view.container.querySelector('[data-suite-menu="org"]');
    assert.ok(menu, "the organization menu opens");
    assert.equal(trigger.getAttribute("aria-controls"), menu.getAttribute("id"));
    const choices = menu.querySelectorAll('[role="menuitemradio"]');
    assert.deepEqual(choices.map((choice) => choice.querySelector("strong")?.textContent), ["Acme Operations", "Acme Platform"]);
    assert.deepEqual(choices.map((choice) => choice.getAttribute("aria-checked")), ["true", "false"]);
    assert.match(choices[1]?.textContent ?? "", /Owner · inherited from Acme Operations/u);

    const userButton = view.container.querySelectorAll("button").find((button) => button.getAttribute("aria-label") === "Ada Lovelace menu");
    await click(userButton);
    const userMenu = view.container.querySelector('[data-suite-menu="user"]');
    assert.ok(userMenu);
    assert.match(userMenu.textContent, /Ada Lovelace/u);
    assert.match(userMenu.textContent, /ada@open-splunk\.test/u);
    assert.match(userMenu.textContent, /Owner · Acme Operations/u);
    assert.match(userMenu.textContent, /Site admin/u);
    assert.ok(userMenu.querySelectorAll('[role="menuitem"]').some((item) => item.textContent === "Administration"));

    assert.ok(links(view.container).includes("Settings"), "administration is offered");
    assert.ok(view.container.querySelector("[data-page-content]"), "the page renders");
  } finally {
    await view.unmount();
  }
});

test("switching organization announces the change and remounts the page in the new organization", async () => {
  at("/datasets/");
  const client = signedIn("ada@open-splunk.test");
  const view = await mount(shell(client, "datasets"));
  try {
    await settle();
    const before = contentMounts;
    await click(organizationTrigger(view.container));
    const platform = view.container.querySelectorAll('[role="menuitemradio"]').find((choice) => choice.textContent.includes("Acme Platform"));
    await click(platform);
    await settle();
    assert.match(organizationTrigger(view.container)?.textContent ?? "", /Acme Platform/u);
    assert.equal(view.container.querySelector('[data-suite-menu="org"]') === null, true, "a successful choice closes the menu");
    assert.ok(view.container.querySelectorAll("span").some((span) => span.getAttribute("aria-live") === "polite" && span.textContent === "Switched to Acme Platform."));
    assert.equal(contentMounts, before + 1, "nothing from the previous organization stays mounted");
    assert.equal((await client.getPrincipal())?.activeOrganization?.id, "3");
  } finally {
    await view.unmount();
  }
});

test("a refused switch reports its typed error in place and keeps the current organization", async () => {
  at("/datasets/");
  const client = signedIn("owen@open-splunk.test");
  const view = await mount(shell(client, "datasets"));
  try {
    await settle();
    await click(organizationTrigger(view.container));
    client.failNext("setActiveOrganization", new IdentityError("rate_limited", "Slow down", { retryAfterMs: 5_000 }));
    const globex = () => view.container.querySelectorAll('[role="menuitemradio"]').find((choice) => choice.textContent.includes("Globex Security"));
    await click(globex());
    await settle();
    const alert = view.container.querySelectorAll("p").find((paragraph) => paragraph.getAttribute("role") === "alert");
    assert.equal(alert?.textContent, "Too many requests. Try switching again in 5 seconds.");
    assert.ok(view.container.querySelector('[data-suite-menu="org"]'), "the menu stays open on the failure");
    assert.equal(globex()?.getAttribute("aria-checked"), "false");
    assert.match(organizationTrigger(view.container)?.textContent ?? "", /Acme Operations/u);

    client.failNext("setActiveOrganization", new IdentityError("network", "offline"));
    await click(globex());
    await settle();
    assert.match(text(view.container), /Could not reach the identity service to switch to Globex Security/u);

    await click(globex());
    await settle();
    assert.match(organizationTrigger(view.container)?.textContent ?? "", /Globex Security/u, "the next attempt succeeds");
  } finally {
    await view.unmount();
  }
});

test("a member is offered no administration and sees a forbidden state on the admin URL", async () => {
  at("/admin/", "?section=server");
  const view = await mount(shell(signedIn("mark@open-splunk.test"), "admin"));
  try {
    await settle();
    assert.ok(!links(view.container).includes("Settings"));
    assert.ok(!links(view.container).includes("Administration"));
    assert.ok(links(view.container).includes("Search"), "searching is still offered");
    assert.equal(view.container.querySelector("[data-page-content]") === null, true, "the admin page does not render");
    const heading = view.container.querySelector("h1");
    assert.equal(heading?.textContent, "You do not have access to Administration");
    assert.match(text(view.container), /Your role in Acme Operations is Member\. Ask an owner or admin of Acme Operations for the Moderator role or higher\./u);
    assert.match(text(view.container), /Any one of these permissions is needed:/u);
    assert.match(text(view.container), /members\.admin/u);
    assert.ok(buttons(view.container, "Switch organization").length === 1, "a member of two organizations may try the other");
    assert.ok(links(view.container).includes("Go to Search"));
  } finally {
    await view.unmount();
  }
});

test("a principal without an organization is told how to join one", async () => {
  at("/datasets/");
  const view = await mount(shell(signedIn("nora@open-splunk.test"), "datasets"));
  try {
    await settle();
    assert.equal(view.container.querySelector("h1")?.textContent, "You are not in an organization yet");
    assert.match(text(view.container), /Signed in as nora@open-splunk\.test/u);
    const productNav = view.container.querySelectorAll("nav").find((nav) => nav.getAttribute("aria-label") === "Search & Reporting navigation");
    assert.equal(productNav === undefined, false, "the product navigation renders");
    assert.equal(productNav?.querySelectorAll("a").length, 0, "no product surface is offered");
    const trigger = organizationTrigger(view.container);
    assert.match(trigger?.textContent ?? "", /No organization/u);
    await click(trigger);
    assert.match(view.container.querySelector('[data-suite-menu="org"]')?.textContent ?? "", /No organizations/u);
    assert.equal(buttons(view.container, "Sign out").length, 1);
  } finally {
    await view.unmount();
  }
});

test("a signed-out principal is sent to sign in with the page to return to; Help stays open", async () => {
  at("/reports/saved-searches/", "?view=mine");
  const client = signedIn(null);
  const view = await mount(shell(client, "reports"));
  try {
    await settle();
    assert.deepEqual(navigations, [["replace", "/signin/?returnTo=%2Freports%2Fsaved-searches%2F%3Fview%3Dmine"]]);
    assert.equal(view.container.querySelector("h1")?.textContent, "Sign in to continue");
    assert.equal(view.container.querySelector("[data-page-content]") === null, true);
    assert.equal(organizationTrigger(view.container) === undefined, true, "there is no organization to switch");
  } finally {
    await view.unmount();
  }

  at("/help/");
  const help = await mount(shell(client, "help"));
  try {
    await settle();
    assert.ok(help.container.querySelector("[data-page-content]"), "the bundled documentation needs no session");
    assert.deepEqual(navigations, []);
  } finally {
    await help.unmount();
  }
});

test("the page waits, without showing content, while the principal is read", async () => {
  at("/datasets/");
  const fake = signedIn("mark@open-splunk.test");
  const { promise: gate, resolve: release } = Promise.withResolvers<void>();
  const client = Object.create(fake) as IdentityClient;
  client.getPrincipal = async () => {
    await gate;
    return fake.getPrincipal();
  };
  const view = await mount(shell(client, "datasets"));
  try {
    assert.equal(view.container.querySelector("[data-page-content]") === null, true);
    assert.ok(view.container.querySelectorAll("div").some((element) => element.getAttribute("aria-busy") === "true"), "the main region is busy");
    assert.match(text(view.container), /Checking your access…/u);
    assert.match(text(view.container), /Loading organizations/u);
    assert.equal(organizationTrigger(view.container) === undefined, true, "the switcher is a placeholder until the principal is known");
    release();
    await settle();
    assert.ok(view.container.querySelector("[data-page-content]"));
  } finally {
    release();
    await view.unmount();
  }
});

test("the loading shell renders the same menus server-side as before identity", () => {
  const client = signedIn("ada@open-splunk.test");
  const markup = renderToStaticMarkup(shell(client, "search"));
  assert.equal(markup.match(/aria-haspopup="menu"/gu)?.length, 3, "the organization placeholder is not a menu");
  assert.doesNotMatch(markup, /Page content/u);
});

test("an unreadable session shows its typed error and recovers on retry", async () => {
  at("/datasets/");
  const client = signedIn("mark@open-splunk.test");
  client.failNext("getPrincipal", new IdentityError("network", "offline"));
  const view = await mount(shell(client, "datasets"));
  try {
    await settle();
    assert.equal(view.container.querySelector("h1")?.textContent, "Identity service unreachable");
    assert.ok(view.container.querySelectorAll("section").some((section) => section.getAttribute("role") === "alert"));
    await click(buttons(view.container, "Try again")[0]);
    await settle();
    assert.ok(view.container.querySelector("[data-page-content]"));
  } finally {
    await view.unmount();
  }
});

test("signing out leaves for the sign-in page, and a failure is reported in the menu", async () => {
  at("/datasets/");
  const client = signedIn("alice@open-splunk.test");
  const view = await mount(shell(client, "datasets"));
  try {
    await settle();
    const openUserMenu = () => click(view.container.querySelectorAll("button").find((button) => button.getAttribute("aria-label") === "Alice Abara menu"));
    await openUserMenu();
    client.failNext("logout", new IdentityError("network", "offline"));
    await click(buttons(view.container, "Sign out")[0]);
    await settle();
    assert.match(
      view.container.querySelectorAll("p").find((paragraph) => paragraph.getAttribute("role") === "alert")?.textContent ?? "",
      /still signed in/u,
    );
    assert.deepEqual(navigations, []);
    assert.ok(await client.getPrincipal(), "the session survives a failed sign-out");

    await click(buttons(view.container, "Sign out")[0]);
    await settle();
    assert.deepEqual(navigations, [["assign", "/signin/"]], "an explicit sign-out carries no return path");
    assert.equal(await client.getPrincipal(), null);
  } finally {
    await view.unmount();
  }
});

test("a page without identity keeps the legacy session and every surface", async () => {
  at("/datasets/");
  const view = await mount(shell(null, "datasets", <PageContent />, "backend"));
  try {
    await settle();
    assert.equal(organizationTrigger(view.container) === undefined, true);
    assert.ok(view.container.querySelectorAll("button").some((button) => button.getAttribute("aria-label") === "Local session menu"));
    assert.ok(links(view.container).includes("Settings"));
    assert.ok(view.container.querySelector("[data-page-content]"));
  } finally {
    await view.unmount();
  }
});

test("the demo shell mounts its own provider and starts later pages from the principal it already read", async () => {
  at("/datasets/");
  // A synchronous server render shows the state before any read resolves; an
  // `act` mount would already have flushed the demo client's read.
  assert.doesNotMatch(renderToStaticMarkup(shell(null, "datasets")), /data-page-content/u, "the first page reads the principal");
  const first = await mount(shell(null, "datasets"));
  try {
    await settle();
    assert.ok(first.container.querySelector("[data-page-content]"));
    assert.match(organizationTrigger(first.container)?.textContent ?? "", /Acme Operations/u);
  } finally {
    await first.unmount();
  }
  assert.match(renderToStaticMarkup(shell(null, "analytics")), /data-page-content/u, "a later page renders at once");
  const second = await mount(shell(null, "analytics"));
  try {
    await settle();
    assert.ok(second.container.querySelector("[data-page-content]"));
  } finally {
    await second.unmount();
  }
});

test("the administration console offers only the sections a moderator holds and refuses the rest by URL", async () => {
  at("/admin/", "?section=server");
  const view = await mount(shell(signedIn("mona@open-splunk.test"), "admin", <AdminConsole dataMode="demo" apiBaseUrl="" />));
  try {
    await settle();
    const sidebar = view.container.querySelectorAll("aside").find((aside) => aside.getAttribute("aria-label") === "Administration navigation");
    assert.deepEqual(sidebar?.querySelectorAll("button").map((item) => item.querySelector("strong")?.textContent), ["System overview"]);
    const heading = view.container.querySelectorAll("h2").find((element) => element.getAttribute("id") === "suite-access-forbidden-title");
    assert.equal(heading?.textContent, "You do not have access to Server settings");
    assert.match(text(view.container), /Only a site administrator can open it\./u);
    assert.equal(buttons(view.container, "Apply preview settings").length, 0, "the section's own actions are withheld");
  } finally {
    await view.unmount();
  }

  at("/admin/", "?section=indexes");
  const admin = await mount(shell(signedIn("alice@open-splunk.test"), "admin", <AdminConsole dataMode="demo" apiBaseUrl="" />));
  try {
    await settle();
    const sidebar = admin.container.querySelectorAll("aside").find((aside) => aside.getAttribute("aria-label") === "Administration navigation");
    assert.deepEqual(
      sidebar?.querySelectorAll("button").map((item) => item.querySelector("strong")?.textContent),
      ["System overview", "Indexes", "Data inputs", "Users & access"],
      "an organization admin manages its data and members, not the server",
    );
    assert.ok(buttons(admin.container, " Simulate index").length + buttons(admin.container, "Simulate index").length > 0);
  } finally {
    await admin.unmount();
  }
});

async function staticPage(email: string, section: Section, page: ReactNode): Promise<string> {
  const client = signedIn(email);
  const principal = await client.getPrincipal();
  return renderToStaticMarkup(
    <IdentityProvider client={client} initialPrincipal={principal}>
      <ProductShell activeSection={section} appName="Search & Reporting" dataMode="demo">{page}</ProductShell>
    </IdentityProvider>,
  );
}

test("a link into a surface the principal cannot open is left out, or shown as its fallback", async () => {
  const offered = <ShellOffer requirement={ADMIN_CONSOLE_ACCESS} fallback={<span>plain</span>}><b>linked</b></ShellOffer>;
  assert.equal(renderToStaticMarkup(offered), "<b>linked</b>", "a page without identity offers everything");
  const member = signedIn("mark@open-splunk.test");
  const markup = renderToStaticMarkup(<IdentityProvider client={member} initialPrincipal={await member.getPrincipal()}>{offered}</IdentityProvider>);
  assert.equal(markup, "<span>plain</span>");
});

test("the home and datasets pages offer administration links only to a principal who may administer", async () => {
  const memberHome = await staticPage("mark@open-splunk.test", "home", <HomeDashboard dataMode="demo" />);
  assert.match(memberHome, /New search/u, "the page renders for a member");
  assert.doesNotMatch(memberHome, /Administration preview|Open settings|Open administration/u);
  const adminHome = await staticPage("alice@open-splunk.test", "home", <HomeDashboard dataMode="demo" />);
  assert.match(adminHome, /Administration preview/u);
  assert.match(adminHome, /Open administration/u);

  const moderatorDatasets = await staticPage("mona@open-splunk.test", "datasets", <DatasetsConsole dataMode="demo" apiBaseUrl="" />);
  assert.match(moderatorDatasets, /Search data/u, "the page renders for a moderator");
  assert.doesNotMatch(moderatorDatasets, /Manage indexes|Manage index /u, "managing indexes takes indexes.admin");
  const adminDatasets = await staticPage("alice@open-splunk.test", "datasets", <DatasetsConsole dataMode="demo" apiBaseUrl="" />);
  assert.match(adminDatasets, /Manage indexes/u);
  assert.match(adminDatasets, /Manage index /u);
});
