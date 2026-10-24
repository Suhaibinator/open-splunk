import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { Children, act, isValidElement, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import { FAKE_SITE_ADMIN_EMAIL, createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";

import { IdentityProvider } from "../_components/identity-provider";
import * as modalModule from "../_components/modal";
import * as selectModule from "../_components/select";
import { SiteOrganizationsPanel } from "./site-organizations-panel";
import { SiteUsersPanel } from "./site-users-panel";

/*
 * Both site-admin panels mounted for real against the deterministic fake:
 * effects run, handlers fire, and every state the panels design is reached
 * through ordinary input or an injected failure. The shared Modal and Select
 * are replaced by plain stand-ins (their focus, inert and popover behaviour
 * have their own contracts); everything else is the shipped code.
 */

const realModal = modalModule.Modal;
const realSelect = selectModule.Select;

function StubModal({ title, subtitle, children, footer }: { title: string; subtitle?: string; children: ReactNode; footer?: ReactNode }) {
  return (
    <div data-modal={title}>
      <h2>{title}</h2>
      {subtitle === undefined ? null : <p>{subtitle}</p>}
      {children}
      {footer}
    </div>
  );
}

function StubSelect({ id, value, disabled, onValueChange, children }: {
  id?: string;
  value?: string;
  disabled?: boolean;
  onValueChange?: (value: string) => void;
  children?: ReactNode;
}) {
  return (
    <div id={id} data-select-value={value}>
      {Children.toArray(children).filter(isValidElement).map((child) => {
        const option = child as ReactElement<{ value: string; children: ReactNode }>;
        return (
          <button key={option.props.value} type="button" disabled={disabled} data-option={option.props.value} onClick={() => onValueChange?.(option.props.value)}>
            {option.props.children}
          </button>
        );
      })}
    </div>
  );
}

const reactErrors: unknown[][] = [];
const consoleError = console.error;

test.before(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = StubModal;
  (selectModule as unknown as { Select: unknown }).Select = StubSelect;
  console.error = (...values: unknown[]) => {
    reactErrors.push(values);
  };
});

test.after(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = realModal;
  (selectModule as unknown as { Select: unknown }).Select = realSelect;
  console.error = consoleError;
});

interface Mounted {
  readonly container: FakeElement;
  text(): string;
  button(label: string): FakeElement;
  maybeButton(label: string): FakeElement | undefined;
  click(element: FakeElement): Promise<void>;
  type(element: FakeElement, value: string): Promise<void>;
  submit(form: FakeElement): Promise<void>;
  key(element: FakeElement, key: string): Promise<void>;
  rows(): FakeElement[];
  unmount(): Promise<void>;
}

const mounts = new Set<Mounted>();

test.afterEach(async () => {
  await Promise.all(Array.from(mounts, (mounted) => mounted.unmount()));
  assert.deepEqual(reactErrors, []);
});

test.beforeEach(() => {
  reactErrors.length = 0;
});

/** Lets the fake's promises, and the renders and timers they cause, settle. */
async function settle(milliseconds = 5): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, milliseconds);
    });
  });
}

function textOf(element: FakeElement): string {
  return element.textContent.replaceAll(/\s+/gu, " ").trim();
}

async function mount(node: ReactNode): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  const mounted: Mounted = {
    button(label) {
      const found = mounted.maybeButton(label);
      assert.ok(found, `no button labelled ${label} in: ${mounted.text()}`);
      return found;
    },
    async click(element) {
      await act(async () => {
        element.dispatchEvent(fakeEvent("click"));
      });
      await settle();
    },
    container,
    async key(element, key) {
      await act(async () => {
        element.dispatchEvent(Object.assign(fakeEvent("keydown"), { key }));
      });
    },
    maybeButton(label) {
      return container.querySelectorAll("button").find((candidate) => textOf(candidate) === label || candidate.getAttribute("aria-label") === label);
    },
    rows() {
      return container.querySelectorAll("tr").filter((row) => row.hasAttribute("data-user-id") || row.hasAttribute("data-organization-id"));
    },
    async submit(form) {
      await act(async () => {
        form.dispatchEvent(fakeEvent("submit"));
      });
      await settle();
    },
    text() {
      return textOf(container);
    },
    async type(element, value) {
      await act(async () => {
        element.value = value;
        element.dispatchEvent(fakeEvent("input"));
      });
    },
    async unmount() {
      mounts.delete(mounted);
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
  mounts.add(mounted);
  await act(async () => root.render(node));
  await settle();
  return mounted;
}

function fakeAs(email: string | null, options: { latencyMs?: number } = {}): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email, now: () => Date.parse("2026-10-24T12:00:00.000Z"), ...options });
}

function users(client: IdentityClient, pageSize?: number): ReactNode {
  return <IdentityProvider client={client}><SiteUsersPanel pageSize={pageSize} /></IdentityProvider>;
}

function organizations(client: IdentityClient, pageSize?: number): ReactNode {
  return <IdentityProvider client={client}><SiteOrganizationsPanel pageSize={pageSize} /></IdentityProvider>;
}

function rowIds(mounted: Mounted, attribute = "data-user-id"): string[] {
  return mounted.rows().map((row) => row.getAttribute(attribute) ?? "");
}

function searchForm(mounted: Mounted): FakeElement {
  const form = mounted.container.querySelector('form[role="search"]');
  assert.ok(form, "no search form");
  return form;
}

function searchInput(mounted: Mounted): FakeElement {
  const input = mounted.container.querySelector('input[type="search"]');
  assert.ok(input, "no search input");
  return input;
}

async function search(mounted: Mounted, value: string): Promise<void> {
  await mounted.type(searchInput(mounted), value);
  await mounted.submit(searchForm(mounted));
}

function menuItems(mounted: Mounted): FakeElement[] {
  return mounted.container.querySelectorAll('[role="menuitem"]');
}

function menuItem(mounted: Mounted, label: string): FakeElement {
  const item = menuItems(mounted).find((candidate) => candidate.querySelector("strong")?.textContent === label);
  assert.ok(item, `no menu item ${label}`);
  return item;
}

function dialog(mounted: Mounted): FakeElement {
  const found = mounted.container.querySelector("[data-modal]");
  assert.ok(found, `no dialog open in: ${mounted.text()}`);
  return found;
}

function dialogForm(mounted: Mounted): FakeElement {
  const form = dialog(mounted).querySelector("form");
  assert.ok(form, "the dialog has no form");
  return form;
}

function dialogSubmit(mounted: Mounted): FakeElement {
  const button = dialog(mounted).querySelectorAll("button").find((candidate) => candidate.getAttribute("type") === "submit");
  assert.ok(button, "the dialog has no submit button");
  return button;
}

// --------------------------------------------------------------------------
// Users: access states
// --------------------------------------------------------------------------

test("identity loading, then the first page loading, each have their own state", async () => {
  const base = fakeAs(FAKE_SITE_ADMIN_EMAIL, { latencyMs: 40 });
  const { promise: principalGate, resolve: releasePrincipal } = Promise.withResolvers<void>();
  const client = Object.create(base) as IdentityClient;
  client.getPrincipal = async () => {
    await principalGate;
    return base.getPrincipal();
  };
  const mounted = await mount(users(client));
  assert.match(mounted.text(), /Checking your access/u);
  assert.equal(mounted.button("Create user").disabled, true);

  releasePrincipal();
  await settle(45);
  const table = mounted.container.querySelector("table");
  assert.ok(table);
  assert.equal(table.getAttribute("aria-busy"), "true");
  assert.match(mounted.text(), /Loading users…/u);
  assert.equal(mounted.rows().length, 0);
  assert.equal(mounted.button("Create user").disabled, false);

  await settle(45);
  assert.equal(mounted.rows().length, 9);
});

test("a signed-in user without users.admin sees why the panel and its action are unavailable", async () => {
  const mounted = await mount(users(fakeAs("alice@open-splunk.test")));
  assert.match(mounted.text(), /Site administrator access required/u);
  assert.match(mounted.text(), /alice@open-splunk\.test/u);
  const create = mounted.button("Create user");
  assert.equal(create.disabled, true);
  const reasonId = create.getAttribute("aria-describedby");
  assert.ok(reasonId);
  assert.match(mounted.container.querySelector(`#${reasonId}`)?.textContent ?? "", /Only a signed-in site administrator/u);
  assert.equal(mounted.rows().length, 0);
});

test("signed out, the panel offers sign-in; with no client it reports identity unavailable", async () => {
  const signedOut = await mount(users(fakeAs(null)));
  assert.match(signedOut.text(), /Sign in to manage users/u);
  assert.match(signedOut.container.querySelector("a")?.getAttribute("href") ?? "", /^\/signin\/?$/u);
  await signedOut.unmount();

  const backend = await mount(<IdentityProvider dataMode="backend"><SiteUsersPanel /></IdentityProvider>);
  assert.match(backend.text(), /Identity is not configured/u);
  assert.equal(backend.maybeButton("Try again"), undefined);
});

// --------------------------------------------------------------------------
// Users: the directory
// --------------------------------------------------------------------------

test("a site admin pages through the directory, with the range announced", async () => {
  const mounted = await mount(users(fakeAs(FAKE_SITE_ADMIN_EMAIL), 4));
  assert.deepEqual(rowIds(mounted), ["1", "2", "3", "4"]);
  const range = mounted.container.querySelector('strong[role="status"]');
  assert.equal(range?.textContent, "Showing 1–4 of 9 users");
  assert.match(mounted.text(), /Page 1 of 3/u);
  assert.equal(mounted.button("Previous").disabled, true);
  // The signed-in administrator's own row says so.
  assert.match(textOf(mounted.rows()[0]), /Ada Lovelace\s?You/u);
  assert.match(textOf(mounted.rows()[0]), /Site admin/u);

  await mounted.click(mounted.button("Next"));
  assert.deepEqual(rowIds(mounted), ["5", "6", "7", "8"]);
  assert.match(textOf(mounted.rows()[3]), /Banned/u);
  await mounted.click(mounted.button("Next"));
  assert.deepEqual(rowIds(mounted), ["9"]);
  assert.match(textOf(mounted.rows()[0]), /Invitation pending/u);
  assert.match(textOf(mounted.rows()[0]), /No sign-in method yet/u);
  assert.equal(mounted.button("Next").disabled, true);
  await mounted.click(mounted.button("Previous"));
  assert.deepEqual(rowIds(mounted), ["5", "6", "7", "8"]);
});

test("search and filters narrow the directory, and an empty match offers to clear", async () => {
  const mounted = await mount(users(fakeAs(FAKE_SITE_ADMIN_EMAIL)));
  await search(mounted, "mona");
  assert.deepEqual(rowIds(mounted), ["4"]);
  await search(mounted, "7");
  assert.deepEqual(rowIds(mounted), ["7"]);

  await search(mounted, "nobody-here");
  assert.match(mounted.text(), /No users match/u);
  assert.match(mounted.text(), /nobody-here/u);
  await mounted.click(mounted.button("Clear search"));
  assert.equal(rowIds(mounted).length, 9);
  assert.equal(searchInput(mounted).value, "");
  await settle();
  assert.equal(browser.document.activeElement, searchInput(mounted));

  const filter = (value: string) => {
    const option = searchForm(mounted).querySelector(`[data-option="${value}"]`);
    assert.ok(option, `no filter option ${value}`);
    return option;
  };
  await mounted.click(filter("banned"));
  assert.deepEqual(rowIds(mounted), ["8"]);
  await mounted.click(filter("site_admins"));
  assert.deepEqual(rowIds(mounted), ["1"]);
  // A search and a filter compose; clearing resets both.
  await search(mounted, "ada");
  assert.deepEqual(rowIds(mounted), ["1"]);
  await search(mounted, "mona");
  assert.match(mounted.text(), /No users match/u);
  await mounted.click(mounted.button("Clear search"));
  assert.equal(rowIds(mounted).length, 9);
  assert.equal(searchForm(mounted).querySelector("[data-select-value]")?.getAttribute("data-select-value"), "all");
});

test("each list failure kind renders its own next step", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);

  client.failNext("adminListUsers");
  const network = await mount(users(client));
  assert.match(network.text(), /Could not load users/u);
  assert.equal(network.container.querySelector('[role="alert"]') === null, false);
  await network.click(network.button("Retry"));
  assert.equal(network.rows().length, 9, network.text());
  await network.unmount();

  client.failNext("adminListUsers", new IdentityError("rate_limited", "Too many requests.", { retryAfterMs: 5_000 }));
  const limited = await mount(users(client));
  assert.match(limited.text(), /Too many requests/u);
  const retry = limited.container.querySelectorAll("button").find((button) => textOf(button).startsWith("Retry in"));
  assert.ok(retry, limited.text());
  assert.equal(retry.disabled, true);
  assert.match(textOf(retry), /Retry in 5 s/u);
  await limited.unmount();

  client.failNext("adminListUsers", new IdentityError("invalid", "Search terms are too long."));
  const invalid = await mount(users(client));
  assert.match(invalid.text(), /Could not search users/u);
  assert.match(invalid.text(), /Search terms are too long/u);
  await invalid.click(invalid.button("Clear search"));
  assert.equal(invalid.rows().length, 9);
  await invalid.unmount();

  client.failNext("adminListUsers", new IdentityError("forbidden", "No."));
  const forbidden = await mount(users(client));
  assert.match(forbidden.text(), /Site administrator access required/u);
  await forbidden.unmount();

  client.failNext("adminListUsers", new IdentityError("unauthenticated", "Sign in."));
  const unauthenticated = await mount(users(client));
  assert.match(unauthenticated.text(), /Sign in again/u);
  assert.match(unauthenticated.container.querySelector("a")?.getAttribute("href") ?? "", /^\/signin\/?$/u);
});

// --------------------------------------------------------------------------
// Users: row actions
// --------------------------------------------------------------------------

test("the row menu takes focus, moves with the arrows, and bans through a confirmation", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const mounted = await mount(users(client));
  const trigger = mounted.button("Actions for Mark Mendez");
  assert.equal(trigger.getAttribute("aria-expanded"), "false");
  await mounted.click(trigger);
  assert.equal(trigger.getAttribute("aria-expanded"), "true");
  assert.deepEqual(menuItems(mounted).map((item) => item.querySelector("strong")?.textContent), [
    "Make site admin",
    "Ban user",
    "Revoke sessions",
    "Delete user",
  ]);
  assert.equal(browser.document.activeElement, menuItem(mounted, "Make site admin"));
  const menu = mounted.container.querySelector('[role="menu"]');
  assert.ok(menu);
  await mounted.key(menu, "ArrowDown");
  assert.equal(browser.document.activeElement, menuItem(mounted, "Ban user"));
  await mounted.key(menu, "End");
  assert.equal(browser.document.activeElement, menuItem(mounted, "Delete user"));
  await mounted.key(menu, "ArrowDown");
  assert.equal(browser.document.activeElement, menuItem(mounted, "Make site admin"));
  await mounted.key(menu, "Escape");
  await settle();
  assert.equal(mounted.container.querySelector('[role="menu"]'), null);
  assert.equal(browser.document.activeElement, trigger);

  await mounted.click(trigger);
  await mounted.click(menuItem(mounted, "Ban user"));
  assert.equal(dialog(mounted).getAttribute("data-modal"), "Ban Mark Mendez?");
  assert.match(textOf(dialog(mounted)), /signed out of 1 active session/u);
  const reason = dialog(mounted).querySelector("textarea");
  assert.ok(reason);
  await mounted.type(reason, "Shared credentials");
  await mounted.submit(dialogForm(mounted));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.match(mounted.container.querySelector("output")?.textContent ?? "", /Mark Mendez is banned\./u);
  const row = mounted.rows().find((candidate) => candidate.getAttribute("data-user-id") === "5");
  assert.ok(row);
  assert.match(textOf(row), /Banned/u);

  await mounted.click(mounted.button("Dismiss notification"));
  assert.equal(mounted.container.querySelector("output"), null);
  await mounted.click(mounted.button("Actions for Mark Mendez"));
  assert.ok(menuItem(mounted, "Unban user"));
  assert.equal(menuItem(mounted, "Revoke sessions").getAttribute("aria-disabled"), "true");
});

test("a failed change stays in the dialog with an alert, and a vanished user reloads the list on close", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const mounted = await mount(users(client));
  client.failNext("adminSetSiteAdmin");
  await mounted.click(mounted.button("Actions for Mona Moreau"));
  await mounted.click(menuItem(mounted, "Make site admin"));
  await mounted.submit(dialogForm(mounted));
  const alert = dialog(mounted).querySelector('[role="alert"]');
  assert.ok(alert);
  assert.match(alert.textContent, /could not be reached\. Nothing was changed/u);
  assert.equal(dialogSubmit(mounted).hasAttribute("aria-disabled"), false);
  await mounted.submit(dialogForm(mounted));
  assert.match(mounted.container.querySelector("output")?.textContent ?? "", /Mona Moreau is now a site admin\./u);

  // Another administrator deleted Owen while this page was open.
  await mounted.click(mounted.button("Actions for Owen Ortiz"));
  await mounted.click(menuItem(mounted, "Ban user"));
  await client.adminDeleteUser("2");
  await mounted.submit(dialogForm(mounted));
  assert.match(textOf(dialog(mounted)), /no longer exists/u);
  await mounted.click(mounted.button("Cancel"));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.equal(rowIds(mounted).includes("2"), false);
});

test("delete waits for the typed email, then removes the row and returns focus to the search", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const mounted = await mount(users(client));
  await mounted.click(mounted.button("Actions for Tess Takahashi"));
  await mounted.click(menuItem(mounted, "Delete user"));
  assert.equal(dialog(mounted).getAttribute("data-modal"), "Delete Tess Takahashi?");
  assert.equal(dialogSubmit(mounted).disabled, true);
  const typed = dialog(mounted).querySelector('input[data-autofocus="typed"]');
  assert.ok(typed);
  await mounted.type(typed, "tess@");
  assert.equal(dialogSubmit(mounted).disabled, true);
  await mounted.submit(dialogForm(mounted));
  assert.ok(mounted.container.querySelector("[data-modal]"), "an unconfirmed delete was sent");
  await mounted.type(typed, "TESS@open-splunk.test");
  assert.equal(dialogSubmit(mounted).disabled, false);
  await mounted.submit(dialogForm(mounted));
  await settle();
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.match(mounted.container.querySelector("output")?.textContent ?? "", /Tess Takahashi was deleted\./u);
  assert.equal(rowIds(mounted).includes("6"), false);
  assert.equal(rowIds(mounted).length, 8);
  assert.equal(browser.document.activeElement, searchInput(mounted));
});

test("the self row explains its disabled actions, and signing out its sessions signs the panel out", async () => {
  const mounted = await mount(users(fakeAs(FAKE_SITE_ADMIN_EMAIL)));
  await mounted.click(mounted.button("Actions for Ada Lovelace"));
  const removeAdmin = menuItem(mounted, "Remove site admin");
  assert.equal(removeAdmin.getAttribute("aria-disabled"), "true");
  const reasonId = removeAdmin.getAttribute("aria-describedby");
  assert.ok(reasonId);
  assert.match(mounted.container.querySelector(`#${reasonId}`)?.textContent ?? "", /cannot remove site administrator access from your own account/u);
  assert.equal(menuItem(mounted, "Ban user").getAttribute("aria-disabled"), "true");
  assert.equal(menuItem(mounted, "Delete user").getAttribute("aria-disabled"), "true");
  assert.equal(menuItem(mounted, "Sign out my sessions").hasAttribute("aria-disabled"), false);
  // A disabled item stays focusable, so the arrows reach it and its reason is read.
  assert.equal(browser.document.activeElement, removeAdmin);
  const menu = mounted.container.querySelector('[role="menu"]');
  assert.ok(menu);
  await mounted.key(menu, "ArrowDown");
  assert.equal(browser.document.activeElement, menuItem(mounted, "Ban user"));
  // Activating a disabled item neither closes the menu nor opens a dialog.
  await mounted.click(menuItem(mounted, "Ban user"));
  assert.ok(mounted.container.querySelector('[role="menu"]'));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);

  await mounted.click(menuItem(mounted, "Sign out my sessions"));
  assert.match(textOf(dialog(mounted)), /including this one/u);
  await mounted.submit(dialogForm(mounted));
  await settle();
  assert.match(mounted.text(), /Sign in to manage users/u);
});

// --------------------------------------------------------------------------
// Users: create
// --------------------------------------------------------------------------

test("create user validates each field, maps a conflict onto its field, and sends the invitation", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const mounted = await mount(users(client));
  await mounted.click(mounted.button("Create user"));
  assert.equal(dialog(mounted).getAttribute("data-modal"), "Create user");

  await mounted.submit(dialogForm(mounted));
  const email = dialog(mounted).querySelector("#site-users-create-email");
  const username = dialog(mounted).querySelector("#site-users-create-username");
  assert.ok(email && username);
  assert.equal(email.getAttribute("aria-invalid"), "true");
  assert.equal(username.getAttribute("aria-invalid"), "true");
  assert.match(dialog(mounted).querySelector("#site-users-create-email-note")?.textContent ?? "", /Enter an email address/u);
  assert.match(dialog(mounted).querySelector('[role="alert"]')?.textContent ?? "", /Fix the highlighted fields/u);

  await mounted.type(email, "ada@open-splunk.test");
  await mounted.type(username, "newbie");
  assert.equal(email.getAttribute("aria-invalid"), null);
  await mounted.submit(dialogForm(mounted));
  assert.equal(email.getAttribute("aria-invalid"), "true");
  assert.match(dialog(mounted).querySelector("#site-users-create-email-note")?.textContent ?? "", /Email already exists/u);

  // The password field appears only with its access mode.
  assert.equal(dialog(mounted).querySelector("#site-users-create-password"), null);
  const passwordMode = dialog(mounted).querySelectorAll('input[type="radio"]').find((radio) => radio.value === "password");
  assert.ok(passwordMode);
  await act(async () => {
    passwordMode.checked = true;
    passwordMode.dispatchEvent(fakeEvent("click"));
  });
  assert.ok(dialog(mounted).querySelector("#site-users-create-password"));
  const invitationMode = dialog(mounted).querySelectorAll('input[type="radio"]').find((radio) => radio.value === "invitation");
  assert.ok(invitationMode);
  await act(async () => {
    invitationMode.checked = true;
    invitationMode.dispatchEvent(fakeEvent("click"));
  });

  await mounted.type(email, "new.person@open-splunk.test");
  await mounted.submit(dialogForm(mounted));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.match(
    mounted.container.querySelector("output")?.textContent ?? "",
    /Created newbie and sent an invitation to new\.person@open-splunk\.test\./u,
  );
  assert.ok(client.lastMessage("account_invitation", "new.person@open-splunk.test"));
  assert.equal(rowIds(mounted).length, 10);
  assert.match(textOf(mounted.rows().at(-1) as FakeElement), /Invitation pending/u);
});

// --------------------------------------------------------------------------
// Organizations
// --------------------------------------------------------------------------

test("organizations list their type and lineage, and gate on orgs.admin", async () => {
  const mounted = await mount(organizations(fakeAs(FAKE_SITE_ADMIN_EMAIL)));
  assert.deepEqual(rowIds(mounted, "data-organization-id"), ["1", "3", "2"]);
  const [acme, platform] = mounted.rows();
  assert.match(textOf(acme), /Acme Operations/u);
  assert.match(textOf(acme), /Top level/u);
  assert.match(textOf(acme), /Root organization/u);
  assert.match(textOf(platform), /Child/u);
  const lineage = platform.querySelector("ol");
  assert.ok(lineage);
  assert.deepEqual(lineage.querySelectorAll("li").map((item) => item.textContent), ["Acme Operations", "Acme Platform"]);
  assert.equal(lineage.querySelectorAll('li[aria-current="true"]').length, 1);
  await mounted.unmount();

  const member = await mount(organizations(fakeAs("owen@open-splunk.test")));
  assert.match(member.text(), /Site administrator access required/u);
  assert.equal(member.button("Create organization").disabled, true);
});

test("organization search, its empty state, and a failed load", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const mounted = await mount(organizations(client));
  await search(mounted, "globex");
  assert.deepEqual(rowIds(mounted, "data-organization-id"), ["2"]);
  await search(mounted, "initech");
  assert.match(mounted.text(), /No organizations match/u);
  await mounted.click(mounted.button("Clear search"));
  assert.equal(mounted.rows().length, 3);
  await mounted.unmount();

  client.failNext("adminListOrganizations");
  const failed = await mount(organizations(client));
  assert.match(failed.text(), /Could not load organizations/u);
  await failed.click(failed.button("Retry"));
  assert.equal(failed.rows().length, 3);
});

test("create organization suggests the slug, picks a parent, and maps a taken slug onto its field", async () => {
  const mounted = await mount(organizations(fakeAs(FAKE_SITE_ADMIN_EMAIL)));
  await mounted.click(mounted.button("Create organization"));
  const name = dialog(mounted).querySelector("#site-orgs-create-name");
  const slug = dialog(mounted).querySelector("#site-orgs-create-slug");
  assert.ok(name && slug);

  await mounted.type(name, "Acme");
  assert.equal(slug.value, "acme");
  await mounted.submit(dialogForm(mounted));
  assert.equal(slug.getAttribute("aria-invalid"), "true");
  assert.match(dialog(mounted).querySelector("#site-orgs-create-slug-note")?.textContent ?? "", /already taken/u);

  await mounted.type(name, "Acme Edge (EU)");
  assert.equal(slug.value, "acme-edge-eu");
  await mounted.type(slug, "edge-eu");
  await mounted.type(name, "Acme Edge Europe");
  assert.equal(slug.value, "edge-eu", "a hand-edited slug was rewritten by the name");

  const parent = dialog(mounted).querySelector('[data-option="3"]');
  assert.ok(parent, textOf(dialog(mounted)));
  await mounted.click(parent);
  assert.equal(dialog(mounted).querySelector("#site-orgs-create-parent")?.getAttribute("data-select-value"), "3");
  await mounted.submit(dialogForm(mounted));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.match(mounted.container.querySelector("output")?.textContent ?? "", /Created Acme Edge Europe\./u);
  const created = mounted.rows().find((row) => textOf(row).includes("Acme Edge Europe"));
  assert.ok(created);
  assert.deepEqual(created.querySelector("ol")?.querySelectorAll("li").map((item) => item.textContent), [
    "Acme Operations",
    "Acme Platform",
    "Acme Edge Europe",
  ]);
});

test("a personal workspace is flagged and is never offered as a parent", async () => {
  const base = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  const client = Object.create(base) as IdentityClient;
  client.adminListOrganizations = async (query, page) => {
    const listed = await base.adminListOrganizations(query, page);
    const personal = {
      createdAt: "2026-02-01T09:00:00.000Z",
      id: "40",
      lineage: [],
      name: "Nora's workspace",
      personalOwnerUserId: "7",
      slug: "nora-personal",
      updatedAt: "2026-02-01T09:00:00.000Z",
    };
    return { ...listed, items: [...listed.items, personal] };
  };
  const mounted = await mount(organizations(client));
  const row = mounted.rows().find((candidate) => candidate.getAttribute("data-organization-id") === "40");
  assert.ok(row, mounted.text());
  assert.match(textOf(row), /Personal/u);
  assert.match(textOf(row), /Root organization/u);

  await mounted.click(mounted.button("Create organization"));
  assert.ok(dialog(mounted).querySelector('[data-option="1"]'));
  assert.equal(dialog(mounted).querySelector('[data-option="40"]'), null);
});

test("a failed create keeps the dialog open with an alert, and a disabled parent picker says why", async () => {
  const client = fakeAs(FAKE_SITE_ADMIN_EMAIL);
  client.failNext("adminListOrganizations");
  client.failNext("adminListOrganizations");
  const mounted = await mount(organizations(client));
  // Both the page and the picker failed; Retry recovers the page only.
  await mounted.click(mounted.button("Retry"));
  assert.equal(mounted.rows().length, 3);
  await mounted.click(mounted.button("Create organization"));
  assert.match(textOf(dialog(mounted)), /only a top-level organization can be created/u);
  assert.equal(dialog(mounted).querySelector('[data-option="__top_level__"]')?.disabled, true);
  assert.equal(dialog(mounted).querySelector('[data-option="1"]'), null);

  const name = dialog(mounted).querySelector("#site-orgs-create-name");
  assert.ok(name);
  await mounted.type(name, "Initech");
  client.failNext("adminCreateOrganization", new IdentityError("rate_limited", "Too many requests.", { retryAfterMs: 3_000 }));
  await mounted.submit(dialogForm(mounted));
  assert.match(dialog(mounted).querySelector('[role="alert"]')?.textContent ?? "", /Try again in 3 s/u);
  await mounted.submit(dialogForm(mounted));
  assert.equal(mounted.container.querySelector("[data-modal]"), null);
  assert.match(mounted.container.querySelector("output")?.textContent ?? "", /Created Initech\./u);
});
