import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { Children, act, isValidElement, useEffect, useState, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type IdentityClient, type OrgRole } from "@/lib/api/identity";
import { createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";

import { IdentityProvider } from "../_components/identity-provider";
import * as modalModule from "../_components/modal";
import * as modalSurfaceModule from "../_components/modal-surface";
import { ProductShell } from "../_components/product-shell";
import * as selectModule from "../_components/select";
import { AdminConsole } from "./admin-console";

/*
 * The product shell and the administration console mounted together, the
 * way /admin/ renders, against the deterministic fake: what happens to the
 * bar, the navigation, the access gate and the identity panels when the
 * principal changes under a page -- a role lowered or a membership removed in
 * another session, ownership handed over, site-admin access withdrawn, the
 * organization switched with work unsaved -- and what a capability-gated
 * section shows when its URL is opened directly.
 *
 * "Elsewhere" changes write the fake's directory directly: they are what
 * another administrator's session would have done, and the page under test
 * must learn of them only through its own requests.
 *
 * Assertions compare strings and booleans, never DOM nodes: a failing
 * deepEqual on a node diffs the whole graph.
 */

const realModal = modalModule.Modal;
const realSelect = selectModule.Select;
const realInstallModalSurface = modalSurfaceModule.installModalSurface;

function StubModal({ title, subtitle, children, footer }: { title: string; subtitle?: string; children: ReactNode; footer?: ReactNode }) {
  return (
    <div data-dialog={title}>
      <h2>{title}</h2>
      {subtitle === undefined ? null : <p>{subtitle}</p>}
      {children}
      {footer}
    </div>
  );
}

/** The shared Select's listbox needs selectors the fake DOM does not match; its contract is tested on its own. */
function StubSelect({ id, value, onValueChange, children }: { id?: string; value?: string; onValueChange?: (value: string) => void; children?: ReactNode }) {
  return (
    <div id={id} data-select-value={value}>
      {Children.toArray(children).filter(isValidElement).map((child) => {
        const option = child as ReactElement<{ value: string; children: ReactNode }>;
        return <button key={option.key ?? option.props.value} type="button" data-option={option.props.value} onClick={() => onValueChange?.(option.props.value)}>{option.props.children}</button>;
      })}
    </div>
  );
}

const reactErrors: string[] = [];
const consoleError = console.error;

test.before(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = StubModal;
  (selectModule as unknown as { Select: unknown }).Select = StubSelect;
  // The drawer's focus trap and inert siblings need layout; `modal-surface` has its own tests.
  (modalSurfaceModule as unknown as { installModalSurface: unknown }).installModalSurface = () => () => undefined;
  console.error = (...values: unknown[]) => {
    reactErrors.push(values.map((value) => String(value).slice(0, 300)).join(" "));
  };
});

test.after(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = realModal;
  (selectModule as unknown as { Select: unknown }).Select = realSelect;
  (modalSurfaceModule as unknown as { installModalSurface: unknown }).installModalSurface = realInstallModalSurface;
  console.error = consoleError;
});

test.beforeEach(() => {
  reactErrors.length = 0;
  confirmations.length = 0;
  confirmAnswer = true;
});

test.afterEach(() => {
  assert.equal(reactErrors.length, 0, reactErrors.slice(0, 3).join("\n"));
});

// --------------------------------------------------------------------------
// The page's window
// --------------------------------------------------------------------------

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
  confirm?: (message: string) => boolean;
  requestAnimationFrame(callback: FrameRequestCallback): number;
};
fakeWindow.requestAnimationFrame = (callback) => {
  callback(0);
  return 1;
};

const navigations: string[] = [];
fakeWindow.location.assign = (href) => navigations.push(`assign ${href}`);
fakeWindow.location.replace = (href) => navigations.push(`replace ${href}`);

const confirmations: string[] = [];
let confirmAnswer = true;
fakeWindow.confirm = (message) => {
  confirmations.push(message);
  return confirmAnswer;
};

function at(pathname: string, search = "") {
  fakeWindow.location.pathname = pathname;
  fakeWindow.location.search = search;
  fakeWindow.location.hash = "";
  fakeWindow.location.href = `https://workspace.test${pathname}${search}`;
  navigations.length = 0;
}

// --------------------------------------------------------------------------
// Mounting and driving
// --------------------------------------------------------------------------

interface Mounted {
  readonly container: FakeElement;
  unmount(): Promise<void>;
}

async function pause(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 5);
    });
  });
}

/** Lets a client call, the renders it causes, and the reads those start all finish. */
async function settle(): Promise<void> {
  await pause();
  await pause();
  await pause();
  await pause();
}

async function mount(node: ReactNode): Promise<Mounted> {
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

function fake(email: string, options: { readonly latencyMs?: number } = {}): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email, now: () => Date.parse("2026-10-24T12:00:00.000Z"), ...options });
}

function adminPage(client: IdentityClient, page: ReactNode = <AdminConsole dataMode="demo" apiBaseUrl="" />, section: "admin" | "datasets" = "admin") {
  return (
    <IdentityProvider client={client}>
      <ProductShell activeSection={section} appName="Settings" dataMode="demo">{page}</ProductShell>
    </IdentityProvider>
  );
}

async function openAdmin(client: IdentityClient, section: string): Promise<Mounted> {
  at("/admin/", `?section=${section}`);
  return mount(adminPage(client));
}

function textOf(element: FakeElement | null | undefined): string {
  return (element?.textContent ?? "").replaceAll(/\s+/gu, " ").trim();
}

/** Elements of `tag` carrying `className`: the fake DOM matches tags and attributes, not classes or descendants. */
function byClass(scope: FakeElement | null | undefined, tag: string, className: string): FakeElement[] {
  return (scope?.querySelectorAll(tag) ?? []).filter((element) => (element.getAttribute("class") ?? "").split(/\s+/u).includes(className));
}

function findButton(scope: FakeElement, name: string): FakeElement | undefined {
  return scope.querySelectorAll("button").find((element) => element.getAttribute("aria-label") === name || textOf(element) === name);
}

function button(scope: FakeElement, name: string): FakeElement {
  const found = findButton(scope, name);
  assert.ok(found, `no button named ${JSON.stringify(name)} in: ${textOf(scope).slice(0, 600)}`);
  return found;
}

async function click(element: FakeElement): Promise<void> {
  await act(async () => {
    element.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 }));
  });
  await settle();
}

async function type(input: FakeElement, value: string): Promise<void> {
  await act(async () => {
    input.value = value;
    input.dispatchEvent(fakeEvent("input"));
  });
}

async function choose(scope: FakeElement, name: string, value: string): Promise<void> {
  const radio = scope.querySelectorAll(`input[name="${name}"]`).find((input) => input.value === value);
  assert.ok(radio, `no ${name} radio for ${value}`);
  await act(async () => {
    radio.checked = true;
    radio.dispatchEvent(fakeEvent("click"));
  });
}

async function submit(form: FakeElement | null): Promise<void> {
  assert.ok(form, "no form to submit");
  await act(async () => {
    form.dispatchEvent(fakeEvent("submit"));
  });
  await settle();
}

function dialog(container: FakeElement, title: string): FakeElement {
  const found = container.querySelector(`[data-dialog="${title}"]`);
  assert.ok(found, `no dialog titled ${JSON.stringify(title)} in: ${textOf(container).slice(0, 600)}`);
  return found;
}

function organizationTrigger(container: FakeElement): FakeElement | undefined {
  return container.querySelectorAll("button").find((element) => element.getAttribute("aria-label")?.startsWith("Organization:") === true);
}

function organizationShown(container: FakeElement): string {
  return textOf(organizationTrigger(container)?.querySelector("strong"));
}

function organizationMenu(container: FakeElement): FakeElement | null {
  return container.querySelector('[data-suite-menu="org"]');
}

function organizationChoices(container: FakeElement): string {
  return (organizationMenu(container)?.querySelectorAll('[role="menuitemradio"]') ?? [])
    .map((choice) => textOf(choice.querySelector("strong")))
    .join(" | ");
}

async function chooseOrganization(container: FakeElement, name: string): Promise<void> {
  const trigger = organizationTrigger(container);
  assert.ok(trigger, "no organization switcher");
  if (container.querySelector('[data-suite-menu="org"]') === null) await click(trigger);
  const choice = container.querySelectorAll('[role="menuitemradio"]').find((element) => textOf(element.querySelector("strong")) === name);
  assert.ok(choice, `no organization choice ${name} among: ${organizationChoices(container)}`);
  await click(choice);
}

/** The user menu's summary: name, email, role and organization, and the site-admin badge. */
async function userSummary(container: FakeElement, name: string): Promise<string> {
  const trigger = findButton(container, `${name} menu`);
  assert.ok(trigger, `no user menu for ${name}`);
  if (container.querySelector('[data-suite-menu="user"]') === null) await click(trigger);
  const summary = textOf(byClass(container.querySelector('[data-suite-menu="user"]'), "div", "suite-user-summary")[0]);
  await click(trigger);
  return summary;
}

function sidebar(container: FakeElement): string {
  const aside = container.querySelectorAll("aside").find((element) => element.getAttribute("aria-label") === "Administration navigation");
  return (aside?.querySelectorAll("button") ?? []).map((item) => textOf(item.querySelector("strong"))).join(" | ");
}

function forbiddenHeading(container: FakeElement): string | null {
  const heading = container.querySelector("#suite-access-forbidden-title");
  return heading === null ? null : `${heading.tagName.toLowerCase()}: ${textOf(heading)}`;
}

function membersState(container: FakeElement): string | null {
  return container.querySelector("[data-members-state]")?.getAttribute("data-members-state") ?? null;
}

function memberRow(container: FakeElement, userId: string): FakeElement | null {
  return container.querySelector(`tr[data-member-id="${userId}"]`);
}

function cell(row: FakeElement | null, label: string): string {
  return textOf(row?.querySelector(`td[data-label="${label}"]`));
}

// --------------------------------------------------------------------------
// Another session's changes
// --------------------------------------------------------------------------

interface FakeDirectory {
  memberships: Array<{ organizationId: string; userId: string; role: OrgRole }>;
  readonly users: Map<string, { readonly user: { userType: string } }>;
}

function directoryOf(client: FakeIdentityClient): FakeDirectory {
  return client as unknown as FakeDirectory;
}

function removeMembershipElsewhere(client: FakeIdentityClient, organizationId: string, userId: string): void {
  const directory = directoryOf(client);
  const before = directory.memberships.length;
  directory.memberships = directory.memberships.filter((entry) => !(entry.organizationId === organizationId && entry.userId === userId));
  assert.equal(directory.memberships.length, before - 1, `user ${userId} had no membership in ${organizationId}`);
}

function setRoleElsewhere(client: FakeIdentityClient, organizationId: string, userId: string, role: OrgRole): void {
  const entry = directoryOf(client).memberships.find((candidate) => candidate.organizationId === organizationId && candidate.userId === userId);
  assert.ok(entry, `user ${userId} has no membership in ${organizationId}`);
  entry.role = role;
}

function withdrawSiteAdminElsewhere(client: FakeIdentityClient, userId: string): void {
  const record = directoryOf(client).users.get(userId);
  assert.ok(record, `no user ${userId}`);
  record.user.userType = "regular";
}

// --------------------------------------------------------------------------
// The principal changing under the page
// --------------------------------------------------------------------------

test("leaving the organization from Members moves the bar, the navigation and the gate with the panel", async () => {
  const client = fake("alice@open-splunk.test");
  const view = await openAdmin(client, "members");
  try {
    assert.equal(organizationShown(view.container), "Acme Operations");
    assert.equal(membersState(view.container), "ready");
    await click(button(memberRow(view.container, "3") ?? view.container, "Leave organization"));
    await click(button(dialog(view.container, "Leave Acme Operations?"), "Leave organization"));

    // Alice held Acme Platform only through Acme Operations, so she is now in no organization.
    assert.equal(organizationShown(view.container), "No organization", "the bar no longer claims the organization she left");
    assert.equal(forbiddenHeading(view.container), "h1: You are not in an organization yet");
    assert.equal(sidebar(view.container), "", "no administration section is offered");
    assert.equal(membersState(view.container), null, "the panel is gone with the console");
    assert.doesNotMatch(await userSummary(view.container, "Alice Abara"), /Admin · Acme Operations/u);
  } finally {
    await view.unmount();
  }
});

test("a demotion in another session shows on the next refused change, across the panel and the shell", async () => {
  const client = fake("alice@open-splunk.test");
  const view = await openAdmin(client, "members");
  try {
    assert.match(await userSummary(view.container, "Alice Abara"), /Admin · Acme Operations/u);
    setRoleElsewhere(client, "1", "3", "moderator");

    // The stale page still offers the change; the server refuses it.
    await click(button(view.container, "Change role for Mark Mendez"));
    const roleDialog = dialog(view.container, "Change role for Mark Mendez");
    await choose(roleDialog, "members-role", "moderator");
    await submit(roleDialog.querySelector("form#members-role-form"));

    assert.equal(sidebar(view.container), "System overview", "a moderator keeps the console but not its member sections");
    assert.equal(forbiddenHeading(view.container), "h2: You do not have access to Members & invitations");
    assert.match(textOf(view.container), /Your role in Acme Operations is Moderator\. Ask an owner or admin of Acme Operations for the Admin role or higher\./u);
    assert.equal(findButton(view.container, "Invite member") === undefined, true, "the panel's actions are withheld");
    assert.match(await userSummary(view.container, "Alice Abara"), /Moderator · Acme Operations/u, "the bar shows the lowered role");
  } finally {
    await view.unmount();
  }
});

test("removal from the active organization elsewhere shows on the next refused change", async () => {
  const client = fake("alice@open-splunk.test");
  const view = await openAdmin(client, "members");
  try {
    removeMembershipElsewhere(client, "1", "3");
    await click(button(view.container, "Remove Tess Takahashi"));
    await click(button(dialog(view.container, "Remove Tess Takahashi?"), "Remove member"));
    assert.equal(organizationShown(view.container), "No organization");
    assert.equal(forbiddenHeading(view.container), "h1: You are not in an organization yet");
    assert.ok((await client.listMembers("1").catch((error: unknown) => error)) instanceof IdentityError, "the fake agrees she lost access");
  } finally {
    await view.unmount();
  }
});

test("a membership removed elsewhere drops out of the switcher once switching to it is refused", async () => {
  at("/datasets/");
  const client = fake("mark@open-splunk.test");
  const view = await mount(adminPage(client, <p data-page-content="">Datasets</p>, "datasets"));
  try {
    await click(organizationTrigger(view.container) ?? view.container);
    assert.equal(organizationChoices(view.container), "Acme Operations | Acme Platform | Globex Security");
    removeMembershipElsewhere(client, "2", "5");
    await chooseOrganization(view.container, "Globex Security");
    const alert = (organizationMenu(view.container)?.querySelectorAll('[role="alert"]') ?? []).map(textOf).join(" ");
    assert.equal(alert, "You no longer have access to Globex Security.");
    assert.equal(organizationChoices(view.container), "Acme Operations | Acme Platform", "the refusal re-read the principal");
    assert.equal(organizationShown(view.container), "Acme Operations");
    assert.equal(view.container.querySelector("[data-page-content]") === null, false, "the page stays in its organization");
  } finally {
    await view.unmount();
  }
});

test("a stale role change refused because the member was promoted elsewhere refreshes the row", async () => {
  const client = fake("alice@open-splunk.test");
  const view = await openAdmin(client, "members");
  try {
    assert.equal(cell(memberRow(view.container, "5"), "Explicit role"), "Member");
    // Another admin promotes Mark past what Alice may manage; the fake leaves
    // band checks to go-common, so the refusal it would send is injected.
    setRoleElsewhere(client, "1", "5", "admin");
    client.failNext("changeMemberRole", new IdentityError("forbidden", "Cannot manage a user with a higher or equal role"));
    await click(button(view.container, "Change role for Mark Mendez"));
    const roleDialog = dialog(view.container, "Change role for Mark Mendez");
    await choose(roleDialog, "members-role", "moderator");
    await submit(roleDialog.querySelector("form#members-role-form"));
    assert.match(textOf(roleDialog.querySelector('[role="alert"]')), /higher or equal role/u);
    await click(button(roleDialog, "Cancel"));

    const mark = memberRow(view.container, "5");
    assert.equal(cell(mark, "Explicit role"), "Admin", "the row shows the role the server now holds");
    assert.equal(button(mark ?? view.container, "Change role for Mark Mendez").getAttribute("aria-disabled"), "true");
    assert.match(cell(mark, "Actions"), /Only a role above admin can change an admin\./u);
  } finally {
    await view.unmount();
  }
});

test("a site admin who loses site-admin access elsewhere loses the site sections on the next refusal", async () => {
  const client = fake("ada@open-splunk.test");
  const view = await openAdmin(client, "users");
  try {
    assert.match(sidebar(view.container), /Users \| Organizations \| Server settings$/u);
    assert.match(await userSummary(view.container, "Ada Lovelace"), /Site admin$/u);
    withdrawSiteAdminElsewhere(client, "1");
    const searchBox = view.container.querySelector('input[type="search"]');
    assert.ok(searchBox, "no user search");
    await type(searchBox, "mark");
    await submit(view.container.querySelector('form[role="search"]'));

    assert.equal(
      sidebar(view.container),
      "System overview | Indexes | Data inputs | Users & access | Members & invitations",
      "an owner without site-admin access manages its organization only",
    );
    assert.equal(forbiddenHeading(view.container), "h2: You do not have access to Users");
    assert.match(textOf(view.container), /Only a site administrator can open it\./u);
    assert.doesNotMatch(await userSummary(view.container, "Ada Lovelace"), /Site admin/u, "the badge goes with the access");
  } finally {
    await view.unmount();
  }
});

test("a site admin who signs out its own sessions from Users is sent to sign in by the shell", async () => {
  const client = fake("ada@open-splunk.test");
  const view = await openAdmin(client, "users");
  try {
    await click(button(view.container, "Actions for Ada Lovelace"));
    const item = view.container.querySelectorAll('[role="menuitem"]').find((element) => textOf(element.querySelector("strong")) === "Sign out my sessions");
    assert.ok(item, "no sign-out item");
    await click(item);
    await submit(view.container.querySelector("[data-dialog]")?.querySelector("form") ?? null);
    assert.equal(navigations.join(", "), "replace /signin/?returnTo=%2Fadmin%2F%3Fsection%3Dusers");
    assert.equal(textOf(view.container.querySelector("h1")), "Sign in to continue");
    assert.equal(organizationTrigger(view.container) === undefined, true, "the bar shows no organization once signed out");
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Last-owner protections
// --------------------------------------------------------------------------

test("the last owner is locked until ownership is shared, then a site-admin owner may step down and the bar follows", async () => {
  const client = fake("ada@open-splunk.test");
  const view = await openAdmin(client, "members");
  try {
    const ada = () => memberRow(view.container, "1") ?? view.container;
    assert.equal(button(ada(), "Change role for Ada Lovelace").getAttribute("aria-disabled"), "true");
    assert.equal(button(ada(), "Leave organization").getAttribute("aria-disabled"), "true");
    assert.match(cell(ada(), "Actions"), /The last owner cannot be demoted or removed\./u);
    // A disabled action opens nothing.
    await click(button(ada(), "Leave organization"));
    assert.equal(view.container.querySelector("[data-dialog]") === null, true);

    await click(button(view.container, "Change role for Owen Ortiz"));
    const promote = dialog(view.container, "Change role for Owen Ortiz");
    await choose(promote, "members-role", "owner");
    assert.match(textOf(promote), /Owners can manage everything/u);
    await submit(promote.querySelector("form#members-role-form"));
    assert.equal(cell(memberRow(view.container, "2"), "Explicit role"), "Owner");
    assert.equal(button(ada(), "Change role for Ada Lovelace").getAttribute("aria-disabled"), null, "two owners unlock the first");

    await click(button(ada(), "Change role for Ada Lovelace"));
    const stepDown = dialog(view.container, "Change your role");
    await choose(stepDown, "members-role", "admin");
    assert.match(textOf(stepDown), /You keep full access as a site admin/u);
    await submit(stepDown.querySelector("form#members-role-form"));
    assert.equal(cell(memberRow(view.container, "1"), "Explicit role"), "Admin");
    assert.match(await userSummary(view.container, "Ada Lovelace"), /Admin · Acme Operations/u, "the bar shows the role she now holds");
    assert.equal(membersState(view.container), "ready", "a site admin keeps the panel");
    // Owen is now the last owner, and his row says so.
    assert.match(cell(memberRow(view.container, "2"), "Explicit role"), /Last owner/u);
  } finally {
    await view.unmount();
  }
});

test("a site admin acting in an organization it holds no role in sees that organization's last owner locked", async () => {
  const client = fake("ada@open-splunk.test");
  await client.setActiveOrganization("2");
  const view = await openAdmin(client, "members");
  try {
    assert.equal(organizationShown(view.container), "Globex Security");
    await click(organizationTrigger(view.container) ?? view.container);
    const globex = view.container.querySelectorAll('[role="menuitemradio"]').find((choice) => textOf(choice.querySelector("strong")) === "Globex Security");
    assert.match(textOf(globex), /Site admin access · not a member/u);
    assert.match(textOf(byClass(view.container, "p", "members-context")[0]), /You are acting as a site admin\./u);
    const owen = memberRow(view.container, "2");
    assert.equal(button(owen ?? view.container, "Change role for Owen Ortiz").getAttribute("aria-disabled"), "true");
    assert.match(cell(owen, "Explicit role"), /Last owner/u);
    assert.equal(button(memberRow(view.container, "5") ?? view.container, "Change role for Mark Mendez").getAttribute("aria-disabled"), null);
    assert.equal(memberRow(view.container, "1") === null, true, "a site admin without a role is not listed as a member");
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Switching organization with work in progress
// --------------------------------------------------------------------------

/** A page holding unsaved work guards `beforeunload`, as the dashboard editor and the settings forms do. */
function UnsavedDraft() {
  const [draft, setDraft] = useState("");
  useEffect(() => {
    if (draft.length === 0) return;
    const protect = (event: Event) => event.preventDefault();
    window.addEventListener("beforeunload", protect);
    return () => window.removeEventListener("beforeunload", protect);
  }, [draft]);
  return <input id="unsaved-draft" value={draft} onChange={(event) => setDraft(event.target.value)} />;
}

test("switching organization with unsaved work asks first, and declining keeps the page and its work", async () => {
  at("/datasets/");
  const client = fake("ada@open-splunk.test");
  const view = await mount(adminPage(client, <UnsavedDraft />, "datasets"));
  try {
    const draft = () => view.container.querySelector("#unsaved-draft");
    await type(draft() ?? view.container, "half-written search");
    confirmAnswer = false;
    await chooseOrganization(view.container, "Acme Platform");
    assert.equal(confirmations.length, 1, "the switch asked before discarding the work");
    assert.match(confirmations[0] ?? "", /not saved\. Switching to Acme Platform discards them/u);
    assert.equal(organizationShown(view.container), "Acme Operations");
    assert.equal(draft()?.value, "half-written search", "the work is kept");
    assert.equal((await client.getPrincipal())?.activeOrganization?.id, "1", "no switch was sent");

    confirmAnswer = true;
    await chooseOrganization(view.container, "Acme Platform");
    assert.equal(confirmations.length, 2);
    assert.equal(organizationShown(view.container), "Acme Platform");
    assert.equal(draft()?.value, "", "the page restarted in the new organization");

    // Nothing unsaved: no question.
    await chooseOrganization(view.container, "Acme Operations");
    assert.equal(confirmations.length, 2);
    assert.equal(organizationShown(view.container), "Acme Operations");
  } finally {
    await view.unmount();
  }
});

test("the drawer's organization list asks the same question before discarding work", async () => {
  at("/datasets/");
  const client = fake("ada@open-splunk.test");
  const view = await mount(adminPage(client, <UnsavedDraft />, "datasets"));
  try {
    await type(view.container.querySelector("#unsaved-draft") ?? view.container, "draft");
    await click(button(view.container, "Open product navigation"));
    const platform = byClass(view.container, "button", "drawer-org").find((element) => textOf(element.querySelector("strong")) === "Acme Platform");
    assert.ok(platform, "no drawer entry for Acme Platform");
    confirmAnswer = false;
    await click(platform);
    assert.equal(confirmations.length, 1);
    assert.equal(organizationShown(view.container), "Acme Operations");
    assert.equal(view.container.querySelector("#unsaved-draft")?.value, "draft");
  } finally {
    await view.unmount();
  }
});

test("an invitation composed before an organization switch is sent to the organization it was composed in", async () => {
  const client = fake("ada@open-splunk.test", { latencyMs: 15 });
  const view = await openAdmin(client, "members");
  try {
    await settle();
    await click(button(view.container, "Invite member"));
    const invite = dialog(view.container, "Invite member");
    await type(invite.querySelector("#members-invite-email") ?? invite, "late.arrival@example.com");
    await click(organizationTrigger(view.container) ?? view.container);
    const platform = view.container.querySelectorAll('[role="menuitemradio"]').find((element) => textOf(element.querySelector("strong")) === "Acme Platform");
    assert.ok(platform, "no Acme Platform choice");
    const form = invite.querySelector("form#members-invite-form");
    assert.ok(form, "no invite form");
    // The switch is sent first and settles first; the invitation is in flight behind it.
    await act(async () => {
      platform.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 }));
      form.dispatchEvent(fakeEvent("submit"));
    });
    await settle();
    await settle();

    assert.equal(organizationShown(view.container), "Acme Platform");
    assert.match(textOf(byClass(view.container, "p", "members-context")[0]), /^Acme Platform/u);
    assert.equal(view.container.querySelector("[data-dialog]") === null, true, "the old organization's dialog is gone");
    assert.equal(textOf(view.container.querySelector("[data-members-announcement]")), "", "the new organization's panel announces nothing it did not do");
    const sentTo = async (organizationId: string) => (await client.listInvitations(organizationId)).items.some((entry) => entry.email === "late.arrival@example.com");
    assert.equal(await sentTo("1"), true, "the invitation went to Acme Operations");
    assert.equal(await sentTo("3"), false, "and not to Acme Platform");
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Capability-gated sections entered by URL
// --------------------------------------------------------------------------

const SECTION_CASES: ReadonlyArray<{ email: string; section: string; expected: string | null }> = [
  { email: "mark@open-splunk.test", section: "overview", expected: "h1: You do not have access to Administration" },
  { email: "mark@open-splunk.test", section: "members", expected: "h1: You do not have access to Administration" },
  { email: "mona@open-splunk.test", section: "overview", expected: null },
  { email: "mona@open-splunk.test", section: "indexes", expected: "h2: You do not have access to Indexes" },
  { email: "mona@open-splunk.test", section: "members", expected: "h2: You do not have access to Members & invitations" },
  { email: "alice@open-splunk.test", section: "members", expected: null },
  { email: "alice@open-splunk.test", section: "users", expected: "h2: You do not have access to Users" },
  { email: "alice@open-splunk.test", section: "organizations", expected: "h2: You do not have access to Organizations" },
  { email: "alice@open-splunk.test", section: "server", expected: "h2: You do not have access to Server settings" },
  // Owen owns Globex Security, but his active organization is Acme Operations, where he is a member.
  { email: "owen@open-splunk.test", section: "users", expected: "h1: You do not have access to Administration" },
  { email: "ada@open-splunk.test", section: "users", expected: null },
  { email: "ada@open-splunk.test", section: "organizations", expected: null },
  { email: "ada@open-splunk.test", section: "server", expected: null },
];

for (const { email, section, expected } of SECTION_CASES) {
  test(`${email} opening ?section=${section} by URL ${expected === null ? "is admitted" : "is refused"}`, async () => {
    const view = await openAdmin(fake(email), section);
    try {
      assert.equal(forbiddenHeading(view.container), expected);
    } finally {
      await view.unmount();
    }
  });
}

const SECTION_PARAMETERS: ReadonlyArray<{ search: string; opens: string }> = [
  { search: "?section=nonsense", opens: "System overview" },
  { search: "?section=", opens: "System overview" },
  { search: "?section=MEMBERS", opens: "System overview" },
  { search: "?section=__proto__", opens: "System overview" },
  { search: "?section=members&section=users", opens: "Members & invitations" },
];

for (const { search, opens } of SECTION_PARAMETERS) {
  test(`the section parameter ${search} opens ${opens} rather than a forbidden or blank page`, async () => {
    at("/admin/", search);
    const view = await mount(adminPage(fake("alice@open-splunk.test")));
    try {
      const aside = view.container.querySelectorAll("aside").find((element) => element.getAttribute("aria-label") === "Administration navigation");
      const active = (aside?.querySelectorAll("button") ?? []).find((item) => item.getAttribute("aria-current") === "page");
      assert.equal(textOf(active?.querySelector("strong")), opens);
      assert.equal(forbiddenHeading(view.container), null);
    } finally {
      await view.unmount();
    }
  });
}

test("a moderator's forbidden Members section names the missing capability and offers no way into the panel", async () => {
  const view = await openAdmin(fake("mona@open-splunk.test"), "members");
  try {
    assert.match(textOf(view.container), /Permissions needed:\s?members\.admin/u);
    assert.equal(membersState(view.container), null, "the panel never mounts, so it sends no member request");
    assert.equal(findButton(view.container, "Invite member") === undefined, true);
    assert.equal(sidebar(view.container).includes("Members"), false);
  } finally {
    await view.unmount();
  }
});
