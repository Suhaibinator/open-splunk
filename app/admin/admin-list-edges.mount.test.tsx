import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";
import { Children, act, isValidElement, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type AdminUser, type IdentityClient, type OrganizationMember, type Page, type PageRequest } from "@/lib/api/identity";
import { createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";

import { IdentityProvider } from "../_components/identity-provider";
import * as modalModule from "../_components/modal";
import * as selectModule from "../_components/select";
import { MembersPanel } from "./members-panel";
import { SiteUsersPanel } from "./site-users-panel";

/*
 * The member and user lists at their edges: empty, ten thousand rows, and the
 * page under the viewer emptied -- by its own last row going, by another
 * session removing it, or by a change moving it out of the filter. An empty
 * page past the first must step back to the last page that still has rows,
 * never read as an empty organization or directory.
 *
 * Assertions compare strings, numbers and booleans, never DOM nodes.
 */

const NOW = Date.parse("2026-10-24T12:00:00.000Z");
const realModal = modalModule.Modal;
const realSelect = selectModule.Select;

function StubModal({ title, children, footer }: { title: string; children: ReactNode; footer?: ReactNode }) {
  return (
    <div data-dialog={title}>
      <h2>{title}</h2>
      {children}
      {footer}
    </div>
  );
}

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
  console.error = (...values: unknown[]) => {
    reactErrors.push(values.map((value) => String(value).slice(0, 300)).join(" "));
  };
});

test.after(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = realModal;
  (selectModule as unknown as { Select: unknown }).Select = realSelect;
  console.error = consoleError;
});

test.beforeEach(() => {
  reactErrors.length = 0;
});

test.afterEach(() => {
  assert.equal(reactErrors.length, 0, reactErrors.slice(0, 3).join("\n"));
});

interface Mounted {
  readonly container: FakeElement;
  unmount(): Promise<void>;
}

async function pause(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 3);
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

function fake(email = "ada@open-splunk.test"): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email, now: () => NOW });
}

function textOf(element: FakeElement | null | undefined): string {
  return (element?.textContent ?? "").replaceAll(/\s+/gu, " ").trim();
}

function button(scope: FakeElement, name: string): FakeElement {
  const found = scope.querySelectorAll("button").find((element) => element.getAttribute("aria-label") === name || textOf(element) === name);
  assert.ok(found, `no button named ${JSON.stringify(name)} in: ${textOf(scope).slice(0, 600)}`);
  return found;
}

async function click(element: FakeElement): Promise<void> {
  await act(async () => {
    element.dispatchEvent(fakeEvent("click"));
  });
  await settle();
}

async function submit(form: FakeElement | null | undefined): Promise<void> {
  assert.ok(form, "no form to submit");
  await act(async () => {
    form.dispatchEvent(fakeEvent("submit"));
  });
  await settle();
}

function dialog(container: FakeElement): FakeElement {
  const found = container.querySelector("[data-dialog]");
  assert.ok(found, `no dialog open in: ${textOf(container).slice(0, 600)}`);
  return found;
}

function pager(container: FakeElement, label: string): { readonly range: string; readonly position: string; readonly previous: boolean; readonly next: boolean } | null {
  const nav = container.querySelector(`nav[aria-label="${label}"]`);
  if (nav === null) return null;
  return {
    range: textOf(nav.querySelector('[role="status"]')),
    position: textOf(nav.querySelector("small")),
    previous: !button(nav, "Previous").hasAttribute("disabled"),
    next: !button(nav, "Next").hasAttribute("disabled"),
  };
}

async function page(container: FakeElement, label: string, direction: "Previous" | "Next"): Promise<void> {
  const nav = container.querySelector(`nav[aria-label="${label}"]`);
  assert.ok(nav, `no pager ${label}`);
  await click(button(nav, direction));
}

const count = (value: number) => value.toLocaleString();

// --------------------------------------------------------------------------
// A large directory, served one page at a time
// --------------------------------------------------------------------------

const LARGE = 10_000;

interface PageLog {
  readonly requested: number[];
}

function largePage<T>(request: PageRequest | undefined, make: (index: number) => T, log: PageLog): Promise<Page<T>> {
  const current = request?.page ?? 1;
  const limit = request?.limit ?? 25;
  log.requested.push(current);
  const start = (current - 1) * limit;
  const items = Array.from({ length: Math.max(0, Math.min(limit, LARGE - start)) }, (_unused, offset) => make(start + offset));
  return Promise.resolve({
    items,
    page: {
      currentPage: current,
      itemsPerPage: limit,
      hasNextPage: start + limit < LARGE,
      hasPrevPage: current > 1,
      totalItems: LARGE,
      totalPages: Math.ceil(LARGE / limit),
    },
  });
}

async function largeMembersClient(log: PageLog): Promise<IdentityClient> {
  const base = fake();
  const template = (await base.listMembers("1")).items.find((member) => member.userId === "5");
  assert.ok(template, "no template member");
  const client = Object.create(base) as IdentityClient;
  client.listMembers = (_organizationId, request) => largePage(request, (index): OrganizationMember => ({
    ...template,
    userId: String(1_000 + index),
    user: { ...template.user, id: String(1_000 + index), displayPrimary: `Member ${String(index + 1).padStart(5, "0")}` },
  }), log);
  return client;
}

test("ten thousand members read as grouped counts, one page requested at a time", async () => {
  const log: PageLog = { requested: [] };
  const client = await largeMembersClient(log);
  const view = await mount(<IdentityProvider client={client}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 25);
    assert.match(textOf(view.container), new RegExp(`${count(LARGE)} members`, "u"));
    assert.deepEqual(pager(view.container, "Members pages"), {
      range: `Showing 1–25 of ${count(LARGE)} members`,
      position: `Page 1 of ${count(400)}`,
      previous: false,
      next: true,
    });
    await page(view.container, "Members pages", "Next");
    await page(view.container, "Members pages", "Next");
    assert.equal(pager(view.container, "Members pages")?.range, `Showing 51–75 of ${count(LARGE)} members`);
    assert.equal(textOf(view.container.querySelector('tr[data-member-id="1050"]')).startsWith("Member 00051"), true);
    assert.deepEqual(log.requested, [1, 2, 3], "each page is asked for once, and only when shown");
  } finally {
    await view.unmount();
  }
});

test("ten thousand accounts read as grouped counts in the site directory", async () => {
  const log: PageLog = { requested: [] };
  const base = fake();
  const template = (await base.adminListUsers({ search: "mark" })).items[0];
  assert.ok(template, "no template account");
  const client = Object.create(base) as IdentityClient;
  client.adminListUsers = (_query, request) => largePage(request, (index): AdminUser => ({
    ...template,
    user: { ...template.user, id: String(1_000 + index), email: `user${index}@example.test`, username: `user${index}` },
  }), log);
  const view = await mount(<IdentityProvider client={client}><SiteUsersPanel pageSize={25} /></IdentityProvider>);
  try {
    assert.equal(view.container.querySelectorAll("tr[data-user-id]").length, 25);
    assert.deepEqual(pager(view.container, "Users pages"), {
      range: `Showing 1–25 of ${count(LARGE)} users`,
      position: `Page 1 of ${count(400)}`,
      previous: false,
      next: true,
    });
    await page(view.container, "Users pages", "Next");
    assert.equal(pager(view.container, "Users pages")?.range, `Showing 26–50 of ${count(LARGE)} users`);
    assert.deepEqual(log.requested, [1, 2]);
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Members: the page under the viewer empties
// --------------------------------------------------------------------------

/**
 * The seeded Acme members three to a page: Ada, Alice, Bart | Mark, Mona,
 * Owen | Tess. The override sits on the fake itself (not on a prototype
 * wrapper), so a removal through the panel writes the directory it lists.
 */
function pagedMembersClient(base: FakeIdentityClient): IdentityClient {
  const listMembers = base.listMembers.bind(base);
  base.listMembers = (organizationId, request) => listMembers(organizationId, { page: request?.page ?? 1, limit: 3 });
  return base;
}

function memberIds(container: FakeElement): string {
  return container.querySelectorAll("tr[data-member-id]").map((row) => row.getAttribute("data-member-id") ?? "").join(",");
}

test("removing the only member on the last page steps back to the page before it", async () => {
  const view = await mount(<IdentityProvider client={pagedMembersClient(fake())}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    await page(view.container, "Members pages", "Next");
    await page(view.container, "Members pages", "Next");
    assert.equal(memberIds(view.container), "6");
    await click(button(view.container, "Remove Tess Takahashi"));
    await click(button(dialog(view.container), "Remove member"));
    assert.equal(memberIds(view.container), "5,4,2", "the page before the emptied one is shown");
    assert.deepEqual(pager(view.container, "Members pages"), { range: "Showing 4–6 of 6 members", position: "Page 2 of 2", previous: true, next: false });
    assert.doesNotMatch(textOf(view.container), /No members/u);
  } finally {
    await view.unmount();
  }
});

test("a member removed by another session empties the page; the refusal steps back instead of showing nobody", async () => {
  const base = fake();
  const view = await mount(<IdentityProvider client={pagedMembersClient(base)}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    await page(view.container, "Members pages", "Next");
    await page(view.container, "Members pages", "Next");
    await click(button(view.container, "Remove Tess Takahashi"));
    // Another admin removes Tess while the confirmation is open.
    const directory = base as unknown as { memberships: Array<{ organizationId: string; userId: string }> };
    directory.memberships = directory.memberships.filter((entry) => !(entry.organizationId === "1" && entry.userId === "6"));
    await click(button(dialog(view.container), "Remove member"));
    assert.match(textOf(dialog(view.container).querySelector('[role="alert"]')), /no longer there/u);
    await click(button(dialog(view.container), "Cancel"));
    assert.equal(memberIds(view.container), "5,4,2");
    assert.equal(pager(view.container, "Members pages")?.position, "Page 2 of 2");
  } finally {
    await view.unmount();
  }
});

test("a server that reports no totals still steps back one page from an empty one", async () => {
  const base = fake();
  let shrink = false;
  const listMembers = base.listMembers.bind(base);
  base.listMembers = async (organizationId, request) => {
    const current = request?.page ?? 1;
    const answer = await listMembers(organizationId, { page: current, limit: 3 });
    const items = shrink && current === 2 ? [] : answer.items;
    return { items, page: { currentPage: current, itemsPerPage: 3, hasNextPage: !shrink && current < 3, hasPrevPage: current > 1 } };
  };
  const view = await mount(<IdentityProvider client={base}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    assert.deepEqual(pager(view.container, "Members pages"), { range: "Showing 1–3 members", position: "Page 1", previous: false, next: true });
    await page(view.container, "Members pages", "Next");
    shrink = true;
    // A refused change reloads the list; the reloaded page comes back empty.
    await click(button(view.container, "Remove Mark Mendez"));
    base.failNext("removeMember", new IdentityError("conflict", "Changed elsewhere"));
    await click(button(dialog(view.container), "Remove member"));
    await click(button(dialog(view.container), "Cancel"));
    assert.equal(memberIds(view.container), "1,3,8");
    assert.equal(pager(view.container, "Members pages"), null, "a single page left needs no pager");
  } finally {
    await view.unmount();
  }
});

test("an organization with no members yet says so, and offers no pager", async () => {
  const client = fake();
  const created = await client.adminCreateOrganization({ name: "Empty Org", slug: "empty-org" });
  await client.setActiveOrganization(created.id);
  const view = await mount(<IdentityProvider client={client}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    assert.match(textOf(view.container), /No members yet/u);
    assert.doesNotMatch(textOf(view.container), /No members on this page/u);
    assert.match(textOf(view.container), /No pending invitations/u);
    assert.equal(pager(view.container, "Members pages"), null);
    assert.equal(view.container.querySelectorAll("button").filter((element) => textOf(element) === "Invite member").length, 2, "the header and the empty state both invite");
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Users: the page under the viewer empties
// --------------------------------------------------------------------------

function userIds(container: FakeElement): string {
  return container.querySelectorAll("tr[data-user-id]").map((row) => row.getAttribute("data-user-id") ?? "").join(",");
}

async function runUserAction(container: FakeElement, name: string, item: string): Promise<void> {
  await click(button(container, `Actions for ${name}`));
  const entry = container.querySelectorAll('[role="menuitem"]').find((element) => textOf(element.querySelector("strong")) === item);
  assert.ok(entry, `no ${item} for ${name}`);
  await click(entry);
}

test("unbanning the only account on a filtered page steps back to the accounts still banned", async () => {
  const client = fake();
  await client.adminBanUser("5");
  const view = await mount(<IdentityProvider client={client}><SiteUsersPanel pageSize={1} /></IdentityProvider>);
  try {
    const banned = view.container.querySelectorAll("button").find((element) => element.getAttribute("data-option") === "banned");
    assert.ok(banned, "no Banned filter");
    await click(banned);
    assert.equal(userIds(view.container), "5");
    await page(view.container, "Users pages", "Next");
    assert.equal(userIds(view.container), "8");
    await runUserAction(view.container, "Bart Becker", "Unban user");
    await submit(dialog(view.container).querySelector("form"));
    assert.equal(userIds(view.container), "5", "the remaining banned account is shown");
    assert.doesNotMatch(textOf(view.container), /No users match/u);
    assert.deepEqual(pager(view.container, "Users pages"), { range: "Showing 1–1 of 1 users", position: "Page 1 of 1", previous: false, next: false });
  } finally {
    await view.unmount();
  }
});

test("an account deleted by another session empties the last page; closing the refusal steps back", async () => {
  const client = fake();
  const view = await mount(<IdentityProvider client={client}><SiteUsersPanel pageSize={4} /></IdentityProvider>);
  try {
    await page(view.container, "Users pages", "Next");
    await page(view.container, "Users pages", "Next");
    assert.equal(userIds(view.container), "9");
    await runUserAction(view.container, "Iris Ivanova", "Ban user");
    await client.adminDeleteUser("9");
    await submit(dialog(view.container).querySelector("form"));
    assert.match(textOf(dialog(view.container)), /no longer exists/u);
    await click(button(dialog(view.container), "Cancel"));
    assert.equal(userIds(view.container), "5,6,7,8");
    assert.doesNotMatch(textOf(view.container), /No users yet/u, "a directory with eight accounts is not empty");
    assert.equal(pager(view.container, "Users pages")?.position, "Page 2 of 2");
  } finally {
    await view.unmount();
  }
});
