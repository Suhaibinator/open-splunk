import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { fakeEvent, type FakeElement, type FakeNode } from "@/lib/testing/fake-dom";
import { Children, act, isValidElement, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import type {
  AdminUser,
  IdentityClient,
  IdentityOrganization,
  OrganizationInvitation,
  OrganizationMember,
  Principal,
} from "@/lib/api/identity";
import { createFakeIdentityClient } from "@/lib/api/identity-fake";
import { leadingCharacter } from "@/lib/product-access";
import {
  CJK_ORGANIZATION,
  EMOJI_FIRST_NAME,
  FLAG_LAST_NAME,
  HEBREW_FIRST_NAME,
  HEBREW_LAST_NAME,
  LONG_EMAIL,
  LONG_WORD_ORGANIZATION,
  MARKUP_ORGANIZATION,
  MIXED_DESCRIPTION,
  RTL_ORGANIZATION,
  isWellFormed,
} from "@/lib/testing/hostile-text";

import { MembersPanel } from "../admin/members-panel";
import { SiteOrganizationsPanel } from "../admin/site-organizations-panel";
import { SiteUsersPanel } from "../admin/site-users-panel";
import { NoOrganizationCard, PrincipalSummary } from "../signin/sign-in-completion";
import { IdentityProvider } from "./identity-provider";
import * as modalModule from "./modal";
import { ProductShell } from "./product-shell";
import * as selectModule from "./select";

/*
 * The identity surfaces mounted for real with the names people actually type:
 * Arabic and Hebrew, one 225-character word, CJK, an emoji with a skin tone
 * and a joiner, a flag, markup, and an address at the length the RFCs allow.
 * Each one has to arrive whole (nothing truncates it in script; CSS decides
 * how it folds), escaped (markup stays text), with a well-formed initial (no
 * half of a surrogate pair), and bidi-isolated wherever it stands on its own,
 * so a right-to-left value cannot pull the punctuation around it into its own
 * run. Assertions compare strings and booleans only: a failing deepEqual over
 * a DOM node diffs the whole fake document.
 */

const NOW = Date.parse("2026-10-24T12:00:00.000Z");

const realModal = modalModule.Modal;
const realSelect = selectModule.Select;

function StubModal({ title, children, footer }: { title: string; children: ReactNode; footer?: ReactNode }) {
  return <div data-modal={title}><h2>{title}</h2>{children}{footer}</div>;
}

function StubSelect({ id, value, children }: { id?: string; value?: string; children?: ReactNode }) {
  return (
    <div id={id} data-select-value={value}>
      {Children.toArray(children).filter(isValidElement).map((child) => {
        const option = child as ReactElement<{ value: string; children: ReactNode }>;
        return <span key={option.props.value}>{option.props.children}</span>;
      })}
    </div>
  );
}

const reactErrors: string[] = [];
const consoleError = console.error;

interface FakeLocation {
  hash: string;
  href?: string;
  pathname: string;
  search: string;
  assign?: (href: string) => void;
  replace?: (href: string) => void;
}

const fakeWindow = window as unknown as { location: FakeLocation; requestAnimationFrame(callback: FrameRequestCallback): number };

test.before(() => {
  (modalModule as unknown as { Modal: unknown }).Modal = StubModal;
  (selectModule as unknown as { Select: unknown }).Select = StubSelect;
  console.error = (...values: unknown[]) => {
    reactErrors.push(values.map((value) => String(value).slice(0, 300)).join(" "));
  };
  fakeWindow.requestAnimationFrame = (callback) => {
    callback(0);
    return 1;
  };
  fakeWindow.location.assign = () => undefined;
  fakeWindow.location.replace = () => undefined;
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

function at(pathname: string) {
  fakeWindow.location.pathname = pathname;
  fakeWindow.location.search = "";
  fakeWindow.location.hash = "";
  fakeWindow.location.href = `https://workspace.test${pathname}`;
}

async function settle(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, 10);
    });
  });
}

async function mount(node: ReactNode) {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  // The principal, then the panel's own reads, then the renders they cause.
  await settle();
  await settle();
  await settle();
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

async function click(element: FakeElement | undefined, what: string): Promise<void> {
  assert.equal(element === undefined, false, `${what} is rendered`);
  await act(async () => element?.dispatchEvent(Object.assign(fakeEvent("click"), { detail: 1 })));
  await settle();
}

/** A client that answers like the fake, except where `overrides` says otherwise. */
function overriding(base: IdentityClient, overrides: Partial<IdentityClient>): IdentityClient {
  return new Proxy(base, {
    get(target, property, receiver) {
      if (Object.hasOwn(overrides, property)) return overrides[property as keyof IdentityClient];
      const value: unknown = Reflect.get(target, property, receiver);
      return typeof value === "function" ? (value as (...values: unknown[]) => unknown).bind(target) : value;
    },
  });
}

const ORGANIZATION_NAMES = [RTL_ORGANIZATION, LONG_WORD_ORGANIZATION, MARKUP_ORGANIZATION, CJK_ORGANIZATION];

/** Renames every organization the principal sees, the same way wherever it appears. */
function renamedOrganizations(principal: Principal): Principal {
  const names = new Map(principal.organizations.map((entry, index) => [
    entry.organization.id,
    ORGANIZATION_NAMES[index % ORGANIZATION_NAMES.length] ?? RTL_ORGANIZATION,
  ]));
  const rename = (organization: IdentityOrganization): IdentityOrganization => ({ ...organization, name: names.get(organization.id) ?? organization.name });
  return {
    ...principal,
    user: { ...principal.user, firstName: EMOJI_FIRST_NAME, lastName: FLAG_LAST_NAME, email: LONG_EMAIL },
    activeOrganization: principal.activeOrganization === null ? null : rename(principal.activeOrganization),
    organizations: principal.organizations.map((entry) => ({ ...entry, organization: rename(entry.organization) })),
  };
}

function hostileAda(): IdentityClient {
  const fake = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test", now: () => NOW });
  return overriding(fake, {
    getPrincipal: async () => {
      const principal = await fake.getPrincipal();
      return principal === null ? null : renamedOrganizations(principal);
    },
  });
}

/** The `tag` elements under `scope` that carry `className`: the fake DOM matches tags and attributes only. */
function byClass(scope: FakeElement | null | undefined, tag: string, className: string): FakeElement[] {
  if (scope === null || scope === undefined) return [];
  return scope.querySelectorAll(tag).filter((element) => (element.getAttribute("class") ?? "").split(" ").includes(className));
}

function textNodes(node: FakeNode): FakeNode[] {
  return node.childNodes.flatMap((child) => (child.nodeType === 3 ? [child] : textNodes(child)));
}

/** Whether a node sits inside a bidi isolate: a `<bdi>`, or an element with `dir="auto"`. */
function isolated(node: FakeNode, container: FakeElement): boolean {
  for (let current = node.parentNode; current !== null && current !== container; current = current.parentNode) {
    if (current.nodeName === "BDI") return true;
    if (current.nodeType === 1 && (current as FakeElement).getAttribute("dir") === "auto") return true;
  }
  return false;
}

function describeNode(node: FakeNode): string {
  const parent = node.parentNode as FakeElement | null;
  const name = parent === null ? "(detached)" : `${parent.nodeName.toLowerCase()}${parent.getAttribute("class") === null ? "" : `.${parent.getAttribute("class")?.replaceAll(" ", ".")}`}`;
  return `${JSON.stringify(node.textContent.slice(0, 24))}… in ${name}`;
}

/** Every text node that is exactly one of `values` and is not bidi-isolated. */
function unisolated(container: FakeElement, values: readonly string[]): string[] {
  const wanted = new Set(values);
  return textNodes(container)
    .filter((node) => wanted.has(node.textContent.trim()) && !isolated(node, container))
    .map(describeNode);
}

/** The values that do not appear in full anywhere in the rendered text. */
function missing(container: FakeElement, values: readonly string[]): string[] {
  const text = container.textContent;
  return values.filter((value) => !text.includes(value)).map((value) => value.slice(0, 24));
}

/** Every text node that holds half of a surrogate pair. */
function malformed(container: FakeElement): string[] {
  return textNodes(container).filter((node) => !isWellFormed(node.textContent)).map(describeNode);
}

const DISPLAY_NAME = `${EMOJI_FIRST_NAME} ${FLAG_LAST_NAME}`;

test("the shell prints a hostile principal whole, escaped, isolated and with whole initials", async () => {
  at("/datasets/");
  const client = hostileAda();
  const view = await mount(
    <IdentityProvider client={client}>
      <ProductShell activeSection="datasets" appName="Search & Reporting" dataMode="demo"><p>Page content</p></ProductShell>
    </IdentityProvider>,
  );
  try {
    const { container } = view;
    const trigger = container.querySelectorAll("button").find((button) => button.getAttribute("aria-label")?.startsWith("Organization:"));
    assert.equal(trigger?.getAttribute("aria-label")?.includes(RTL_ORGANIZATION), true, "the trigger's accessible name carries the whole active name");
    const activeName = trigger?.querySelector("strong");
    assert.equal(activeName?.getAttribute("title"), RTL_ORGANIZATION, "the truncated active name keeps its full text on title");

    const userButton = container.querySelectorAll("button").find((button) => button.getAttribute("aria-label") === `${DISPLAY_NAME} menu`);
    assert.equal(userButton === undefined, false, "the user button is named for the whole display name");
    assert.equal(userButton?.querySelector("span")?.textContent, "👩🏽‍💻🇳🇿", "the bar's avatar shows one whole character per name");
    assert.equal(userButton?.querySelector("b")?.getAttribute("title"), DISPLAY_NAME, "the truncated bar name keeps its full text on title");

    await click(trigger, "the organization trigger");
    const menu = container.querySelector('[data-suite-menu="org"]');
    assert.equal(menu === null, false, "the organization menu opens");
    const shown = Math.min(ORGANIZATION_NAMES.length, menu?.querySelectorAll('[role="menuitemradio"]').length ?? 0);
    const listed = ORGANIZATION_NAMES.slice(0, shown);
    assert.equal(shown >= 2, true, `the seeded principal lists ${shown} organizations; the corpus needs two`);
    assert.deepEqual(missing(container, listed), [], "every organization name is printed whole");
    assert.deepEqual(unisolated(container, [...listed, DISPLAY_NAME, LONG_EMAIL]), []);
    assert.deepEqual(malformed(container), []);
    const initials = byClass(menu, "i", "suite-org-icon").map((icon) => icon.textContent);
    assert.deepEqual(initials.slice(0, shown), listed.map((name) => leadingCharacter(name).toUpperCase()));
    assert.equal(container.querySelectorAll('img[src="x"]').length, 0, "markup in a name stays text");

    await click(trigger, "the organization trigger");
    await click(userButton, "the user button");
    const userMenu = container.querySelector('[data-suite-menu="user"]');
    assert.equal(userMenu === null, false, "the user menu opens");
    assert.deepEqual(missing(userMenu as FakeElement, [DISPLAY_NAME, LONG_EMAIL]), [], "the summary prints the name and address whole");
    assert.deepEqual(unisolated(container, [DISPLAY_NAME, LONG_EMAIL]), []);
    assert.equal(byClass(userMenu, "div", "suite-user-summary")[0]?.querySelector("span")?.textContent, "👩🏽‍💻🇳🇿");
    assert.deepEqual(malformed(container), []);
  } finally {
    await view.unmount();
  }
});

const MEMBER_NAMES = [`${HEBREW_FIRST_NAME} ${HEBREW_LAST_NAME}`, DISPLAY_NAME, LONG_WORD_ORGANIZATION, `${RTL_ORGANIZATION} (ops)`];
const MEMBER_ADDRESSES = [LONG_EMAIL, "ünïcødé@exämple.test", `${"q".repeat(64)}@example.test`, "a@b.test"];
const INVITED_ADDRESS = `invite.${LONG_EMAIL}`;

/* The rewrites a hostile client applies to each listed row, by its position in the page. */

function hostileMember(member: OrganizationMember, index: number): OrganizationMember {
  return {
    ...member,
    user: {
      ...member.user,
      displayPrimary: MEMBER_NAMES[index % MEMBER_NAMES.length] ?? DISPLAY_NAME,
      displaySecondary: MEMBER_ADDRESSES[index % MEMBER_ADDRESSES.length],
    },
  };
}

function hostileInvitation(invitation: OrganizationInvitation): OrganizationInvitation {
  return { ...invitation, email: INVITED_ADDRESS };
}

function invitationToHostileOrganization(invitation: OrganizationInvitation, index: number): OrganizationInvitation {
  return { ...invitation, organization: { ...invitation.organization, name: ORGANIZATION_NAMES[index % ORGANIZATION_NAMES.length] ?? RTL_ORGANIZATION } };
}

/** Every user but the first (the signed-in administrator), renamed in alternating scripts. */
function hostileAdminUser(admin: AdminUser, index: number): AdminUser {
  if (index === 0) return admin;
  return {
    ...admin,
    user: {
      ...admin.user,
      email: `${index}.${LONG_EMAIL}`,
      firstName: index % 2 === 0 ? HEBREW_FIRST_NAME : EMOJI_FIRST_NAME,
      lastName: index % 2 === 0 ? HEBREW_LAST_NAME : FLAG_LAST_NAME,
      username: `${LONG_WORD_ORGANIZATION.slice(0, 60)}${index}`,
    },
  };
}

/** Every organization renamed; all but the first described at length. */
function hostileDirectoryOrganization(organization: IdentityOrganization, index: number): IdentityOrganization {
  return {
    ...organization,
    description: index === 0 ? undefined : `${MIXED_DESCRIPTION} ${index}`,
    name: `${ORGANIZATION_NAMES[index % ORGANIZATION_NAMES.length] ?? RTL_ORGANIZATION} ${index}`,
  };
}

function hostileMembersClient(): IdentityClient {
  const base = hostileAda();
  return overriding(base, {
    listMembers: async (organizationId, page) => {
      const listed = await base.listMembers(organizationId, page);
      return { ...listed, items: listed.items.map(hostileMember) };
    },
    listInvitations: async (organizationId, page) => {
      const listed = await base.listInvitations(organizationId, page);
      return { ...listed, items: listed.items.map(hostileInvitation) };
    },
  });
}

test("the members panel prints hostile names and addresses whole and isolated", async () => {
  at("/admin/");
  const view = await mount(<IdentityProvider client={hostileMembersClient()}><MembersPanel now={() => NOW} /></IdentityProvider>);
  try {
    const { container } = view;
    assert.equal(container.querySelector("[data-members-state]")?.getAttribute("data-members-state"), "ready");
    const rows = container.querySelectorAll("tr[data-member-id]").length;
    assert.equal(rows >= 2, true, `the seeded organization lists ${rows} members; the corpus needs two`);
    const names = MEMBER_NAMES.slice(0, Math.min(rows, MEMBER_NAMES.length));
    const addresses = MEMBER_ADDRESSES.slice(0, Math.min(rows, MEMBER_ADDRESSES.length));
    assert.deepEqual(missing(container, [...names, ...addresses, RTL_ORGANIZATION]), []);
    const invitations = container.querySelectorAll("tr").filter((row) => row.textContent.includes(INVITED_ADDRESS)).length;
    assert.equal(invitations > 0, true, "the invitation list prints the invited address");
    assert.deepEqual(unisolated(container, [...names, ...addresses, INVITED_ADDRESS, RTL_ORGANIZATION]), []);
    assert.deepEqual(malformed(container), []);
  } finally {
    await view.unmount();
  }
});

function hostileDirectoryClient(): IdentityClient {
  const base = hostileAda();
  return overriding(base, {
    adminListUsers: async (query, page) => {
      const listed = await base.adminListUsers(query, page);
      return { ...listed, items: listed.items.map(hostileAdminUser) };
    },
    adminListOrganizations: async (query, page) => {
      const listed = await base.adminListOrganizations(query, page);
      return { ...listed, items: listed.items.map(hostileDirectoryOrganization) };
    },
  });
}

test("the site directory prints hostile users whole and isolated, address and username apart", async () => {
  at("/admin/");
  const view = await mount(<IdentityProvider client={hostileDirectoryClient()}><SiteUsersPanel /></IdentityProvider>);
  try {
    const { container } = view;
    const rows = container.querySelectorAll("tr[data-user-id]");
    assert.equal(rows.length >= 3, true, `${rows.length} user rows; the corpus needs three`);
    const values = [1, 2].flatMap((index) => {
      const name = index % 2 === 0 ? `${HEBREW_FIRST_NAME} ${HEBREW_LAST_NAME}` : DISPLAY_NAME;
      return [name, `${index}.${LONG_EMAIL}`, `${LONG_WORD_ORGANIZATION.slice(0, 60)}${index}`];
    });
    assert.deepEqual(missing(container, values), []);
    assert.deepEqual(unisolated(container, values), []);
    assert.deepEqual(malformed(container), []);
  } finally {
    await view.unmount();
  }
});

test("the site directory caps a long description on a block that carries the whole text on title", async () => {
  at("/admin/");
  const view = await mount(<IdentityProvider client={hostileDirectoryClient()}><SiteOrganizationsPanel /></IdentityProvider>);
  try {
    const { container } = view;
    const rows = container.querySelectorAll("tr[data-organization-id]");
    assert.equal(rows.length >= 2, true, `${rows.length} organization rows; the corpus needs two`);
    const descriptions = byClass(container, "span", "site-directory-description");
    assert.equal(descriptions.length, rows.length - 1, "every described row caps its description; the undescribed one says so");
    assert.equal(descriptions.every((block) => block.nodeName === "SPAN" && block.getAttribute("title") === block.textContent), true);
    assert.equal(descriptions.every((block) => block.parentNode?.nodeName === "TD"), true, "the cap sits inside the cell, not on it");
    assert.equal(byClass(container, "td", "site-directory-description").length, 0, "a cell's own max-width does not bound its column");
    assert.equal(byClass(container, "span", "site-directory-muted").some((note) => note.textContent === "No description"), true);
    const names = rows.map((_, index) => `${ORGANIZATION_NAMES[index % ORGANIZATION_NAMES.length] ?? RTL_ORGANIZATION} ${index}`);
    assert.deepEqual(missing(container, names), []);
    assert.deepEqual(unisolated(container, [...names, ...descriptions.map((block) => block.textContent)]), []);
    assert.deepEqual(malformed(container), []);
    assert.equal(container.querySelectorAll('img[src="x"]').length, 0, "markup in a name stays text");
  } finally {
    await view.unmount();
  }
});

test("the sign-in card greets a hostile principal by its whole name, with whole initials", async () => {
  const fake = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test", now: () => NOW });
  const principal = await fake.getPrincipal();
  assert.equal(principal === null, false);
  if (principal === null) return;
  const hostile = renamedOrganizations(principal);
  const view = await mount(<PrincipalSummary principal={hostile} />);
  try {
    const { container } = view;
    assert.equal(byClass(container, "span", "signin-identity-mark")[0]?.textContent, "👩🏽‍💻🇳🇿");
    assert.deepEqual(missing(container, [DISPLAY_NAME, LONG_EMAIL, RTL_ORGANIZATION]), []);
    assert.deepEqual(unisolated(container, [DISPLAY_NAME, LONG_EMAIL, RTL_ORGANIZATION]), []);
    assert.deepEqual(malformed(container), []);
  } finally {
    await view.unmount();
  }
});

test("the no-organization card lists invitations to hostile organizations whole and isolated", async () => {
  const fake = createFakeIdentityClient({ initialSignedInEmail: "nora@open-splunk.test", now: () => NOW });
  const principal = await fake.getPrincipal();
  assert.equal(principal === null, false);
  if (principal === null) return;
  const client = overriding(fake, {
    getPrincipal: async () => principal,
    listMyInvitations: async (page) => {
      const listed = await fake.listMyInvitations(page);
      return { ...listed, items: listed.items.map(invitationToHostileOrganization) };
    },
  });
  const hostile: Principal = { ...principal, user: { ...principal.user, firstName: HEBREW_FIRST_NAME, lastName: HEBREW_LAST_NAME } };
  const view = await mount(
    <IdentityProvider client={client}>
      <NoOrganizationCard principal={hostile} navigate={() => undefined} onSignOut={() => undefined} />
    </IdentityProvider>,
  );
  try {
    const { container } = view;
    const invitations = container.querySelector('[aria-label="Pending invitations"]')?.querySelectorAll("li").length ?? 0;
    assert.equal(invitations > 0, true, "nora's pending invitation is listed");
    const names = ORGANIZATION_NAMES.slice(0, Math.min(invitations, ORGANIZATION_NAMES.length));
    assert.equal(byClass(container, "span", "signin-identity-mark")[0]?.textContent, "דל");
    assert.deepEqual(missing(container, [...names, `${HEBREW_FIRST_NAME} ${HEBREW_LAST_NAME}`]), []);
    assert.deepEqual(unisolated(container, [...names, `${HEBREW_FIRST_NAME} ${HEBREW_LAST_NAME}`]), []);
    assert.deepEqual(malformed(container), []);
  } finally {
    await view.unmount();
  }
});
