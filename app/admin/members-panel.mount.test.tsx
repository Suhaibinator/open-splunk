import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { act, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import {
  FAKE_EXPIRED_INVITATION_ID,
  FAKE_PENDING_INVITATION_ID,
  createFakeIdentityClient,
  type FakeIdentityClient,
} from "@/lib/api/identity-fake";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";

import { IdentityProvider } from "../_components/identity-provider";
import * as modalModule from "../_components/modal";
import { MembersPanel, MembersSection } from "./members-panel";

const NOW = Date.parse("2026-10-24T12:00:00.000Z");

interface Mounted {
  readonly container: FakeElement;
  unmount(): Promise<void>;
}

/**
 * The modal's focus trap and inert siblings need layout the fake DOM does not
 * have, and they hold their own contract (`modal.test.tsx`); here a dialog is
 * its title, body and footer, so the panel's own behaviour is what is tested.
 */
function stubModal(): () => void {
  const real = modalModule.Modal;
  const mutable = modalModule as unknown as { Modal: typeof real };
  mutable.Modal = ({ title, subtitle, children, footer }: { title: string; subtitle?: string; children: ReactNode; footer?: ReactNode }) => (
    <div data-dialog={title}>
      <h2>{title}</h2>
      {subtitle === undefined ? null : <p data-dialog-subtitle="">{subtitle}</p>}
      {children}
      {footer}
    </div>
  );
  return () => {
    mutable.Modal = real;
  };
}

const restoreModal = stubModal();
test.after(() => {
  restoreModal();
});

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

async function tick(): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      setImmediate(resolve);
    });
  });
}

/** Lets a client call, the render it causes, and the effects that render starts all finish. */
async function settle(): Promise<void> {
  await tick();
  await tick();
  await tick();
}

function fake(email: string | null = "ada@open-splunk.test"): FakeIdentityClient {
  return createFakeIdentityClient({ initialSignedInEmail: email, now: () => NOW });
}

async function mountPanel(client: IdentityClient): Promise<Mounted> {
  return mount(<IdentityProvider client={client}><MembersPanel now={() => NOW} /></IdentityProvider>);
}

function state(container: FakeElement): string | null {
  return container.querySelector("[data-members-state]")?.getAttribute("data-members-state") ?? null;
}

function buttonNamed(scope: FakeElement, name: string): FakeElement {
  const button = scope.querySelectorAll("button").find((element) => (
    element.getAttribute("aria-label") === name || element.textContent.trim() === name
  ));
  assert.ok(button, `no button named ${JSON.stringify(name)} in: ${scope.textContent}`);
  return button;
}

function hasButton(scope: FakeElement, name: string): boolean {
  return scope.querySelectorAll("button").some((element) => element.getAttribute("aria-label") === name || element.textContent.trim() === name);
}

async function click(element: FakeElement): Promise<void> {
  await act(async () => {
    element.dispatchEvent(fakeEvent("click"));
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
    // A browser checks the radio before it dispatches the click.
    radio.checked = true;
    radio.dispatchEvent(fakeEvent("click"));
  });
}

async function submit(scope: FakeElement, formId: string): Promise<void> {
  const form = scope.querySelector(`form#${formId}`);
  assert.ok(form, `no form ${formId}`);
  await act(async () => {
    form.dispatchEvent(fakeEvent("submit"));
  });
  await settle();
}

function dialog(container: FakeElement, title: string): FakeElement {
  const found = container.querySelector(`[data-dialog="${title}"]`);
  assert.ok(found, `no dialog titled ${JSON.stringify(title)}`);
  return found;
}

function memberRow(container: FakeElement, userId: string): FakeElement | null {
  return container.querySelector(`tr[data-member-id="${userId}"]`);
}

function invitationRow(container: FakeElement, id: string): FakeElement | null {
  return container.querySelector(`tr[data-invitation-id="${id}"]`);
}

function cell(row: FakeElement, label: string): string {
  return row.querySelector(`td[data-label="${label}"]`)?.textContent ?? "";
}

function toast(container: FakeElement): string {
  return container.querySelector("[data-members-announcement]")?.textContent ?? "";
}

test("the panel shows a loading state until the principal is read", async () => {
  const base = fake();
  const { promise: gate, resolve: release } = Promise.withResolvers<void>();
  const client = Object.create(base) as IdentityClient;
  client.getPrincipal = async () => {
    await gate;
    return base.getPrincipal();
  };
  const view = await mountPanel(client);
  try {
    assert.equal(state(view.container), "loading");
    assert.match(view.container.textContent, /Checking your access/u);
    release();
    await settle();
    assert.equal(state(view.container), "ready");
  } finally {
    await view.unmount();
  }
});

test("a backend page without server identity explains that identity is not configured", async () => {
  const view = await mount(<MembersSection dataMode="backend" />);
  try {
    assert.equal(state(view.container), "unavailable");
    assert.match(view.container.textContent, /Identity is not configured/u);
    assert.equal(hasButton(view.container, "Try again"), false, "a missing configuration is not retryable");
  } finally {
    await view.unmount();
  }
});

test("an identity outage before anyone is known can be retried", async () => {
  const client = fake();
  client.failNext("getPrincipal");
  const view = await mountPanel(client);
  try {
    assert.equal(state(view.container), "unavailable");
    assert.match(view.container.textContent, /could not be reached/u);
    await click(buttonNamed(view.container, "Try again"));
    assert.equal(state(view.container), "ready");
  } finally {
    await view.unmount();
  }
});

test("a signed-out viewer is sent to sign in", async () => {
  const view = await mountPanel(fake(null));
  try {
    assert.equal(state(view.container), "signed_out");
    const signIn = view.container.querySelectorAll("a").find((anchor) => anchor.textContent === "Sign in");
    assert.ok(signIn, view.container.textContent);
    // The export appends the trailing slash (`trailingSlash: true`); next/link under node:test does not.
    assert.match(signIn.getAttribute("href") ?? "", /^\/signin\/?$/u);
  } finally {
    await view.unmount();
  }
});

test("a user without an organization is told how to join one", async () => {
  const view = await mountPanel(fake("nora@open-splunk.test"));
  try {
    assert.equal(state(view.container), "no_organization");
    assert.match(view.container.textContent, /Accept an invitation/u);
  } finally {
    await view.unmount();
  }
});

for (const [email, role] of [["mark@open-splunk.test", "a member"], ["mona@open-splunk.test", "a moderator"]] as const) {
  test(`${role} sees why the panel is closed to them, and no invite control`, async () => {
    const view = await mountPanel(fake(email));
    try {
      assert.equal(state(view.container), "forbidden");
      assert.match(view.container.textContent, new RegExp(`needs the Admin role\\. You are ${role}\\.`, "u"));
      assert.equal(hasButton(view.container, "Invite member"), false);
      assert.equal(view.container.querySelector("table") === null, true);
    } finally {
      await view.unmount();
    }
  });
}

test("an owner sees every member with explicit and effective roles, and the pending invitations", async () => {
  const view = await mountPanel(fake());
  try {
    assert.equal(state(view.container), "ready");
    const rows = view.container.querySelectorAll("tr[data-member-id]");
    assert.equal(rows.length, 7);
    const ada = memberRow(view.container, "1");
    assert.ok(ada);
    assert.match(cell(ada, "Member"), /Ada Lovelace \(you\)/u);
    assert.match(cell(ada, "Explicit role"), /Owner.*Last owner/u);
    const adaChange = buttonNamed(ada, "Change role for Ada Lovelace");
    assert.equal(adaChange.getAttribute("aria-disabled"), "true");
    assert.match(cell(ada, "Actions"), /last owner cannot be demoted or removed/u);
    const describedBy = adaChange.getAttribute("aria-describedby");
    assert.ok(describedBy !== null && ada.querySelector(`small[id="${describedBy}"]`), "the lock reason is not attached to the control");

    const bart = memberRow(view.container, "8");
    assert.ok(bart);
    assert.match(cell(bart, "Status"), /Banned/u);

    assert.match(view.container.textContent, /7 members/u);
    const pending = invitationRow(view.container, FAKE_PENDING_INVITATION_ID);
    const expired = invitationRow(view.container, FAKE_EXPIRED_INVITATION_ID);
    assert.ok(pending && expired);
    assert.match(cell(pending, "Expires"), /Pending/u);
    assert.match(cell(pending, "Invited by"), /Alice Abara/u);
    assert.match(cell(expired, "Expires"), /Expired/u);
    assert.match(cell(expired, "Invited by"), /You/u);
  } finally {
    await view.unmount();
  }
});

test("an inherited role reads as inherited and offers no removal", async () => {
  const client = fake();
  await client.setActiveOrganization("3");
  const view = await mountPanel(client);
  try {
    const alice = memberRow(view.container, "3");
    assert.ok(alice);
    assert.match(cell(alice, "Explicit role"), /None/u);
    assert.match(cell(alice, "Effective role"), /Admin.*Inherited from Acme Operations/u);
    assert.equal(buttonNamed(alice, "Remove Alice Abara").getAttribute("aria-disabled"), "true");
    assert.match(cell(alice, "Actions"), /Inherited from Acme Operations; remove them there\./u);
    // A disabled action does nothing when activated.
    await click(buttonNamed(alice, "Remove Alice Abara"));
    assert.equal(view.container.querySelector('[data-dialog="Remove Alice Abara?"]') === null, true);
  } finally {
    await view.unmount();
  }
});

test("an admin may only grant roles below its own and cannot change an owner", async () => {
  const view = await mountPanel(fake("alice@open-splunk.test"));
  try {
    const owner = memberRow(view.container, "1");
    assert.ok(owner);
    assert.equal(buttonNamed(owner, "Change role for Ada Lovelace").getAttribute("aria-disabled"), "true");
    const self = memberRow(view.container, "3");
    assert.ok(self);
    assert.match(cell(self, "Actions"), /Ask an owner to change your own role\./u);

    await click(buttonNamed(view.container, "Invite member"));
    const invite = dialog(view.container, "Invite member");
    const disabled = invite.querySelectorAll('input[name="members-invite-role"]').filter((radio) => radio.hasAttribute("disabled")).map((radio) => radio.value);
    assert.deepEqual(disabled, ["admin", "owner"]);
    await click(buttonNamed(invite, "Cancel"));

    await click(buttonNamed(view.container, "Change role for Mona Moreau"));
    const roleDialog = dialog(view.container, "Change role for Mona Moreau");
    const roleDisabled = roleDialog.querySelectorAll('input[name="members-role"]').filter((radio) => radio.hasAttribute("disabled")).map((radio) => radio.value);
    assert.deepEqual(roleDisabled, ["admin", "owner"]);
    assert.ok(buttonNamed(roleDialog, "Save role").hasAttribute("disabled"), "saving the current role must be disabled");
    await choose(roleDialog, "members-role", "member");
    assert.match(roleDialog.textContent, /Loses.*Edit knowledge objects/u);
    await submit(roleDialog, "members-role-form");
    const mona = memberRow(view.container, "4");
    assert.ok(mona);
    assert.match(cell(mona, "Explicit role"), /Member/u);
    assert.match(toast(view.container), /Mona Moreau's role is now member\./u);
    assert.equal(view.container.querySelector('[data-dialog="Change role for Mona Moreau"]') === null, true);
  } finally {
    await view.unmount();
  }
});

test("inviting validates the address, sends the invitation and lists it", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    await click(buttonNamed(view.container, "Invite member"));
    let invite = dialog(view.container, "Invite member");
    const email = invite.querySelector("#members-invite-email");
    assert.ok(email);

    await submit(invite, "members-invite-form");
    assert.equal(email.getAttribute("aria-invalid"), "true");
    assert.match(invite.textContent, /Enter the email address to invite\./u);
    assert.equal(client.sentMessages().length, 0, "an empty address must not reach the server");

    await type(email, "new.person@example.com");
    await choose(invite, "members-invite-role", "owner");
    assert.match(invite.textContent, /Owners can manage everything/u, "inviting an owner warns what the role holds");
    await choose(invite, "members-invite-role", "moderator");
    assert.equal(/Owners can manage everything/u.test(invite.textContent), false, "the warning follows the chosen role");
    await submit(invite, "members-invite-form");
    assert.equal(view.container.querySelector('[data-dialog="Invite member"]') === null, true);
    assert.match(toast(view.container), /Invitation sent to new\.person@example\.com as moderator\./u);
    const sent = client.lastMessage("organization_invitation", "new.person@example.com");
    assert.ok(sent);
    const row = invitationRow(view.container, sent.token);
    assert.ok(row, "the new invitation is not listed");
    assert.match(cell(row, "Role"), /Moderator/u);

    await click(buttonNamed(view.container, "Invite member"));
    invite = dialog(view.container, "Invite member");
    const again = invite.querySelector("#members-invite-email");
    assert.ok(again);
    await type(again, "new.person@example.com");
    await submit(invite, "members-invite-form");
    assert.equal(again.getAttribute("aria-invalid"), "true");
    assert.match(invite.textContent, /already has a pending invitation/u);

    await type(again, "mark@open-splunk.test");
    assert.equal(again.getAttribute("aria-invalid"), null, "editing clears the server's complaint");
    await submit(invite, "members-invite-form");
    assert.match(invite.textContent, /already a member/u);
  } finally {
    await view.unmount();
  }
});

test("an invitation that fails in transit keeps the dialog open with an alert", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    await click(buttonNamed(view.container, "Invite member"));
    const invite = dialog(view.container, "Invite member");
    const email = invite.querySelector("#members-invite-email");
    assert.ok(email);
    await type(email, "later@example.com");
    client.failNext("inviteMember");
    await submit(invite, "members-invite-form");
    const alert = invite.querySelector('[role="alert"]');
    assert.ok(alert);
    assert.match(alert.textContent, /Could not send the invitation.*could not be reached/u);
    assert.equal(buttonNamed(invite, "Send invitation").hasAttribute("disabled"), false, "the admin can retry");

    client.failNext("inviteMember", new IdentityError("rate_limited", "Too many", { retryAfterMs: 4000 }));
    await submit(invite, "members-invite-form");
    assert.match(invite.querySelector('[role="alert"]')?.textContent ?? "", /Wait 4 seconds/u);

    await submit(invite, "members-invite-form");
    assert.equal(view.container.querySelector('[data-dialog="Invite member"]') === null, true);
  } finally {
    await view.unmount();
  }
});

test("an owner hands over ownership: promote another owner, then lower its own role", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    await click(buttonNamed(view.container, "Change role for Owen Ortiz"));
    let roleDialog = dialog(view.container, "Change role for Owen Ortiz");
    await choose(roleDialog, "members-role", "owner");
    assert.match(roleDialog.textContent, /Owners can manage everything/u);
    assert.match(roleDialog.textContent, /Gains.*Administer the organization/u);
    await submit(roleDialog, "members-role-form");
    const owen = memberRow(view.container, "2");
    assert.ok(owen);
    assert.match(cell(owen, "Explicit role"), /Owner/u);

    const ada = memberRow(view.container, "1");
    assert.ok(ada);
    assert.equal(buttonNamed(ada, "Change role for Ada Lovelace").getAttribute("aria-disabled"), null, "a second owner lifts the last-owner lock");
    await click(buttonNamed(ada, "Change role for Ada Lovelace"));
    roleDialog = dialog(view.container, "Change your role");
    await choose(roleDialog, "members-role", "admin");
    assert.match(roleDialog.textContent, /You are lowering your own role/u);
    assert.match(roleDialog.textContent, /keep full access as a site admin/u);
    assert.equal(buttonNamed(roleDialog, "Lower my role").getAttribute("type"), "submit");
    await submit(roleDialog, "members-role-form");
    assert.match(toast(view.container), /Your role is now admin\./u);
    const after = memberRow(view.container, "1");
    assert.ok(after);
    assert.match(cell(after, "Explicit role"), /Admin/u);
    assert.equal(state(view.container), "ready", "a site admin keeps the panel after lowering its role");
  } finally {
    await view.unmount();
  }
});

test("an owner without site admin hands over ownership by promoting another owner and leaving", async () => {
  const client = fake("owen@open-splunk.test");
  await client.setActiveOrganization("2");
  const view = await mountPanel(client);
  try {
    const owenRow = () => {
      const row = memberRow(view.container, "2");
      assert.ok(row);
      return row;
    };
    assert.match(cell(owenRow(), "Actions"), /last owner cannot be demoted or removed/u);
    assert.equal(buttonNamed(owenRow(), "Leave organization").getAttribute("aria-disabled"), "true");

    await click(buttonNamed(view.container, "Change role for Mark Mendez"));
    const roleDialog = dialog(view.container, "Change role for Mark Mendez");
    await choose(roleDialog, "members-role", "owner");
    assert.match(roleDialog.textContent, /then leave the organization or ask a site admin/u);
    await submit(roleDialog, "members-role-form");
    const mark = memberRow(view.container, "5");
    assert.ok(mark);
    assert.match(cell(mark, "Explicit role"), /Owner/u);

    // go-common never lets an owner outrank itself, so its own role stays locked; leaving is open.
    assert.equal(buttonNamed(owenRow(), "Change role for Owen Ortiz").getAttribute("aria-disabled"), "true");
    assert.match(cell(owenRow(), "Actions"), /Owners cannot change their own role/u);
    const leave = buttonNamed(owenRow(), "Leave organization");
    assert.equal(leave.getAttribute("aria-disabled"), null);
    await click(leave);
    const confirm = dialog(view.container, "Leave Globex Security?");
    assert.match(confirm.textContent, /You lose your owner role in Globex Security immediately/u);
    await click(buttonNamed(confirm, "Leave organization"));
    assert.equal(view.container.querySelector('[data-dialog="Leave Globex Security?"]') === null, true);
    // The session is re-read: without a role in Globex the panel closes to them.
    assert.notEqual(state(view.container), "ready");
    assert.equal(hasButton(view.container, "Invite member"), false);
  } finally {
    await view.unmount();
  }
});

test("an admin may leave, and loses the panel once it has", async () => {
  const view = await mountPanel(fake("alice@open-splunk.test"));
  try {
    const self = memberRow(view.container, "3");
    assert.ok(self);
    await click(buttonNamed(self, "Leave organization"));
    const confirm = dialog(view.container, "Leave Acme Operations?");
    assert.match(confirm.textContent, /Only an admin can add you back/u);
    await click(buttonNamed(confirm, "Leave organization"));
    assert.notEqual(state(view.container), "ready");
  } finally {
    await view.unmount();
  }
});

test("a change the server refuses explains it in the dialog and re-reads the session", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    let reads = 0;
    const getPrincipal = client.getPrincipal.bind(client);
    client.getPrincipal = () => {
      reads += 1;
      return getPrincipal();
    };
    await click(buttonNamed(view.container, "Change role for Mark Mendez"));
    const roleDialog = dialog(view.container, "Change role for Mark Mendez");
    await choose(roleDialog, "members-role", "moderator");
    client.failNext("changeMemberRole", new IdentityError("forbidden", "Cannot manage a user with a higher or equal role"));
    await submit(roleDialog, "members-role-form");
    const alert = roleDialog.querySelector('[role="alert"]');
    assert.ok(alert, "the refusal is announced");
    assert.match(alert.textContent, /Could not change the role\..*higher or equal role/u);
    assert.equal(reads, 1, "a refusal may mean the actor's own role changed");
    assert.equal(buttonNamed(roleDialog, "Save role").hasAttribute("disabled"), false, "the dialog stays usable");

    client.failNext("changeMemberRole", new IdentityError("invalid", "Invalid organization role"));
    await submit(roleDialog, "members-role-form");
    assert.match(roleDialog.querySelector('[role="alert"]')?.textContent ?? "", /Invalid organization role/u);
  } finally {
    await view.unmount();
  }
});

test("a child organization locks inherited-only members and shows where the role comes from", async () => {
  const client = fake();
  await client.setActiveOrganization("3");
  const view = await mountPanel(client);
  try {
    const owen = memberRow(view.container, "2");
    assert.ok(owen);
    assert.equal(buttonNamed(owen, "Change role for Owen Ortiz").getAttribute("aria-disabled"), "true");
    assert.match(cell(owen, "Actions"), /Inherited from Acme Operations; change it there\./u);
    const mona = memberRow(view.container, "4");
    assert.ok(mona);
    assert.match(cell(mona, "Explicit role"), /Admin/u);
    assert.equal(buttonNamed(mona, "Change role for Mona Moreau").getAttribute("aria-disabled"), null);
    await click(buttonNamed(mona, "Remove Mona Moreau"));
    assert.match(dialog(view.container, "Remove Mona Moreau?").textContent, /organizations under it/u);
  } finally {
    await view.unmount();
  }
});

test("the server's last-owner guard is shown when the page could not count owners", async () => {
  const client = fake();
  await client.changeMemberRole("1", "2", "owner");
  const view = await mountPanel(client);
  try {
    // A concurrent demotion elsewhere: the panel still believes there are two owners.
    await client.changeMemberRole("1", "2", "member");
    await click(buttonNamed(view.container, "Change role for Ada Lovelace"));
    const roleDialog = dialog(view.container, "Change your role");
    await choose(roleDialog, "members-role", "admin");
    await submit(roleDialog, "members-role-form");
    assert.match(roleDialog.querySelector('[role="alert"]')?.textContent ?? "", /at least one owner/u);
    // The conflict refreshed the list, so the lock is back once the dialog closes.
    await click(buttonNamed(roleDialog, "Cancel"));
    const ada = memberRow(view.container, "1");
    assert.ok(ada);
    assert.equal(buttonNamed(ada, "Change role for Ada Lovelace").getAttribute("aria-disabled"), "true");
  } finally {
    await view.unmount();
  }
});

test("removing a member asks first, then drops the row", async () => {
  const view = await mountPanel(fake());
  try {
    await click(buttonNamed(view.container, "Remove Mark Mendez"));
    const confirm = dialog(view.container, "Remove Mark Mendez?");
    assert.match(confirm.textContent, /loses their member role in Acme Operations immediately/u);
    await click(buttonNamed(confirm, "Cancel"));
    assert.ok(memberRow(view.container, "5"), "cancel must not remove");

    await click(buttonNamed(view.container, "Remove Mark Mendez"));
    await click(buttonNamed(dialog(view.container, "Remove Mark Mendez?"), "Remove member"));
    assert.equal(memberRow(view.container, "5") === null, true);
    assert.match(toast(view.container), /Mark Mendez was removed from Acme Operations\./u);
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 6);
  } finally {
    await view.unmount();
  }
});

test("revoking an invitation asks first; one already gone refreshes the list", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    await click(buttonNamed(view.container, "Revoke invitation for late@open-splunk.test"));
    await click(buttonNamed(dialog(view.container, "Revoke invitation?"), "Revoke invitation"));
    assert.equal(invitationRow(view.container, FAKE_EXPIRED_INVITATION_ID) === null, true);
    assert.match(toast(view.container), /The invitation for late@open-splunk\.test was revoked\./u);

    await click(buttonNamed(view.container, "Revoke invitation for nora@open-splunk.test"));
    await client.revokeInvitation("1", FAKE_PENDING_INVITATION_ID);
    const confirm = dialog(view.container, "Revoke invitation?");
    await click(buttonNamed(confirm, "Revoke invitation"));
    assert.match(confirm.querySelector('[role="alert"]')?.textContent ?? "", /no longer there.*refreshed/u);
    await click(buttonNamed(confirm, "Cancel"));
    assert.match(view.container.textContent, /No pending invitations/u);
  } finally {
    await view.unmount();
  }
});

test("a list that fails to load says why and can be retried", async () => {
  const client = fake();
  client.failNext("listMembers");
  client.failNext("listInvitations", new IdentityError("rate_limited", "slow down", { retryAfterMs: 3000 }));
  const view = await mountPanel(client);
  try {
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 0);
    const alerts = view.container.querySelectorAll('[role="alert"]').map((element) => element.textContent);
    assert.equal(alerts.length, 2);
    assert.match(alerts[0] ?? "", /Could not load members.*could not be reached/u);
    assert.match(alerts[1] ?? "", /Too many requests.*Wait 3 seconds/u);
    const retries = view.container.querySelectorAll("button").filter((button) => button.textContent === "Try again");
    assert.equal(retries.length, 2);
    await click(retries[0] as FakeElement);
    await click(retries[1] as FakeElement);
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 7);
    assert.equal(view.container.querySelectorAll("tr[data-invitation-id]").length, 2);
  } finally {
    await view.unmount();
  }
});

test("a session that ends mid-change returns the panel to the signed-out state", async () => {
  const client = fake();
  const view = await mountPanel(client);
  try {
    await click(buttonNamed(view.container, "Remove Tess Takahashi"));
    await client.logout();
    await click(buttonNamed(dialog(view.container, "Remove Tess Takahashi?"), "Remove member"));
    assert.equal(state(view.container), "signed_out");
  } finally {
    await view.unmount();
  }
});

test("members page through the server's pages", async () => {
  const base = fake();
  const client = Object.create(base) as IdentityClient;
  // Three to a page puts the seven Acme members on three pages.
  client.listMembers = (organizationId, page) => base.listMembers(organizationId, { page: page?.page ?? 1, limit: 3 });
  const view = await mountPanel(client);
  try {
    const pagerText = () => view.container.querySelector('nav[aria-label="Members pages"]')?.textContent ?? "";
    const pager = () => {
      const found = view.container.querySelector('nav[aria-label="Members pages"]');
      assert.ok(found, "no members pager");
      return found;
    };
    assert.match(pagerText(), /Page 1 of 3/u);
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 3);
    assert.equal(buttonNamed(pager(), "Previous").hasAttribute("disabled"), true);
    // With other pages unseen, the owner count is unknown and the server guards the last owner.
    const ada = memberRow(view.container, "1");
    assert.ok(ada);
    assert.equal(buttonNamed(ada, "Change role for Ada Lovelace").getAttribute("aria-disabled"), null);
    await click(buttonNamed(pager(), "Next"));
    await click(buttonNamed(pager(), "Next"));
    assert.match(pagerText(), /Page 3 of 3/u);
    assert.equal(view.container.querySelectorAll("tr[data-member-id]").length, 1);
    assert.equal(buttonNamed(pager(), "Next").hasAttribute("disabled"), true);
    assert.equal(view.container.querySelector('nav[aria-label="Invitation pages"]') === null, true, "one page of invitations needs no pager");
  } finally {
    await view.unmount();
  }
});

test("the demo section mounts its own provider over the demo directory", async () => {
  const view = await mount(<MembersSection dataMode="demo" now={() => NOW} />);
  try {
    assert.equal(state(view.container), "ready");
    assert.match(view.container.textContent, /Acme Operations/u);
  } finally {
    await view.unmount();
  }
});
