import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the fake browser has to be on `globalThis` before
// `react-dom/client` is evaluated, because that module decides at load time
// whether it has a DOM.
import { browser } from "@/lib/testing/fake-browser";
import { Children, act, isValidElement, type ReactElement, type ReactNode } from "react";
import { createRoot } from "react-dom/client";

import type { IdentityClient } from "@/lib/api/identity";
import { FAKE_SITE_ADMIN_EMAIL, createFakeIdentityClient } from "@/lib/api/identity-fake";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";

import { IdentityProvider } from "../_components/identity-provider";
import * as modalModule from "../_components/modal";
import * as selectModule from "../_components/select";
import { MembersPanel } from "./members-panel";
import { SiteOrganizationsPanel } from "./site-organizations-panel";
import { SiteUsersPanel } from "./site-users-panel";

/*
 * Keyboard-only and screen-reader paths through the identity administration
 * panels: where focus goes while a request is pending and after it is
 * refused, whether each control's name and description are the ones a screen
 * reader should read, and that a page turn keeps focus on the pager.
 *
 * The fake DOM refuses focus to a disabled control and takes focus from a
 * control as it becomes disabled, as a browser does; that is what makes a
 * focus drop observable here. The shared Modal is a plain `<dialog>` (its
 * focus trap has its own contract) and the Select a row of buttons.
 * Assertions compare primitives only.
 */

const NOW = Date.parse("2026-10-24T12:00:00.000Z");
const realModal = modalModule.Modal;
const realSelect = selectModule.Select;

function StubModal({ title, children, footer }: { title: string; children: ReactNode; footer?: ReactNode }) {
  return (
    <dialog aria-label={title} data-modal={title} open>
      {children}
      {footer}
    </dialog>
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

async function wait(milliseconds = 5): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, milliseconds);
    });
  });
}

interface Mounted {
  readonly container: FakeElement;
  unmount(): Promise<void>;
}

async function mount(node: ReactNode, settleMs = 5): Promise<Mounted> {
  const container = browser.document.body.appendChild(browser.document.createElement("div"));
  const root = createRoot(container as unknown as Element);
  await act(async () => root.render(node));
  await wait(settleMs);
  return {
    container,
    unmount: async () => {
      await act(async () => root.unmount());
      container.parentNode?.removeChild(container);
    },
  };
}

function textOf(element: FakeElement): string {
  return element.textContent.replaceAll(/\s+/gu, " ").trim();
}

function button(scope: FakeElement, name: string): FakeElement {
  const found = scope.querySelectorAll("button").find((candidate) => candidate.getAttribute("aria-label") === name || textOf(candidate) === name);
  assert.ok(found, `no button ${JSON.stringify(name)} in: ${textOf(scope).slice(0, 400)}`);
  return found;
}

function dialog(scope: FakeElement, title: string): FakeElement {
  const found = scope.querySelector(`[data-modal="${title}"]`);
  assert.ok(found, `no dialog ${title}`);
  return found;
}

function form(scope: FakeElement): FakeElement {
  const found = scope.querySelector("form");
  assert.ok(found, "no form");
  return found;
}

async function click(element: FakeElement, settleMs = 5): Promise<void> {
  await act(async () => {
    element.focus();
    element.dispatchEvent(fakeEvent("click"));
  });
  await wait(settleMs);
}

async function type(input: FakeElement, value: string): Promise<void> {
  await act(async () => {
    input.value = value;
    input.dispatchEvent(fakeEvent("input"));
  });
}

async function submit(target: FakeElement, settleMs = 5): Promise<void> {
  await act(async () => {
    target.dispatchEvent(fakeEvent("submit"));
  });
  await wait(settleMs);
}

async function focus(element: FakeElement): Promise<void> {
  await act(async () => {
    element.focus();
  });
}

function active(): FakeElement | null {
  return browser.document.activeElement;
}

function textOfIds(ids: string | null): string {
  return (ids ?? "").split(/\s+/u).filter((id) => id.length > 0).map((id) => {
    const target = browser.document.getElementById(id);
    assert.ok(target, `#${id} is referenced, so it must exist`);
    return textOf(target);
  }).join(" ");
}

/** A client whose `method` waits for `release()` before answering, counting its calls. */
function holding<K extends "inviteMember" | "changeMemberRole" | "removeMember">(base: IdentityClient, method: K) {
  const client = Object.create(base) as IdentityClient;
  let gate: Promise<void> = Promise.resolve();
  const control: { open?: () => void } = {};
  const state = { calls: 0 };
  const original = base[method].bind(base) as (...values: unknown[]) => Promise<unknown>;
  (client as unknown as Record<string, unknown>)[method] = async (...values: unknown[]) => {
    state.calls += 1;
    await gate;
    return original(...values);
  };
  return {
    client,
    state,
    hold() {
      gate = new Promise((resolve) => { control.open = resolve; });
    },
    release() {
      control.open?.();
    },
  };
}

function members(client: IdentityClient): ReactNode {
  return <IdentityProvider client={client}><MembersPanel now={() => NOW} /></IdentityProvider>;
}

function owner() {
  return createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test", now: () => NOW });
}

// --------------------------------------------------------------------------
// Members & invitations
// --------------------------------------------------------------------------

test("a refused invitation returns focus to the email field it disabled while sending", async () => {
  const held = holding(owner(), "inviteMember");
  const view = await mount(members(held.client));
  try {
    await click(button(view.container, "Invite member"));
    const invite = dialog(view.container, "Invite member");
    const email = invite.querySelector("#members-invite-email");
    assert.ok(email);
    await type(email, "nora@open-splunk.test");
    await focus(email);

    held.hold();
    await submit(form(invite));
    const send = button(invite, "Sending…");
    assert.equal(email.disabled, true, "the field is disabled while the invitation is in flight");
    assert.equal(send.getAttribute("aria-disabled"), "true");
    assert.equal(send.hasAttribute("disabled"), false, "a disabled submit button would drop focus");

    await submit(form(invite));
    await click(send);
    assert.equal(held.state.calls, 1, "a pending invitation is not sent twice");

    held.release();
    await wait(10);
    assert.equal(email.getAttribute("aria-invalid"), "true");
    assert.match(textOfIds(email.getAttribute("aria-describedby")), /pending invitation/u);
    assert.equal(active() === email, true, "focus returns to the field the server refused");
  } finally {
    await view.unmount();
  }
});

test("Enter in an empty invite email speaks the error that focus cannot", async () => {
  const view = await mount(members(owner()));
  try {
    await click(button(view.container, "Invite member"));
    const invite = dialog(view.container, "Invite member");
    const email = invite.querySelector("#members-invite-email");
    assert.ok(email);
    await focus(email);
    await submit(form(invite));
    assert.equal(active() === email, true);
    const region = form(invite).querySelectorAll('[role="alert"]').find((candidate) => candidate.nodeName === "SPAN");
    assert.ok(region, "the invite form keeps an announcement region mounted");
    assert.equal(region.textContent, textOfIds(email.getAttribute("aria-describedby")));
    assert.match(region.textContent, /Enter the email address to invite/u);
  } finally {
    await view.unmount();
  }
});

test("each role choice is named by its role alone and described by its note", async () => {
  const view = await mount(members(owner()));
  try {
    await click(button(view.container, "Change role for Owen Ortiz"));
    const roleDialog = dialog(view.container, "Change role for Owen Ortiz");
    const radios = roleDialog.querySelectorAll('input[name="members-role"]');
    assert.equal(radios.length, 4);
    for (const radio of radios) {
      const name = textOfIds(radio.getAttribute("aria-labelledby"));
      const note = textOfIds(radio.getAttribute("aria-describedby"));
      assert.match(name, /^(Member|Moderator|Admin|Owner)( \(current\))?$/u, `radio ${radio.value} is named ${name}`);
      assert.notEqual(note, "", `radio ${radio.value} has a description`);
      assert.equal(name.includes(note), false, `radio ${radio.value}'s name does not repeat its description`);
    }
  } finally {
    await view.unmount();
  }
});

test("a role change refused after Enter on a choice puts focus back on the chosen role", async () => {
  const base = owner();
  const held = holding(base, "changeMemberRole");
  const view = await mount(members(held.client));
  try {
    await click(button(view.container, "Change role for Owen Ortiz"));
    const roleDialog = dialog(view.container, "Change role for Owen Ortiz");
    const target = roleDialog.querySelectorAll('input[name="members-role"]').find((radio) => radio.value === "admin");
    assert.ok(target);
    await act(async () => {
      target.checked = true;
      target.dispatchEvent(fakeEvent("click"));
    });
    await focus(target);

    held.hold();
    await submit(form(roleDialog));
    const save = button(roleDialog, "Saving…");
    assert.equal(save.hasAttribute("disabled"), false, "the pending save keeps its focusability");
    assert.equal(save.getAttribute("aria-disabled"), "true");
    assert.equal(active() === null, true, "the role choices are disabled while saving, which takes focus");

    base.failNext("changeMemberRole");
    held.release();
    await wait(10);
    assert.match(textOf(roleDialog), /Could not change the role/u);
    const checked = roleDialog.querySelectorAll('input[name="members-role"]').find((radio) => radio.checked);
    assert.equal(checked?.value, "admin");
    assert.equal(active() === checked, true, "focus returns to the chosen role, not the document");
  } finally {
    await view.unmount();
  }
});

test("removing a member keeps focus on the confirmation while it is pending", async () => {
  const held = holding(owner(), "removeMember");
  const view = await mount(members(held.client));
  try {
    await click(button(view.container, "Remove Owen Ortiz"));
    const confirm = dialog(view.container, "Remove Owen Ortiz?");
    held.hold();
    const remove = button(confirm, "Remove member");
    await click(remove);
    assert.equal(textOf(remove), "Removing…");
    assert.equal(remove.hasAttribute("disabled"), false);
    assert.equal(remove.getAttribute("aria-disabled"), "true");
    assert.equal(active() === remove, true, "focus stays on the pending confirmation");
    await click(remove);
    assert.equal(held.state.calls, 1, "a pending removal is not sent twice");
    held.release();
    await wait(10);
    assert.equal(view.container.querySelector('[data-modal="Remove Owen Ortiz?"]') === null, true);
    const heading = view.container.querySelector("#members-heading");
    assert.equal(active() === heading, true, "the removed row's trigger is gone, so focus goes to the table heading");
  } finally {
    await view.unmount();
  }
});

// --------------------------------------------------------------------------
// Site directory
// --------------------------------------------------------------------------

function siteAdmin(latencyMs = 0) {
  return createFakeIdentityClient({ initialSignedInEmail: FAKE_SITE_ADMIN_EMAIL, latencyMs, now: () => NOW });
}

test("turning pages from the keyboard keeps focus on the pager, and off a button that turns disabled", async () => {
  const view = await mount(<IdentityProvider client={siteAdmin(25)}><SiteUsersPanel pageSize={4} /></IdentityProvider>, 120);
  try {
    const pager = view.container.querySelector('nav[aria-label="Users pages"]');
    assert.ok(pager, "the pager renders once the first page loads");
    const next = button(pager, "Next");
    const previous = button(pager, "Previous");
    assert.equal(previous.disabled, true, "the first page has no previous page");

    await click(next);
    assert.equal(next.getAttribute("aria-disabled"), "true", "a page in flight holds the buttons");
    assert.equal(next.hasAttribute("disabled"), false, "without disabling the focused one");
    assert.equal(active() === next, true, "focus stays on Next while the page loads");
    assert.match(textOf(pager), /Loading users/u);
    await click(next);
    await wait(120);
    assert.match(textOf(pager), /Page 2 of 3/u, "a press while loading does not skip a page");
    assert.equal(active() === next, true);

    await click(next);
    await wait(120);
    assert.match(textOf(pager), /Page 3 of 3/u);
    assert.equal(next.disabled, true, "the last page has no next page");
    assert.equal(active() === previous, true, "focus moves to Previous rather than the document");
  } finally {
    await view.unmount();
  }
});

test("create user sends focus to the first field to fix, and back to a field the server refused", async () => {
  const view = await mount(<IdentityProvider client={siteAdmin()}><SiteUsersPanel /></IdentityProvider>);
  try {
    await click(button(view.container, "Create user"));
    const create = dialog(view.container, "Create user");
    const submitButton = create.querySelectorAll("button").find((candidate) => candidate.getAttribute("type") === "submit");
    assert.ok(submitButton);
    await focus(submitButton);
    await submit(form(create));
    const email = create.querySelector("#site-users-create-email");
    const username = create.querySelector("#site-users-create-username");
    assert.ok(email && username);
    assert.equal(active() === email, true, "focus goes to the first invalid field, not nowhere");

    await type(email, "ada@open-splunk.test");
    await type(username, "newbie");
    await focus(username);
    await submit(form(create));
    assert.equal(email.getAttribute("aria-invalid"), "true");
    assert.match(textOfIds(email.getAttribute("aria-describedby")), /already exists/u);
    assert.equal(active() === email, true, "the disabled-while-pending fields are re-enabled before focus moves");

    const reason = create.querySelector("textarea");
    assert.ok(reason);
    assert.match(textOfIds(reason.getAttribute("aria-describedby")), /audit log/u, "the reason's hint is its description");
  } finally {
    await view.unmount();
  }
});

test("a row action refused after Enter in the confirmation field returns focus to that field", async () => {
  const base = siteAdmin();
  const view = await mount(<IdentityProvider client={base}><SiteUsersPanel /></IdentityProvider>);
  try {
    await click(button(view.container, "Actions for Tess Takahashi"));
    const deleteItem = view.container.querySelectorAll('[role="menuitem"]').find((item) => item.querySelector("strong")?.textContent === "Delete user");
    assert.ok(deleteItem);
    assert.equal(textOfIds(deleteItem.getAttribute("aria-labelledby")), "Delete user");
    await click(deleteItem);
    const confirm = dialog(view.container, "Delete Tess Takahashi?");
    const typed = confirm.querySelector('input[data-autofocus="typed"]');
    assert.ok(typed);
    await type(typed, "tess@open-splunk.test");
    await focus(typed);
    base.failNext("adminDeleteUser");
    await submit(form(confirm), 10);
    assert.ok(confirm.querySelector('[role="alert"]'), "the failure is announced");
    assert.equal(active() === typed, true, "focus is back in the dialog's first field");
  } finally {
    await view.unmount();
  }
});

test("a disabled row action is named by its label and described by its reason, once each", async () => {
  const view = await mount(<IdentityProvider client={siteAdmin()}><SiteUsersPanel /></IdentityProvider>);
  try {
    await click(button(view.container, "Actions for Ada Lovelace"));
    const items = view.container.querySelectorAll('[role="menuitem"]');
    assert.notEqual(items.length, 0);
    for (const item of items) {
      const label = item.querySelector("strong")?.textContent ?? "";
      assert.equal(textOfIds(item.getAttribute("aria-labelledby")), label);
      if (item.getAttribute("aria-disabled") === "true") {
        const reason = textOfIds(item.getAttribute("aria-describedby"));
        assert.notEqual(reason, "");
        assert.equal(label.includes(reason), false);
      } else {
        assert.equal(item.hasAttribute("aria-describedby"), false);
      }
    }
  } finally {
    await view.unmount();
  }
});

test("create organization sends focus to the empty name, then to a slug the server refused", async () => {
  const view = await mount(<IdentityProvider client={siteAdmin()}><SiteOrganizationsPanel /></IdentityProvider>);
  try {
    await click(button(view.container, "Create organization"));
    const create = dialog(view.container, "Create organization");
    const name = create.querySelector("#site-orgs-create-name");
    const slug = create.querySelector("#site-orgs-create-slug");
    assert.ok(name && slug);
    await focus(button(create, "Cancel"));
    await submit(form(create));
    assert.equal(active() === name, true);

    await type(name, "Acme");
    await focus(name);
    await submit(form(create));
    assert.equal(slug.getAttribute("aria-invalid"), "true");
    assert.equal(active() === slug, true, "focus follows the refusal to the slug");
  } finally {
    await view.unmount();
  }
});
