import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the harness installs the fake browser before
// `react-dom/client` is evaluated anywhere in this process.
import {
  browser,
  clickButton,
  findByText,
  focusedId,
  mount,
  settle,
  submitForm,
  typeInto,
} from "@/lib/testing/react-mount";
import { act, type ReactNode } from "react";

import type { IdentityClient } from "@/lib/api/identity";
import { FAKE_IDENTITY_PASSWORD, createFakeIdentityClient, type FakeIdentityClient } from "@/lib/api/identity-fake";
import { fakeEvent, type FakeElement } from "@/lib/testing/fake-dom";

import { IdentityProvider } from "../_components/identity-provider";
import { IdentitySignIn } from "./identity-sign-in";
import { PasswordResetRequest } from "./password-reset-request";
import { RegistrationForm } from "./registration-form";
import { TokenPasswordFlow } from "./token-password-flow";

/*
 * Keyboard-only and screen-reader paths through the sign-in surfaces: what
 * has focus after each refusal, what is announced when focus cannot say it,
 * and whether every control is named and pointed at its note. Every
 * assertion compares primitives; a failing deep-equal over a node graph
 * would diff the whole fake document.
 */

const SIGN_IN_FORM = "Sign in with email";
const OTP_FORM = "Verification code";

async function mountWith(client: IdentityClient, node: ReactNode) {
  const view = await mount(<IdentityProvider client={client}>{node}</IdentityProvider>);
  await settle();
  return view;
}

function signIn(client: IdentityClient, initialEmail = "") {
  return mountWith(client, <IdentitySignIn initialEmail={initialEmail} navigate={() => undefined} returnTo="/search/" />);
}

function element(container: FakeElement, selector: string): FakeElement {
  const found = container.querySelector(selector);
  assert.ok(found, `no ${selector}`);
  return found;
}

async function focus(target: FakeElement): Promise<void> {
  await act(async () => {
    target.focus();
  });
}

/** The form's always-mounted announcement region: the `span` alert, not an `AuthAlert`. */
function announcement(container: FakeElement, formLabel: string): string {
  const form = element(container, `form[aria-label="${formLabel}"]`);
  const region = form.querySelectorAll('[role="alert"]').find((candidate) => candidate.nodeName === "SPAN");
  assert.ok(region, `the ${formLabel} form keeps an announcement region mounted`);
  return region.textContent;
}

/** The text a control's `aria-describedby` resolves to, each id required to exist. */
function description(control: FakeElement): string {
  const ids = (control.getAttribute("aria-describedby") ?? "").split(/\s+/u).filter((id) => id.length > 0);
  assert.notEqual(ids.length, 0, `#${control.id} has a description`);
  return ids.map((id) => {
    const note = browser.document.getElementById(id);
    assert.ok(note, `#${control.id} is described by #${id}, which must exist`);
    return note.textContent.trim();
  }).join(" ");
}

/** Every text-like input in `scope` has exactly one `<label for>` naming it, and a resolvable description. */
function assertLabelled(scope: FakeElement): number {
  const inputs = scope.querySelectorAll("input").filter((input) => input.type !== "radio" && input.type !== "checkbox" && input.type !== "hidden");
  for (const input of inputs) {
    const labels = scope.querySelectorAll("label").filter((label) => label.getAttribute("for") === input.id);
    assert.equal(labels.length, 1, `#${input.id} has one label`);
    assert.notEqual(labels[0]?.textContent.trim() ?? "", "", `#${input.id}'s label has text`);
    description(input);
  }
  return inputs.length;
}

test("Enter in an empty email field announces the error, since focus cannot move to where it already is", async () => {
  const view = await signIn(createFakeIdentityClient());
  try {
    assert.equal(focusedId(), "signin-email", "the step opens on the empty email field");
    await submitForm(view.container, SIGN_IN_FORM);
    const email = element(view.container, "#signin-email");
    assert.equal(email.getAttribute("aria-invalid"), "true");
    assert.equal(description(email), "Enter your email address.");
    assert.equal(focusedId(), "signin-email");
    assert.equal(announcement(view.container, SIGN_IN_FORM), "Enter your email address.", "the alert speaks the refusal");

    await submitForm(view.container, SIGN_IN_FORM);
    const again = announcement(view.container, SIGN_IN_FORM);
    assert.equal(again.trim(), "Enter your email address.");
    assert.notEqual(again, "Enter your email address.", "a repeated refusal changes the text, so it is read again");

    await typeInto(view.container, "signin-email", "a");
    assert.equal(announcement(view.container, SIGN_IN_FORM), "", "typing makes the announcement stale, so it is cleared");
  } finally {
    await view.unmount();
  }
});

test("a refusal submitted from the button moves focus to the field and does not also announce it", async () => {
  const view = await signIn(createFakeIdentityClient());
  try {
    const submit = findByText(view.container, "button", "Sign in");
    assert.ok(submit);
    await focus(submit);
    assert.equal(browser.document.activeElement === submit, true, "the submit button has focus");
    await submitForm(view.container, SIGN_IN_FORM);
    assert.equal(focusedId(), "signin-email", "focus lands on the first field to fix, which reads its error");
    assert.equal(announcement(view.container, SIGN_IN_FORM), "", "nothing is said twice");
  } finally {
    await view.unmount();
  }
});

test("a missing password, with the email filled, moves focus from the email to the password", async () => {
  const view = await signIn(createFakeIdentityClient(), "alice@open-splunk.test");
  try {
    assert.equal(focusedId(), "signin-password", "a prefilled address opens on the password");
    await focus(element(view.container, "#signin-email"));
    await submitForm(view.container, SIGN_IN_FORM);
    assert.equal(focusedId(), "signin-password");
    assert.equal(element(view.container, "#signin-email").getAttribute("aria-invalid"), null);
    assert.equal(description(element(view.container, "#signin-password")), "Enter your password.");
    assert.equal(announcement(view.container, SIGN_IN_FORM), "");
  } finally {
    await view.unmount();
  }
});

test("a wrong code typed and entered in the code field is announced, not just painted", async () => {
  const view = await signIn(createFakeIdentityClient());
  try {
    await typeInto(view.container, "signin-email", "tess@open-splunk.test");
    await typeInto(view.container, "signin-password", FAKE_IDENTITY_PASSWORD);
    await submitForm(view.container, SIGN_IN_FORM);
    assert.equal(focusedId(), "signin-otp");

    await submitForm(view.container, OTP_FORM);
    assert.equal(announcement(view.container, OTP_FORM), "Enter the 6-digit code from your authenticator app.");

    await typeInto(view.container, "signin-otp", "000000");
    assert.equal(announcement(view.container, OTP_FORM), "");
    await submitForm(view.container, OTP_FORM);
    const otp = element(view.container, "#signin-otp");
    assert.equal(otp.getAttribute("aria-invalid"), "true");
    assert.match(description(otp), /That code was not accepted/u);
    assert.match(announcement(view.container, OTP_FORM), /That code was not accepted/u);
    assert.equal(view.container.querySelectorAll('[role="alert"]').length, 1, "only the announcement region speaks; the note stays quiet");
  } finally {
    await view.unmount();
  }
});

test("every sign-in field has its own label and a description that exists", async () => {
  const view = await signIn(createFakeIdentityClient());
  try {
    assert.equal(assertLabelled(view.container), 2);
    const toggle = findByText(view.container, "button", "Show");
    assert.ok(toggle);
    assert.equal(toggle.getAttribute("aria-controls"), "signin-password");
    assert.equal(toggle.getAttribute("aria-label"), "Show password");
    assert.equal(toggle.getAttribute("aria-pressed"), "false");
    assert.equal(toggle.getAttribute("type"), "button", "the toggle never submits the form");
    await act(async () => {
      toggle.focus();
      toggle.dispatchEvent(fakeEvent("click"));
    });
    assert.equal(element(view.container, "#signin-password").getAttribute("type"), "text");
    assert.equal(toggle.getAttribute("aria-pressed"), "true");
    assert.equal(toggle.getAttribute("aria-label"), "Hide password");
    assert.equal(browser.document.activeElement === toggle, true, "the toggle keeps focus");

    const submit = findByText(view.container, "button", "Sign in");
    assert.ok(submit);
    assert.equal(submit.hasAttribute("disabled"), false, "the primary action is never disabled, so it never drops focus");
  } finally {
    await view.unmount();
  }
});

test("the reset request announces an invalid address entered from its field", async () => {
  const view = await mountWith(createFakeIdentityClient(), <PasswordResetRequest />);
  try {
    assert.equal(focusedId(), "reset-email");
    assert.equal(assertLabelled(view.container), 1);
    await typeInto(view.container, "reset-email", "not-an-address");
    await submitForm(view.container, "Request a password reset");
    assert.equal(focusedId(), "reset-email");
    assert.match(announcement(view.container, "Request a password reset"), /valid email address/u);
  } finally {
    await view.unmount();
  }
});

function registration(client: FakeIdentityClient, initialEmail: string) {
  return mountWith(client, <RegistrationForm initialEmail={initialEmail} onCancel={() => undefined} onRegistered={() => undefined} returnTo="/search/" />);
}

test("registration sends focus to the first field to fix in reading order, and announces from inside it", async () => {
  const view = await registration(createFakeIdentityClient(), "someone.new@open-splunk.test");
  try {
    assert.equal(focusedId(), "register-first-name", "a prefilled address opens on the first name");
    assert.equal(assertLabelled(view.container), 5);
    await typeInto(view.container, "register-first-name", "Some");
    await submitForm(view.container, "Create an account");
    assert.equal(focusedId(), "register-last-name", "last name is the first empty field after first name");
    assert.equal(announcement(view.container, "Create an account"), "", "focus moved, so the field reads its own error");

    await submitForm(view.container, "Create an account");
    assert.equal(focusedId(), "register-last-name");
    assert.equal(announcement(view.container, "Create an account"), "Enter your last name.");

    await typeInto(view.container, "register-last-name", "One");
    await typeInto(view.container, "register-username", "alice");
    await typeInto(view.container, "register-password", "a-long-new-passphrase");
    await focus(element(view.container, "#register-username"));
    await submitForm(view.container, "Create an account");
    assert.equal(focusedId(), "register-username", "the server's clash lands on the username");
    assert.match(announcement(view.container, "Create an account"), /different username/u, "entered from the username field, so it is announced");
  } finally {
    await view.unmount();
  }
});

test("a set-password link announces a mismatch entered from the confirmation", async () => {
  const client = createFakeIdentityClient();
  await client.requestPasswordReset("alice@open-splunk.test");
  const token = client.lastMessage("password_reset", "alice@open-splunk.test")?.token ?? null;
  const view = await mountWith(client, (
    <TokenPasswordFlow
      flow="reset_complete"
      intro="Choose a password."
      pendingLabel="Saving…"
      run={(code, newPassword) => client.completePasswordReset({ verificationCode: code, newPassword })}
      submitLabel="Save new password"
      successDetail="Your password was changed."
      successTitle="Password updated"
      title="Choose a new password"
      token={token}
    />
  ));
  try {
    assert.equal(assertLabelled(view.container), 2);
    await typeInto(view.container, "new-password", "a-long-new-passphrase");
    await typeInto(view.container, "confirm-password", "different");
    await focus(element(view.container, "#confirm-password"));
    await submitForm(view.container, "Choose a new password");
    assert.equal(focusedId(), "confirm-password");
    assert.equal(announcement(view.container, "Choose a new password"), "The two passwords do not match.");
    await clickButton(view.container, "Show");
    assert.equal(announcement(view.container, "Choose a new password"), "The two passwords do not match.", "a toggle is not an edit");
  } finally {
    await view.unmount();
  }
});
