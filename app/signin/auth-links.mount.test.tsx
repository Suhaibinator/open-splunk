import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the harness installs the fake browser before
// `react-dom/client` is evaluated anywhere in this process.
import {
  browser,
  clickButton,
  findByText,
  focusedId,
  linkHref,
  mount,
  settle,
  submitForm,
  typeInto,
} from "@/lib/testing/react-mount";
import type { ReactNode } from "react";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import {
  FAKE_ACCOUNT_INVITATION_TOKEN,
  FAKE_EXPIRED_INVITATION_ID,
  FAKE_PENDING_INVITATION_ID,
  createFakeIdentityClient,
  type FakeIdentityClient,
} from "@/lib/api/identity-fake";
import { DEMO_NEW_USER_INVITATION } from "@/lib/demo/identity-data";

import { IdentityProvider } from "../_components/identity-provider";
import { EmailVerification } from "./email-verification";
import { OrganizationInvitationFlow } from "./organization-invitation";
import { PasswordResetRequest } from "./password-reset-request";
import { PENDING_RETURN_STORAGE_KEY, organizationInvitationHref } from "./return-to";
import { TokenPasswordFlow } from "./token-password-flow";

const NEW_USER = DEMO_NEW_USER_INVITATION.email;
const NEW_PASSWORD = "a-long-new-passphrase";

function recorder(): { readonly visits: string[]; readonly navigate: (url: string) => void } {
  const visits: string[] = [];
  return { visits, navigate: (url) => { visits.push(url); } };
}

async function mountWith(client: IdentityClient, node: ReactNode) {
  const view = await mount(<IdentityProvider client={client}>{node}</IdentityProvider>);
  await settle();
  return { ...view, text: () => view.container.textContent };
}

// --------------------------------------------------------------------------
// Request a password reset
// --------------------------------------------------------------------------

test("a reset request confirms without saying whether the address has an account", async () => {
  const client = createFakeIdentityClient();
  const view = await mountWith(client, <PasswordResetRequest demo initialEmail="alice@open-splunk.test" />);
  assert.equal(focusedId(), "reset-email");
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("Check your email"), true);
  assert.equal(view.text().includes("If an account uses that address"), true);
  assert.equal(linkHref(view.container, "Open the reset link")?.startsWith("/auth/email/reset?token=fake-reset-"), true);
  assert.equal(linkHref(view.container, "Back to sign in"), "/signin?email=alice%40open-splunk.test");

  await clickButton(view.container, "Use a different address");
  await typeInto(view.container, "reset-email", "nobody@open-splunk.test");
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("If an account uses that address"), true, "an unknown address reads the same");
  assert.equal(view.text().includes("No demo account uses that address"), true);
  await view.unmount();
});

test("a second reset request inside the cooldown counts down instead of sending", async () => {
  const client = createFakeIdentityClient();
  await client.requestPasswordReset("alice@open-splunk.test");
  const view = await mountWith(client, <PasswordResetRequest initialEmail="alice@open-splunk.test" />);
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("A reset email was just sent"), true);
  assert.equal(findByText(view.container, "button", "Try again in")?.getAttribute("aria-disabled"), "true");
  await view.unmount();
});

test("the reset request validates the address before sending", async () => {
  const view = await mountWith(createFakeIdentityClient(), <PasswordResetRequest />);
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("Enter your email address."), true);
  assert.equal(view.text().includes("Check your email"), false);
  await view.unmount();
});

// --------------------------------------------------------------------------
// Complete a reset, or set an invited account's first password
// --------------------------------------------------------------------------

function resetFlow(client: FakeIdentityClient, token: string | null): ReactNode {
  return (
    <TokenPasswordFlow
      flow="reset_complete"
      intro="Choose a password."
      pendingLabel="Saving…"
      recovery={{ href: "/signin/reset/", label: "Request a new link" }}
      run={(code, newPassword) => client.completePasswordReset({ verificationCode: code, newPassword })}
      submitLabel="Save new password"
      successDetail="Your password was changed."
      successTitle="Password updated"
      title="Choose a new password"
      token={token}
    />
  );
}

async function resetCode(client: FakeIdentityClient, email: string): Promise<string> {
  await client.requestPasswordReset(email);
  const message = client.lastMessage("password_reset", email);
  assert.ok(message, "the fake emailed a reset code");
  return message.token;
}

test("a reset link checks the two passwords, shows the policy's reason, then sets the password once", async () => {
  const client = createFakeIdentityClient();
  const code = await resetCode(client, "alice@open-splunk.test");
  const view = await mountWith(client, resetFlow(client, code));
  assert.equal(focusedId(), "new-password");

  await typeInto(view.container, "new-password", NEW_PASSWORD);
  await typeInto(view.container, "confirm-password", "something-else");
  await submitForm(view.container, "Choose a new password");
  assert.equal(view.text().includes("The two passwords do not match."), true);
  assert.equal(focusedId(), "confirm-password");

  await typeInto(view.container, "new-password", "short");
  await typeInto(view.container, "confirm-password", "short");
  await submitForm(view.container, "Choose a new password");
  assert.equal(view.text().includes("Passwords need at least 8 characters."), true, "the server's policy is shown on the field");
  assert.equal(focusedId(), "new-password");

  await typeInto(view.container, "new-password", NEW_PASSWORD);
  await typeInto(view.container, "confirm-password", NEW_PASSWORD);
  await submitForm(view.container, "Choose a new password");
  assert.equal(view.text().includes("Password updated"), true);
  assert.equal(linkHref(view.container, "Sign in"), "/signin");
  await view.unmount();

  const login = await client.loginWithEmail({ email: "alice@open-splunk.test", password: NEW_PASSWORD });
  assert.equal(login.status, "signed_in");

  const spent = await mountWith(client, resetFlow(client, code));
  await typeInto(spent.container, "new-password", NEW_PASSWORD);
  await typeInto(spent.container, "confirm-password", NEW_PASSWORD);
  await submitForm(spent.container, "Choose a new password");
  assert.equal(spent.text().includes("This reset link no longer works"), true);
  assert.equal(linkHref(spent.container, "Request a new link"), "/signin/reset");
  assert.equal(spent.container.querySelector("#new-password") === null, true, "a dead link offers no form");
  await spent.unmount();
});

test("a link with no token says it is incomplete and offers a fresh one", async () => {
  const client = createFakeIdentityClient();
  const view = await mountWith(client, resetFlow(client, null));
  assert.equal(view.text().includes("This link is incomplete"), true);
  assert.equal(linkHref(view.container, "Request a new link"), "/signin/reset");
  await view.unmount();
});

test("an administrator-created account sets its first password from the invitation link", async () => {
  const client = createFakeIdentityClient();
  const flow = (
    <TokenPasswordFlow
      flow="account_invitation"
      intro="Choose a password."
      pendingLabel="Setting up…"
      run={async (token, password) => { await client.acceptInvitation({ kind: "account", token, password }); }}
      submitLabel="Set password"
      successDetail="Your account is ready."
      successTitle="Account ready"
      title="Set up your account"
      token={FAKE_ACCOUNT_INVITATION_TOKEN}
    />
  );
  const view = await mountWith(client, flow);
  await typeInto(view.container, "new-password", NEW_PASSWORD);
  await typeInto(view.container, "confirm-password", NEW_PASSWORD);
  await submitForm(view.container, "Set up your account");
  assert.equal(view.text().includes("Account ready"), true);
  await view.unmount();
});

// --------------------------------------------------------------------------
// Organization invitations
// --------------------------------------------------------------------------

function invitationClient(signedInEmail: string | null = null): FakeIdentityClient {
  return createFakeIdentityClient({ additionalInvitations: [DEMO_NEW_USER_INVITATION], initialSignedInEmail: signedInEmail });
}

async function mountInvitation(client: IdentityClient, invitationId: string | null, invitedEmail: string | null, demo = false) {
  const route = recorder();
  const view = await mountWith(client, (
    <OrganizationInvitationFlow demo={demo} invitationId={invitationId} invitedEmail={invitedEmail} navigate={route.navigate} />
  ));
  return { ...view, ...route };
}

test("signed out, an invitation offers sign-in that comes back to it", async () => {
  const view = await mountInvitation(invitationClient(), FAKE_PENDING_INVITATION_ID, "nora@open-splunk.test");
  assert.equal(view.text().includes("You're invited to Open Splunk"), true);
  assert.equal(view.text().includes("nora@open-splunk.test"), true);
  await clickButton(view.container, "Sign in to accept");
  const target = new URL(view.visits[0] ?? "", "https://example.invalid");
  assert.equal(target.pathname, "/signin/");
  assert.equal(target.searchParams.get("returnTo"), organizationInvitationHref(FAKE_PENDING_INVITATION_ID, "nora@open-splunk.test"));
  assert.equal(target.searchParams.get("email"), "nora@open-splunk.test");
  await view.unmount();
});

test("an unknown address registers, verifies by email, comes back and accepts", async () => {
  browser.storage.delete(PENDING_RETURN_STORAGE_KEY);
  const client = invitationClient();
  const invitationHref = organizationInvitationHref(DEMO_NEW_USER_INVITATION.id, NEW_USER);
  const signedOut = await mountInvitation(client, DEMO_NEW_USER_INVITATION.id, NEW_USER, true);
  await clickButton(signedOut.container, "create an account");
  assert.equal(signedOut.container.querySelector("#register-email")?.value, NEW_USER, "the invited address is prefilled");
  assert.equal(signedOut.container.querySelector("#register-username")?.value, "new-analyst");
  assert.equal(focusedId(), "register-first-name", "focus starts on the first field the link could not fill");

  await submitForm(signedOut.container, "Create an account");
  assert.equal(signedOut.text().includes("Enter your first name."), true);
  assert.equal(focusedId(), "register-first-name");

  await typeInto(signedOut.container, "register-first-name", "Nia");
  await typeInto(signedOut.container, "register-last-name", "Analyst");
  await typeInto(signedOut.container, "register-password", NEW_PASSWORD);
  await submitForm(signedOut.container, "Create an account");
  assert.equal(signedOut.text().includes("Verify your email"), true);
  const verifyHref = linkHref(signedOut.container, "Open the verification link");
  assert.equal(verifyHref?.startsWith("/auth/email/verify?token=fake-verify-"), true);
  assert.equal(browser.storage.has(PENDING_RETURN_STORAGE_KEY), true, "the invitation is remembered for the emailed link");
  await signedOut.unmount();

  const token = new URL(verifyHref ?? "", "https://example.invalid").searchParams.get("token");
  const verifyRoute = recorder();
  const verify = await mountWith(client, <EmailVerification navigate={verifyRoute.navigate} token={token} />);
  assert.deepEqual(verifyRoute.visits, [invitationHref], "verification resumes the invitation");
  assert.equal(browser.storage.has(PENDING_RETURN_STORAGE_KEY), false, "the remembered return is spent");
  await verify.unmount();

  const signedIn = await mountInvitation(client, DEMO_NEW_USER_INVITATION.id, NEW_USER);
  assert.equal(signedIn.text().includes("Join Globex Security"), true);
  assert.equal(signedIn.text().includes("member"), true);
  await clickButton(signedIn.container, "Accept invitation");
  assert.equal(signedIn.text().includes("You joined Globex Security"), true);
  await clickButton(signedIn.container, "Open Globex Security");
  assert.deepEqual(signedIn.visits, ["/"]);
  const principal = await client.getPrincipal();
  assert.equal(principal?.activeOrganization?.name, "Globex Security");
  await signedIn.unmount();
});

test("registration names the field that clashed and links a taken address to sign-in", async () => {
  const client = invitationClient();
  const view = await mountInvitation(client, DEMO_NEW_USER_INVITATION.id, "alice@open-splunk.test");
  await clickButton(view.container, "create an account");
  await typeInto(view.container, "register-first-name", "Alice");
  await typeInto(view.container, "register-last-name", "Again");
  await typeInto(view.container, "register-username", "alice-two");
  await typeInto(view.container, "register-password", NEW_PASSWORD);
  await submitForm(view.container, "Create an account");
  assert.equal(view.text().includes("An account already uses this email"), true);
  assert.equal(linkHref(view.container, "Sign in instead")?.startsWith("/signin?returnTo="), true);

  await typeInto(view.container, "register-email", "someone.new@open-splunk.test");
  await typeInto(view.container, "register-username", "alice");
  await submitForm(view.container, "Create an account");
  assert.equal(view.text().includes("Choose a different username."), true);
  assert.equal(focusedId(), "register-username");

  client.failNext("registerWithEmail", new IdentityError("forbidden", "Email signup is invite-only"));
  await typeInto(view.container, "register-username", "someone-new");
  await submitForm(view.container, "Create an account");
  assert.equal(view.text().includes("Accounts here are by invitation"), true);
  await view.unmount();
});

test("signed in as the invitee, declining is final and says so", async () => {
  const client = invitationClient("nora@open-splunk.test");
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, null);
  assert.equal(view.text().includes("Join Acme Operations"), true);
  await clickButton(view.container, "Decline");
  assert.equal(view.text().includes("Invitation declined"), true);
  const remaining = await client.listMyInvitations();
  assert.equal(remaining.items.length, 0);
  await view.unmount();
});

test("an invitation for someone else, a missing id and an expired invitation each explain themselves", async () => {
  const other = await mountInvitation(invitationClient("ada@open-splunk.test"), FAKE_PENDING_INVITATION_ID, null);
  assert.equal(other.text().includes("This invitation isn't available"), true);
  assert.equal(findByText(other.container, "button", "Sign in with a different account") === null, false);
  await other.unmount();

  const missing = await mountInvitation(invitationClient(), null, null);
  assert.equal(missing.text().includes("This invitation link is incomplete"), true);
  await missing.unmount();

  const client = invitationClient();
  await client.registerWithEmail({ email: "late@open-splunk.test", password: NEW_PASSWORD, username: "late", firstName: "Lee", lastName: "Late" });
  const verification = client.lastMessage("email_verification", "late@open-splunk.test");
  assert.ok(verification);
  await client.completeEmailVerification(verification.token);
  const expired = await mountInvitation(client, FAKE_EXPIRED_INVITATION_ID, null);
  assert.equal(expired.text().includes("This invitation has expired"), true);
  assert.equal(findByText(expired.container, "button", "Accept invitation") === null, true);
  await expired.unmount();
});

test("an accept the server refuses as already-member, or that fails in transit, is handled in place", async () => {
  const client = invitationClient("nora@open-splunk.test");
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, null);
  client.failNext("acceptInvitation");
  await clickButton(view.container, "Accept invitation");
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(findByText(view.container, "button", "Accept invitation") === null, false, "the invitation can be accepted again");

  client.failNext("acceptInvitation", new IdentityError("conflict", "already a member"));
  await clickButton(view.container, "Accept invitation");
  assert.equal(view.text().includes("You're already in Acme Operations"), true);
  await view.unmount();
});

test("a verification link that was already used says so and offers sign-in", async () => {
  const client = createFakeIdentityClient();
  await client.registerWithEmail({ email: "twice@open-splunk.test", password: NEW_PASSWORD, username: "twice", firstName: "Tw", lastName: "Ice" });
  const message = client.lastMessage("email_verification", "twice@open-splunk.test");
  assert.ok(message);
  await client.completeEmailVerification(message.token);
  await client.logout();
  const route = recorder();
  const view = await mountWith(client, <EmailVerification navigate={route.navigate} token={message.token} />);
  assert.equal(view.text().includes("This verification link no longer works"), true);
  assert.equal(linkHref(view.container, "Go to sign in"), "/signin");
  assert.equal(route.visits.length, 0);
  await view.unmount();

  const incomplete = await mountWith(client, <EmailVerification navigate={route.navigate} token={null} />);
  assert.equal(incomplete.text().includes("This link is incomplete"), true);
  await incomplete.unmount();
});
