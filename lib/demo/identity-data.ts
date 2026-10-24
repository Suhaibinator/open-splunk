/**
 * The demo export's identity: the in-memory fake, one per page realm, with
 * its session kept in `sessionStorage` so a reload or a full navigation keeps
 * whoever signed in.
 *
 * The preview starts signed in as the seeded site admin, so every demo page
 * keeps the full capability set it had before identity existed; signing out
 * (remembered for the tab) is how a viewer reaches the sign-in flows.
 */

import {
  FAKE_ACCOUNT_INVITATION_TOKEN,
  FAKE_EXPIRED_INVITATION_ID,
  FAKE_IDENTITY_ACCOUNTS,
  FAKE_IDENTITY_OTP_CODE,
  FAKE_IDENTITY_PASSWORD,
  FAKE_PENDING_INVITATION_ID,
  FAKE_SITE_ADMIN_EMAIL,
  createFakeIdentityClient,
  type FakeIdentityAccount,
  type FakeIdentityClient,
  type FakeIdentityStorage,
  type FakeSeedInvitation,
} from "@/lib/api/identity-fake";

export const DEMO_IDENTITY_STORAGE_KEY = "open-splunk.demo-identity-session";

/**
 * The demo's one invitation for an address with no account, so a viewer can
 * walk register-then-accept. The fixed fake seed has none; its id sits past
 * the seeded ones and below the ids the fake hands out.
 */
export const DEMO_NEW_USER_INVITATION: FakeSeedInvitation = {
  id: "3",
  organizationId: "2",
  email: "new.analyst@open-splunk.test",
  role: "member",
  invitedByUserId: "2",
};

export interface DemoIdentityLink {
  readonly label: string;
  readonly description: string;
  readonly href: string;
}

/** Links into the emailed flows the demo can reach without an outbox. */
export const DEMO_IDENTITY_LINKS: readonly DemoIdentityLink[] = [
  {
    label: "Accept an invitation as a new user",
    description: `${DEMO_NEW_USER_INVITATION.email} has no account yet`,
    href: `/auth/invitation/?id=${DEMO_NEW_USER_INVITATION.id}&email=${encodeURIComponent(DEMO_NEW_USER_INVITATION.email)}`,
  },
  {
    label: "Accept an invitation as Nora",
    description: "nora@open-splunk.test, no organization yet",
    href: `/auth/invitation/?id=${FAKE_PENDING_INVITATION_ID}&email=${encodeURIComponent("nora@open-splunk.test")}`,
  },
  {
    label: "Open an expired invitation",
    description: "late@open-splunk.test, expired",
    href: `/auth/invitation/?id=${FAKE_EXPIRED_INVITATION_ID}&email=${encodeURIComponent("late@open-splunk.test")}`,
  },
  {
    label: "Set a password for an invited account",
    description: "iris@open-splunk.test, created by an administrator",
    href: `/auth/email/accept-invitation/?token=${encodeURIComponent(FAKE_ACCOUNT_INVITATION_TOKEN)}`,
  },
];

/** What a demo sign-in screen may show so a viewer can try each flow. */
export const DEMO_IDENTITY_HINTS: {
  readonly accounts: readonly FakeIdentityAccount[];
  readonly password: string;
  readonly otpCode: string;
} = {
  accounts: FAKE_IDENTITY_ACCOUNTS,
  password: FAKE_IDENTITY_PASSWORD,
  otpCode: FAKE_IDENTITY_OTP_CODE,
};

function tabStorage(): FakeIdentityStorage | null {
  try {
    return typeof window === "undefined" ? null : window.sessionStorage;
  } catch {
    // Blocked site data: the session lasts until the page unloads.
    return null;
  }
}

let demoIdentityClient: FakeIdentityClient | null = null;

/** The demo export's identity client; the same instance for the life of the page. */
export function getDemoIdentityClient(): FakeIdentityClient {
  demoIdentityClient ??= createFakeIdentityClient({
    initialSignedInEmail: FAKE_SITE_ADMIN_EMAIL,
    additionalInvitations: [DEMO_NEW_USER_INVITATION],
    storage: tabStorage(),
    storageKey: DEMO_IDENTITY_STORAGE_KEY,
  });
  return demoIdentityClient;
}
