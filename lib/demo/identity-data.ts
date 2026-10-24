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
  FAKE_IDENTITY_ACCOUNTS,
  FAKE_IDENTITY_OTP_CODE,
  FAKE_IDENTITY_PASSWORD,
  FAKE_SITE_ADMIN_EMAIL,
  createFakeIdentityClient,
  type FakeIdentityAccount,
  type FakeIdentityClient,
  type FakeIdentityStorage,
} from "@/lib/api/identity-fake";

export const DEMO_IDENTITY_STORAGE_KEY = "open-splunk.demo-identity-session";

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
    storage: tabStorage(),
    storageKey: DEMO_IDENTITY_STORAGE_KEY,
  });
  return demoIdentityClient;
}
