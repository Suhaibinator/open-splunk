import type { IdentityClient } from "@/lib/api/identity";
import { FakeIdentityClient, type FakeOutboxMessage } from "@/lib/api/identity-fake";

/** The emailed-link paths go-common writes into its messages. */
const OUTBOX_PATHS: Readonly<Record<FakeOutboxMessage["kind"], (token: string) => string>> = {
  password_reset: (token) => `/auth/email/reset/?token=${encodeURIComponent(token)}`,
  account_invitation: (token) => `/auth/email/accept-invitation/?token=${encodeURIComponent(token)}`,
  email_verification: (token) => `/auth/email/verify/?token=${encodeURIComponent(token)}`,
  organization_invitation: (token) => `/auth/invitation/?id=${encodeURIComponent(token)}`,
};

/**
 * The link the demo directory would have emailed, for a preview with no
 * inbox to read it from; null for any client that sends real email.
 */
export function demoOutboxLink(client: IdentityClient | null, kind: FakeOutboxMessage["kind"], email: string): string | null {
  if (!(client instanceof FakeIdentityClient)) return null;
  const message = client.lastMessage(kind, email);
  return message === undefined ? null : OUTBOX_PATHS[kind](message.token);
}
