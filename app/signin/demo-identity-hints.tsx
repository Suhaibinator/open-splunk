"use client";

import Link from "next/link";

import { DEMO_IDENTITY_HINTS, DEMO_IDENTITY_LINKS } from "@/lib/demo/identity-data";

export interface DemoIdentityHintsProps {
  /** Fills the form with an account; null where there is no form to fill. */
  readonly onChooseAccount: ((email: string, password: string) => void) | null;
}

/**
 * The demo directory, disclosed on request: who each seeded account is, the
 * shared password and authenticator code, and links into the emailed flows
 * the preview has no inbox for. A connected server never renders it.
 */
export function DemoIdentityHints({ onChooseAccount }: DemoIdentityHintsProps) {
  return (
    <details className="signin-demo">
      <summary>Demo accounts and flows</summary>
      <p>
        Every account uses the password <code>{DEMO_IDENTITY_HINTS.password}</code>; the two-step account&apos;s code is{" "}
        <code>{DEMO_IDENTITY_HINTS.otpCode}</code>.
      </p>
      <ul aria-label="Demo accounts">
        {DEMO_IDENTITY_HINTS.accounts.map((account) => (
          <li key={account.email}>
            <span>
              <b>{account.email}</b>
              <small>{account.description}</small>
            </span>
            {onChooseAccount === null ? null : (
              <button
                aria-label={`Use ${account.email}`}
                className="button button--secondary button--compact"
                onClick={() => onChooseAccount(account.email, DEMO_IDENTITY_HINTS.password)}
                type="button"
              >
                Use
              </button>
            )}
          </li>
        ))}
      </ul>
      <ul aria-label="Demo flows">
        {DEMO_IDENTITY_LINKS.map((link) => (
          <li key={link.href}>
            <span>
              <Link href={link.href}>{link.label}</Link>
              <small>{link.description}</small>
            </span>
          </li>
        ))}
      </ul>
    </details>
  );
}
