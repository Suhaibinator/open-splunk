"use client";

import Link from "next/link";
import { useEffect, useRef, useState, type FormEvent } from "react";

import { useIdentity } from "../_components/identity-provider";
import { AuthAlert, AuthCardHeader, AuthNotice, SubmitButton, TextField } from "./auth-frame";
import { describeAuthError, type AuthErrorCopy } from "./auth-error-copy";
import { useCooldown } from "./auth-hooks";
import { demoOutboxLink } from "./demo-outbox";
import { signInHref } from "./return-to";

const EMAIL_SHAPE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;

export interface PasswordResetRequestProps {
  readonly initialEmail?: string;
  readonly demo?: boolean;
}

/**
 * `/signin/reset/`: asks for a reset email. go-common answers the same for
 * an address with or without an account, so the confirmation never says
 * whether one exists.
 */
export function PasswordResetRequest({ initialEmail = "", demo = false }: PasswordResetRequestProps) {
  const { client } = useIdentity();
  const [email, setEmail] = useState(initialEmail);
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [failure, setFailure] = useState<AuthErrorCopy | null>(null);
  const [pending, setPending] = useState(false);
  const [sentTo, setSentTo] = useState<string | null>(null);
  const cooldown = useCooldown();
  const input = useRef<HTMLInputElement>(null);

  useEffect(() => {
    input.current?.focus();
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (pending || cooldown.remainingMs > 0 || client === null) return;
    const address = email.trim();
    const error = address.length === 0 ? "Enter your email address." : EMAIL_SHAPE.test(address) ? null : "Enter a valid email address, like name@example.com.";
    setFieldError(error);
    if (error !== null) {
      input.current?.focus();
      return;
    }
    setPending(true);
    setFailure(null);
    try {
      await client.requestPasswordReset(address);
      setSentTo(address);
    } catch (caught) {
      const copy = describeAuthError(caught, "reset_request");
      setFailure(copy);
      if (copy.kind === "rate_limited" && copy.retryAfterMs !== null) cooldown.start(copy.retryAfterMs);
    } finally {
      setPending(false);
    }
  }

  if (sentTo !== null) {
    const link = demo ? demoOutboxLink(client, "password_reset", sentTo) : null;
    return (
      <>
        <AuthCardHeader icon="check" title="Check your email" intro="If an account uses that address, a reset link is on its way." focusOnMount />
        <AuthNotice tone="success" title={`Sent to ${sentTo}`}>
          The link works once. If it does not arrive in a few minutes, check your spam folder.
        </AuthNotice>
        {demo ? (
          <AuthNotice title="Demo inbox">
            {link === null
              ? "No demo account uses that address, so nothing was sent."
              : <>The preview has no email. <Link href={link}>Open the reset link</Link>.</>}
          </AuthNotice>
        ) : null}
        <div className="signin-actions">
          <Link className="button button--primary button--block signin-action" href={signInHref({ email: sentTo })}>Back to sign in</Link>
          <button className="button button--ghost button--block" type="button" onClick={() => { setSentTo(null); setFailure(null); }}>
            Use a different address
          </button>
        </div>
      </>
    );
  }

  return (
    <>
      <AuthCardHeader title="Reset your password" intro="Enter the email address you sign in with and we'll send a link to choose a new password." />
      <form className="signin-form" noValidate aria-busy={pending} aria-label="Request a password reset" onSubmit={(event) => { void submit(event); }}>
        <TextField
          autoComplete="email"
          error={fieldError}
          id="reset-email"
          inputMode="email"
          inputRef={input}
          label="Email"
          onValueChange={(value) => { setEmail(value); setFieldError(null); }}
          type="email"
          value={email}
        />
        {failure === null ? null : <AuthAlert copy={failure} />}
        <SubmitButton cooldownMs={cooldown.remainingMs} pending={pending} pendingLabel="Sending…">Send reset link</SubmitButton>
      </form>
      <Link className="button button--ghost button--block signin-action" href={signInHref({ email: email.trim() })}>Back to sign in</Link>
    </>
  );
}
