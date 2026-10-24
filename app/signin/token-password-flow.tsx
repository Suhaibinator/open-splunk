"use client";

import Link from "next/link";
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";

import { AuthAlert, AuthCardHeader, AuthNotice, PasswordField, SubmitButton } from "./auth-frame";
import { describeAuthError, type AuthErrorCopy, type AuthFlow } from "./auth-error-copy";
import { useCooldown } from "./auth-hooks";
import { signInHref } from "./return-to";

export const PASSWORD_HINT = "Use at least 8 characters. A passphrase is easiest to remember.";

export interface TokenPasswordFlowProps {
  /** The token the emailed link carried; null when the link had none. */
  readonly token: string | null;
  readonly flow: Extract<AuthFlow, "reset_complete" | "account_invitation">;
  readonly title: string;
  readonly intro: ReactNode;
  readonly submitLabel: string;
  readonly pendingLabel: string;
  readonly successTitle: string;
  readonly successDetail: string;
  /** Where a dead link sends the user for a fresh one, if anywhere. */
  readonly recovery?: { readonly href: string; readonly label: string };
  readonly run: (token: string, password: string) => Promise<void>;
}

/**
 * A page opened from an emailed one-time link that sets a password: a reset,
 * or an administrator-created account's first password. Neither signs in;
 * success hands over to the sign-in page.
 */
export function TokenPasswordFlow({
  token,
  flow,
  title,
  intro,
  submitLabel,
  pendingLabel,
  successTitle,
  successDetail,
  recovery,
  run,
}: TokenPasswordFlowProps) {
  const [password, setPassword] = useState("");
  const [confirmation, setConfirmation] = useState("");
  const [errors, setErrors] = useState<{ password: string | null; confirmation: string | null }>({ password: null, confirmation: null });
  const [failure, setFailure] = useState<AuthErrorCopy | null>(null);
  const [deadLink, setDeadLink] = useState<AuthErrorCopy | null>(null);
  const [pending, setPending] = useState(false);
  const [done, setDone] = useState(false);
  const cooldown = useCooldown();
  const passwordInput = useRef<HTMLInputElement>(null);
  const confirmationInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    passwordInput.current?.focus();
  }, []);

  if (token === null || token.length === 0) {
    return (
      <>
        <AuthCardHeader icon="warning" title="This link is incomplete" intro="Open the link from your email again, or copy the whole address into the browser." focusOnMount />
        <div className="signin-actions">
          {recovery === undefined ? null : <Link className="button button--primary button--block signin-action" href={recovery.href}>{recovery.label}</Link>}
          <Link className="button button--ghost button--block" href={signInHref()}>Back to sign in</Link>
        </div>
      </>
    );
  }

  if (done) {
    return (
      <>
        <AuthCardHeader icon="check" title={successTitle} intro={successDetail} focusOnMount />
        <AuthNotice tone="success" title="Password saved">Sign in with your new password.</AuthNotice>
        <Link className="button button--primary button--block signin-action" href={signInHref()}>Sign in</Link>
      </>
    );
  }

  if (deadLink !== null) {
    return (
      <>
        <AuthCardHeader icon="warning" title={deadLink.title} intro={deadLink.detail} focusOnMount />
        <div className="signin-actions">
          {recovery === undefined ? null : <Link className="button button--primary button--block signin-action" href={recovery.href}>{recovery.label}</Link>}
          <Link className="button button--ghost button--block" href={signInHref()}>Back to sign in</Link>
        </div>
      </>
    );
  }

  const linkToken = token;

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (pending || cooldown.remainingMs > 0) return;
    const next = {
      password: password.length === 0 ? "Choose a password." : null,
      confirmation: confirmation !== password ? "The two passwords do not match." : null,
    };
    setErrors(next);
    if (next.password !== null) { passwordInput.current?.focus(); return; }
    if (next.confirmation !== null) { confirmationInput.current?.focus(); return; }
    setPending(true);
    setFailure(null);
    try {
      await run(linkToken, password);
      setDone(true);
    } catch (caught) {
      const copy = describeAuthError(caught, flow);
      if (copy.deadLink) {
        // The token itself was refused: no password will make this link work.
        setDeadLink(copy);
      } else if (copy.kind === "invalid") {
        setErrors({ password: copy.detail, confirmation: null });
        passwordInput.current?.focus();
      } else {
        setFailure(copy);
        if (copy.kind === "rate_limited" && copy.retryAfterMs !== null) cooldown.start(copy.retryAfterMs);
      }
    } finally {
      setPending(false);
    }
  }

  return (
    <>
      <AuthCardHeader title={title} intro={intro} />
      <form className="signin-form" noValidate aria-busy={pending} aria-label={title} onSubmit={(event) => { void submit(event); }}>
        <PasswordField
          autoComplete="new-password"
          error={errors.password}
          hint={PASSWORD_HINT}
          id="new-password"
          inputRef={passwordInput}
          label="New password"
          onValueChange={(value) => { setPassword(value); setErrors((current) => ({ ...current, password: null })); }}
          value={password}
        />
        <PasswordField
          autoComplete="new-password"
          error={errors.confirmation}
          id="confirm-password"
          inputRef={confirmationInput}
          label="Confirm password"
          onValueChange={(value) => { setConfirmation(value); setErrors((current) => ({ ...current, confirmation: null })); }}
          value={confirmation}
        />
        {failure === null ? null : <AuthAlert copy={failure} />}
        <SubmitButton cooldownMs={cooldown.remainingMs} pending={pending} pendingLabel={pendingLabel}>{submitLabel}</SubmitButton>
      </form>
    </>
  );
}
