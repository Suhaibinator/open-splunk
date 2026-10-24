"use client";

import Link from "next/link";
import { useEffect, useRef, useState, type FormEvent } from "react";

import { useIdentity } from "../_components/identity-provider";
import { AuthAlert, PasswordField, SubmitButton, TextField } from "./auth-frame";
import { describeAuthError, type AuthErrorCopy } from "./auth-error-copy";
import { useCooldown } from "./auth-hooks";
import { emailFieldError } from "./email-address";
import { signInHref } from "./return-to";
import { PASSWORD_HINT } from "./token-password-flow";

type Field = "email" | "firstName" | "lastName" | "username" | "password";
type FieldErrors = Readonly<Record<Field, string | null>>;

const NO_ERRORS: FieldErrors = { email: null, firstName: null, lastName: null, username: null, password: null };

/** A username suggestion from the address's local part, in the characters usernames allow. */
export function suggestUsername(email: string): string {
  const local = email.trim().split("@")[0] ?? "";
  return local.replace(/[^A-Za-z0-9_-]+/gu, "-").replace(/^-+|-+$/gu, "").slice(0, 32);
}

export interface RegistrationFormProps {
  readonly initialEmail: string;
  /** Where the sign-in link offered for a taken address returns to. */
  readonly returnTo: string;
  readonly onRegistered: (email: string) => void;
  readonly onCancel: () => void;
}

/** Creates an account; go-common then emails a link that verifies the address and signs in. */
export function RegistrationForm({ initialEmail, returnTo, onRegistered, onCancel }: RegistrationFormProps) {
  const { client } = useIdentity();
  const [values, setValues] = useState<Readonly<Record<Field, string>>>({
    email: initialEmail,
    firstName: "",
    lastName: "",
    username: suggestUsername(initialEmail),
    password: "",
  });
  const [errors, setErrors] = useState<FieldErrors>(NO_ERRORS);
  const [failure, setFailure] = useState<AuthErrorCopy | null>(null);
  const [pending, setPending] = useState(false);
  const cooldown = useCooldown();
  const emailRef = useRef<HTMLInputElement>(null);
  const firstNameRef = useRef<HTMLInputElement>(null);
  const lastNameRef = useRef<HTMLInputElement>(null);
  const usernameRef = useRef<HTMLInputElement>(null);
  const passwordRef = useRef<HTMLInputElement>(null);
  // Read once: the form opens on the first field the link could not prefill.
  const [prefilled] = useState(() => initialEmail.length > 0);

  useEffect(() => {
    (prefilled ? firstNameRef : emailRef).current?.focus();
  }, [prefilled]);

  function focus(field: Field): void {
    const target = { email: emailRef, firstName: firstNameRef, lastName: lastNameRef, username: usernameRef, password: passwordRef }[field];
    target.current?.focus();
  }

  function update(field: Field, value: string): void {
    setValues((current) => ({ ...current, [field]: value }));
    setErrors((current) => ({ ...current, [field]: null }));
  }

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (pending || cooldown.remainingMs > 0 || client === null) return;
    const email = values.email.trim();
    const next: FieldErrors = {
      email: emailFieldError(email),
      firstName: values.firstName.trim().length === 0 ? "Enter your first name." : null,
      lastName: values.lastName.trim().length === 0 ? "Enter your last name." : null,
      username: values.username.trim().length < 3 ? "Usernames need at least 3 characters." : null,
      password: values.password.length === 0 ? "Choose a password." : null,
    };
    setErrors(next);
    const firstInvalid = (Object.keys(next) as Field[]).find((field) => next[field] !== null);
    if (firstInvalid !== undefined) {
      focus(firstInvalid);
      return;
    }
    setPending(true);
    setFailure(null);
    try {
      await client.registerWithEmail({
        email,
        firstName: values.firstName.trim(),
        lastName: values.lastName.trim(),
        username: values.username.trim(),
        password: values.password,
      });
      setPending(false);
      onRegistered(email);
    } catch (caught) {
      setPending(false);
      const copy = describeAuthError(caught, "register");
      if (copy.kind === "conflict" && copy.title.includes("username")) {
        setErrors({ ...NO_ERRORS, username: copy.detail });
        focus("username");
        return;
      }
      if (copy.kind === "invalid" && /password/iu.test(copy.detail)) {
        setErrors({ ...NO_ERRORS, password: copy.detail });
        focus("password");
        return;
      }
      setFailure(copy);
      if (copy.kind === "rate_limited" && copy.retryAfterMs !== null) cooldown.start(copy.retryAfterMs);
    }
  }

  return (
    <form className="signin-form" noValidate aria-busy={pending} aria-label="Create an account" onSubmit={(event) => { void submit(event); }}>
      <TextField autoComplete="email" error={errors.email} hint="Use the address the invitation was sent to." id="register-email" inputMode="email" inputRef={emailRef} label="Email" onValueChange={(value) => update("email", value)} type="email" value={values.email} />
      <TextField autoComplete="given-name" error={errors.firstName} id="register-first-name" inputRef={firstNameRef} label="First name" onValueChange={(value) => update("firstName", value)} value={values.firstName} />
      <TextField autoComplete="family-name" error={errors.lastName} id="register-last-name" inputRef={lastNameRef} label="Last name" onValueChange={(value) => update("lastName", value)} value={values.lastName} />
      <TextField autoComplete="username" error={errors.username} hint="Letters, digits, dashes and underscores." id="register-username" inputRef={usernameRef} label="Username" maxLength={50} onValueChange={(value) => update("username", value)} value={values.username} />
      <PasswordField autoComplete="new-password" error={errors.password} hint={PASSWORD_HINT} id="register-password" inputRef={passwordRef} label="Password" onValueChange={(value) => update("password", value)} value={values.password} />
      {failure === null ? null : (
        <AuthAlert
          copy={failure}
          action={failure.kind === "conflict" ? <Link href={signInHref({ returnTo, email: values.email.trim() })}>Sign in instead</Link> : undefined}
        />
      )}
      <SubmitButton cooldownMs={cooldown.remainingMs} pending={pending} pendingLabel="Creating account…">Create account</SubmitButton>
      <button className="button button--ghost button--block signin-action" type="button" onClick={onCancel}>Back</button>
    </form>
  );
}
