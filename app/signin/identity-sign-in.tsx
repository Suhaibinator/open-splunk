"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState, type FormEvent } from "react";

import type { OAuthProvider, OtpChallenge, Principal } from "@/lib/api/identity";
import { DEMO_IDENTITY_HINTS } from "@/lib/demo/identity-data";

import { AppIcon } from "../_components/app-icon";
import { useIdentity } from "../_components/identity-provider";
import { SubmitFocusAnnouncement, useSubmitFocus } from "../_components/submit-focus";
import {
  AuthAlert,
  AuthCardHeader,
  AuthLoading,
  AuthNotice,
  PasswordField,
  SubmitButton,
  TextField,
} from "./auth-frame";
import { WRONG_CREDENTIALS_TITLE, describeAuthError, type AuthErrorCopy } from "./auth-error-copy";
import { useCooldown, useResource, type AuthNavigate } from "./auth-hooks";
import { DemoIdentityHints } from "./demo-identity-hints";
import { emailFieldError } from "./email-address";
import { passwordResetRequestHref } from "./return-to";
import {
  NoOrganizationCard,
  PrincipalSummary,
  RedirectingCard,
  principalCanEnter,
} from "./sign-in-completion";

const OTP_SHAPE = /^\d{6}$/u;
/** How long a rate-limited action waits when the server named no `Retry-After`. */
const DEFAULT_COOLDOWN_MS = 30_000;

export interface IdentitySignInProps {
  readonly navigate: AuthNavigate;
  /** Already validated with `safeReturnTo`. */
  readonly returnTo: string;
  readonly initialEmail?: string;
  /** Shows the demo directory's accounts and links. */
  readonly demo?: boolean;
  /** Offered on a connected server, where the administrator bearer session still exists. */
  readonly onUseAdministratorToken?: () => void;
}

type Step =
  | { readonly kind: "credentials"; readonly notice: AuthErrorCopy | null }
  | { readonly kind: "otp"; readonly challenge: OtpChallenge }
  | { readonly kind: "redirecting"; readonly principal: Principal }
  | { readonly kind: "no_organization"; readonly principal: Principal };

/** The email, password, second-factor and provider sign-in card. */
export function IdentitySignIn({ navigate, returnTo, initialEmail = "", demo = false, onUseAdministratorToken }: IdentitySignInProps) {
  const identity = useIdentity();
  const [step, setStep] = useState<Step>({ kind: "credentials", notice: null });
  const [email, setEmail] = useState(initialEmail);
  const [signOutError, setSignOutError] = useState<AuthErrorCopy | null>(null);
  const [signingOut, setSigningOut] = useState(false);
  const cooldown = useCooldown();

  function finish(principal: Principal): void {
    identity.adoptPrincipal(principal);
    if (principalCanEnter(principal, returnTo)) {
      setStep({ kind: "redirecting", principal });
      navigate(returnTo);
    } else {
      setStep({ kind: "no_organization", principal });
    }
  }

  async function signOut(): Promise<void> {
    setSigningOut(true);
    setSignOutError(null);
    try {
      await identity.signOut();
      setStep({ kind: "credentials", notice: null });
    } catch (error) {
      setSignOutError(describeAuthError(error, "session"));
    } finally {
      setSigningOut(false);
    }
  }

  if (step.kind === "redirecting") return <RedirectingCard principal={step.principal} target={returnTo} />;
  if (step.kind === "no_organization") {
    return <NoOrganizationCard principal={step.principal} navigate={navigate} onSignOut={() => { void signOut(); }} />;
  }

  if (identity.status === "loading") {
    return (
      <>
        <AuthCardHeader title="Sign in" intro="Checking for an existing session…" />
        <AuthLoading label="Checking for an existing session" />
      </>
    );
  }

  if (identity.status === "unavailable") {
    const copy = describeAuthError(identity.error, "session");
    return (
      <>
        <AuthCardHeader icon="warning" title="Sign-in is unavailable" intro="Open Splunk could not check your session." focusOnMount />
        <AuthAlert
          copy={copy}
          action={(
            <button className="button button--secondary button--compact" type="button" onClick={() => { void identity.refresh().catch(() => undefined); }}>
              Try again
            </button>
          )}
        />
        {onUseAdministratorToken === undefined ? null : (
          <button className="button button--ghost button--block signin-action" type="button" onClick={onUseAdministratorToken}>
            Use an administrator token instead
          </button>
        )}
      </>
    );
  }

  if (identity.status === "signed_in" && identity.principal !== null && step.kind === "credentials") {
    const { principal } = identity;
    return (
      <>
        <AuthCardHeader icon="check" title="You're already signed in" intro="Continue where you were going, or switch accounts." />
        <PrincipalSummary principal={principal} />
        {signOutError === null ? null : <AuthAlert copy={signOutError} />}
        <div className="signin-actions">
          <SubmitButton pending={false} pendingLabel="Continuing…" type="button" onClick={() => finish(principal)}>
            Continue as {principal.user.firstName.length > 0 ? principal.user.firstName : principal.user.email}
          </SubmitButton>
          <SubmitButton pending={signingOut} pendingLabel="Signing out…" type="button" variant="secondary" onClick={() => { void signOut(); }}>
            Sign in with a different account
          </SubmitButton>
        </div>
        {demo ? <DemoIdentityHints onChooseAccount={null} /> : null}
      </>
    );
  }

  if (step.kind === "otp") {
    return (
      <OtpStep
        challenge={step.challenge}
        onSignedIn={finish}
        onRestart={(notice, retryAfterMs) => {
          if (retryAfterMs !== null) cooldown.start(retryAfterMs);
          setStep({ kind: "credentials", notice });
        }}
        demo={demo}
      />
    );
  }

  return (
    <CredentialsStep
      cooldownMs={cooldown.remainingMs}
      demo={demo}
      email={email}
      navigate={navigate}
      notice={step.notice}
      onChallenge={(challenge) => setStep({ kind: "otp", challenge })}
      onEmailChange={setEmail}
      onRateLimited={(milliseconds) => cooldown.start(milliseconds)}
      onSignedIn={finish}
      onUseAdministratorToken={onUseAdministratorToken}
      returnTo={returnTo}
    />
  );
}

interface CredentialsStepProps {
  readonly email: string;
  readonly onEmailChange: (email: string) => void;
  readonly notice: AuthErrorCopy | null;
  readonly cooldownMs: number;
  readonly onRateLimited: (milliseconds: number) => void;
  readonly onSignedIn: (principal: Principal) => void;
  readonly onChallenge: (challenge: OtpChallenge) => void;
  readonly navigate: AuthNavigate;
  readonly returnTo: string;
  readonly demo: boolean;
  readonly onUseAdministratorToken: (() => void) | undefined;
}

function CredentialsStep({
  email,
  onEmailChange,
  notice,
  cooldownMs,
  onRateLimited,
  onSignedIn,
  onChallenge,
  navigate,
  returnTo,
  demo,
  onUseAdministratorToken,
}: CredentialsStepProps) {
  const { client } = useIdentity();
  const [password, setPassword] = useState("");
  const [errors, setErrors] = useState<{ email: string | null; password: string | null }>({ email: null, password: null });
  const [failure, setFailure] = useState<AuthErrorCopy | null>(notice);
  const [pending, setPending] = useState(false);
  const emailInput = useRef<HTMLInputElement>(null);
  const passwordInput = useRef<HTMLInputElement>(null);
  // Read once: the step opens on whichever field still needs filling, and
  // refocusing as the user types would fight them.
  const [openOnPassword] = useState(() => email.length > 0);
  const { attachForm, attachRegion, refocus } = useSubmitFocus();

  useEffect(() => {
    (openOnPassword ? passwordInput : emailInput).current?.focus();
  }, [openOnPassword]);

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (pending || cooldownMs > 0 || client === null) return;
    const address = email.trim();
    const next = {
      email: emailFieldError(address),
      password: password.length === 0 ? "Enter your password." : null,
    };
    setErrors(next);
    if (next.email !== null || next.password !== null) {
      refocus();
      return;
    }
    setPending(true);
    setFailure(null);
    try {
      const result = await client.loginWithEmail({ email: address, password });
      setPending(false);
      if (result.status === "otp_required") onChallenge(result.challenge);
      else onSignedIn(result.principal);
    } catch (error) {
      const copy = describeAuthError(error, "sign_in");
      setPending(false);
      setFailure(copy);
      if (copy.kind === "rate_limited") onRateLimited(copy.retryAfterMs ?? DEFAULT_COOLDOWN_MS);
      if (copy.kind === "unauthenticated") {
        setPassword("");
        passwordInput.current?.focus();
      }
    }
  }

  return (
    <>
      <AuthCardHeader title="Sign in" intro="Use your work email and password, or a connected provider." />
      <OAuthProviders navigate={navigate} returnTo={returnTo} />
      <form className="signin-form" noValidate aria-busy={pending} aria-label="Sign in with email" onSubmit={(event) => { void submit(event); }} ref={attachForm}>
        <SubmitFocusAnnouncement attach={attachRegion} />
        <TextField
          autoComplete="username"
          error={errors.email}
          id="signin-email"
          inputMode="email"
          inputRef={emailInput}
          label="Email"
          onValueChange={(value) => { onEmailChange(value); setErrors((current) => ({ ...current, email: null })); }}
          type="email"
          value={email}
        />
        <PasswordField
          autoComplete="current-password"
          error={errors.password}
          id="signin-password"
          inputRef={passwordInput}
          label="Password"
          onValueChange={(value) => { setPassword(value); setErrors((current) => ({ ...current, password: null })); }}
          value={password}
        />
        <Link className="signin-inline-link" href={passwordResetRequestHref(email.trim())}>Forgot password?</Link>
        {failure === null ? null : (
          <AuthAlert
            copy={failure}
            action={failure.title === WRONG_CREDENTIALS_TITLE ? (
              <Link href={passwordResetRequestHref(email.trim())}>Reset your password</Link>
            ) : undefined}
          />
        )}
        <SubmitButton cooldownMs={cooldownMs} pending={pending} pendingLabel="Signing in…">Sign in</SubmitButton>
      </form>
      {onUseAdministratorToken === undefined ? null : (
        <button className="button button--ghost button--block signin-action" type="button" onClick={onUseAdministratorToken}>
          Use an administrator token instead
        </button>
      )}
      {demo ? <DemoIdentityHints onChooseAccount={(address, demoPassword) => {
        onEmailChange(address);
        setPassword(demoPassword);
        setErrors({ email: null, password: null });
        setFailure(null);
      }} /> : null}
    </>
  );
}

/** The single-sign-on buttons. No providers renders nothing, so the card does not promise one. */
function OAuthProviders({ navigate, returnTo }: { readonly navigate: AuthNavigate; readonly returnTo: string }) {
  const { client } = useIdentity();
  const load = useMemo(() => (client === null ? null : () => client.listOAuthProviders()), [client]);
  const { resource: state, retry } = useResource<readonly OAuthProvider[]>(load);
  const [starting, setStarting] = useState<string | null>(null);
  const [failure, setFailure] = useState<AuthErrorCopy | null>(null);

  async function begin(provider: OAuthProvider): Promise<void> {
    if (starting !== null || client === null) return;
    setStarting(provider.slug);
    setFailure(null);
    try {
      const { authorizationUrl } = await client.beginOAuth(provider.slug, returnTo);
      // The page is leaving; the button keeps its pending state until it does.
      navigate(authorizationUrl);
    } catch (error) {
      setStarting(null);
      setFailure(describeAuthError(error, "oauth_start"));
    }
  }

  if (state.status === "loading") {
    return (
      <div className="signin-providers" aria-busy="true" role="status">
        <span className="sr-only">Loading sign-in providers</span>
        <span className="skeleton skeleton--line" aria-hidden="true" />
      </div>
    );
  }
  if (state.status === "error") {
    return (
      <div className="signin-providers">
        <AuthAlert
          copy={{ ...describeAuthError(state.error, "providers"), title: "Single sign-on providers could not be loaded" }}
          action={(
            <button className="button button--secondary button--compact" type="button" onClick={retry}>
              Try again
            </button>
          )}
        />
        <div className="signin-divider"><span>or use email</span></div>
      </div>
    );
  }
  if (state.value.length === 0) return null;
  return (
    <div className="signin-providers">
      <ul aria-label="Sign in with a provider">
        {state.value.map((provider) => (
          <li key={provider.slug}>
            <SubmitButton
              onClick={() => { void begin(provider); }}
              pending={starting === provider.slug}
              pendingLabel={`Opening ${provider.displayName}…`}
              type="button"
              unavailable={starting !== null && starting !== provider.slug}
              variant="secondary"
            >
              <span className="signin-provider-mark" aria-hidden="true">{provider.displayName.slice(0, 1).toUpperCase()}</span>
              Continue with {provider.displayName}
            </SubmitButton>
          </li>
        ))}
      </ul>
      {failure === null ? null : <AuthAlert copy={failure} />}
      <div className="signin-divider"><span>or use email</span></div>
    </div>
  );
}

interface OtpStepProps {
  readonly challenge: OtpChallenge;
  readonly onSignedIn: (principal: Principal) => void;
  /** Back to the password step, saying why; the challenge is gone. */
  readonly onRestart: (notice: AuthErrorCopy | null, retryAfterMs: number | null) => void;
  readonly demo: boolean;
}

function OtpStep({ challenge, onSignedIn, onRestart, demo }: OtpStepProps) {
  const { client } = useIdentity();
  const [code, setCode] = useState("");
  const [fieldError, setFieldError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [networkFailure, setNetworkFailure] = useState<AuthErrorCopy | null>(null);
  const input = useRef<HTMLInputElement>(null);
  const { attachForm, attachRegion, refocus } = useSubmitFocus();

  useEffect(() => {
    input.current?.focus();
  }, []);

  async function submit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (pending || client === null) return;
    const trimmed = code.replace(/\s+/gu, "");
    if (!OTP_SHAPE.test(trimmed)) {
      setFieldError("Enter the 6-digit code from your authenticator app.");
      refocus();
      return;
    }
    setPending(true);
    setFieldError(null);
    try {
      const principal = await client.verifyLoginOtp(challenge, trimmed);
      setPending(false);
      onSignedIn(principal);
    } catch (error) {
      const copy = describeAuthError(error, "otp");
      setPending(false);
      if (copy.kind === "otp_required") {
        setCode("");
        setFieldError(`${copy.title}. ${copy.detail}`);
        refocus();
        return;
      }
      if (copy.kind === "network") {
        setFieldError(null);
        setNetworkFailure(copy);
        return;
      }
      onRestart(copy, copy.kind === "rate_limited" ? copy.retryAfterMs ?? DEFAULT_COOLDOWN_MS : null);
    }
  }

  return (
    <>
      <AuthCardHeader
        icon="settings"
        title="Two-step verification"
        intro={<>Enter the code from your authenticator app for <b>{challenge.email}</b>.</>}
      />
      <form className="signin-form" noValidate aria-busy={pending} aria-label="Verification code" onSubmit={(event) => { void submit(event); }} ref={attachForm}>
        <SubmitFocusAnnouncement attach={attachRegion} />
        <TextField
          autoComplete="one-time-code"
          error={fieldError}
          hint="6 digits. The code changes every 30 seconds."
          id="signin-otp"
          inputMode="numeric"
          inputRef={input}
          label="Authenticator code"
          maxLength={7}
          onValueChange={(value) => { setCode(value); setFieldError(null); setNetworkFailure(null); }}
          value={code}
        />
        {networkFailure === null ? null : <AuthAlert copy={networkFailure} />}
        <SubmitButton pending={pending} pendingLabel="Verifying…">Verify and sign in</SubmitButton>
      </form>
      <button className="button button--ghost button--block signin-action" type="button" onClick={() => onRestart(null, null)}>
        <AppIcon name="chevron-left" size="xs" /> Use a different account
      </button>
      {demo ? <AuthNotice title="Demo code">The demo authenticator code is {DEMO_IDENTITY_HINTS.otpCode}.</AuthNotice> : null}
    </>
  );
}
