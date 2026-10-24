"use client";

import {
  useEffect,
  useRef,
  useState,
  type ChangeEvent,
  type CSSProperties,
  type ReactNode,
  type RefObject,
} from "react";

import { OPEN_SPLUNK_BUILD_LABEL } from "@/lib/build-identity";

import { AppIcon, type AppIconName } from "../_components/app-icon";
import { FieldNote, fieldControlProps, type FieldError } from "../_components/field-validation";
import { Wordmark } from "../_components/wordmark";
import { formatWait, type AuthErrorCopy } from "./auth-error-copy";

/**
 * The page every sign-in surface shares: the product story on the left (hidden
 * below 760px) and one card on the right. Each flow owns only what goes in
 * the card.
 */

const SIGNAL_HEIGHTS = ["38%", "58%", "45%", "76%", "52%", "88%", "63%", "70%", "48%", "82%"];

export interface SignInStoryProps {
  readonly kicker: string;
  readonly headline: string;
  readonly intro: string;
  readonly footer: string;
}

export function SignInStory({ kicker, headline, intro, footer }: SignInStoryProps) {
  return (
    <section className="signin-story" aria-label="Open Splunk product introduction">
      <Wordmark href="/" size="hero" />
      <div className="signin-story-copy">
        <span className="signin-kicker">{kicker}</span>
        <h2>{headline}</h2>
        <p>{intro}</p>
        <div className="signin-capabilities">
          <span><i aria-hidden="true">⌕</i><b>Event-first search</b><small>Investigate raw events and pivot instantly.</small></span>
          <span><i aria-hidden="true">⌁</i><b>Search analytics</b><small>Transform searches into tables and charts.</small></span>
          <span><i aria-hidden="true">▣</i><b>Self-hosted control</b><small>Keep collection and retention in your environment.</small></span>
        </div>
      </div>
      <div className="signin-signal" aria-hidden="true">
        {SIGNAL_HEIGHTS.map((height, index) => (
          // A constant list: the position is the bar's identity.
          <span key={`${height}-${String(index)}`} style={{ "--signal-height": height } as CSSProperties} />
        ))}
      </div>
      <footer>{footer}</footer>
    </section>
  );
}

const IDENTITY_STORY: SignInStoryProps = {
  kicker: "OPEN SPLUNK",
  headline: "Search your machine data in one place.",
  intro: "Sign in to run searches, share dashboards, and manage your organization's data.",
  footer: "Self-hosted · your data stays in your environment",
};

export interface AuthFrameProps {
  readonly children: ReactNode;
  readonly story?: SignInStoryProps;
  /** The card's footer line; the build label leads it. */
  readonly footerNote?: string;
}

export function AuthFrame({ children, story = IDENTITY_STORY, footerNote = "Signed-in session" }: AuthFrameProps) {
  return (
    <main className="signin-page">
      <SignInStory {...story} />
      <section className="signin-panel">
        <div className="signin-card">
          <Wordmark className="signin-mobile-brand" href="/" size="hero" />
          {children}
          <footer><span>Open Splunk {OPEN_SPLUNK_BUILD_LABEL}</span><span>{footerNote}</span></footer>
        </div>
      </section>
    </main>
  );
}

export interface AuthCardHeaderProps {
  readonly title: string;
  readonly intro?: ReactNode;
  readonly icon?: AppIconName;
  /** Takes focus when the step this header names appears, so a screen reader announces the new step. */
  readonly focusOnMount?: boolean;
}

export function AuthCardHeader({ title, intro, icon = "chevron-right", focusOnMount = false }: AuthCardHeaderProps) {
  const heading = useRef<HTMLHeadingElement>(null);
  useEffect(() => {
    if (focusOnMount) heading.current?.focus();
  }, [focusOnMount]);
  return (
    <header>
      <span className="signin-lock" aria-hidden="true"><AppIcon name={icon} size="md" /></span>
      <h1 ref={heading} tabIndex={-1}>{title}</h1>
      {intro === undefined ? null : <p>{intro}</p>}
    </header>
  );
}

/** An announced failure: the region is a live alert, so it is read out when it appears. */
export function AuthAlert({ copy, action }: { readonly copy: AuthErrorCopy; readonly action?: ReactNode }) {
  return (
    <div className="signin-error signin-message" role="alert" data-kind={copy.kind}>
      <span aria-hidden="true"><AppIcon name="warning" size="sm" /></span>
      <div>
        <strong>{copy.title}</strong>
        {copy.detail.length > 0 ? <p>{copy.detail}</p> : null}
        {action === undefined ? null : <div className="signin-message-action">{action}</div>}
      </div>
    </div>
  );
}

/** A polite announcement: progress, a sent email, a completed step. */
export function AuthNotice({ title, children, tone = "info" }: { readonly title?: string; readonly children?: ReactNode; readonly tone?: "info" | "success" }) {
  return (
    <div className="signin-help-notice signin-message" role="status" data-tone={tone}>
      <span aria-hidden="true"><AppIcon name={tone === "success" ? "check" : "info"} size="sm" /></span>
      <div>
        {title === undefined ? null : <strong>{title}</strong>}
        {children === undefined ? null : <p>{children}</p>}
      </div>
    </div>
  );
}

/** The card while a flow reads what it needs; the label is what a screen reader hears. */
export function AuthLoading({ label }: { readonly label: string }) {
  return (
    <div className="signin-loading" role="status" aria-busy="true">
      <span className="sr-only">{label}</span>
      <span className="skeleton skeleton--line skeleton--line-medium" aria-hidden="true" />
      <span className="skeleton skeleton--line" aria-hidden="true" />
      <span className="skeleton skeleton--block" aria-hidden="true" />
      <span className="skeleton skeleton--line skeleton--line-short" aria-hidden="true" />
    </div>
  );
}

export interface SubmitButtonProps {
  readonly children: ReactNode;
  readonly pending: boolean;
  readonly pendingLabel: string;
  /** While positive the button stays unavailable and says how long is left. */
  readonly cooldownMs?: number;
  readonly unavailable?: boolean;
  readonly variant?: "primary" | "secondary";
  readonly type?: "submit" | "button";
  readonly onClick?: () => void;
}

/**
 * The flow's one primary action. It is never `disabled`: a disabled button
 * drops keyboard focus to the document, so a pending or rate-limited action is
 * `aria-disabled` and the handler refuses it instead.
 */
export function SubmitButton({
  children,
  pending,
  pendingLabel,
  cooldownMs = 0,
  unavailable = false,
  variant = "primary",
  type = "submit",
  onClick,
}: SubmitButtonProps) {
  const blocked = pending || cooldownMs > 0 || unavailable;
  return (
    <button
      aria-disabled={blocked ? "true" : undefined}
      className={`button button--${variant} button--block signin-action`}
      data-pending={pending ? "true" : undefined}
      onClick={(event) => {
        if (blocked) {
          event.preventDefault();
          return;
        }
        onClick?.();
      }}
      type={type}
    >
      {pending ? <AppIcon name="loading" size="sm" spin /> : null}
      {pending ? pendingLabel : cooldownMs > 0 ? `Try again in ${formatWait(cooldownMs)}` : children}
    </button>
  );
}

export interface TextFieldProps {
  readonly id: string;
  readonly label: string;
  readonly value: string;
  readonly onValueChange: (value: string) => void;
  readonly error: FieldError;
  readonly hint?: ReactNode;
  readonly type?: "email" | "text";
  readonly autoComplete: string;
  readonly inputMode?: "email" | "numeric" | "text";
  readonly maxLength?: number;
  readonly readOnly?: boolean;
  readonly inputRef?: RefObject<HTMLInputElement | null>;
}

/** A label, its input and the note under it, as direct children of the form the card styles. */
export function TextField({
  id,
  label,
  value,
  onValueChange,
  error,
  hint,
  type = "text",
  autoComplete,
  inputMode,
  maxLength,
  readOnly = false,
  inputRef,
}: TextFieldProps) {
  return (
    <>
      <label htmlFor={id}>{label}</label>
      <input
        {...fieldControlProps(id, error)}
        autoCapitalize="none"
        autoComplete={autoComplete}
        id={id}
        inputMode={inputMode}
        maxLength={maxLength}
        name={id}
        onChange={(event: ChangeEvent<HTMLInputElement>) => onValueChange(event.target.value)}
        readOnly={readOnly}
        ref={inputRef}
        spellCheck={false}
        type={type}
        value={value}
      />
      <FieldNote error={error} fieldId={id}>{hint}</FieldNote>
    </>
  );
}

export interface PasswordFieldProps {
  readonly id: string;
  readonly label: string;
  readonly value: string;
  readonly onValueChange: (value: string) => void;
  readonly error: FieldError;
  readonly hint?: ReactNode;
  readonly autoComplete: "current-password" | "new-password";
  readonly inputRef?: RefObject<HTMLInputElement | null>;
}

/** A password input with a Show/Hide toggle that keeps focus in the field. */
export function PasswordField({ id, label, value, onValueChange, error, hint, autoComplete, inputRef }: PasswordFieldProps) {
  const [visible, setVisible] = useState(false);
  return (
    <>
      <label htmlFor={id}>{label}</label>
      <div className="signin-password-field" data-invalid={error !== null}>
        <input
          {...fieldControlProps(id, error)}
          autoCapitalize="none"
          autoComplete={autoComplete}
          id={id}
          name={id}
          onChange={(event: ChangeEvent<HTMLInputElement>) => onValueChange(event.target.value)}
          ref={inputRef}
          spellCheck={false}
          type={visible ? "text" : "password"}
          value={value}
        />
        <button
          aria-controls={id}
          aria-label={visible ? `Hide ${label.toLowerCase()}` : `Show ${label.toLowerCase()}`}
          aria-pressed={visible}
          onClick={() => setVisible((current) => !current)}
          type="button"
        >
          {visible ? "Hide" : "Show"}
        </button>
      </div>
      <FieldNote error={error} fieldId={id}>{hint}</FieldNote>
    </>
  );
}
