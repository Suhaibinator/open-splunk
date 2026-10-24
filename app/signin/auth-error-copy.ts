/**
 * What each sign-in surface says about each typed identity failure.
 *
 * Screens switch on `IdentityError.kind`, never on a message; the message the
 * server wrote is shown only where it is the most specific thing anyone knows
 * (a password policy, say). go-common deliberately answers some failures with
 * one status for several causes -- a banned account and a wrong password are
 * both 401 -- and where it adds a distinguishing message the copy reads it.
 */

import { IdentityError, isIdentityError, type IdentityErrorKind } from "@/lib/api/identity";

export type AuthFlow =
  | "session"
  | "sign_in"
  | "otp"
  | "providers"
  | "oauth_start"
  | "oauth_callback"
  | "reset_request"
  | "reset_complete"
  | "account_invitation"
  | "register"
  | "verify_email"
  | "invitations"
  | "accept_invitation"
  | "decline_invitation";

export interface AuthErrorCopy {
  readonly kind: IdentityErrorKind;
  readonly title: string;
  readonly detail: string;
  /** Milliseconds to wait before the same request may be sent again; set for `rate_limited`. */
  readonly retryAfterMs: number | null;
  /**
   * The link or attempt itself is spent -- an expired token, a consumed OAuth
   * state, a discarded second-factor challenge -- so retrying the same request
   * cannot succeed and the surface offers a way to start over instead.
   */
  readonly deadLink: boolean;
}

/** Normalises anything a client threw into an `IdentityError`. */
export function toIdentityError(error: unknown): IdentityError {
  if (isIdentityError(error)) return error;
  return new IdentityError("network", error instanceof Error ? error.message : "The identity service failed.", { cause: error });
}

/** "45 seconds", "1 minute", "3 minutes": a wait rounded up so the user never retries early. */
export function formatWait(milliseconds: number): string {
  const seconds = Math.max(1, Math.ceil(milliseconds / 1000));
  if (seconds < 60) return seconds === 1 ? "1 second" : `${seconds} seconds`;
  const minutes = Math.ceil(seconds / 60);
  return minutes === 1 ? "1 minute" : `${minutes} minutes`;
}

const BANNED = /banned|suspended/iu;
const UNVERIFIED = /not verified/iu;
const USERNAME = /username/iu;

const LINK_PROBLEM = /token|link|invitation/iu;

/** The sign-in failure that offers a password reset beside it. */
export const WRONG_CREDENTIALS_TITLE = "Email or password is incorrect";

function copy(error: IdentityError, title: string, detail: string): AuthErrorCopy {
  return { kind: error.kind, title, detail, retryAfterMs: error.retryAfterMs ?? null, deadLink: false };
}

function dead(error: IdentityError, title: string, detail: string): AuthErrorCopy {
  return { ...copy(error, title, detail), deadLink: true };
}

function specific(error: IdentityError, flow: AuthFlow): AuthErrorCopy | null {
  const { message } = error;
  switch (error.kind) {
    case "unauthenticated":
      if (BANNED.test(message)) {
        return copy(error, "This account is suspended", "An administrator has suspended this account. Contact your administrator to restore access.");
      }
      if (flow === "sign_in") return copy(error, WRONG_CREDENTIALS_TITLE, "Check both and try again, or reset your password.");
      // The callback's own query (`error_description`) is anyone's text, so the
      // provider's reason is never echoed onto the page.
      if (flow === "oauth_callback") return copy(error, "The provider did not sign you in", "The sign-in was cancelled or refused at the provider. Start again.");
      if (flow === "otp") return copy(error, "Sign in again", "This sign-in attempt is no longer valid. Enter your email and password again.");
      if (flow === "verify_email") return copy(error, "This account cannot sign in", message);
      return copy(error, "Your session has ended", "Sign in again to continue.");
    case "forbidden":
      if (UNVERIFIED.test(message)) {
        return copy(error, "Verify your email address first", "Follow the link in the verification email we sent when you created the account, then sign in.");
      }
      if (flow === "accept_invitation" || flow === "decline_invitation") {
        return copy(error, "This invitation is for another address", "Sign in with the account the invitation was sent to.");
      }
      if (flow === "register") {
        return copy(error, "Accounts here are by invitation", "This server only creates accounts for invited addresses. Use the address your invitation was sent to.");
      }
      if (flow === "oauth_start" || flow === "oauth_callback") {
        return copy(error, "New accounts can't sign up with this provider", "This server does not create accounts from provider sign-in. Ask an administrator to invite you, then sign in.");
      }
      return null;
    case "otp_required":
      return copy(error, "That code was not accepted", "Enter the current 6-digit code from your authenticator app. Codes change every 30 seconds.");
    case "rate_limited": {
      const wait = error.retryAfterMs === undefined ? "a moment" : formatWait(error.retryAfterMs);
      if (flow === "otp") return copy(error, "Too many codes tried", `For your security this sign-in was stopped. Wait ${wait}, then sign in again.`);
      if (flow === "reset_request") return copy(error, "A reset email was just sent", `Wait ${wait} before asking for another, and check your spam folder.`);
      if (flow === "oauth_callback") return copy(error, "Too many sign-in attempts", `Wait ${wait}, then start again.`);
      return copy(error, "Too many attempts", `Wait ${wait} before trying again.`);
    }
    case "invalid":
      if (flow === "otp") return dead(error, "This sign-in attempt expired", "Enter your email and password again to get a new code prompt.");
      if (flow === "oauth_callback") return dead(error, "This sign-in link has expired", "It was already used or took too long. Start the sign-in again.");
      if (flow === "reset_complete" && LINK_PROBLEM.test(message)) {
        return dead(error, "This reset link no longer works", "Reset links work once and expire. Request a new one.");
      }
      if (flow === "verify_email") return dead(error, "This verification link no longer works", "It was already used or has expired. Try signing in; if your address is unverified, register again.");
      if (flow === "account_invitation" && LINK_PROBLEM.test(message)) return dead(error, "This invitation link no longer works", "It was already used, replaced by a newer invitation, or has expired. Ask your administrator to resend it.");
      if (flow === "accept_invitation") return dead(error, "This invitation has expired", "Ask an administrator of the organization to invite you again.");
      return copy(error, "Check the details and try again", message);
    case "conflict":
      if (flow === "register") {
        return USERNAME.test(message)
          ? copy(error, "That username is taken", "Choose a different username.")
          : copy(error, "An account already uses this email", "Sign in instead, or reset the password if you have forgotten it.");
      }
      if (flow === "oauth_callback") {
        return copy(error, "This provider account belongs to someone else", "It is linked to a different Open Splunk user. Sign in with your email and password instead.");
      }
      if (flow === "accept_invitation") return copy(error, "You are already a member", "This organization already lists you, so there is nothing to accept.");
      return null;
    case "not_found":
      if (flow === "accept_invitation" || flow === "decline_invitation") {
        return copy(error, "This invitation is no longer available", "It was already answered or revoked.");
      }
      return null;
    default:
      return null;
  }
}

const GENERIC_TITLES: Readonly<Record<IdentityErrorKind, string>> = {
  unauthenticated: "Sign in to continue",
  forbidden: "You do not have access",
  otp_required: "A verification code is required",
  rate_limited: "Too many attempts",
  invalid: "Check the details and try again",
  not_found: "Not found",
  conflict: "That conflicts with existing data",
  network: "The identity service could not be reached",
  not_configured: "Sign-in is not configured",
};

/** The title and detail a surface shows for `error`. */
export function describeAuthError(error: unknown, flow: AuthFlow): AuthErrorCopy {
  const typed = toIdentityError(error);
  const found = specific(typed, flow);
  if (found !== null) return found;
  if (typed.kind === "network") return copy(typed, GENERIC_TITLES.network, "Check your connection and try again.");
  return copy(typed, GENERIC_TITLES[typed.kind], typed.message);
}
