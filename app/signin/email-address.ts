/**
 * The one email-address check every sign-in form shares, and the gate on an
 * address a link's query string asks a form to prefill.
 */

/** Something, an `@`, something, a dot, something: the server has the final say. */
const EMAIL_SHAPE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;
/** RFC 5321's limit on a forward path. */
const MAXIMUM_EMAIL_LENGTH = 254;

export function isEmailShaped(address: string): boolean {
  return address.length <= MAXIMUM_EMAIL_LENGTH && EMAIL_SHAPE.test(address);
}

/** Why `address` (already trimmed) cannot be submitted, or null when it can. */
export function emailFieldError(address: string): string | null {
  if (address.length === 0) return "Enter your email address.";
  return isEmailShaped(address) ? null : "Enter a valid email address, like name@example.com.";
}

/**
 * The address a query string names, when it is one. A link is anyone's text,
 * and a prefill is also shown back ("sent to …"), so anything that is not an
 * email address prefills nothing.
 */
export function prefillEmail(raw: string | null | undefined): string {
  if (typeof raw !== "string") return "";
  const trimmed = raw.trim();
  return isEmailShaped(trimmed) ? trimmed : "";
}
