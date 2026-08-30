import type { ReactNode } from "react";

import { AppIcon } from "./app-icon";

/**
 * The one way a form tells an administrator which field it cannot accept.
 *
 * Before this component the product had four hand-rolled answers to that
 * question and one of them was silence. The search-limits form computed a
 * per-field message and set `aria-invalid`, but nothing painted the attribute
 * outside the sign-in card and the knowledge-manager filter grid, and the
 * message rendered in the same muted grey as the hint it replaced -- so the only
 * visible signal that anything was wrong was a greyed-out Apply button that
 * named no field. The index and token policy fields did not validate at all
 * until submit, where `indexPolicyFromForm` threw and the reason arrived as a
 * toast that pointed at nothing. The knowledge-manager filters had the shape
 * this component generalises, and the reports rename dialog had two thirds of
 * it.
 *
 * What is shared is small and easy to get wrong by hand, which is exactly why it
 * is a component: an invalid control is marked with `aria-invalid` so the
 * stylesheet can paint it and a screen reader announces the state, and it is
 * pointed at the note that says why with `aria-describedby`, so the message is
 * read out on focus rather than being a colour a keyboard user never sees. The
 * note carries a warning glyph as well as the error colour, because colour alone
 * does not distinguish an error for a reader who cannot see the hue.
 *
 * The note is deliberately not a live region. These fields validate on every
 * keystroke, and `role="alert"` would interrupt a screen reader with a
 * half-typed value's complaint on every character. A form that needs to announce
 * something on submit keeps its own status region -- the knowledge-manager
 * filters and the token dialogs each have one -- and this pairing stays quiet
 * until the field is focused.
 */
export type FieldError = string | null;

/** The note's id, derived from the control's so a caller states one name. */
export function fieldNoteId(fieldId: string): string {
  return `${fieldId}-note`;
}

export interface FieldControlProps {
  "aria-describedby": string;
  "aria-invalid": true | undefined;
}

/**
 * The attributes an invalid control carries, spread onto the input or select.
 *
 * `aria-invalid` is `undefined` rather than `false` when the field is valid:
 * React drops the attribute entirely, and the stylesheet keys on its presence,
 * so a valid field is styled by the absence of a rule rather than by a second
 * one that has to undo the first.
 */
export function fieldControlProps(fieldId: string, error: FieldError): FieldControlProps {
  return {
    "aria-describedby": fieldNoteId(fieldId),
    "aria-invalid": error === null ? undefined : true,
  };
}

export interface FieldNoteProps {
  /** The hint shown while the field is valid. */
  children?: ReactNode;
  error: FieldError;
  /** The control's own id; the note derives its id from it. */
  fieldId: string;
}

/**
 * The line under a field: its hint while the value is good, its error when not.
 *
 * The error replaces the hint rather than stacking under it because every
 * message this product writes restates the constraint the hint carried -- "Enter
 * 1–65,536 MiB." against "1–65,536 MiB; default 512 MiB." -- so showing both
 * says the same thing twice and moves the rest of the form down a line while
 * somebody is typing.
 */
export function FieldNote({ children, error, fieldId }: FieldNoteProps) {
  if (error === null) return <small id={fieldNoteId(fieldId)}>{children}</small>;
  return (
    <small className="field-error" id={fieldNoteId(fieldId)}>
      <AppIcon name="warning" size="xs" />
      {error}
    </small>
  );
}
