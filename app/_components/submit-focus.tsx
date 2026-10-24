"use client";

import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Where keyboard focus goes after a form refuses a submission.
 *
 * Two things went wrong when each form did this by hand. A form that disables
 * its fields while a request is in flight called `focus()` on the invalid
 * field in the same tick it cleared `pending`, before React had re-enabled the
 * field -- and focusing a disabled control does nothing, so after every
 * refused invitation or account the keyboard user was left on the document.
 * And a field that already had focus (the user pressed Enter in it) was
 * "focused" again, which a screen reader does not announce: the error
 * appeared, the field turned red, and nothing was said.
 *
 * This hook acts after the commit that shows the errors, when the fields are
 * enabled again. It focuses the first control the form marks `aria-invalid`;
 * when focus is already there it speaks that control's description (the
 * `FieldNote` error) through the form's `SubmitFocusAnnouncement`, if it has
 * one, instead. A refusal that names no field leaves focus where it is,
 * unless the pending state took it away, in which case it returns to the
 * form's first enabled control (the checked radio, for a choice).
 */
export interface SubmitFocus {
  /** The callback ref for the form whose fields are focused. */
  readonly attachForm: (form: HTMLFormElement | null) => void;
  /** The callback ref `SubmitFocusAnnouncement` puts on its alert region. */
  readonly attachRegion: (region: HTMLSpanElement | null) => void;
  /** Call when a submission is refused; focus moves after the resulting commit. */
  refocus(): void;
}

const CONTROL_SELECTOR = "input, select, textarea, button";

function descriptionOf(control: HTMLElement): string {
  const ids = (control.getAttribute("aria-describedby") ?? "").split(/\s+/u).filter((id) => id.length > 0);
  const text = ids
    .map((id) => control.ownerDocument.getElementById(id)?.textContent?.trim() ?? "")
    .filter((part) => part.length > 0)
    .join(" ");
  return text.length > 0 ? text : "Check this field.";
}

/** Writes `message` into the region; an identical message is varied so it is read again. */
function announce(region: HTMLSpanElement, message: string): void {
  region.textContent = region.textContent === message ? `${message} ` : message;
}

function firstEnabledControl(form: HTMLFormElement): HTMLElement | null {
  const controls = Array.from(form.querySelectorAll<HTMLElement>(CONTROL_SELECTOR))
    .filter((control) => !(control as HTMLInputElement).disabled && control.getAttribute("type") !== "hidden");
  return controls.find((control) => (control as HTMLInputElement).type === "radio" && (control as HTMLInputElement).checked)
    ?? controls[0]
    ?? null;
}

export function useSubmitFocus(): SubmitFocus {
  const form = useRef<HTMLFormElement | null>(null);
  const region = useRef<HTMLSpanElement | null>(null);
  const [request, setRequest] = useState(0);

  useEffect(() => {
    const current = form.current;
    const speaker = region.current;
    if (request === 0 || current === null) return;
    const invalid = current.querySelector<HTMLElement>('[aria-invalid="true"]');
    if (invalid !== null && document.activeElement === invalid && speaker !== null) {
      announce(speaker, descriptionOf(invalid));
      // The announcement is about the value as it was; typing makes it stale.
      const clear = () => { speaker.textContent = ""; };
      current.addEventListener("input", clear);
      return () => current.removeEventListener("input", clear);
    }
    if (speaker !== null) speaker.textContent = "";
    if (invalid !== null) {
      invalid.focus();
      return;
    }
    const scope = current.closest("dialog") ?? current;
    if (!scope.contains(document.activeElement)) firstEnabledControl(current)?.focus();
  }, [request]);

  const attachForm = useCallback((element: HTMLFormElement | null) => { form.current = element; }, []);
  const attachRegion = useCallback((element: HTMLSpanElement | null) => { region.current = element; }, []);
  const refocus = useCallback(() => setRequest((current) => current + 1), []);
  return { attachForm, attachRegion, refocus };
}

/**
 * The alert a refused submission speaks through when focus could not say it.
 * It stays mounted, and empty until needed, so the text change is announced.
 * `attach` is the hook's `attachRegion`.
 */
export function SubmitFocusAnnouncement({ attach }: { readonly attach: (region: HTMLSpanElement | null) => void }) {
  return <span className="sr-only" ref={attach} role="alert" />;
}
