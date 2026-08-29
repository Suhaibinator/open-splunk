import type { ReactNode } from "react";

/**
 * The one status vocabulary the product reports outcomes in.
 *
 * Before Phase 3 there were six parallel spellings of the same six ideas --
 * `status-icon--success`, `status-dot--healthy`, `status-label--complete`,
 * `mini-status.state-success`, `job-state-icon.state-success` and the reports
 * module's own `statusScheduled` -- so "did it work?" was answered by a
 * different class per page and a new page picked whichever it happened to copy.
 * These six names are the whole vocabulary now, and `.status--*` in
 * `app/globals.css` is the only place they are painted.
 *
 * `running` is `info` plus the pulse rather than a seventh colour: an in-flight
 * state is informational, and making callers spell it as two classes only
 * invited half of them to forget the second one.
 */
export type StatusTone = "success" | "info" | "warning" | "error" | "neutral" | "running";

export interface StatusDotProps {
  /** Extra classes for a feature's own layout, never for colour. */
  className?: string;
  tone: StatusTone;
}

/**
 * A bare swatch, for a summary tile whose label is a separate element.
 *
 * It is `aria-hidden` because the tone is decoration: every call site in the
 * product states the outcome in adjacent text, and a dot that announced itself
 * would read the state twice.
 */
export function StatusDot({ className, tone }: StatusDotProps) {
  return <span aria-hidden="true" className={statusClassName("dot", tone, className)} />;
}

export interface StatusLabelProps {
  children: ReactNode;
  /** Extra classes for a feature's own layout, never for colour. */
  className?: string;
  tone: StatusTone;
}

/**
 * A swatch and its text, the shape tables use to report a row's outcome.
 *
 * The tone rides on the dot rather than on the row because `.status--*` paints
 * a background: on the row it would flood the cell. That is the same reason the
 * component exists at all -- the pairing is easy to get wrong by hand, and the
 * wrong version still renders.
 */
export function StatusLabel({ children, className, tone }: StatusLabelProps) {
  return (
    <span className={["status", "status--label", className ?? ""].filter(Boolean).join(" ")}>
      <i className={statusClassName("dot", tone)} />
      {children}
    </span>
  );
}

/** Joins the block, one shape and one tone into the class list they render as. */
export function statusClassName(shape: "dot" | "icon" | "label", tone: StatusTone, className?: string): string {
  return ["status", `status--${shape}`, `status--${tone}`, className ?? ""].filter(Boolean).join(" ");
}
