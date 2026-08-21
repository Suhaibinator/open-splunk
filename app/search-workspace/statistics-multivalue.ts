import { createElement, type ReactNode } from "react";

// Preserve the statistics-specific API while sharing canonical presentation
// with event-row rendering.
export { flatMultivalueDisplay as statsFlatMultivalueDisplay } from "../../lib/search/multivalue-presentation";

/** Preserve display-only multivalue separators that contain a line break. */
export function statsFlatMultivalueWhiteSpace(
  delimiter: string | undefined,
): "nowrap" | "pre-wrap" {
  return delimiter !== undefined && /[\r\n]/u.test(delimiter)
    ? "pre-wrap"
    : "nowrap";
}

/**
 * Keep newline-delimited presentation inside the fixed-height box required by
 * statistics virtualization. The complete text remains in the DOM while CSS
 * clips only the visual wrapper.
 */
export function StatsFlatMultivalueValue({
  delimiter,
  value,
}: {
  delimiter: string | undefined;
  value: ReactNode;
}): ReactNode {
  if (typeof value !== "string" || statsFlatMultivalueWhiteSpace(delimiter) === "nowrap") {
    return value;
  }
  return createElement(
    "span",
    { className: "statistics-multivalue-lines" },
    value,
  );
}
