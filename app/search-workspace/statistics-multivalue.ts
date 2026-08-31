import { createElement, type ReactNode } from "react";

import { flatMultivalueMembers } from "../../lib/search/multivalue-presentation";

import type { StatsDensity } from "./model";

// Preserve the statistics-specific API while sharing canonical presentation
// with event-row rendering.
export { flatMultivalueDisplay as statsFlatMultivalueDisplay } from "../../lib/search/multivalue-presentation";

/** Members beyond this cap are summarised in the cell tooltip's tail line. */
export const STATS_MULTIVALUE_TITLE_MEMBER_CAP = 40;

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

/**
 * Members for the per-line cell layout. Only an invisible delimiter — the
 * server default `" "` and nomv's `"\n"` — carries no meaning of its own, so
 * only those columns may drop the separator and stack their members. An
 * explicit visible delimiter keeps the joined-string presentation.
 */
export function statsMultivalueLineMembers(
  value: unknown,
  delimiter: string | undefined,
): string[] | undefined {
  if (delimiter === undefined || !/^\s*$/u.test(delimiter)) return undefined;
  return flatMultivalueMembers(value);
}

/**
 * Members that fit the fixed virtual row height. Rows must stay uniform for
 * virtualization, so a longer list trades its last visible line for the button
 * that opens the rest.
 */
export function statsMultivalueVisibleMemberCount(
  density: StatsDensity,
  memberCount: number,
): number {
  if (memberCount <= 2) return memberCount;
  return density === "compact" ? 1 : 2;
}

/** Full membership for the cell tooltip, capped so the tooltip stays readable. */
export function statsMultivalueTitle(members: string[]): string {
  if (members.length <= STATS_MULTIVALUE_TITLE_MEMBER_CAP) return members.join("\n");
  const remaining = members.length - STATS_MULTIVALUE_TITLE_MEMBER_CAP;
  return [
    ...members.slice(0, STATS_MULTIVALUE_TITLE_MEMBER_CAP),
    `… +${remaining.toString()} more`,
  ].join("\n");
}

/** One line per visible member, with the overflow count opening the full list. */
export function StatsMultivalueList({
  fieldName,
  members,
  visibleMemberCount,
  onShowAll,
}: {
  fieldName: string;
  members: string[];
  visibleMemberCount: number;
  onShowAll: () => void;
}): ReactNode {
  return createElement(
    "span",
    { className: "statistics-multivalue-list" },
    members.slice(0, visibleMemberCount).map((member, index) => createElement(
      "span",
      { className: "statistics-multivalue-item", key: `${index.toString()}-${member}` },
      member,
    )),
    visibleMemberCount < members.length
      ? createElement(
        "button",
        {
          "aria-haspopup": "dialog",
          "aria-label": `Show all ${members.length.toString()} values for ${fieldName}`,
          className: "statistics-multivalue-more",
          onClick: onShowAll,
          type: "button",
        },
        `+${(members.length - visibleMemberCount).toString()} more`,
      )
      : null,
  );
}
