const MEDIUM_DATE_TIME = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

/** Formats a timestamp as a medium date with a short time, or the supplied fallback. */
export function formatMediumDateTime(value: Date | null | undefined, fallback: string): string {
  if (value === null || value === undefined || Number.isNaN(value.valueOf())) return fallback;
  return MEDIUM_DATE_TIME.format(value);
}
