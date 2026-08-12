// statsFlatMultivalueDisplay applies optional display metadata without
// changing the authoritative typed cell retained by the statistics model.
// The compiler attaches the metadata only to list()/values() Array(String)
// outputs, so fail closed for any other runtime member type.
export function statsFlatMultivalueDisplay(
  value: unknown,
  delimiter: string | undefined,
): string | undefined {
  if (delimiter === undefined || !Array.isArray(value)) return undefined;
  if (!value.every((member) => typeof member === "string")) return undefined;
  return value.join(delimiter);
}
