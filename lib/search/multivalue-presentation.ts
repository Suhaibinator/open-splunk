// Apply optional flat-display metadata without changing the authoritative
// typed list retained by the result model. Native members use the shared
// canonical scalar text; containers and non-finite numbers fail closed.
export function flatMultivalueDisplay(
  value: unknown,
  delimiter: string | undefined,
): string | undefined {
  if (delimiter === undefined || !Array.isArray(value)) return undefined;
  const members: string[] = [];
  for (const member of value) {
    if (member === null) {
      members.push("null");
    } else if (typeof member === "string") {
      members.push(member);
    } else if (typeof member === "boolean") {
      members.push(member ? "true" : "false");
    } else if (typeof member === "number" && Number.isFinite(member)) {
      members.push(Object.is(member, -0) ? "-0" : member.toString());
    } else {
      return undefined;
    }
  }
  return members.join(delimiter);
}
