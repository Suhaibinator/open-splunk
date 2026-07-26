export const MAXIMUM_SAVED_SEARCH_NAME_BYTES = 255;

const CONTROL_CHARACTER = /\p{Cc}/u;

export function savedSearchNameValidationError(value: string): string | null {
  const name = value.trim();
  if (name.length === 0) return "Enter a saved-search name.";
  if (CONTROL_CHARACTER.test(name)) return "Names cannot contain control characters.";
  const bytes = new TextEncoder().encode(name).byteLength;
  if (bytes > MAXIMUM_SAVED_SEARCH_NAME_BYTES) {
    return `Names can use at most ${MAXIMUM_SAVED_SEARCH_NAME_BYTES} UTF-8 bytes; this name uses ${bytes}.`;
  }
  return null;
}

export function truncateUtf8ToByteLength(value: string, maximumBytes: number): string {
  const encoder = new TextEncoder();
  let byteLength = 0;
  let result = "";
  for (const character of value) {
    const characterBytes = encoder.encode(character).byteLength;
    if (byteLength + characterBytes > maximumBytes) break;
    result += character;
    byteLength += characterBytes;
  }
  return result;
}

export function savedSearchNameWithSuffix(name: string, suffix: string): string {
  const suffixBytes = new TextEncoder().encode(suffix).byteLength;
  const base = truncateUtf8ToByteLength(
    name.trim(),
    Math.max(0, MAXIMUM_SAVED_SEARCH_NAME_BYTES - suffixBytes),
  ).trimEnd();
  return `${base}${suffix}`.trim();
}

export function duplicateSavedSearchName(name: string, copyNumber = 1): string {
  return savedSearchNameWithSuffix(
    name,
    copyNumber <= 1 ? " copy" : ` copy ${copyNumber}`,
  );
}

export function nextDuplicateSavedSearchName(
  name: string,
  existingNames: readonly string[],
): string {
  const normalizedNames = new Set(existingNames.map((candidate) => candidate.trim().toLocaleLowerCase()));
  let copyNumber = 1;
  let candidate = duplicateSavedSearchName(name, copyNumber);
  while (normalizedNames.has(candidate.toLocaleLowerCase())) {
    copyNumber += 1;
    candidate = duplicateSavedSearchName(name, copyNumber);
  }
  return candidate;
}
