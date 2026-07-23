export class RepeatedPageCursorError extends Error {
  public constructor(resourceLabel: string) {
    super(`${resourceLabel} returned a repeated page cursor.`);
    this.name = "RepeatedPageCursorError";
  }
}

export function recordNextPageToken(
  seenTokens: Set<string>,
  nextPageToken: string | null | undefined,
  resourceLabel: string,
): string | null {
  const normalized = nextPageToken?.trim() || null;
  if (normalized === null) return null;
  if (seenTokens.has(normalized)) throw new RepeatedPageCursorError(resourceLabel);
  seenTokens.add(normalized);
  return normalized;
}
