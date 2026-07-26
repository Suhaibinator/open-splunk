export class RepeatedPageCursorError extends Error {
  public constructor(resourceLabel: string) {
    super(`${resourceLabel} returned a repeated page cursor.`);
    this.name = "RepeatedPageCursorError";
  }
}

export const MAXIMUM_BROWSER_RESULT_COLUMNS = 64;

export function validateBrowserResultColumnCount(columnCount: number): string | null {
  if (
    !Number.isSafeInteger(columnCount)
    || columnCount <= 0
    || columnCount > MAXIMUM_BROWSER_RESULT_COLUMNS
  ) {
    return `Search results returned ${columnCount} columns; the browser supports 1–${MAXIMUM_BROWSER_RESULT_COLUMNS}.`;
  }
  return null;
}

export function assertBrowserResultColumnCount(columnCount: number): void {
  const validationError = validateBrowserResultColumnCount(columnCount);
  if (validationError !== null) throw new RangeError(validationError);
}

export function assertBrowserResultPageBounds({
  columnCount,
  pageSize,
  rowCount,
}: {
  columnCount: number;
  pageSize: number;
  rowCount: number;
}): void {
  if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
    throw new RangeError("Search result page size must be a positive safe integer.");
  }
  if (!Number.isSafeInteger(rowCount) || rowCount < 0 || rowCount > pageSize) {
    throw new RangeError(
      `Search results returned ${rowCount} rows for a requested page size of ${pageSize}.`,
    );
  }
  assertBrowserResultColumnCount(columnCount);
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

function pageNumberForKey(key: string, pageSize: number): number | null {
  const prefix = `${pageSize}:`;
  if (!key.startsWith(prefix)) return null;
  const pageNumber = Number(key.slice(prefix.length));
  return Number.isSafeInteger(pageNumber) && pageNumber > 0 ? pageNumber : null;
}

/**
 * Invalidates one cursor chain from the first untrusted page onward. Cursor
 * pages are snapshot-relative, so retaining a downstream token or cached page
 * after an earlier edge changes would permit gaps or mixed snapshots.
 */
export function pruneCursorChainFrom<T>(
  pages: Map<string, T>,
  pageTokens: Map<string, string | undefined>,
  pageStarts: Map<string, number>,
  seenTokens: Set<string>,
  pageSize: number,
  firstPageToPrune: number,
): void {
  if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
    throw new RangeError("Cursor page size must be a positive safe integer.");
  }
  if (!Number.isSafeInteger(firstPageToPrune) || firstPageToPrune <= 0) {
    throw new RangeError("The first cursor page to prune must be a positive safe integer.");
  }
  for (const collection of [pages, pageTokens, pageStarts]) {
    for (const key of collection.keys()) {
      const pageNumber = pageNumberForKey(key, pageSize);
      if (pageNumber !== null && pageNumber >= firstPageToPrune) {
        collection.delete(key);
      }
    }
  }
  seenTokens.clear();
  for (const token of pageTokens.values()) {
    const normalized = token?.trim();
    if (normalized) seenTokens.add(normalized);
  }
}
