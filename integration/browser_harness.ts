export const BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX = "... [truncated]";
export const MAXIMUM_BROWSER_DIAGNOSTIC_BYTES = 4 << 10;
export const MAXIMUM_OBSERVED_MATCHING_WEBSOCKETS = 1;
export const MAXIMUM_RECORDED_DIAGNOSTICS = 32;

export interface BoundedRecorder {
  add(value: string): void;
  snapshot(): string[];
}

export function boundedRecorder(
  limit = MAXIMUM_RECORDED_DIAGNOSTICS,
  maximumEntryBytes = MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
): BoundedRecorder {
  assertPositiveSafeInteger(limit, "diagnostic entry limit");
  assertPositiveSafeInteger(maximumEntryBytes, "diagnostic byte limit");
  if (maximumEntryBytes < BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX.length) {
    throw new RangeError(
      `diagnostic byte limit must be at least ${BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX.length}`,
    );
  }

  const values: string[] = [];
  let overflow = 0;
  return {
    add(value) {
      if (values.length < limit) {
        values.push(boundedDiagnostic(value, maximumEntryBytes));
      } else if (overflow < Number.MAX_SAFE_INTEGER) {
        overflow += 1;
      }
    },
    snapshot() {
      return overflow === 0
        ? [...values]
        : [...values, `... ${overflow} additional ${overflow === 1 ? "entry" : "entries"}`];
    },
  };
}

export class BoundedObservationRegistry<Key> {
  readonly #cleanups = new Map<Key, () => void>();

  public constructor(
    private readonly maximum = MAXIMUM_OBSERVED_MATCHING_WEBSOCKETS,
  ) {
    assertPositiveSafeInteger(maximum, "observation limit");
  }

  public get size(): number {
    return this.#cleanups.size;
  }

  public has(key: Key): boolean {
    return this.#cleanups.has(key);
  }

  public tryObserve(key: Key, attach: () => () => void): boolean {
    if (this.#cleanups.has(key)) return true;
    if (this.#cleanups.size >= this.maximum) return false;
    this.#cleanups.set(key, attach());
    return true;
  }

  public clear(): void {
    const cleanups = [...this.#cleanups.values()];
    this.#cleanups.clear();
    for (const cleanup of cleanups) cleanup();
  }
}

export function boundedDiagnostic(
  value: string,
  maximumBytes = MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
): string {
  assertPositiveSafeInteger(maximumBytes, "diagnostic byte limit");
  if (maximumBytes < BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX.length) {
    throw new RangeError(
      `diagnostic byte limit must be at least ${BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX.length}`,
    );
  }
  const prefixByteLimit =
    maximumBytes - BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX.length;
  let byteLength = 0;
  let prefixEnd = 0;
  for (let index = 0; index < value.length;) {
    const codePoint = value.codePointAt(index)!;
    const codeUnits = codePoint > 0xffff ? 2 : 1;
    byteLength += utf8CodePointBytes(codePoint);
    index += codeUnits;
    if (byteLength <= prefixByteLimit) prefixEnd = index;
    if (byteLength > maximumBytes) {
      return value.slice(0, prefixEnd) + BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX;
    }
  }
  return value;
}

function utf8CodePointBytes(codePoint: number): number {
  if (codePoint <= 0x7f) return 1;
  if (codePoint <= 0x7ff) return 2;
  if (codePoint <= 0xffff) return 3;
  return 4;
}

function assertPositiveSafeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}
