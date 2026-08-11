const UTF8 = new TextEncoder();
const CONTROL_CHARACTER = /\p{Cc}/u;

export function isGoUnicodeSpace(codePoint: number): boolean {
  return (codePoint >= 0x09 && codePoint <= 0x0d)
    || codePoint === 0x20
    || codePoint === 0x85
    || codePoint === 0xa0
    || codePoint === 0x1680
    || (codePoint >= 0x2000 && codePoint <= 0x200a)
    || codePoint === 0x2028
    || codePoint === 0x2029
    || codePoint === 0x202f
    || codePoint === 0x205f
    || codePoint === 0x3000;
}

/**
 * Returns a detached server string only when it matches Go's bounded metadata
 * contract: valid Unicode, a UTF-8 byte ceiling, no control characters, and
 * no edge rune that strings.TrimSpace would remove.
 */
export function canonicalBoundedServerText(
  value: unknown,
  maximumBytes: number,
  allowEmpty = false,
): string | null {
  if (
    typeof value !== "string"
    || (!allowEmpty && value.length === 0)
    || value.length > maximumBytes
    || !value.isWellFormed()
  ) {
    return null;
  }
  const first = value.codePointAt(0);
  const last = value.codePointAt(value.length - 1);
  if (
    (first !== undefined && isGoUnicodeSpace(first))
    || (last !== undefined && isGoUnicodeSpace(last))
    || CONTROL_CHARACTER.test(value)
    || UTF8.encode(value).byteLength > maximumBytes
  ) {
    return null;
  }
  return value.slice(0);
}
