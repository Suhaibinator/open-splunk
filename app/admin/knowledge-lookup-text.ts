/** Splits a textarea value into trimmed, non-empty lines. */
export function lines(value: string): string[] {
  return value
    .split("\n")
    .map((entry) => entry.replace(/^[\t\n\v\f\r ]+|[\t\n\v\f\r ]+$/g, ""))
    .filter((entry) => entry.length > 0);
}

/** Renders selector patterns back into a newline-separated textarea value. */
export function joinedPatterns(patterns: ReadonlyArray<{ value: string }>): string {
  return patterns.map((pattern) => pattern.value).join("\n");
}
