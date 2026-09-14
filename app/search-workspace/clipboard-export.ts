function spreadsheetCellString(value: unknown): string {
  const text = value !== null && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
  // Match server CSV text protection without turning genuine numeric cells
  // into text. Escaping apostrophes too keeps distinct headers distinct; the
  // full-width variants cover formula prefixes recognized in some locales.
  return typeof value !== "number" && typeof value !== "bigint" && /^[=+\-@\t\r\n'＝＋－＠]/u.test(text)
    ? `'${text}`
    : text;
}

function escapeClipboardCell(value: unknown): string {
  const text = spreadsheetCellString(value);
  return /[\t\r\n"]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function escapeCsvCell(value: unknown): string {
  return `"${spreadsheetCellString(value).replaceAll('"', '""')}"`;
}

export function serializeRowsAsCsv(
  fields: string[],
  fieldLabels: Record<string, string>,
  rows: Record<string, unknown>[],
): string {
  return [
    fields.map((field) => escapeCsvCell(fieldLabels[field] ?? field)).join(","),
    ...rows.map((row) => fields.map((field) => escapeCsvCell(row[field])).join(",")),
  ].join("\n");
}

export function serializeRowsForClipboard(
  fields: string[],
  fieldLabels: Record<string, string>,
  rows: Record<string, unknown>[],
): string {
  return [
    fields.map((field) => escapeClipboardCell(fieldLabels[field] ?? field)).join("\t"),
    ...rows.map((row) => fields.map((field) => escapeClipboardCell(row[field])).join("\t")),
  ].join("\r\n");
}

export function serializeRowsAsJsonLinesForClipboard(
  fields: string[],
  rows: Record<string, unknown>[],
): string {
  return rows
    .map((row) => JSON.stringify(Object.fromEntries(fields.map((field) => [field, row[field] ?? null]))))
    .join("\n");
}
