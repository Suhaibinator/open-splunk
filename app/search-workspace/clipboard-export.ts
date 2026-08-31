function clipboardCellString(value: unknown): string {
  return value !== null && typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
}

function escapeClipboardCell(value: unknown): string {
  const text = clipboardCellString(value);
  return /[\t\r\n"]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
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
