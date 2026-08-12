import type { ResultColumn } from "@/gen/ts/open_splunk/v1/result";
import { ValueType } from "@/gen/ts/open_splunk/v1/value";

export const MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES = 16 << 10;

const UTF8 = new TextEncoder();

function validUnicodeScalars(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

/** Validate the presence-sensitive flat-display contract on an untrusted column. */
export function validFlatMultivalueColumnPresentation(
	column: Pick<ResultColumn, "flatMultivalueDelimiter" | "multivalue" | "statsSparkline" | "valueType">,
): boolean {
  const delimiter = column.flatMultivalueDelimiter;
  if (column.statsSparkline) {
    return delimiter === undefined
      && column.valueType === ValueType.VALUE_TYPE_LIST
      && column.multivalue;
  }
  if (delimiter === undefined) return true;
  return column.valueType === ValueType.VALUE_TYPE_LIST
    && column.multivalue
    && validUnicodeScalars(delimiter)
    && UTF8.encode(delimiter).byteLength <= MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES;
}
