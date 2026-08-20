import type { ResultColumn } from "@/gen/ts/open_splunk/result";
import { ValueType } from "@/gen/ts/open_splunk/value";

export const MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES = 16 << 10;

const UTF8 = new TextEncoder();

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
    && delimiter.isWellFormed()
    && UTF8.encode(delimiter).byteLength <= MAXIMUM_FLAT_MULTIVALUE_DELIMITER_BYTES;
}
