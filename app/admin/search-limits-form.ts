import type { SearchLimits } from "@/gen/ts/open_splunk/server_settings_api";

export interface SearchLimitForm {
  runtimeSeconds: string;
  memoryMiB: string;
  rowsRead: string;
  bytesReadMiB: string;
  groupedRows: string;
  threads: string;
  resultRows: string;
  resultBytesMiB: string;
  totalResultBytesMiB: string;
  retentionMinutes: string;
  concurrency: string;
}

const MIB = 1n << 20n;

function durationSeconds(value: SearchLimits["maximumRuntime"]): bigint {
  return value?.seconds ?? 0n;
}

export function searchLimitsToForm(value: SearchLimits): SearchLimitForm {
  return {
    runtimeSeconds: durationSeconds(value.maximumRuntime).toString(),
    memoryMiB: (value.maximumMemoryBytes / MIB).toString(),
    rowsRead: value.maximumRowsToRead.toString(),
    bytesReadMiB: (value.maximumBytesToRead / MIB).toString(),
    groupedRows: value.maximumGroupedRows.toString(),
    threads: value.maximumThreads.toString(),
    resultRows: value.maximumResultRows.toString(),
    resultBytesMiB: (value.maximumResultBytes / MIB).toString(),
    totalResultBytesMiB: (value.maximumTotalResultBytes / MIB).toString(),
    retentionMinutes: (durationSeconds(value.resultRetention) / 60n).toString(),
    concurrency: value.maximumConcurrentSearches.toString(),
  };
}

export function parsePositiveInteger(value: string): bigint | null {
  if (!/^[1-9][0-9]*$/.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

export function sameSearchLimits(left: SearchLimits, right: SearchLimits): boolean {
  return left.maximumRuntime?.seconds === right.maximumRuntime?.seconds
    && left.maximumRuntime?.nanos === right.maximumRuntime?.nanos
    && left.maximumMemoryBytes === right.maximumMemoryBytes
    && left.maximumRowsToRead === right.maximumRowsToRead
    && left.maximumBytesToRead === right.maximumBytesToRead
    && left.maximumGroupedRows === right.maximumGroupedRows
    && left.maximumThreads === right.maximumThreads
    && left.maximumResultRows === right.maximumResultRows
    && left.maximumResultBytes === right.maximumResultBytes
    && left.maximumTotalResultBytes === right.maximumTotalResultBytes
    && left.maximumConcurrentSearches === right.maximumConcurrentSearches
    && left.resultRetention?.seconds === right.resultRetention?.seconds
    && left.resultRetention?.nanos === right.resultRetention?.nanos;
}

export function searchLimitsFromForm(value: SearchLimitForm, exactBase?: SearchLimits): SearchLimits | null {
  const parsed = Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, parsePositiveInteger(item)]),
  ) as Record<keyof SearchLimitForm, bigint | null>;
  if (Object.values(parsed).some((item) => item === null)) return null;
  const concurrency = parsed.concurrency!;
  if (concurrency > 0xffff_ffffn) return null;
  const result: SearchLimits = {
    maximumRuntime: { seconds: parsed.runtimeSeconds!, nanos: 0 },
    maximumMemoryBytes: parsed.memoryMiB! * MIB,
    maximumRowsToRead: parsed.rowsRead!,
    maximumBytesToRead: parsed.bytesReadMiB! * MIB,
    maximumGroupedRows: parsed.groupedRows!,
    maximumThreads: parsed.threads!,
    maximumResultRows: parsed.resultRows!,
    maximumResultBytes: parsed.resultBytesMiB! * MIB,
    maximumTotalResultBytes: parsed.totalResultBytesMiB! * MIB,
    maximumConcurrentSearches: Number(concurrency),
    resultRetention: { seconds: parsed.retentionMinutes! * 60n, nanos: 0 },
  };
  if (exactBase !== undefined) {
    const baseForm = searchLimitsToForm(exactBase);
    if (value.runtimeSeconds === baseForm.runtimeSeconds) result.maximumRuntime = exactBase.maximumRuntime;
    if (value.memoryMiB === baseForm.memoryMiB) result.maximumMemoryBytes = exactBase.maximumMemoryBytes;
    if (value.bytesReadMiB === baseForm.bytesReadMiB) result.maximumBytesToRead = exactBase.maximumBytesToRead;
    if (value.resultBytesMiB === baseForm.resultBytesMiB) result.maximumResultBytes = exactBase.maximumResultBytes;
    if (value.totalResultBytesMiB === baseForm.totalResultBytesMiB) result.maximumTotalResultBytes = exactBase.maximumTotalResultBytes;
    if (value.retentionMinutes === baseForm.retentionMinutes) result.resultRetention = exactBase.resultRetention;
  }
  return result;
}
