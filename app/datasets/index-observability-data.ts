import type { ListIndexFieldsResponse } from "@/gen/ts/open_splunk/index_api";
import type { FieldProfile } from "@/gen/ts/open_splunk/result";

export interface IndexFieldSnapshot {
  fields: FieldProfile[];
  nextPageToken: string | null;
  totalSize: bigint | null;
  totalSizeExact: boolean;
}

export interface IndexObservationQuery {
  earliest: string;
  latest: string;
  nameFilter: string;
}

export function nextObservedIndexId(current: string | null, requested: string): string | null {
  return current === requested ? null : requested;
}

export function retainVisibleObservedIndexId(
  current: string | null,
  visibleIndexIds: readonly string[],
): string | null {
  return current !== null && visibleIndexIds.includes(current) ? current : null;
}

export function normalizeIndexObservationQuery(
  value: IndexObservationQuery,
): IndexObservationQuery | null {
  const earliest = value.earliest.trim();
  const latest = value.latest.trim();
  if (!earliest || !latest) return null;
  return { earliest, latest, nameFilter: value.nameFilter.trim() };
}

function normalizedToken(value: string | undefined): string | null {
  const token = value?.trim();
  return token ? token : null;
}

/** Validates the immutable field snapshot contract before exposing it to the UI. */
export function mergeIndexFieldPage(
  current: IndexFieldSnapshot | null,
  response: ListIndexFieldsResponse,
  requestedPageToken?: string,
): IndexFieldSnapshot {
  if (response.page === undefined) {
    throw new TypeError("The field catalog response omitted pagination metadata.");
  }
  if (current === null && requestedPageToken !== undefined) {
    throw new TypeError("A continuation page cannot start a field snapshot.");
  }
  if (current !== null && requestedPageToken === undefined) {
    throw new TypeError("A field continuation requires its page token.");
  }
  if (current !== null && current.nextPageToken !== requestedPageToken) {
    throw new TypeError("The field continuation did not match the current snapshot cursor.");
  }

  const nextPageToken = normalizedToken(response.page.nextPageToken);
  if (nextPageToken !== null && nextPageToken === requestedPageToken) {
    throw new TypeError("The field catalog repeated its page cursor.");
  }

  const existingNames = new Set((current?.fields ?? []).map((field) => field.fieldName));
  const pageNames = new Set<string>();
  for (const field of response.fields) {
    if (!field.fieldName.trim()) throw new TypeError("The field catalog returned an unnamed field.");
    if (existingNames.has(field.fieldName) || pageNames.has(field.fieldName)) {
      throw new TypeError(`The field catalog repeated field ${field.fieldName}.`);
    }
    pageNames.add(field.fieldName);
  }

  return {
    fields: [...(current?.fields ?? []), ...response.fields],
    nextPageToken,
    totalSize: response.page.totalSize ?? null,
    totalSizeExact: response.page.totalSizeExact,
  };
}

export function fieldCountLabel(snapshot: IndexFieldSnapshot): string {
  const loaded = snapshot.fields.length.toLocaleString();
  if (snapshot.totalSize === undefined || snapshot.totalSize === null) {
    return `${loaded} field${snapshot.fields.length === 1 ? "" : "s"} loaded`;
  }
  const total = snapshot.totalSize.toLocaleString();
  if (snapshot.totalSizeExact) {
    return snapshot.nextPageToken === null
      ? `${total} field${snapshot.totalSize === 1n ? "" : "s"}`
      : `${loaded} of ${total} fields loaded`;
  }
  return `${loaded} field${snapshot.fields.length === 1 ? "" : "s"} loaded · approximately ${total} total`;
}

export function formatStorageBytes(bytes: bigint): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let value = bytes < 0n ? 0n : bytes;
  let unit = 0;
  let divisor = 1n;
  while (unit < units.length - 1 && value >= 1_024n) {
    value /= 1_024n;
    divisor *= 1_024n;
    unit += 1;
  }
  if (unit === 0) return `${value.toLocaleString()} ${units[unit]}`;
  const tenths = ((bytes < 0n ? 0n : bytes) * 10n + divisor / 2n) / divisor;
  return `${Number(tenths) / 10} ${units[unit]}`;
}
